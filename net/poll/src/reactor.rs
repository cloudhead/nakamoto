//! Poll-based reactor. This is a single-threaded reactor using a `poll` loop.
use bitcoin::consensus::encode;
use bitcoin::network::message::RawNetworkMessage;

use crossbeam_channel as chan;

use nakamoto_common::block::time::{LocalDuration, LocalTime};

use nakamoto_p2p::error::Error;
use nakamoto_p2p::event::{self, Event};
use nakamoto_p2p::protocol::Machine;
use nakamoto_p2p::protocol::{Command, DisconnectReason, Input, Link, Out};

use log::*;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::io;
use std::io::prelude::*;
use std::net;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::time;
use std::time::SystemTime;

use crate::fallible;
use crate::socket::Socket;
use crate::time::TimeoutManager;

/// Maximum time to wait when reading from a socket.
const READ_TIMEOUT: time::Duration = time::Duration::from_secs(6);
/// Maximum time to wait when writing to a socket.
const WRITE_TIMEOUT: time::Duration = time::Duration::from_secs(3);
/// Maximum amount of time to wait for i/o.
const WAIT_TIMEOUT: LocalDuration = LocalDuration::from_mins(60);

#[must_use]
#[derive(Debug, PartialEq, Eq)]
enum Control {
    Continue,
    Shutdown,
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum Source {
    Peer(net::SocketAddr),
    Listener,
    Waker,
}

/// A single-threaded non-blocking reactor.
pub struct Reactor<R: Write + Read, E> {
    peers: HashMap<net::SocketAddr, Socket<R, RawNetworkMessage>>,
    connecting: HashSet<net::SocketAddr>,
    inputs: VecDeque<Input>,
    commands: chan::Receiver<Command>,
    publisher: E,
    sources: popol::Sources<Source>,
    waker: Arc<popol::Waker>,
    timeouts: TimeoutManager<()>,
}

/// The `R` parameter represents the underlying stream type, eg. `net::TcpStream`.
impl<R: Write + Read + AsRawFd, E> Reactor<R, E> {
    /// Register a peer with the reactor.
    fn register_peer(&mut self, addr: net::SocketAddr, stream: R, link: Link) {
        self.sources
            .register(Source::Peer(addr), &stream, popol::interest::ALL);
        self.peers.insert(addr, Socket::from(stream, addr, link));
    }

    /// Unregister a peer from the reactor.
    fn unregister_peer(&mut self, addr: net::SocketAddr, reason: DisconnectReason) {
        self.connecting.remove(&addr);
        self.inputs.push_back(Input::Disconnected(addr, reason));
        self.sources.unregister(&Source::Peer(addr));
        self.peers.remove(&addr);
    }
}

impl<E: event::Publisher> nakamoto_p2p::reactor::Reactor<E> for Reactor<net::TcpStream, E> {
    type Waker = Arc<popol::Waker>;

    /// Construct a new reactor, given a channel to send events on.
    fn new(publisher: E, commands: chan::Receiver<Command>) -> Result<Self, io::Error> {
        let peers = HashMap::new();
        let inputs: VecDeque<Input> = VecDeque::new();

        let mut sources = popol::Sources::new();
        let waker = Arc::new(popol::Waker::new(&mut sources, Source::Waker)?);
        let timeouts = TimeoutManager::new();
        let connecting = HashSet::new();

        Ok(Self {
            peers,
            connecting,
            sources,
            inputs,
            commands,
            publisher,
            waker,
            timeouts,
        })
    }

    /// Run the given protocol with the reactor.
    fn run<F, M>(&mut self, listen_addrs: &[net::SocketAddr], machine: F) -> Result<(), Error>
    where
        F: FnOnce(chan::Sender<Out>) -> M,
        M: Machine,
    {
        let listener = if listen_addrs.is_empty() {
            None
        } else {
            let listener = self::listen(listen_addrs)?;
            let local_addr = listener.local_addr()?;

            self.sources
                .register(Source::Listener, &listener, popol::interest::READ);
            self.publisher.publish(Event::Listening(local_addr));

            info!("Listening on {}", local_addr);

            Some(listener)
        };

        info!("Initializing protocol..");

        let (tx, rx) = chan::unbounded();
        let mut protocol = machine(tx);
        let local_time = SystemTime::now().into();

        protocol.initialize(local_time);

        if let Control::Shutdown = self.process(&rx, local_time) {
            return Ok(());
        }

        // Drain input events in case some were added during the processing of outputs.
        while let Some(event) = self.inputs.pop_front() {
            protocol.step(event, local_time);

            if let Control::Shutdown = self.process(&rx, local_time) {
                return Ok(());
            }
        }

        // I/O readiness events populated by `popol::Sources::wait_timeout`.
        let mut events = popol::Events::new();
        // Timeouts populated by `TimeoutManager::wake`.
        let mut timeouts = Vec::with_capacity(32);

        loop {
            trace!(
                "Polling {} sources and {} timeouts..",
                self.sources.len(),
                self.timeouts.len()
            );

            let timeout = self.timeouts.next().unwrap_or(WAIT_TIMEOUT).into();
            let result = self.sources.wait_timeout(&mut events, timeout); // Blocking.
            let local_time = SystemTime::now().into();

            match result {
                Ok(()) => {
                    for (source, ev) in events.iter() {
                        match source {
                            Source::Peer(addr) => {
                                if ev.errored || ev.hangup {
                                    // Let the subsequent read fail.
                                    trace!("{}: Socket error triggered: {:?}", addr, ev);
                                }
                                if ev.invalid {
                                    // File descriptor was closed and is invalid.
                                    // Nb. This shouldn't happen. It means the source wasn't
                                    // properly unregistered, or there is a duplicate source.
                                    error!("{}: Socket is invalid, removing", addr);

                                    self.sources.unregister(&Source::Peer(*addr));
                                    continue;
                                }

                                if ev.writable {
                                    self.handle_writable(&addr, source)?;
                                }
                                if ev.readable {
                                    self.handle_readable(&addr);
                                }
                            }
                            Source::Listener => loop {
                                if let Some(ref listener) = listener {
                                    let (conn, addr) = match listener.accept() {
                                        Ok((conn, addr)) => (conn, addr),
                                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(e) => {
                                            error!("Accept error: {}", e.to_string());
                                            break;
                                        }
                                    };
                                    trace!("{}: Accepting peer connection", addr);

                                    conn.set_nonblocking(true)?;

                                    let local_addr = conn.local_addr()?;
                                    let link = Link::Inbound;

                                    self.inputs.push_back(Input::Connected {
                                        addr,
                                        local_addr,
                                        link,
                                    });
                                    self.register_peer(addr, conn, link);
                                }
                            },
                            Source::Waker => {
                                trace!("Woken up by waker ({} command(s))", self.commands.len());
                                debug_assert!(!self.commands.is_empty());

                                popol::Waker::reset(ev.source).ok();

                                for cmd in self.commands.try_iter() {
                                    self.inputs.push_back(Input::Command(cmd));
                                }
                            }
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                    // Nb. The way this is currently used basically ignores which keys have
                    // timed out. So as long as *something* timed out, we wake the protocol.
                    self.timeouts.wake(local_time, &mut timeouts);

                    if !timeouts.is_empty() {
                        timeouts.clear();
                        self.inputs.push_back(Input::Tick);
                    }
                }
                Err(err) => return Err(err.into()),
            }

            while let Some(event) = self.inputs.pop_front() {
                protocol.step(event, local_time);

                if let Control::Shutdown = self.process(&rx, local_time) {
                    return Ok(());
                }
            }
        }
    }

    /// Wake the waker.
    fn wake(waker: &Arc<popol::Waker>) -> io::Result<()> {
        waker.wake()
    }

    /// Return a new waker.
    ///
    /// Used to wake up the main event loop.
    fn waker(&self) -> Arc<popol::Waker> {
        self.waker.clone()
    }
}

impl<E: event::Publisher> Reactor<net::TcpStream, E> {
    /// Process protocol state machine outputs.
    fn process(&mut self, outputs: &chan::Receiver<Out>, local_time: LocalTime) -> Control {
        // Note that there may be messages destined for a peer that has since been
        // disconnected.
        for out in outputs.try_iter() {
            match out {
                Out::Message(addr, msg) => {
                    if let Some(peer) = self.peers.get_mut(&addr) {
                        let src = self.sources.get_mut(&Source::Peer(addr)).unwrap();

                        {
                            let mut s = format!("{:?}", msg.payload);

                            if s.len() > 96 {
                                s.truncate(96);
                                s.push_str("...");
                            }
                            trace!("{}: Sending: {}", addr, s);
                        }

                        peer.queue(msg);

                        if let Err(err) = peer.drain(&mut self.inputs, src) {
                            error!("{}: Write error: {}", addr, err.to_string());

                            peer.disconnect().ok();
                            self.unregister_peer(
                                addr,
                                DisconnectReason::ConnectionError(err.to_string()),
                            );
                        }
                    }
                }
                // TODO: Use connection timeout, or handle timeouts in connection manager.
                Out::Connect(addr, _timeout) => {
                    trace!("Connecting to {}...", &addr);

                    match self::dial(&addr) {
                        Ok(stream) => {
                            trace!("{:#?}", stream);

                            self.register_peer(addr, stream, Link::Outbound);
                            self.connecting.insert(addr);
                            self.inputs.push_back(Input::Connecting { addr });
                        }
                        Err(err) => {
                            self.inputs.push_back(Input::Tick);

                            error!("{}: Connection error: {}", addr, err.to_string());
                        }
                    }
                }
                Out::Disconnect(addr, reason) => {
                    if let Some(peer) = self.peers.get(&addr) {
                        trace!("{}: Disconnecting: {}", addr, reason);

                        // Shutdown the connection, ignoring any potential errors.
                        // If the socket was already disconnected, this will yield
                        // an error that is safe to ignore (`ENOTCONN`). The other
                        // possible errors relate to an invalid file descriptor.
                        peer.disconnect().ok();

                        self.unregister_peer(addr, reason);
                    }
                }
                Out::SetTimeout(timeout) => {
                    self.timeouts.register((), local_time + timeout);
                }
                Out::Event(event) => {
                    trace!("Event: {:?}", event);

                    self.publisher.publish(event);
                }
                Out::Shutdown => {
                    info!("Shutdown received");

                    return Control::Shutdown;
                }
            }
        }
        Control::Continue
    }

    fn handle_readable(&mut self, addr: &net::SocketAddr) {
        let socket = self.peers.get_mut(&addr).unwrap();

        trace!("{}: Socket is readable", addr);

        // Nb. Normally, since `poll`, which `popol` is based on, is
        // level-triggered, we would be notified again if there was
        // still data to be read on the socket. However, since our
        // socket abstraction actually returns *decoded messages*, this
        // doesn't apply. Thus, we have to loop to not miss messages.
        loop {
            match socket.read() {
                Ok(msg) => {
                    self.inputs.push_back(Input::Received(*addr, msg));
                }
                Err(encode::Error::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(err) => {
                    match err {
                        encode::Error::Io(ref err)
                            if err.kind() == io::ErrorKind::UnexpectedEof =>
                        {
                            trace!("{}: Remote peer closed the connection", addr)
                        }
                        _ => trace!("{}: Read error: {}", addr, err.to_string()),
                    }

                    socket.disconnect().ok();
                    self.unregister_peer(*addr, DisconnectReason::ConnectionError(err.to_string()));

                    break;
                }
            }
        }
    }

    fn handle_writable(&mut self, addr: &net::SocketAddr, source: &Source) -> io::Result<()> {
        trace!("{}: Socket is writable", addr);

        let src = self.sources.get_mut(source).unwrap();
        let socket = self.peers.get_mut(&addr).unwrap();

        if self.connecting.remove(addr) {
            let local_addr = socket.local_address()?;

            self.inputs.push_back(Input::Connected {
                addr: socket.address,
                local_addr,
                link: socket.link,
            });
        }

        if let Err(err) = socket.drain(&mut self.inputs, src) {
            error!("{}: Write error: {}", addr, err.to_string());

            socket.disconnect().ok();
            self.unregister_peer(*addr, DisconnectReason::ConnectionError(err.to_string()));
        }
        Ok(())
    }
}

/// Connect to a peer given a remote address.
fn dial(addr: &net::SocketAddr) -> Result<net::TcpStream, Error> {
    use socket2::{Domain, Socket, Type};
    fallible! { Error::Io(io::ErrorKind::Other.into()) };

    let domain = if addr.is_ipv4() {
        Domain::ipv4()
    } else {
        Domain::ipv6()
    };
    let sock = Socket::new(domain, Type::stream(), None)?;

    sock.set_read_timeout(Some(READ_TIMEOUT))?;
    sock.set_write_timeout(Some(WRITE_TIMEOUT))?;
    sock.set_nonblocking(true)?;

    match sock.connect(&(*addr).into()) {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
        Err(e) => return Err(e.into()),
    }
    Ok(sock.into_tcp_stream())
}

// Listen for connections on the given address.
fn listen<A: net::ToSocketAddrs>(addr: A) -> Result<net::TcpListener, Error> {
    let sock = net::TcpListener::bind(addr)?;

    sock.set_nonblocking(true)?;

    Ok(sock)
}
