//! Poll-based reactor. This is a single-threaded reactor using a `poll` loop.
use crossbeam_channel as chan;

use nakamoto_net::error::Error;
use nakamoto_net::event::Publisher;
use nakamoto_net::time::{LocalDuration, LocalTime};
use nakamoto_net::{Disconnect, Io, PeerId};
use nakamoto_net::{Link, Service};

use log::*;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
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
/// Socket read buffer size.
const READ_BUFFER_SIZE: usize = 1024 * 192;

#[derive(Debug, PartialEq, Eq, Clone)]
enum Source<Id: PeerId> {
    Peer(Id),
    Listener,
    Waker,
}

#[derive(Clone)]
pub struct Waker(Arc<popol::Waker>);

impl Waker {
    fn new<Id: PeerId>(sources: &mut popol::Sources<Source<Id>>) -> io::Result<Self> {
        let waker = Arc::new(popol::Waker::new(sources, Source::Waker)?);

        Ok(Self(waker))
    }
}

impl nakamoto_net::Waker for Waker {
    fn wake(&self) -> io::Result<()> {
        self.0.wake()
    }
}

/// A single-threaded non-blocking reactor.
pub struct Reactor<R: Write + Read, Id: PeerId = net::SocketAddr> {
    peers: HashMap<Id, Socket<R>>,
    connecting: HashSet<Id>,
    sources: popol::Sources<Source<Id>>,
    waker: Waker,
    timeouts: TimeoutManager<()>,
    shutdown: chan::Receiver<()>,
    listening: chan::Sender<net::SocketAddr>,
}

/// The `R` parameter represents the underlying stream type, eg. `net::TcpStream`.
impl<R: Write + Read + AsRawFd, Id: PeerId> Reactor<R, Id> {
    /// Register a peer with the reactor.
    fn register_peer(&mut self, addr: Id, stream: R, link: Link) {
        let socket_addr = addr.to_socket_addr();
        self.sources
            .register(Source::Peer(addr.clone()), &stream, popol::interest::ALL);
        self.peers
            .insert(addr, Socket::from(stream, socket_addr, link));
    }

    /// Unregister a peer from the reactor.
    fn unregister_peer<S>(
        &mut self,
        addr: Id,
        reason: Disconnect<S::DisconnectReason>,
        service: &mut S,
    ) where
        S: Service<Id>,
    {
        self.connecting.remove(&addr);
        self.peers.remove(&addr);
        self.sources.unregister(&Source::Peer(addr.clone()));

        service.disconnected(&addr, reason);
    }
}

impl<Id: PeerId> nakamoto_net::Reactor<Id> for Reactor<net::TcpStream, Id> {
    type Waker = Waker;

    /// Construct a new reactor, given a channel to send events on.
    fn new(
        shutdown: chan::Receiver<()>,
        listening: chan::Sender<net::SocketAddr>,
    ) -> Result<Self, io::Error> {
        let peers = HashMap::new();

        let mut sources = popol::Sources::new();
        let waker = Waker::new(&mut sources)?;
        let timeouts = TimeoutManager::new(LocalDuration::from_secs(1));
        let connecting = HashSet::new();

        Ok(Self {
            peers,
            connecting,
            sources,
            waker,
            timeouts,
            shutdown,
            listening,
        })
    }

    /// Run the given service with the reactor.
    fn run<S, E>(
        &mut self,
        listen_addrs: &[net::SocketAddr],
        mut service: S,
        mut publisher: E,
        commands: chan::Receiver<S::Command>,
    ) -> Result<(), Error>
    where
        S: Service<Id>,
        S::DisconnectReason: Into<Disconnect<S::DisconnectReason>>,
        E: Publisher<S::Event>,
    {
        let listener = if listen_addrs.is_empty() {
            None
        } else {
            let listener = self::listen(listen_addrs)?;
            let local_addr = listener.local_addr()?;

            self.sources
                .register(Source::Listener, &listener, popol::interest::READ);
            self.listening.send(local_addr).ok();

            info!(target: "net", "Listening on {}", local_addr);

            Some(listener)
        };

        info!(target: "net", "Initializing service..");

        let local_time = SystemTime::now().into();
        service.initialize(local_time);

        self.process(&mut service, &mut publisher, local_time);

        // I/O readiness events populated by `popol::Sources::wait_timeout`.
        let mut events = Vec::with_capacity(32);
        // Timeouts populated by `TimeoutManager::wake`.
        let mut timeouts = Vec::with_capacity(32);

        loop {
            let timeout = self
                .timeouts
                .next(SystemTime::now())
                .unwrap_or(WAIT_TIMEOUT)
                .into();

            trace!(
                "Polling {} source(s) and {} timeout(s), waking up in {:?}..",
                self.sources.len(),
                self.timeouts.len(),
                timeout
            );

            let result = self.sources.wait_timeout(&mut events, timeout); // Blocking.
            let local_time = SystemTime::now().into();

            service.tick(local_time);

            match result {
                Ok(n) => {
                    trace!("Woke up with {n} source(s) ready");

                    for ev in events.drain(..) {
                        match &ev.key {
                            Source::Peer(addr) => {
                                let socket_addr = addr.to_socket_addr();
                                if ev.is_error() || ev.is_hangup() {
                                    // Let the subsequent read fail.
                                    trace!("{}: Socket error triggered: {:?}", socket_addr, ev);
                                }
                                if ev.is_invalid() {
                                    // File descriptor was closed and is invalid.
                                    // Nb. This shouldn't happen. It means the source wasn't
                                    // properly unregistered, or there is a duplicate source.
                                    error!(target: "net", "{}: Socket is invalid, removing", socket_addr);

                                    self.sources.unregister(&ev.key);
                                    continue;
                                }

                                if ev.is_writable() {
                                    self.handle_writable(addr.clone(), &ev.key, &mut service)?;
                                }
                                if ev.is_readable() {
                                    self.handle_readable(addr.clone(), &mut service);
                                }
                            }
                            Source::Listener => loop {
                                if let Some(ref listener) = listener {
                                    let (conn, socket_addr) = match listener.accept() {
                                        Ok((conn, socket_addr)) => (conn, socket_addr),
                                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(e) => {
                                            error!(target: "net", "Accept error: {}", e.to_string());
                                            break;
                                        }
                                    };
                                    let addr = Id::from(socket_addr);
                                    trace!("{}: Accepting peer connection", socket_addr);

                                    conn.set_nonblocking(true)?;

                                    let local_addr = conn.local_addr()?;
                                    let link = Link::Inbound;

                                    self.register_peer(addr.clone(), conn, link);

                                    service.connected(addr, &local_addr, link);
                                }
                            },
                            Source::Waker => {
                                trace!("Woken up by waker ({} command(s))", commands.len());

                                // Exit reactor loop if a shutdown was received.
                                if let Ok(()) = self.shutdown.try_recv() {
                                    return Ok(());
                                }
                                popol::Waker::reset(ev.source).ok();

                                // Nb. This assert has triggered once, but I wasn't available
                                // to reproduce it.
                                debug_assert!(!commands.is_empty());

                                for cmd in commands.try_iter() {
                                    service.command_received(cmd);
                                }
                            }
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                    // Nb. The way this is currently used basically ignores which keys have
                    // timed out. So as long as *something* timed out, we wake the service.
                    self.timeouts.wake(local_time, &mut timeouts);

                    if !timeouts.is_empty() {
                        timeouts.clear();
                        service.timer_expired();
                    }
                }
                Err(err) => return Err(err.into()),
            }
            self.process(&mut service, &mut publisher, local_time);
        }
    }

    /// Return a new waker.
    ///
    /// Used to wake up the main event loop.
    fn waker(&self) -> Self::Waker {
        self.waker.clone()
    }
}

impl<Id: PeerId> Reactor<net::TcpStream, Id> {
    /// Process service state machine outputs.
    fn process<S, E>(&mut self, service: &mut S, publisher: &mut E, local_time: LocalTime)
    where
        S: Service<Id>,
        E: Publisher<S::Event>,
        S::DisconnectReason: Into<Disconnect<S::DisconnectReason>>,
    {
        // Note that there may be messages destined for a peer that has since been
        // disconnected.
        while let Some(out) = service.next() {
            match out {
                Io::Write(addr, bytes) => {
                    if let Some(socket) = self.peers.get_mut(&addr) {
                        if let Some(source) = self.sources.get_mut(&Source::Peer(addr)) {
                            socket.push(&bytes);
                            source.set(popol::interest::WRITE);
                        }
                    }
                }
                Io::Connect(addr) => {
                    let socket_addr = addr.to_socket_addr();
                    trace!("Connecting to {}...", socket_addr);

                    match self::dial(&socket_addr) {
                        Ok(stream) => {
                            trace!("Stream established with {}", socket_addr);

                            self.register_peer(addr.clone(), stream, Link::Outbound);
                            self.connecting.insert(addr.clone());

                            service.attempted(&addr);
                        }
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                            // Ignore. We are already establishing a connection through
                            // this socket.
                        }
                        Err(err) => {
                            error!(target: "net", "{}: Dial error: {}", socket_addr, err.to_string());

                            service.disconnected(&addr, Disconnect::DialError(Arc::new(err)));
                        }
                    }
                }
                Io::Disconnect(addr, reason) => {
                    if let Some(peer) = self.peers.get(&addr) {
                        trace!("{}: Disconnecting: {}", addr.to_socket_addr(), reason);

                        // Shutdown the connection, ignoring any potential errors.
                        // If the socket was already disconnected, this will yield
                        // an error that is safe to ignore (`ENOTCONN`). The other
                        // possible errors relate to an invalid file descriptor.
                        peer.disconnect().ok();

                        self.unregister_peer(addr, reason.into(), service);
                    }
                }
                Io::SetTimer(timeout) => {
                    self.timeouts.register((), local_time + timeout);
                }
                Io::Event(event) => {
                    publisher.publish(event);
                }
            }
        }
    }

    fn handle_readable<S>(&mut self, addr: Id, service: &mut S)
    where
        S: Service<Id>,
    {
        // Nb. If the socket was readable and writable at the same time, and it was disconnected
        // during an attempt to write, it will no longer be registered and hence available
        // for reads.
        if let Some(socket) = self.peers.get_mut(&addr) {
            let mut buffer = [0; READ_BUFFER_SIZE];

            let socket_addr = addr.to_socket_addr();
            trace!("{}: Socket is readable", socket_addr);

            // Nb. Since `poll`, which this reactor is based on, is *level-triggered*,
            // we will be notified again if there is still data to be read on the socket.
            // Hence, there is no use in putting this socket read in a loop, as the second
            // invocation would likely block.
            match socket.read(&mut buffer) {
                Ok(count) => {
                    if count > 0 {
                        trace!("{}: Read {} bytes", socket_addr, count);

                        service.message_received(&addr, Cow::Borrowed(&buffer[..count]));
                    } else {
                        trace!("{}: Read 0 bytes", socket_addr);
                        // If we get zero bytes read as a return value, it means the peer has
                        // performed an orderly shutdown.
                        socket.disconnect().ok();
                        self.unregister_peer(
                            addr,
                            Disconnect::ConnectionError(Arc::new(io::Error::from(
                                io::ErrorKind::ConnectionReset,
                            ))),
                            service,
                        );
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    // This shouldn't normally happen, since this function is only called
                    // when there's data on the socket. We leave it here in case external
                    // conditions change.
                }
                Err(err) => {
                    trace!("{}: Read error: {}", socket_addr, err.to_string());

                    socket.disconnect().ok();
                    self.unregister_peer(addr, Disconnect::ConnectionError(Arc::new(err)), service);
                }
            }
        }
    }

    fn handle_writable<S: Service<Id>>(
        &mut self,
        addr: Id,
        source: &Source<Id>,
        service: &mut S,
    ) -> io::Result<()> {
        let socket_addr = addr.to_socket_addr();
        trace!("{}: Socket is writable", socket_addr);

        let source = self.sources.get_mut(source).unwrap();
        let socket = self.peers.get_mut(&addr).unwrap();

        // "A file descriptor for a socket that is connecting asynchronously shall indicate
        // that it is ready for writing, once a connection has been established."
        //
        // Since we perform a non-blocking connect, we're only really connected once the socket
        // is writable.
        if self.connecting.remove(&addr) {
            let local_addr = socket.local_address()?;

            service.connected(addr.clone(), &local_addr, socket.link);
        }

        match socket.flush() {
            // In this case, we've written all the data, we
            // are no longer interested in writing to this
            // socket.
            Ok(()) => {
                source.unset(popol::interest::WRITE);
            }
            // In this case, the write couldn't complete. Set
            // our interest to `WRITE` to be notified when the
            // socket is ready to write again.
            Err(err)
                if [io::ErrorKind::WouldBlock, io::ErrorKind::WriteZero].contains(&err.kind()) =>
            {
                source.set(popol::interest::WRITE);
            }
            Err(err) => {
                error!(target: "net", "{}: Write error: {}", socket_addr, err.to_string());

                socket.disconnect().ok();
                self.unregister_peer(addr, Disconnect::ConnectionError(Arc::new(err)), service);
            }
        }
        Ok(())
    }
}

/// Connect to a peer given a remote address.
fn dial(addr: &net::SocketAddr) -> Result<net::TcpStream, io::Error> {
    use socket2::{Domain, Socket, Type};
    fallible! { io::Error::from(io::ErrorKind::Other) };

    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::STREAM, None)?;

    sock.set_read_timeout(Some(READ_TIMEOUT))?;
    sock.set_write_timeout(Some(WRITE_TIMEOUT))?;
    sock.set_nonblocking(true)?;

    match sock.connect(&(*addr).into()) {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(e) if e.raw_os_error() == Some(libc::EALREADY) => {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists))
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
        Err(e) => return Err(e),
    }
    Ok(sock.into())
}

// Listen for connections on the given address.
fn listen<A: net::ToSocketAddrs>(addr: A) -> Result<net::TcpListener, Error> {
    let sock = net::TcpListener::bind(addr)?;

    sock.set_nonblocking(true)?;

    Ok(sock)
}
