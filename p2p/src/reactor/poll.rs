pub use popol::Waker;

use bitcoin::consensus::encode::Decodable;
use bitcoin::consensus::encode::{self, Encodable};
use bitcoin::network::stream_reader::StreamReader;

use crossbeam_channel as chan;

use nakamoto_common::block::time::LocalTime;

use crate::error::Error;
use crate::event::Event;
use crate::fallible;
use crate::protocol::{Component, Input, Link, Message, Output, Protocol};
use crate::reactor::time::TimeoutManager;

use log::*;

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::io;
use std::io::prelude::*;
use std::net;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::time;
use std::time::SystemTime;

/// Maximum peer-to-peer message size.
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
/// Maximum time to wait when reading from a socket.
pub const READ_TIMEOUT: time::Duration = time::Duration::from_secs(6);
/// Maximum time to wait when writing to a socket.
pub const WRITE_TIMEOUT: time::Duration = time::Duration::from_secs(3);

#[derive(Debug)]
pub struct Socket<R: Read + Write, M> {
    raw: StreamReader<R>,
    address: net::SocketAddr,
    local_address: net::SocketAddr,
    queue: VecDeque<M>,
}

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

impl<M: Encodable + Decodable + Debug> Socket<net::TcpStream, M> {
    pub fn disconnect(&self) -> io::Result<()> {
        self.raw.stream.shutdown(net::Shutdown::Both)
    }
}

impl<R: Read + Write, M: Encodable + Decodable + Debug> Socket<R, M> {
    /// Create a new socket from a `io::Read` and an address pair.
    fn from(r: R, local_address: net::SocketAddr, address: net::SocketAddr) -> Self {
        let raw = StreamReader::new(r, Some(MAX_MESSAGE_SIZE));
        let queue = VecDeque::new();

        Self {
            raw,
            local_address,
            address,
            queue,
        }
    }

    fn read(&mut self) -> Result<M, encode::Error> {
        fallible! { encode::Error::Io(io::ErrorKind::Other.into()) };

        match self.raw.read_next::<M>() {
            Ok(msg) => {
                trace!("{}: (read) {:#?}", self.address, msg);

                Ok(msg)
            }
            Err(err) => Err(err),
        }
    }

    fn write(&mut self, msg: &M) -> Result<usize, encode::Error> {
        fallible! { encode::Error::Io(io::ErrorKind::Other.into()) };

        let mut buf = [0u8; MAX_MESSAGE_SIZE];

        match msg.consensus_encode(&mut buf[..]) {
            Ok(len) => {
                trace!("{}: (write) {:#?}", self.address, msg);

                // TODO: Is it possible to get a `WriteZero` here, given
                // the non-blocking socket?
                self.raw.stream.write_all(&buf[..len])?;
                self.raw.stream.flush()?;

                Ok(len)
            }
            Err(encode::Error::Io(err)) if err.kind() == io::ErrorKind::WriteZero => {
                unreachable!();
            }
            Err(err) => Err(err),
        }
    }

    fn drain<C>(
        &mut self,
        events: &mut VecDeque<Input<M, C>>,
        source: &mut popol::Source,
    ) -> Result<(), encode::Error> {
        while let Some(msg) = self.queue.pop_front() {
            match self.write(&msg) {
                Ok(n) => {
                    events.push_back(Input::Sent(self.address, n));
                }
                Err(encode::Error::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {
                    source.set(popol::interest::WRITE);
                    self.queue.push_front(msg);

                    return Ok(());
                }
                Err(err) => {
                    // An unexpected error occured. Push the message back to the front of the
                    // queue in case we're able to recover from it.
                    self.queue.push_front(msg);

                    return Err(err);
                }
            }
        }
        source.unset(popol::interest::WRITE);

        Ok(())
    }
}

pub struct Reactor<R: Write + Read, M: Message, C, T> {
    peers: HashMap<net::SocketAddr, Socket<R, M>>,
    events: VecDeque<Input<M, C>>,
    subscriber: chan::Sender<Event<M::Payload>>,
    sources: popol::Sources<Source>,
    waker: Arc<popol::Waker>,
    timeouts: TimeoutManager<(net::SocketAddr, T)>,
}

impl<R: Write + Read + AsRawFd, M: Message + Encodable + Decodable + Debug, C, T>
    Reactor<R, M, C, T>
{
    pub fn new(subscriber: chan::Sender<Event<M::Payload>>) -> Result<Self, io::Error> {
        let peers = HashMap::new();
        let events: VecDeque<Input<M, C>> = VecDeque::new();

        let mut sources = popol::Sources::new();
        let waker = Arc::new(popol::Waker::new(&mut sources, Source::Waker)?);
        let timeouts = TimeoutManager::new();

        Ok(Self {
            peers,
            sources,
            events,
            subscriber,
            waker,
            timeouts,
        })
    }

    pub fn waker(&mut self) -> Arc<popol::Waker> {
        self.waker.clone()
    }

    fn register_peer(
        &mut self,
        addr: net::SocketAddr,
        local_addr: net::SocketAddr,
        stream: R,
        link: Link,
    ) {
        self.events.push_back(Input::Connected {
            addr,
            local_addr,
            link,
        });
        self.sources
            .register(Source::Peer(addr), &stream, popol::interest::ALL);
        self.peers
            .insert(addr, Socket::from(stream, local_addr, addr));
    }

    fn unregister_peer(&mut self, addr: net::SocketAddr) {
        self.events.push_back(Input::Disconnected(addr));
        self.sources.unregister(&Source::Peer(addr));
        self.peers.remove(&addr);
    }
}

impl<M: Message + Decodable + Encodable + Debug, C: Send + Sync + Clone>
    Reactor<net::TcpStream, M, C, Component>
where
    M::Payload: Debug + Send + Sync,
{
    /// Run the given protocol with the reactor.
    pub fn run<P: Protocol<M, Command = C>>(
        &mut self,
        mut protocol: P,
        commands: chan::Receiver<C>,
        listen_addrs: &[net::SocketAddr],
    ) -> Result<(), Error> {
        let listener = if listen_addrs.is_empty() {
            None
        } else {
            let listener = self::listen(listen_addrs)?;
            let local_addr = listener.local_addr()?;

            self.sources
                .register(Source::Listener, &listener, popol::interest::READ);
            self.subscriber.send(Event::Listening(local_addr))?;

            info!("Listening on {}", local_addr);

            Some(listener)
        };

        {
            info!("Initializing protocol..");

            let local_time = SystemTime::now().into();
            let outs = protocol.initialize(local_time);

            match self.process::<P>(outs, local_time)? {
                Control::Shutdown => return Ok(()),
                _ => {}
            }
        }

        // I/O readiness events populated by `popol::Sources::wait_timeout`.
        let mut events = popol::Events::new();
        // Timeouts populated by `TimeoutManager::wake`.
        let mut timeouts = Vec::with_capacity(32);

        loop {
            let timeout = self.timeouts.next().unwrap_or(P::IDLE_TIMEOUT).into();

            match self.sources.wait_timeout(&mut events, timeout) {
                Ok(()) => {
                    for (source, ev) in events.iter() {
                        match source {
                            Source::Peer(addr) => {
                                if ev.errored || ev.invalid || ev.hangup {
                                    // Let the subsequent read fail.
                                }
                                if ev.writable {
                                    self.handle_writable(&addr, source);
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
                                    conn.set_nonblocking(true)?;

                                    self.register_peer(
                                        addr,
                                        conn.local_addr()?,
                                        conn,
                                        Link::Inbound,
                                    );
                                }
                            },
                            Source::Waker => {
                                for cmd in commands.try_iter() {
                                    self.events.push_back(Input::Command(cmd));
                                }
                            }
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                    let local_time = SystemTime::now().into();

                    self.timeouts.wake(local_time, &mut timeouts);

                    if !timeouts.is_empty() {
                        for (peer, ctx) in timeouts.drain(..) {
                            self.events.push_back(Input::Timeout(peer, ctx));
                        }
                    } else {
                        self.events.push_back(Input::Tick(local_time));
                    }
                }
                Err(err) => return Err(err.into()),
            }

            let local_time = SystemTime::now().into();

            while let Some(event) = self.events.pop_front() {
                let outs = protocol.step(event);
                let control = self.process::<P>(outs, local_time)?;

                if control == Control::Shutdown {
                    return Ok(());
                }
            }
        }
    }

    /// Process protocol state machine outputs.
    fn process<P: Protocol<M>>(
        &mut self,
        outputs: Vec<Output<M>>,
        local_time: LocalTime,
    ) -> Result<Control, Error> {
        // Note that there may be messages destined for a peer that has since been
        // disconnected.
        for out in outputs.into_iter() {
            match out {
                Output::Message(addr, msg) => {
                    if let Some(peer) = self.peers.get_mut(&addr) {
                        let src = self.sources.get_mut(&Source::Peer(addr)).unwrap();

                        peer.queue.push_back(msg);

                        if let Err(err) = peer.drain(&mut self.events, src) {
                            error!("{}: Write error: {}", addr, err.to_string());

                            peer.disconnect().ok();
                            self.unregister_peer(addr);
                        }
                    }
                }
                Output::Connect(addr) => {
                    let stream = self::dial(&addr)?;
                    let local_addr = stream.local_addr()?;
                    let addr = stream.peer_addr()?;

                    trace!("{:#?}", stream);

                    self.register_peer(addr, local_addr, stream, Link::Outbound);
                }
                Output::Disconnect(addr) => {
                    if let Some(peer) = self.peers.get(&addr) {
                        debug!("{}: Disconnecting..", addr);

                        // Shutdown the connection, ignoring any potential errors.
                        // If the socket was already disconnected, this will yield
                        // an error that is safe to ignore (`ENOTCONN`). The other
                        // possible errors relate to an invalid file descriptor.
                        peer.disconnect().ok();

                        self.unregister_peer(addr);
                    }
                }
                Output::SetTimeout(key, component, timeout) => {
                    self.timeouts
                        .register((key, component), local_time + timeout);
                }
                Output::Event(event) => {
                    trace!("Event: {:?}", event);

                    self.subscriber.try_send(event).unwrap(); // FIXME
                }
                Output::Shutdown => {
                    info!("Shutdown received");

                    return Ok(Control::Shutdown);
                }
            }
        }
        Ok(Control::Continue)
    }

    fn handle_readable(&mut self, addr: &net::SocketAddr) {
        let socket = self.peers.get_mut(&addr).unwrap();

        // Nb. Normally, since `poll`, which `popol` is based on, is
        // level-triggered, we would be notified again if there was
        // still data to be read on the socket. However, since our
        // socket abstraction actually returns *decoded messages*, this
        // doesn't apply. Thus, we have to loop to not miss messages.
        loop {
            match socket.read() {
                Ok(msg) => {
                    self.events.push_back(Input::Received(*addr, msg));
                }
                Err(encode::Error::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(err) => {
                    error!("{}: Read error: {}", addr, err.to_string());

                    socket.disconnect().ok();
                    self.unregister_peer(*addr);

                    break;
                }
            }
        }
    }

    fn handle_writable(&mut self, addr: &net::SocketAddr, source: &Source) {
        let src = self.sources.get_mut(source).unwrap();
        let socket = self.peers.get_mut(&addr).unwrap();

        if let Err(err) = socket.drain(&mut self.events, src) {
            error!("{}: Write error: {}", addr, err.to_string());

            socket.disconnect().ok();
            self.unregister_peer(*addr);
        }
    }
}

/// Connect to a peer given a remote address.
pub fn dial(addr: &net::SocketAddr) -> Result<net::TcpStream, Error> {
    fallible! { Error::Io(io::ErrorKind::Other.into()) };
    debug!("Connecting to {}...", &addr);

    let sock = net::TcpStream::connect(addr)?;

    // TODO: We probably don't want the same timeouts for read and write.
    // For _write_, we want something much shorter.
    sock.set_read_timeout(Some(READ_TIMEOUT))?;
    sock.set_write_timeout(Some(WRITE_TIMEOUT))?;
    sock.set_nonblocking(true)?;

    Ok(sock)
}

// Listen for connections on the given address.
pub fn listen<A: net::ToSocketAddrs>(addr: A) -> Result<net::TcpListener, Error> {
    let sock = net::TcpListener::bind(addr)?;

    sock.set_nonblocking(true)?;

    Ok(sock)
}
