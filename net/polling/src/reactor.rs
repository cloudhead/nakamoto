//! Polling-based reactor. This is a single-threaded reactor using a `polling` loop.
use crossbeam_channel as chan;

use nakamoto_net::error::Error;
use nakamoto_net::event::Publisher;
use nakamoto_net::time::{LocalDuration, LocalTime};
use nakamoto_net::{Disconnect, Io, PeerId};
use nakamoto_net::{Link, Service};

use log::*;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
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
/// Polling key used for events from Listener socket
const LISTENER_SOURCE_KEY: usize = 0;
/// Initial polling key used for peer sockets
const INITIAL_PEER_SOURCE_KEY: usize = 1;

#[derive(Clone)]
pub struct Waker(Arc<polling::Poller>);

impl Waker {
    fn new(poller: Arc<polling::Poller>) -> io::Result<Self> {
        Ok(Self(poller))
    }
}

impl nakamoto_net::Waker for Waker {
    fn wake(&self) -> io::Result<()> {
        self.0.notify()
    }
}

/// A single-threaded non-blocking reactor.
pub struct Reactor<Id: PeerId = net::SocketAddr> {
    peers: HashMap<Id, Socket<net::TcpStream>>,
    connecting: HashSet<Id>,
    poller: Arc<polling::Poller>,
    sources: HashMap<usize, Id>,
    source_keys: HashMap<Id, usize>,
    source_key: AtomicUsize,
    waker: Waker,
    timeouts: TimeoutManager<()>,
    shutdown: chan::Receiver<()>,
    listening: chan::Sender<net::SocketAddr>,
}

impl<Id: PeerId> Reactor<Id> {

    /// Register a peer with the reactor.
    fn register_peer(&mut self, addr: Id, stream: net::TcpStream, link: Link) {
        let source_key = self.source_key.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let socket_addr = addr.to_socket_addr();
        let socket = Socket::from(stream, socket_addr, link);

        self.sources.insert(source_key, addr.clone());
        self.source_keys.insert(addr.clone(), source_key);
        
        if let Err(e) = self.poller.add(socket.raw(), interest_from_socket(source_key, &socket)) {
            error!("Failed to register interest: {}", e.to_string());
        }

        self.peers
            .insert(addr, socket);
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

        if let Some(key) = self.source_keys.remove(&addr) {
            self.sources.remove(&key);
        }

        if let Some(socket) = self.peers.remove(&addr) {
            if let Err(e) = self.poller.delete(socket.raw()) {
                error!("Failed to unregister interest: {}", e.to_string());
            }
        }

        service.disconnected(&addr, reason);
    }
}

impl<Id: PeerId> nakamoto_net::Reactor<Id> for Reactor<Id> {
    type Waker = Waker;

    /// Construct a new reactor, given a channel to send events on.
    fn new(
        shutdown: chan::Receiver<()>,
        listening: chan::Sender<net::SocketAddr>,
    ) -> Result<Self, io::Error> {
        let peers = HashMap::new();

        let poller = Arc::new(polling::Poller::new()?);
        let waker = Waker::new(poller.clone())?;
        let sources = HashMap::new();
        let source_keys = HashMap::new();
        let source_key = AtomicUsize::new(INITIAL_PEER_SOURCE_KEY);
        let timeouts = TimeoutManager::new(LocalDuration::from_secs(1));
        let connecting = HashSet::new();

        Ok(Self {
            peers,
            connecting,
            sources,
            source_keys,
            source_key,
            poller,
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

            if let Err(e) = self.poller.add(&listener, polling::Event::readable(LISTENER_SOURCE_KEY)) {
                error!("Failed to register listener's interest: {}", e.to_string());
            }

            self.listening.send(local_addr).ok();

            info!(target: "net", "Listening on {}", local_addr);

            Some(listener)
        };

        info!(target: "net", "Initializing service..");

        let local_time = SystemTime::now().into();
        service.initialize(local_time);

        self.process(&mut service, &mut publisher, local_time);

        // I/O readiness events populated by `polling::Poller::wait`.
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

            let result = self.poller.wait(&mut events, Some(timeout)); // Blocking.
            let local_time = SystemTime::now().into();

            service.tick(local_time);

            match result {
                Ok(n) => {
                    trace!("Woke up with {n} source(s) ready");

                    // Nb. There's no way to differentiate between a timeout, notify(), and spurious wake-ups.  
                    // It's safe to cleanup timeouts in all of these cases because we don't actually care
                    // what key timed out.
                    if n == 0 {
                        self.timeouts.wake(local_time, &mut timeouts);

                        if !timeouts.is_empty() {
                            timeouts.clear();
                            service.timer_expired();
                        }
                    }

                    for ev in events.drain(..) {
                        match ev.key {
                            LISTENER_SOURCE_KEY => {
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

                                    if let Err(e) = self.poller.modify(listener, polling::Event::readable(LISTENER_SOURCE_KEY)) {
                                        error!(target: "net", "Failed to modify interest of listener: {}", e.to_string());
                                    }
                                }
                            }
                            key => {
                                if let Some(addr) = self.sources.get(&key).map(|addr| addr.clone()) {
                                    let readable_interest_update = if ev.readable {
                                        Some(self.handle_readable(addr.clone(), &mut service))
                                    } else { None };

                                    let writable_interest_update = if ev.writable {
                                        Some(self.handle_writable(addr.clone(), &mut service)?)
                                    } else { None };

                                    if let Some(socket) = self.peers.get_mut(&addr) {
                                        socket.readable_interest = readable_interest_update.unwrap_or(socket.readable_interest);
                                        socket.writable_interest = writable_interest_update.unwrap_or(socket.writable_interest);
    
                                        if let Err(e) = self.poller.modify(socket.raw(), interest_from_socket(key, socket)) {
                                            error!(target: "net", "Failed to modify polling interest: {}", e.to_string());
                                        }
                                    };                                    
                                }
                            }
                        }
                    }

                    // Exit reactor loop if a shutdown was received.
                    if let Ok(()) = self.shutdown.try_recv() {
                        return Ok(());
                    }

                    for cmd in commands.try_iter() {
                        service.command_received(cmd);
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

impl<Id: PeerId> Reactor<Id> {
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
                        if let Some(key) = self.source_keys.get(&addr) {
                            socket.push(&bytes);
                            socket.writable_interest = true;
                            if let Err(e) = self.poller.modify(socket.raw(), interest_from_socket(*key, socket)) {
                                error!(target: "net", "Failed to register interest: {}", e.to_string());
                            }
                        }                        
                    }
                }
                Io::Connect(addr) => {
                    let socket_addr = addr.to_socket_addr();
                    trace!("Connecting to {}...", socket_addr);

                    match self::dial(&socket_addr) {
                        Ok(stream) => {
                            trace!("{:#?}", stream);

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
                    trace!("Event: {:?}", event);

                    publisher.publish(event);
                }
            }
        }
    }

    fn handle_readable<S>(&mut self, addr: Id, service: &mut S) -> bool
    where
        S: Service<Id>,
    {
        let mut register_interest = false;
        let mut disconnect_error = None;

        // Nb. If the socket was readable and writable at the same time, and it was disconnected
        // during an attempt to write, it will no longer be registered and hence available
        // for reads.
        if let Some(socket) = self.peers.get_mut(&addr) {
            let mut buffer = [0; READ_BUFFER_SIZE];

            let socket_addr = addr.to_socket_addr();
            trace!("{}: Socket is readable", socket_addr);

            loop {
                match socket.read(&mut buffer) {
                    Ok(count) => {
                        if count > 0 {
                            trace!("{}: Read {} bytes", socket_addr, count);
                            service.message_received(&addr, Cow::Borrowed(&buffer[..count]));
                            register_interest = true;
                        } else {
                            trace!("{}: Read 0 bytes", socket_addr);
                            // If we get zero bytes read as a return value, it means the peer has
                            // performed an orderly shutdown.
                            socket.disconnect().ok();
                            disconnect_error = Some(Disconnect::ConnectionError(Arc::new(io::Error::from(
                                io::ErrorKind::ConnectionReset,
                            ))));

                            break;
                        }
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        // This shouldn't normally happen, since this function is only called
                        // when there's data on the socket. We leave it here in case external
                        // conditions change.
                        register_interest = true;
                        break;
                    }
                    Err(err) => {
                        trace!("{}: Read error: {}", socket_addr, err.to_string());

                        socket.disconnect().ok();
                        disconnect_error = Some(Disconnect::ConnectionError(Arc::new(err)));
                        
                        break;
                    }
                }
            }
        }

        if let Some(disconnect_error) = disconnect_error {
            self.unregister_peer(addr, disconnect_error, service);
        }

        register_interest
    }

    fn handle_writable<S: Service<Id>>(
        &mut self,
        addr: Id,
        service: &mut S,
    ) -> io::Result<bool> {
        let mut should_register_interest = false;

        let socket_addr = addr.to_socket_addr();
        trace!("{}: Socket is writable", socket_addr);

        let socket = self.peers.get_mut(&addr);
        if socket.is_none() {
            return Ok(should_register_interest)
        }
        let socket = socket.unwrap();

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
            // In this case, we've written all the data
            // and have nothing left to do.
            Ok(()) => {}
            // In this case, the write couldn't complete. Set
            // our interest to `WRITE` to be notified when the
            // socket is ready to write again.
            Err(err)
                if [io::ErrorKind::WouldBlock, io::ErrorKind::WriteZero].contains(&err.kind()) =>
            {
                should_register_interest = true;
            }
            Err(err) => {
                error!(target: "net", "{}: Write error: {}", socket_addr, err.to_string());

                socket.disconnect().ok();
                self.unregister_peer(addr, Disconnect::ConnectionError(Arc::new(err)), service);
            }
        }
        Ok(should_register_interest)
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

fn interest_from_socket(key: usize, socket: &Socket<net::TcpStream>) -> polling::Event {
    polling::Event {
        key,
        readable: socket.readable_interest,
        writable: socket.writable_interest
    }
}