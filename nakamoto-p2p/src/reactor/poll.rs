use bitcoin::consensus::encode::Decodable;
use bitcoin::consensus::encode::{self, Encodable};
use bitcoin::network::stream_reader::StreamReader;

use crate::address_book::AddressBook;
use crate::error::Error;
use crate::protocol::{Event, Link, Output, Protocol};

use log::*;

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::io;
use std::io::prelude::*;
use std::net;
use std::os::unix::io::AsRawFd;
use std::time::SystemTime;

/// Maximum peer-to-peer message size.
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

#[derive(Debug)]
pub struct Socket<R: Read + Write, M> {
    raw: StreamReader<R>,
    address: net::SocketAddr,
    local_address: net::SocketAddr,
    queue: VecDeque<M>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum Source {
    Peer(net::SocketAddr),
    Listener,
}

impl<M: Encodable + Decodable + Debug> Socket<net::TcpStream, M> {
    pub fn disconnect(&self) -> io::Result<()> {
        self.raw.stream.shutdown(net::Shutdown::Both)
    }
}

impl<R: Read + Write, M: Encodable + Decodable + Debug> Socket<R, M> {
    /// Create a new socket from a `io::Read` and an address pair.
    pub fn from(r: R, local_address: net::SocketAddr, address: net::SocketAddr) -> Self {
        let raw = StreamReader::new(r, Some(MAX_MESSAGE_SIZE));
        let queue = VecDeque::new();

        Self {
            raw,
            local_address,
            address,
            queue,
        }
    }

    pub fn read(&mut self) -> Result<M, encode::Error> {
        match self.raw.read_next::<M>() {
            Ok(msg) => {
                trace!("{}: (read) {:#?}", self.address, msg);

                Ok(msg)
            }
            Err(err) => Err(err),
        }
    }

    pub fn write(&mut self, msg: &M) -> Result<usize, encode::Error> {
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

    pub fn drain(&mut self, events: &mut VecDeque<Event<M>>, descriptor: &mut popol::Source) {
        while let Some(msg) = self.queue.pop_front() {
            match self.write(&msg) {
                Ok(n) => {
                    events.push_back(Event::Sent(self.address, n));
                }
                Err(encode::Error::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {
                    descriptor.set(popol::events::WRITE);
                    self.queue.push_front(msg);

                    return;
                }
                Err(err) => {
                    panic!(err.to_string());
                }
            }
        }
        descriptor.unset(popol::events::WRITE);
    }
}

pub struct Reactor<R: Write + Read, M> {
    peers: HashMap<net::SocketAddr, Socket<R, M>>,
    sources: popol::Sources<Source>,
    events: VecDeque<Event<M>>,
}

impl<R: Write + Read + AsRawFd, M: Encodable + Decodable + Debug> Reactor<R, M> {
    pub fn new() -> Self {
        let peers = HashMap::new();
        let sources = popol::Sources::new();
        let events: VecDeque<Event<M>> = VecDeque::new();

        Self {
            peers,
            sources,
            events,
        }
    }

    fn register_peer(
        &mut self,
        addr: net::SocketAddr,
        local_addr: net::SocketAddr,
        stream: R,
        link: Link,
    ) {
        self.events.push_back(Event::Connected {
            addr,
            local_addr,
            link,
        });
        self.sources.register(
            Source::Peer(addr),
            &stream,
            popol::events::READ | popol::events::WRITE,
        );
        self.peers
            .insert(addr, Socket::from(stream, local_addr, addr));
    }

    fn unregister_peer(&mut self, addr: net::SocketAddr) {
        self.events.push_back(Event::Disconnected(addr));
        self.sources.unregister(&Source::Peer(addr));
        self.peers.remove(&addr);
    }
}

impl<M: Decodable + Encodable + Send + Sync + Debug + 'static> Reactor<net::TcpStream, M> {
    /// Run the given protocol with the reactor.
    pub fn run<P: Protocol<M>>(
        &mut self,
        mut protocol: P,
        addrs: AddressBook,
        listen_addrs: &[net::SocketAddr],
    ) -> Result<Vec<()>, Error> {
        // TODO(perf): This could be slow..
        for addr in addrs.iter() {
            let stream = self::dial::<_, P>(&addr)?;
            let local_addr = stream.peer_addr()?;
            let addr = stream.peer_addr()?;

            info!("Connected to {}", &addr);
            trace!("{:#?}", stream);

            self.register_peer(addr, local_addr, stream, Link::Outbound);
        }

        let listener = self::listen(listen_addrs)?;
        self.sources
            .register(Source::Listener, &listener, popol::events::READ);

        info!("Listening on {}", listener.local_addr()?);

        // Inbound connected peers. Used as a temporary buffer.
        let mut connected = Vec::new();
        // Disconnected peers to unregister.
        let mut disconnected = Vec::new();

        loop {
            match popol::wait(&mut self.sources, P::PING_INTERVAL.into())? {
                popol::Wait::Timeout => {
                    self.events.push_back(Event::Idle);
                }
                popol::Wait::Ready(evs) => {
                    for (source, ev) in evs {
                        match source {
                            Source::Peer(addr) => {
                                let socket = self.peers.get_mut(&addr).unwrap();

                                if ev.errored || ev.hangup {
                                    // Let the subsequent read fail.
                                }
                                if ev.readable {
                                    loop {
                                        match socket.read() {
                                            Ok(msg) => {
                                                self.events.push_back(Event::Received(addr, msg));
                                            }
                                            Err(encode::Error::Io(err))
                                                if err.kind() == io::ErrorKind::WouldBlock =>
                                            {
                                                break;
                                            }
                                            Err(err) => {
                                                error!("{}: Read error: {}", addr, err.to_string());

                                                socket.disconnect().ok();
                                                disconnected.push(addr);

                                                break;
                                            }
                                        }
                                    }
                                }
                                if ev.writable {
                                    socket.drain(&mut self.events, ev.source);
                                }
                            }
                            Source::Listener => loop {
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
                                connected.push((addr, conn));
                            },
                        }
                    }
                }
            }

            for (addr, stream) in connected.drain(..) {
                let local_addr = stream.local_addr()?;
                stream.set_nonblocking(true)?;

                self.register_peer(addr, local_addr, stream, Link::Inbound);
            }
            for addr in disconnected.drain(..) {
                self.unregister_peer(addr);
            }

            let local_time = SystemTime::now().into();

            while let Some(event) = self.events.pop_front() {
                let outs = protocol.step(event, local_time);

                // Note that there may be messages destined for a peer that has since been
                // disconnected.
                for out in outs.into_iter() {
                    match out {
                        Output::Message(addr, msg) => {
                            if let Some(peer) = self.peers.get_mut(&addr) {
                                let descriptor = self.sources.get_mut(&Source::Peer(addr)).unwrap();

                                peer.queue.push_back(msg);
                                peer.drain(&mut self.events, descriptor);
                            }
                        }
                        Output::Disconnect(addr) => {
                            if let Some(peer) = self.peers.get(&addr) {
                                // Shutdown the connection, ignoring any potential errors.
                                // If the socket was already disconnected, this will yield
                                // an error that is safe to ignore (`ENOTCONN`). The other
                                // possible errors relate to an invalid file descriptor.
                                peer.disconnect().ok();

                                self.unregister_peer(addr);
                            }
                        }
                        _ => todo!(),
                    }
                }
            }
        }
    }
}

/// Connect to a peer given a remote address.
pub fn dial<M: Encodable + Decodable + Send + Sync + Debug + 'static, P: Protocol<M>>(
    addr: &net::SocketAddr,
) -> Result<net::TcpStream, Error> {
    debug!("Connecting to {}...", &addr);

    let sock = net::TcpStream::connect(addr)?;

    // TODO: We probably don't want the same timeouts for read and write.
    // For _write_, we want something much shorter.
    sock.set_read_timeout(Some(P::IDLE_TIMEOUT.into()))?;
    sock.set_write_timeout(Some(P::IDLE_TIMEOUT.into()))?;
    sock.set_nonblocking(true)?;

    Ok(sock)
}

// Listen for connections on the given address.
pub fn listen<A: net::ToSocketAddrs>(addr: A) -> Result<net::TcpListener, Error> {
    let sock = net::TcpListener::bind(addr)?;

    sock.set_nonblocking(true)?;

    Ok(sock)
}
