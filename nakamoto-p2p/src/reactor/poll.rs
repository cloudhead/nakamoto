use bitcoin::consensus::encode::Decodable;
use bitcoin::consensus::encode::{self, Encodable};
use bitcoin::network::stream_reader::StreamReader;

use crate::address_book::AddressBook;
use crate::error::Error;
use crate::protocol::{Event, Link, Protocol};

use log::*;

use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::io;
use std::io::prelude::*;
use std::net;

/// Maximum peer-to-peer message size.
pub const MAX_MESSAGE_SIZE: usize = 6 * 1024;

#[derive(Debug)]
pub struct Socket<R: Read + Write, M> {
    raw: StreamReader<R>,
    address: net::SocketAddr,
    local_address: net::SocketAddr,
    queue: VecDeque<M>,
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
                trace!("{}: {:#?}", self.address, msg);

                Ok(msg)
            }
            Err(err) => Err(err),
        }
    }

    pub fn write(&mut self, msg: &M) -> Result<usize, encode::Error> {
        let mut buf = [0u8; MAX_MESSAGE_SIZE];

        match msg.consensus_encode(&mut buf[..]) {
            Ok(len) => {
                trace!("{}: {:#?}", self.address, msg);

                self.raw.stream.write_all(&buf[..len])?;
                self.raw.stream.flush()?;

                Ok(len)
            }
            Err(err) => Err(err),
        }
    }

    pub fn drain(&mut self, events: &mut VecDeque<Event<M>>, descriptor: &mut popol::Descriptor) {
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

/// Run the given protocol with the reactor.
pub fn run<P: Protocol<M>, M: Decodable + Encodable + Send + Sync + Debug + 'static>(
    mut protocol: P,
    addrs: AddressBook,
) -> Result<Vec<()>, Error> {
    let mut peers = HashMap::new();
    let mut descriptors = popol::Descriptors::new();
    let mut events: VecDeque<Event<M>> = VecDeque::new();

    // TODO(perf): This could be slow..
    for addr in addrs.iter() {
        let stream = self::dial::<_, P>(&addr)?;
        let local_addr = stream.peer_addr()?;
        let addr = stream.peer_addr()?;

        debug!("Connected to {}", &addr);
        trace!("{:#?}", stream);

        events.push_back(Event::Connected {
            addr,
            local_addr,
            link: Link::Outbound,
        });

        descriptors.register(addr, &stream, popol::events::READ | popol::events::WRITE);
        peers.insert(addr, Socket::from(stream, local_addr, addr));
    }

    loop {
        match popol::wait(&mut descriptors, P::PING_INTERVAL)? {
            popol::Wait::Timeout => {
                // TODO: Ping peers, nothing was received in a while. Find out
                // who to ping.
            }
            popol::Wait::Ready(evs) => {
                for (addr, ev) in evs {
                    let socket = peers.get_mut(&addr).unwrap();

                    if ev.errored || ev.hangup {
                        // Let the subsequent read fail.
                    }
                    if ev.readable {
                        loop {
                            match socket.read() {
                                Ok(msg) => {
                                    events.push_back(Event::Received(addr, msg));
                                }
                                Err(encode::Error::Io(err))
                                    if err.kind() == io::ErrorKind::WouldBlock =>
                                {
                                    break;
                                }
                                Err(err) => {
                                    panic!(err.to_string());
                                }
                            }
                        }
                    }
                    if ev.writable {
                        socket.drain(&mut events, ev.descriptor);
                    }
                }
            }
        }

        while let Some(event) = events.pop_front() {
            let msgs = protocol.step(event);

            for (addr, msg) in msgs.into_iter() {
                let peer = peers.get_mut(&addr).unwrap();
                let descriptor = descriptors.get_mut(addr).unwrap();

                peer.queue.push_back(msg);
                peer.drain(&mut events, descriptor);
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
    sock.set_read_timeout(Some(P::IDLE_TIMEOUT))?;
    sock.set_write_timeout(Some(P::IDLE_TIMEOUT))?;
    sock.set_nonblocking(true)?;

    Ok(sock)
}
