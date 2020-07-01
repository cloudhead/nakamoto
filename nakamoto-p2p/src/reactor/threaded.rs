use bitcoin::consensus::encode::Decodable;
use bitcoin::consensus::encode::{self, Encodable};
use bitcoin::network::stream_reader::StreamReader;

use crate::address_book::AddressBook;
use crate::error::Error;
use crate::protocol::{Event, Link, Protocol};

use log::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::prelude::*;
use std::net;

use crossbeam_channel as crossbeam;

/// Stack size for spawned threads, in bytes.
/// Since we're creating a thread per peer, we want to keep the stack size small.
const THREAD_STACK_SIZE: usize = 1024 * 1024;

/// Maximum peer-to-peer message size.
pub const MAX_MESSAGE_SIZE: usize = 6 * 1024;

/// Command sent to a writer thread.
#[derive(Debug)]
pub enum Command<T> {
    Write(net::SocketAddr, T),
    Disconnect(net::SocketAddr),
    Quit,
}

/// Reader socket.
#[derive(Debug)]
pub struct Reader<R: Read + Write, M> {
    events: crossbeam::Sender<Event<M>>,
    raw: StreamReader<R>,
    address: net::SocketAddr,
    local_address: net::SocketAddr,
}

impl<R: Read + Write, M: Decodable + Encodable + Debug + Send + Sync + 'static> Reader<R, M> {
    /// Create a new peer from a `io::Read` and an address pair.
    pub fn from(
        r: R,
        local_address: net::SocketAddr,
        address: net::SocketAddr,
        events: crossbeam::Sender<Event<M>>,
    ) -> Self {
        let raw = StreamReader::new(r, Some(MAX_MESSAGE_SIZE));

        Self {
            raw,
            local_address,
            address,
            events,
        }
    }

    pub fn run(&mut self, link: Link) -> Result<(), Error> {
        self.events.send(Event::Connected {
            addr: self.address,
            local_addr: self.local_address,
            link,
        })?;

        loop {
            match self.read() {
                Ok(msg) => self.events.send(Event::Received(self.address, msg))?,
                Err(err) => {
                    self.events.send(Event::Error(self.address, err.into()))?;
                    break;
                }
            }
        }
        Ok(())
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
}

///////////////////////////////////////////////////////////////////////////////////////////////

/// Writer socket.
pub struct Writer<T> {
    address: net::SocketAddr,
    raw: T,
}

impl<T: Write> Writer<T> {
    pub fn write<M: Encodable + Debug>(&mut self, msg: M) -> Result<usize, Error> {
        let mut buf = [0u8; MAX_MESSAGE_SIZE];

        match msg.consensus_encode(&mut buf[..]) {
            Ok(len) => {
                trace!("{}: {:#?}", self.address, msg);

                self.raw.write_all(&buf[..len])?;
                self.raw.flush()?;

                Ok(len)
            }
            Err(err) => panic!(err.to_string()),
        }
    }

    fn thread<M: Encodable + Send + Sync + Debug + 'static>(
        mut peers: HashMap<net::SocketAddr, Self>,
        cmds: crossbeam::Receiver<Command<M>>,
        events: crossbeam::Sender<Event<M>>,
    ) -> Result<(), Error> {
        loop {
            let cmd = cmds.recv().unwrap();

            match cmd {
                Command::Write(addr, msg) => {
                    let peer = peers.get_mut(&addr).unwrap();

                    // TODO: Watch out for head-of-line blocking here!
                    // TODO: We can't just set this to non-blocking, because
                    // this will also affect the read-end.
                    match peer.write(msg) {
                        Ok(nbytes) => {
                            events.send(Event::Sent(addr, nbytes))?;
                        }
                        Err(err) => {
                            events.send(Event::Error(addr, err))?;
                        }
                    }
                }
                Command::Disconnect(addr) => {
                    peers.remove(&addr);
                }
                Command::Quit => break,
            }
        }
        Ok(())
    }
}

impl<T: Write> std::ops::Deref for Writer<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl<T: Write> std::ops::DerefMut for Writer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw
    }
}

/// Run the given protocol with the reactor.
pub fn run<P: Protocol<M>, M: Decodable + Encodable + Send + Sync + Debug + 'static>(
    mut protocol: P,
    addrs: AddressBook,
) -> Result<Vec<()>, Error> {
    use std::thread;

    let (events_tx, events_rx): (crossbeam::Sender<Event<M>>, _) = crossbeam::bounded(1);
    let (cmds_tx, cmds_rx) = crossbeam::bounded(1);

    let mut spawned = Vec::with_capacity(addrs.len());
    let mut peers = HashMap::new();

    for addr in addrs.iter() {
        let (mut conn, writer) = self::dial::<_, P>(&addr, events_tx.clone())?;

        debug!("Connected to {}", &addr);
        trace!("{:#?}", conn);

        peers.insert(*addr, writer);

        let handle = thread::Builder::new()
            .name(addr.to_string())
            .stack_size(THREAD_STACK_SIZE)
            .spawn(move || conn.run(Link::Outbound))?;

        spawned.push(handle);
    }

    thread::Builder::new().spawn(move || Writer::thread(peers, cmds_rx, events_tx))?;

    loop {
        let result = events_rx.recv_timeout(P::PING_INTERVAL);

        match result {
            Ok(event) => {
                let msgs = protocol.step(event);

                for (addr, msg) in msgs.into_iter() {
                    cmds_tx.send(Command::Write(addr, msg)).unwrap();
                }
            }
            Err(crossbeam::RecvTimeoutError::Disconnected) => {
                // TODO: We need to connect to new peers.
                // This always means that all senders have been dropped.
                break;
            }
            Err(crossbeam::RecvTimeoutError::Timeout) => {
                // TODO: Ping peers, nothing was received in a while. Find out
                // who to ping.
            }
        }
    }

    spawned
        .into_iter()
        .flat_map(thread::JoinHandle::join)
        .collect()
}

/// Connect to a peer given a remote address.
pub fn dial<M: Encodable + Decodable + Send + Sync + Debug + 'static, P: Protocol<M>>(
    addr: &net::SocketAddr,
    events_tx: crossbeam::Sender<Event<M>>,
) -> Result<(Reader<net::TcpStream, M>, Writer<net::TcpStream>), Error> {
    debug!("Connecting to {}...", &addr);

    let sock = net::TcpStream::connect(addr)?;

    // TODO: We probably don't want the same timeouts for read and write.
    // For _write_, we want something much shorter.
    sock.set_read_timeout(Some(P::IDLE_TIMEOUT))?;
    sock.set_write_timeout(Some(P::IDLE_TIMEOUT))?;

    let w = sock.try_clone()?;
    let address = sock.peer_addr()?;
    let local_address = sock.local_addr()?;

    Ok((
        Reader::from(sock, local_address, address, events_tx),
        Writer { raw: w, address },
    ))
}
