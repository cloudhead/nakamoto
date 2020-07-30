use bitcoin::consensus::encode::Decodable;
use bitcoin::consensus::encode::{self, Encodable};
use bitcoin::network::stream_reader::StreamReader;

use crate::error::Error;
use crate::protocol::{Input, Link, Message, Output, Protocol};

use log::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::prelude::*;
use std::net;
use std::time::SystemTime;

use crossbeam_channel as crossbeam;

/// Stack size for spawned threads, in bytes.
/// Since we're creating a thread per peer, we want to keep the stack size small.
const THREAD_STACK_SIZE: usize = 1024 * 1024;

/// Maximum peer-to-peer message size.
pub const MAX_MESSAGE_SIZE: usize = 6 * 1024;

/// Command sent to a writer thread.
#[derive(Debug)]
pub enum Command<T, S: Write> {
    Write(net::SocketAddr, T),
    Connect(net::SocketAddr, Writer<S>),
    Disconnect(net::SocketAddr),
    Quit,
}

/// Reader socket.
#[derive(Debug)]
pub struct Reader<R: Read + Write, M, C: Send + Sync> {
    events: crossbeam::Sender<Input<M, C>>,
    raw: StreamReader<R>,
    address: net::SocketAddr,
    local_address: net::SocketAddr,
}

impl<
        R: Read + Write,
        M: Decodable + Encodable + Debug + Send + Sync + 'static,
        C: Debug + Send + Sync + 'static,
    > Reader<R, M, C>
{
    /// Create a new peer from a `io::Read` and an address pair.
    pub fn from(
        r: R,
        local_address: net::SocketAddr,
        address: net::SocketAddr,
        events: crossbeam::Sender<Input<M, C>>,
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
        self.events.send(Input::Connected {
            addr: self.address,
            local_addr: self.local_address,
            link,
        })?;

        loop {
            match self.read() {
                Ok(msg) => self.events.send(Input::Received(self.address, msg))?,
                Err(err) => {
                    panic!(err);
                }
            }
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
}

///////////////////////////////////////////////////////////////////////////////////////////////

/// Writer socket.
#[derive(Debug)]
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

    fn thread<M: Encodable + Send + Sync + Debug + 'static, C: Debug + Send + Sync + 'static>(
        cmds: crossbeam::Receiver<Command<M, T>>,
        events: crossbeam::Sender<Input<M, C>>,
    ) -> Result<(), Error> {
        let mut peers: HashMap<net::SocketAddr, Self> = HashMap::new();

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
                            events.send(Input::Sent(addr, nbytes))?;
                        }
                        Err(err) => {
                            panic!(err);
                        }
                    }
                }
                Command::Connect(addr, writer) => {
                    peers.insert(addr, writer);
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
pub fn run<
    P: Protocol<M, Command = C>,
    M: Message + Decodable + Encodable + Debug,
    C: Debug + Send + Sync + 'static,
>(
    mut protocol: P,
) -> Result<Vec<()>, Error> {
    use std::thread;

    let (events_tx, events_rx): (crossbeam::Sender<Input<M, C>>, _) = crossbeam::bounded(1);
    let (cmds_tx, cmds_rx) = crossbeam::bounded(1);

    let mut spawned = Vec::new();

    let writer_events_tx = events_tx.clone();
    thread::Builder::new().spawn(move || Writer::thread(cmds_rx, writer_events_tx))?;

    let local_time = SystemTime::now().into();

    loop {
        let result = events_rx.recv_timeout(P::PING_INTERVAL.into());

        match result {
            Ok(event) => {
                let outs = protocol.step(event, local_time);

                for out in outs.into_iter() {
                    match out {
                        Output::Message(addr, msg) => {
                            cmds_tx.send(Command::Write(addr, msg)).unwrap();
                        }
                        Output::Connect(addr) => {
                            let (mut conn, writer) =
                                self::dial::<_, _, P>(&addr, events_tx.clone())?;

                            debug!("Connected to {}", &addr);
                            trace!("{:#?}", conn);

                            cmds_tx.send(Command::Connect(addr, writer)).unwrap();

                            let handle = thread::Builder::new()
                                .name(addr.to_string())
                                .stack_size(THREAD_STACK_SIZE)
                                .spawn(move || conn.run(Link::Outbound))?;

                            spawned.push(handle);
                        }
                        _ => todo!(),
                    }
                }
            }
            Err(crossbeam::RecvTimeoutError::Disconnected) => {
                // TODO: We need to connect to new peers.
                // This always means that all senders have been dropped.
                break;
            }
            Err(crossbeam::RecvTimeoutError::Timeout) => {
                events_tx.send(Input::Idle).unwrap();
            }
        }
    }

    spawned
        .into_iter()
        .flat_map(thread::JoinHandle::join)
        .collect()
}

/// Connect to a peer given a remote address.
pub fn dial<
    M: Message + Encodable + Decodable + Debug,
    C: Debug + Send + Sync + 'static,
    P: Protocol<M>,
>(
    addr: &net::SocketAddr,
    events_tx: crossbeam::Sender<Input<M, C>>,
) -> Result<(Reader<net::TcpStream, M, C>, Writer<net::TcpStream>), Error> {
    debug!("Connecting to {}...", &addr);

    let sock = net::TcpStream::connect(addr)?;

    // TODO: We probably don't want the same timeouts for read and write.
    // For _write_, we want something much shorter.
    sock.set_read_timeout(Some(P::IDLE_TIMEOUT.into()))?;
    sock.set_write_timeout(Some(P::IDLE_TIMEOUT.into()))?;

    let w = sock.try_clone()?;
    let address = sock.peer_addr()?;
    let local_address = sock.local_addr()?;

    Ok((
        Reader::from(sock, local_address, address, events_tx),
        Writer { raw: w, address },
    ))
}
