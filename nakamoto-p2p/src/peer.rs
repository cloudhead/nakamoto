//! Peer protocol.
//!
//! The steps for an *outbound* handshake are:
//!
//!   1. Send "version" message.
//!   2. Expect "version" message from remote.
//!   3. Expect "verack" message from remote.
//!   4. Send "verack" message.
//!
//! The steps for an *inbound* handshake are:
//!
//!   1. Expect "version" message from remote.
//!   2. Send "version" message.
//!   3. Send "verack" message.
//!   4. Expect "verack" message from remote.
//!
use std::io::{Read, Write};
use std::net;
use std::sync::{mpsc, Arc, RwLock};
use std::time::{self, SystemTime, UNIX_EPOCH};

use log::*;

use bitcoin::consensus::encode::Encodable;
use bitcoin::hash_types::BlockHash;
use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::GetHeadersMessage;
use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::stream_reader::StreamReader;
use bitcoin::util::hash::BitcoinHash;

use nakamoto_chain::block::{tree::BlockTree, Height};

use crate::error::Error;

pub mod network;
pub use network::Network;

/// Peer-to-peer protocol version.
pub const PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &str = "/nakamoto:0.0.0/";
/// Maximum peer-to-peer message size.
pub const MAX_MESSAGE_SIZE: usize = 6 * 1024;
/// Duration of inactivity before timing out a peer.
pub const IDLE_TIMEOUT: time::Duration = time::Duration::from_secs(60 * 5);
/// Duration of inactivity before timing out a peer during handshake.
pub const HANDSHAKE_TIMEOUT: time::Duration = time::Duration::from_secs(30);
/// How long to wait between sending pings.
pub const PING_INTERVAL: time::Duration = time::Duration::from_secs(60);

/// A time offset, in seconds.
pub type TimeOffset = i64;

/// Peer event, used to notify peer manager.
#[derive(Debug)]
pub enum Event {
    Connected(net::SocketAddr),
    Handshaked(net::SocketAddr),
    Disconnected(net::SocketAddr),
}

/// Peer config.
#[derive(Debug, Copy, Clone)]
pub struct Config {
    pub network: network::Network,
    pub services: ServiceFlags,
    pub protocol_version: u32,
    pub relay: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: network::Network::Mainnet,
            services: ServiceFlags::NONE,
            protocol_version: PROTOCOL_VERSION,
            relay: false,
        }
    }
}

impl Config {
    pub fn port(&self) -> u16 {
        self.network.port()
    }
}

/// Peer connection state.
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub enum State {
    /// The peer is connected, but no handshake has taken place.
    Connected,
    /// Received the remote "version".
    VersionReceived,
    /// Received the remote "verack".
    VerackReceived,
    /// The peer handshake has been successful.
    Ready,
    /// We are waiting for headers from the peer.
    ExpectingHeaders,
    /// We are listening for any non-requested message from the peer.
    Listening,
    /// The peer has disconnected. If this was caused by an error, the error is included.
    Disconnected,
}

/// Link direction of the peer connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
    /// Inbound conneciton.
    Inbound,
    /// Outbound connection.
    Outbound,
}

/// A peer connection.
#[derive(Debug)]
pub struct Connection<R: Read> {
    /// Remote peer address.
    pub address: net::SocketAddr,
    /// Local peer address.
    pub local_address: net::SocketAddr,
    /// Peer configuration.
    pub config: Config,
    /// Peer connection state.
    pub state: State,
    /// The peer's best height.
    pub height: Height,
    /// Whether this is an inbound or outbound peer connection.
    pub link: Link,
    /// An offset in seconds, between this peer's clock and ours.
    /// A positive offset means the peer's clock is ahead of ours.
    pub time_offset: TimeOffset,
    /// Underling peer connection.
    raw: StreamReader<R>,
    /// Last time we heard from this peer.
    last_active: time::Instant,
}

impl<R: Read + Write> Connection<R> {
    /// Create a new peer from a `io::Read` and an address pair.
    pub fn from(
        r: R,
        local_address: net::SocketAddr,
        address: net::SocketAddr,
        link: Link,
        config: Config,
    ) -> Self {
        let raw = StreamReader::new(r, Some(MAX_MESSAGE_SIZE));
        let state = State::Connected;
        let height = 0;
        let last_active = time::Instant::now();
        let time_offset = 0;

        Self {
            height,
            state,
            config,
            raw,
            address,
            local_address,
            time_offset,
            last_active,
            link,
        }
    }

    pub fn run<T: BlockTree>(
        &mut self,
        block_cache: Arc<RwLock<T>>,
        _events: mpsc::Sender<Event>,
    ) -> Result<(), Error> {
        let (tip, height) = {
            let cache = block_cache.read().expect("lock is not poisoned");
            let (hash, _) = cache.tip();

            (hash, cache.height())
        };

        self.init(height)?;

        loop {
            while let Ok(inbound) = self.read() {
                let outbound = self.receive(inbound);

                for msg in outbound.into_iter() {
                    self.write(msg)?;
                }

                if self.is_ready() {
                    debug!("{}: Handshake successful", self.address);
                    self.sync(&[tip], block_cache.clone())?;
                }
            }
        }
    }

    pub fn transition(&mut self, state: State) {
        debug!("{}: {:?} -> {:?}", self.address, self.state, state);
        assert!(state > self.state);

        self.state = state;
    }

    pub fn init(&mut self, height: Height) -> Result<(), Error> {
        if self.link == Link::Outbound {
            self.write(self.version(height))?;
        }
        Ok(())
    }

    pub fn is_ready(&self) -> bool {
        self.state == State::Ready
    }

    pub fn receive(&mut self, msg: NetworkMessage) -> Vec<NetworkMessage> {
        let (state, responses) = match self.link {
            Link::Inbound => match (&self.state, msg) {
                (State::Connected, NetworkMessage::Version(version)) => {
                    self.receive_version(version);
                    (
                        State::VersionReceived,
                        vec![self.version(0), NetworkMessage::Verack],
                    )
                }
                (State::VersionReceived, NetworkMessage::Verack) => (State::Ready, vec![]),
                otherwise => todo!("{:?}", otherwise),
            },
            Link::Outbound => match (&self.state, msg) {
                (State::Connected, NetworkMessage::Version(version)) => {
                    self.receive_version(version);

                    (State::VersionReceived, vec![])
                }
                (State::VersionReceived, NetworkMessage::Verack) => {
                    (State::Ready, vec![NetworkMessage::Verack])
                }
                otherwise => todo!("{:?}", otherwise),
            },
        };
        self.transition(state);

        responses
    }

    fn receive_version(
        &mut self,
        VersionMessage {
            start_height,
            timestamp,
            ..
        }: VersionMessage,
    ) {
        let height = 0;
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        if height > start_height as Height + 1 {
            // We're ahead of this peer by more than one block. Don't use it
            // for IBD.
            todo!();
        }
        self.height = start_height as Height;
        self.time_offset = timestamp - local_time;

        // TODO: Check version
        // TODO: Check services
        // TODO: Check start_height
    }

    fn version(&self, start_height: Height) -> NetworkMessage {
        let start_height = start_height as i32;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        NetworkMessage::Version(VersionMessage {
            version: self.config.protocol_version,
            services: self.config.services,
            timestamp,
            receiver: Address::new(
                &self.address,
                ServiceFlags::NETWORK | ServiceFlags::COMPACT_FILTERS,
            ),
            sender: Address::new(&self.local_address, ServiceFlags::NONE),
            nonce: 0,
            user_agent: USER_AGENT.to_owned(),
            start_height,
            relay: self.config.relay,
        })
    }

    pub fn write(&mut self, msg: NetworkMessage) -> Result<(), Error> {
        let mut buf = [0u8; MAX_MESSAGE_SIZE];
        let msg = RawNetworkMessage {
            magic: self.config.network.magic(),
            payload: msg,
        };

        match msg.consensus_encode(&mut buf[..]) {
            Ok(len) => {
                debug!(
                    "{}: Sending {:?} message ({} bytes)",
                    self.address,
                    msg.cmd(),
                    len,
                );
                trace!("{}: {:#?}", self.address, msg);

                self.raw.stream.write_all(&buf[..len])?;
                self.raw.stream.flush()?;

                Ok(())
            }
            Err(err) => panic!(err.to_string()),
        }
    }

    pub fn read(&mut self) -> Result<NetworkMessage, Error> {
        match self.raw.read_next::<RawNetworkMessage>() {
            Ok(msg) => {
                debug!("{}: Received {:?}", self.address, msg.cmd());
                trace!("{}: {:#?}", self.address, msg);

                self.last_active = time::Instant::now();

                if msg.magic == self.config.network.magic() {
                    Ok(msg.payload)
                } else {
                    todo!()
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn sync<T: BlockTree>(
        &mut self,
        locator_hashes: &[BlockHash],
        tree: Arc<RwLock<T>>,
    ) -> Result<(), Error> {
        if locator_hashes.is_empty() {
            return Ok(());
        }
        let mut locator_hashes = locator_hashes.to_vec();

        loop {
            let get_headers = NetworkMessage::GetHeaders(GetHeadersMessage {
                version: self.config.protocol_version,
                // Starting hashes, highest heights first.
                locator_hashes,
                // Using the zero hash means *fetch as many blocks as possible*.
                stop_hash: BlockHash::default(),
            });
            self.write(get_headers)?;

            match self.read()? {
                NetworkMessage::Headers(headers) => {
                    debug!("{}: Received {} headers", self.address, headers.len());

                    if let (Some(first), Some(last)) = (headers.first(), headers.last()) {
                        trace!(
                            "{}: Range = {}..{}",
                            self.address,
                            first.bitcoin_hash(),
                            last.bitcoin_hash()
                        );
                    } else {
                        info!("{}: Finished synchronizing", self.address);
                        break;
                    }

                    let length = headers.len();

                    match tree
                        .write()
                        .expect("lock has not been poisoned")
                        .import_blocks(headers.into_iter())
                    {
                        Ok((tip, height)) => {
                            self.height = height;
                            locator_hashes = vec![tip];

                            info!("Imported {} headers from {}", length, self.address);
                            info!("Chain height = {}, tip = {}", height, tip);
                            // TODO: We can break here if we've received less than 2'000 headers.
                        }
                        Err(err) => {
                            error!("Error importing headers: {}", err);
                            return Err(Error::from(err));
                        }
                    }
                }
                _ => todo!(),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, Connection, Link};
    use std::io;
    use std::io::prelude::*;

    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    type Queue = VecDeque<Vec<u8>>;
    type Pair = (Socket, Socket);

    struct Socket {
        inbound: Arc<Mutex<Queue>>,
        outbound: Arc<Mutex<Queue>>,
    }

    impl Socket {
        fn pair() -> Pair {
            let left = Arc::new(Mutex::new(Queue::new()));
            let right = Arc::new(Mutex::new(Queue::new()));

            (
                Socket {
                    inbound: left.clone(),
                    outbound: right.clone(),
                },
                Socket {
                    inbound: right,
                    outbound: left,
                },
            )
        }
    }

    struct Peer {
        conn: Connection<Socket>,
    }

    impl Peer {
        fn from(conn: Connection<Socket>) -> Self {
            Self { conn }
        }

        fn init(&mut self, height: super::Height) -> Result<(), super::Error> {
            self.conn.init(height)
        }

        fn tick(&mut self) -> Result<(), super::Error> {
            while let Ok(inbound) = self.conn.read() {
                let outbound = self.conn.receive(inbound);

                for msg in outbound.into_iter() {
                    self.conn.write(msg)?;
                }
            }
            Ok(())
        }
    }

    impl io::Read for Socket {
        fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
            if let Some(msg) = self.inbound.lock().unwrap().pop_front() {
                buf.write_all(msg.as_slice())?;
                Ok(msg.len())
            } else {
                Ok(0)
            }
        }
    }

    impl io::Write for Socket {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.outbound.lock().unwrap().push_back(buf.to_vec());
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_handshake() -> Result<(), Box<dyn std::error::Error>> {
        let cfg = Config::default();
        let (a, b) = Socket::pair();

        let a_addr = ([192, 168, 1, 2], 8333).into();
        let b_addr = ([192, 168, 1, 3], 8333).into();

        let mut alice = Peer::from(Connection::from(a, a_addr, b_addr, Link::Outbound, cfg));
        let mut bob = Peer::from(Connection::from(b, b_addr, a_addr, Link::Inbound, cfg));

        alice.init(0)?;
        bob.init(0)?;

        while !(alice.conn.is_ready() && bob.conn.is_ready()) {
            alice.tick()?;
            bob.tick()?;
        }
        Ok(())
    }
}
