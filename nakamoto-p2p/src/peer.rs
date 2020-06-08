use std::io::{Read, Write};
use std::net;
use std::sync::{mpsc, Arc, RwLock};
use std::time;

use log::*;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::encode::Encodable;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;
use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::GetHeadersMessage;
use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::stream_reader::StreamReader;
use bitcoin::util::hash::BitcoinHash;

use bitcoin_hashes::sha256d;
use nakamoto_chain::block::{tree::BlockTree, Height};

use crate::error::Error;

/// Peer-to-peer protocol version.
pub const PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &'static str = "/nakamoto:0.0.0/";
/// Maximum peer-to-peer message size.
pub const MAX_MESSAGE_SIZE: usize = 6 * 1024;
/// Duration of inactivity before timing out a peer.
pub const IDLE_TIMEOUT: time::Duration = time::Duration::from_secs(60 * 5);
/// Duration of inactivity before timing out a peer during handshake.
pub const HANDSHAKE_TIMEOUT: time::Duration = time::Duration::from_secs(30);
/// How long to wait between sending pings.
pub const PING_INTERVAL: time::Duration = time::Duration::from_secs(60);

/// Bitcoin network.
#[derive(Debug, Copy, Clone)]
pub enum Network {
    /// Bitcoin Mainnet.
    Mainnet,
    /// Bitcoin Testnet.
    Testnet,
    /// Bitcoin regression test net.
    Regtest,
}

impl From<Network> for bitcoin::Network {
    fn from(value: Network) -> Self {
        match value {
            Network::Mainnet => Self::Bitcoin,
            Network::Testnet => Self::Testnet,
            Network::Regtest => Self::Regtest,
        }
    }
}

impl Network {
    /// ```
    /// use nakamoto_p2p::peer::Network;
    /// use bitcoin::util::hash::BitcoinHash;
    ///
    /// let network = Network::Mainnet;
    /// let genesis = network.genesis();
    ///
    /// assert_eq!(network.genesis_hash(), genesis.bitcoin_hash());
    /// ```
    pub fn genesis(&self) -> BlockHeader {
        use bitcoin::blockdata::constants;

        constants::genesis_block((*self).into()).header
    }

    pub fn genesis_hash(&self) -> BlockHash {
        use bitcoin_hashes::Hash;
        use nakamoto_chain::genesis;

        let hash = match self {
            Self::Mainnet => genesis::MAINNET,
            Self::Testnet => genesis::TESTNET,
            Self::Regtest => genesis::REGTEST,
        };
        BlockHash::from(
            sha256d::Hash::from_slice(hash)
                .expect("the genesis hash has the right number of bytes"),
        )
    }

    pub fn params(&self) -> Params {
        Params::new((*self).into())
    }

    fn magic(&self) -> u32 {
        bitcoin::Network::from(*self).magic()
    }
}

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
    pub network: Network,
    pub services: ServiceFlags,
    pub protocol_version: u32,
    pub relay: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: Network::Mainnet,
            services: ServiceFlags::NONE,
            protocol_version: PROTOCOL_VERSION,
            relay: false,
        }
    }
}

impl Config {
    pub fn port(&self) -> u16 {
        match self.network {
            Network::Mainnet => 8333,
            Network::Testnet => 18333,
            Network::Regtest => 18334,
        }
    }
}

/// Peer connection state.
#[derive(Debug)]
pub enum State {
    /// The peer is connected, but no handshake has taken place.
    Connected,
    /// The peer handshake has been successful.
    Handshaked,
    /// The peer has disconnected. If this was caused by an error, the error is included.
    Disconnected(Option<Error>),
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
        config: Config,
    ) -> Self {
        let raw = StreamReader::new(r, Some(MAX_MESSAGE_SIZE));
        let state = State::Connected;
        let height = 0;
        let last_active = time::Instant::now();

        Self {
            height,
            state,
            config,
            raw,
            address,
            local_address,
            last_active,
        }
    }

    pub fn run<T: BlockTree>(
        &mut self,
        block_cache: Arc<RwLock<T>>,
        _events: mpsc::Sender<Event>,
    ) -> Result<(), Error> {
        let (tip, height) = {
            let cache = block_cache.read().expect("lock is not poisoned");
            (*cache.tip(), cache.height())
        };

        self.handshake(height)?;
        self.sync(&[tip], block_cache)?;

        Ok(())
    }

    /// Establish a peer handshake. This must be called as soon as the peer is connected.
    ///
    /// The steps are:
    ///
    ///   1. Send "version" message.
    ///   2. Expect "version" message.
    ///   3. Expect "verack" message.
    ///   4. Send "verack" message.
    ///
    pub fn handshake(&mut self, height: Height) -> Result<(), Error> {
        use std::time::*;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let start_height = height as i32;

        self.write(NetworkMessage::Version(VersionMessage {
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
        }))?;

        match self.read()? {
            NetworkMessage::Version(VersionMessage { start_height, .. }) => {
                if height > start_height as Height + 1 {
                    // We're ahead of this peer by more than one block, which means
                    // we won't be getting much from it. Abort.
                    todo!();
                }
                self.height = start_height as Height;

                // TODO: Check version
                // TODO: Check services
                // TODO: Check start_height
            }
            _ => todo!(),
        }

        match self.read()? {
            NetworkMessage::Verack => {}
            _ => todo!(),
        }

        self.write(NetworkMessage::Verack)?;
        self.state = State::Handshaked;

        debug!("{}: Handshake successful", self.address);

        Ok(())
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
            Err(err) => panic!(err.to_string()),
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
