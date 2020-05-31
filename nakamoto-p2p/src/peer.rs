use std::io::{Read, Write};
use std::net;
use std::ops;
use std::sync::{Arc, RwLock};

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
use nakamoto_chain::blocktree::BlockTree;

use crate::error::Error;

/// Peer-to-peer protocol version.
pub const PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &'static str = "/nakamoto:0.0.0/";
/// Maximum peer-to-peer message size.
pub const MAX_MESSAGE_SIZE: usize = 6 * 1024;

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

/// A peer on the network.
#[derive(Debug)]
pub struct Peer<R: Read + Write> {
    /// Remote peer address.
    pub address: net::SocketAddr,
    /// Local peer address.
    pub local_address: net::SocketAddr,
    /// Peer configuration.
    pub config: Config,
    /// Peer connection.
    conn: StreamReader<R>,
}

impl Peer<net::TcpStream> {
    /// Connect to a peer given a remote address.
    pub fn connect(addr: &net::SocketAddr, config: &Config) -> Result<Self, Error> {
        let stream = net::TcpStream::connect(addr)?;
        let address = stream.peer_addr()?;
        let local_address = stream.local_addr()?;
        let config = config.clone();
        let conn = StreamReader::new(stream, Some(MAX_MESSAGE_SIZE));

        Ok(Self {
            config,
            conn,
            address,
            local_address,
        })
    }
}

impl<R: Read + Write> Peer<R> {
    /// Create a new peer from a `io::Read` and an address pair.
    pub fn new(
        r: R,
        local_address: net::SocketAddr,
        address: net::SocketAddr,
        config: &Config,
    ) -> Self {
        let conn = StreamReader::new(r, Some(MAX_MESSAGE_SIZE));
        let config = config.clone();

        Self {
            config,
            conn,
            address,
            local_address,
        }
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
    pub fn handshake(&mut self, start_height: i32) -> Result<(), Error> {
        use std::time::*;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

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
            NetworkMessage::Version(VersionMessage { .. }) => {
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

        debug!("Handshake with {} successful", self.address);

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
                    "Sending {:?} message ({} bytes) to {}",
                    msg.cmd(),
                    len,
                    self.address
                );
                trace!("{:#?}", msg);

                self.conn.stream.write_all(&buf[..len]).map_err(Error::from)
            }
            Err(_) => todo!(),
        }
    }

    pub fn read(&mut self) -> Result<NetworkMessage, Error> {
        match self.conn.read_next::<RawNetworkMessage>() {
            Ok(msg) => {
                debug!("Received {:?} from {}", msg.cmd(), self.address);
                trace!("{:#?}", msg);

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
        _range: ops::Range<usize>,
        tree: Arc<RwLock<T>>,
    ) -> Result<(), Error> {
        loop {
            let tip = tree
                .read()
                .expect("lock has not been poisoned")
                .tip()
                .clone();
            let get_headers = NetworkMessage::GetHeaders(GetHeadersMessage {
                version: self.config.protocol_version,
                // Starting hashes, highest heights first.
                locator_hashes: vec![tip],
                // Using the zero hash means *fetch as many blocks as possible*.
                stop_hash: BlockHash::default(),
            });
            self.write(get_headers)?;

            // TODO: Handle timeout.
            match self.read()? {
                NetworkMessage::Headers(headers) => {
                    debug!("Received {} headers from {}", headers.len(), self.address);

                    if let (Some(first), Some(last)) = (headers.first(), headers.last()) {
                        debug!("Range = {}..{}", first.bitcoin_hash(), last.bitcoin_hash());
                    } else {
                        info!("Finished synchronizing with {}", self.address);
                        break;
                    }

                    let length = headers.len();

                    // TODO: Handle case where we partially import blocks, eg. the first `n`.

                    match tree
                        .write()
                        .expect("lock has not been poisoned")
                        .import_blocks(headers.into_iter())
                    {
                        Ok((tip, height)) => {
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
