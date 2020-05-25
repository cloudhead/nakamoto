use std::io::{Read, Write};
use std::net;
use std::ops;
use std::sync::{Arc, RwLock};

use log::*;

use bitcoin::consensus::encode::Encodable;
use bitcoin::hash_types::BlockHash;
use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::GetHeadersMessage;
use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::stream_reader::StreamReader;

use bitcoin_hashes::sha256d;
use nakamoto_chain::blocktree::{BlockTree, Blockchain};

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
    /// Private simulation network, used for development.
    Simnet,
}

impl Network {
    pub fn genesis_hash(&self) -> BlockHash {
        use bitcoin_hashes::Hash;

        let hash: &[u8; 32] = match self {
            #[rustfmt::skip]
            Self::Mainnet => &[
                0x6f, 0xe2, 0x8c, 0x0a, 0xb6, 0xf1, 0xb3, 0x72,
                0xc1, 0xa6, 0xa2, 0x46, 0xae, 0x63, 0xf7, 0x4f,
                0x93, 0x1e, 0x83, 0x65, 0xe1, 0x5a, 0x08, 0x9c,
                0x68, 0xd6, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00,
            ],
            #[rustfmt::skip]
            Self::Testnet => &[
                0x43, 0x49, 0x7f, 0xd7, 0xf8, 0x26, 0x95, 0x71,
                0x08, 0xf4, 0xa3, 0x0f, 0xd9, 0xce, 0xc3, 0xae,
                0xba, 0x79, 0x97, 0x20, 0x84, 0xe9, 0x0e, 0xad,
                0x01, 0xea, 0x33, 0x09, 0x00, 0x00, 0x00, 0x00,
            ],
            #[rustfmt::skip]
            Self::Simnet => &[
                0xf6, 0x7a, 0xd7, 0x69, 0x5d, 0x9b, 0x66, 0x2a,
                0x72, 0xff, 0x3d, 0x8e, 0xdb, 0xbb, 0x2d, 0xe0,
                0xbf, 0xa6, 0x7b, 0x13, 0x97, 0x4b, 0xb9, 0x91,
                0x0d, 0x11, 0x6d, 0x5c, 0xbd, 0x86, 0x3e, 0x68,
            ],
        };
        BlockHash::from(
            sha256d::Hash::from_slice(hash)
                .expect("the genesis hash has the right number of bytes"),
        )
    }

    fn magic(&self) -> u32 {
        match self {
            Self::Mainnet => bitcoin::Network::Bitcoin.magic(),
            Self::Testnet => bitcoin::Network::Testnet.magic(),
            Self::Simnet => 0x12141c16,
        }
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
            network: Network::Simnet,
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
            Network::Simnet => 18555,
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
    pub fn connect(addr: &str, config: &Config) -> Result<Self, Error> {
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
    ///     1. Send "version" message.
    ///     2. Expect "version" message.
    ///     3. Expect "verack" message.
    ///     4. Send "verack" message.
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
            Err(_) => todo!(),
        }
    }

    pub fn sync<T: BlockTree>(
        &mut self,
        range: ops::Range<usize>,
        tree: Arc<RwLock<T>>,
    ) -> Result<(), Error> {
        loop {
            let get_headers = NetworkMessage::GetHeaders(GetHeadersMessage {
                version: self.config.protocol_version,
                // Starting hashes, highest heights first.
                locator_hashes: vec![self.config.network.genesis_hash()],
                // Using the zero hash means *fetch as many blocks as possible*.
                stop_hash: BlockHash::default(),
            });
            self.write(get_headers)?;

            match self.read()? {
                NetworkMessage::Headers(headers) => {
                    debug!("Received {} headers from {}", headers.len(), self.address);

                    // TODO
                    // Partially validate these block headers by ensuring that all fields follow
                    // consensus rules and that the hash of the header is below the target
                    // threshold according to the nBits field.

                    let chain = Blockchain::try_from(headers)?;
                    let length = chain.len();
                    let mut tree = tree.write().expect("lock has not been poisoned");

                    tree.insert_chain(chain);

                    info!("Imported {} blocks from {}", length, self.address);
                    info!("Chain height = {}, tip = {}", tree.height(), tree.tip());
                }
                _ => todo!(),
            }
            break;
        }
        Ok(())
    }
}
