use std::io::{self, Write};
use std::net;
use std::ops;

use log::*;

use bitcoin::consensus::encode::Encodable;
use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::stream_reader::StreamReader;

pub const PROTOCOL_VERSION: u32 = 70012;
pub const USER_AGENT: &'static str = "/nakamoto:0.0.0/";
pub const MAX_MESSAGE_SIZE: usize = 6 * 1024;

/// Peer config.
#[derive(Debug, Copy, Clone)]
pub struct Config {
    pub network: bitcoin::Network,
    pub services: ServiceFlags,
    pub protocol_version: u32,
    pub relay: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: bitcoin::Network::Testnet,
            services: ServiceFlags::NONE,
            protocol_version: PROTOCOL_VERSION,
            relay: false,
        }
    }
}

impl Config {
    pub fn port(&self) -> u16 {
        match self.network {
            bitcoin::Network::Bitcoin => 8333,
            bitcoin::Network::Testnet => 18333,
            bitcoin::Network::Regtest => unimplemented!(),
        }
    }
}

/// A peer on the network.
#[derive(Debug)]
pub struct Peer {
    /// Remote peer address.
    pub address: net::SocketAddr,
    /// Local peer address.
    pub local_address: net::SocketAddr,
    /// Peer configuration.
    pub config: Config,
    /// Peer connection.
    conn: StreamReader<net::TcpStream>,
}

impl Peer {
    /// Connect to a peer given a remote address.
    pub fn connect(addr: &str, config: &Config) -> io::Result<Self> {
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

    /// Establish a peer handshake. This must be called as soon as the peer is connected.
    ///
    /// The steps are:
    ///
    ///     1. Send "version" message.
    ///     2. Expect "version" message.
    ///     3. Expect "verack" message.
    ///     4. Send "verack" message.
    ///
    pub fn handshake(&mut self, start_height: i32) -> io::Result<()> {
        use std::time::*;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        self.write(RawNetworkMessage {
            magic: self.config.network.magic(),
            payload: NetworkMessage::Version(VersionMessage {
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
            }),
        })?;

        match self.read()? {
            NetworkMessage::Version(VersionMessage { .. }) => {
                // TODO: Check version
                // TODO: Check services
                // TODO: Check start_height
            }
            _ => unimplemented!(),
        }

        match self.read()? {
            NetworkMessage::Verack => {}
            _ => unimplemented!(),
        }

        self.write(RawNetworkMessage {
            magic: self.config.network.magic(),
            payload: NetworkMessage::Verack,
        })?;

        debug!("Handshake with {} successful", self.address);

        Ok(())
    }

    pub fn write(&mut self, msg: RawNetworkMessage) -> io::Result<()> {
        let mut buf = [0u8; MAX_MESSAGE_SIZE];

        match msg.consensus_encode(&mut buf[..]) {
            Ok(len) => {
                debug!(
                    "Sending {:?} message ({} bytes) to {}",
                    msg.cmd(),
                    len,
                    self.address
                );
                trace!("{:#?}", msg);

                self.conn.stream.write_all(&buf[..len])
            }
            Err(_) => unimplemented!(),
        }
    }

    pub fn read(&mut self) -> io::Result<NetworkMessage> {
        match self.conn.read_next::<RawNetworkMessage>() {
            Ok(msg) => {
                debug!("Received {:?} from {}", msg.cmd(), self.address);
                trace!("{:#?}", msg);

                match msg {
                    RawNetworkMessage { magic, payload } => {
                        if magic == self.config.network.magic() {
                            Ok(payload)
                        } else {
                            unimplemented!()
                        }
                    }
                }
            }
            Err(_) => unimplemented!(),
        }
    }

    pub fn sync(&mut self, _range: ops::Range<usize>) -> io::Result<Vec<()>> {
        unimplemented!()
    }
}
