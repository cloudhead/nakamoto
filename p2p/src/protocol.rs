//! Bitcoin protocol state machine.
#![warn(missing_docs)]
use crossbeam_channel as chan;
use log::*;

pub mod addrmgr;
pub mod channel;
pub mod connmgr;
pub mod peermgr;
pub mod pingmgr;
pub mod spvmgr;
pub mod syncmgr;

#[cfg(test)]
mod tests;

use addrmgr::AddressManager;
use channel::Channel;
use connmgr::ConnectionManager;
use peermgr::PeerManager;
use pingmgr::PingManager;
use spvmgr::SpvManager;
use syncmgr::SyncManager;

use crate::address_book::AddressBook;
use crate::event::Event;

use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::net;
use std::ops::Range;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::{GetBlocksMessage, GetHeadersMessage};

use nakamoto_common::block::filter::Filters;
use nakamoto_common::block::time::{AdjustedTime, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{self, BlockTree, ImportResult};
use nakamoto_common::block::Transaction;
use nakamoto_common::block::{BlockHash, Height};
use nakamoto_common::network::{self, Network};

/// Peer-to-peer protocol version.
/// For now, we only support `70012`, due to lacking `sendcmpct` support.
pub const PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &str = "/nakamoto:0.0.0/";

/// Block locators. Consists of starting hashes and a stop hash.
type Locators = (Vec<BlockHash>, BlockHash);

/// Upstream communication channel. The protocol interacts with the peer network via this channel.
type Upstream = Channel;

/// Identifies a peer.
pub type PeerId = net::SocketAddr;

/// A timeout.
pub type Timeout = LocalDuration;

/// Link direction of the peer connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
    /// Inbound conneciton.
    Inbound,
    /// Outbound connection.
    Outbound,
}

impl Link {
    /// Check whether the link is outbound.
    pub fn is_outbound(&self) -> bool {
        *self == Link::Outbound
    }
}

/// A command or request that can be sent to the protocol.
#[derive(Debug, Clone)]
pub enum Command {
    /// Get the tip of the active chain.
    GetTip(chan::Sender<BlockHeader>),
    /// Get a block from the active chain.
    GetBlock(BlockHash),
    /// Get block filters.
    GetFilters(Range<Height>),
    /// Broadcast to outbound peers.
    Broadcast(NetworkMessage),
    /// Send a message to a random peer.
    Query(NetworkMessage, chan::Sender<Option<net::SocketAddr>>),
    /// Connect to a peer.
    Connect(net::SocketAddr),
    /// Disconnect from a peer.
    Disconnect(net::SocketAddr),
    /// Import headers directly into the block store.
    ImportHeaders(
        Vec<BlockHeader>,
        chan::Sender<Result<ImportResult, tree::Error>>,
    ),
    /// Submit a transaction to the network.
    SubmitTransaction(Transaction),
    /// Shutdown the protocol.
    Shutdown,
}

/// A protocol input event, parametrized over the network message type.
/// These are input events generated outside of the protocol.
#[derive(Debug, Clone)]
pub enum Input {
    /// New connection with a peer.
    Connected {
        /// Remote peer id.
        addr: PeerId,
        /// Local peer id.
        local_addr: PeerId,
        /// Link direction.
        link: Link,
    },
    /// Disconnected from peer.
    Disconnected(PeerId),
    /// Received a message from a remote peer.
    Received(PeerId, RawNetworkMessage),
    /// Sent a message to a remote peer, of the given size.
    Sent(PeerId, usize),
    /// An external command has been received.
    Command(Command),
    /// A timeout has been reached.
    Timeout,
}

/// Output of a state transition (step) of the `Protocol` state machine.
#[derive(Debug)]
pub enum Out {
    /// Send a message to a peer.
    Message(PeerId, RawNetworkMessage),
    /// Connect to a peer.
    Connect(PeerId, Timeout),
    /// Disconnect from a peer.
    Disconnect(PeerId, DisconnectReason),
    /// Set a timeout.
    SetTimeout(Timeout),
    /// An event has occured.
    Event(Event),
    /// Shutdown protocol.
    Shutdown,
}

impl From<Event> for Out {
    fn from(event: Event) -> Self {
        Out::Event(event)
    }
}

/// Disconnect reason.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DisconnectReason {
    /// Peer is misbehaving.
    PeerMisbehaving,
    /// Peer protocol version is too old or too recent.
    PeerProtocolVersion(u32),
    /// Peer doesn't have the required services.
    PeerServices(ServiceFlags),
    /// Peer chain is too far behind.
    PeerHeight(Height),
    /// Peer magic is invalid.
    PeerMagic(u32),
    /// Peer timed out.
    PeerTimeout,
    /// Connection to self was detected.
    SelfConnection,
    /// Inbound connection limit reached.
    ConnectionLimit,
    /// Peer was forced to disconnect by external command.
    Command,
}

impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerMisbehaving => write!(f, "peer misbehaving"),
            Self::PeerProtocolVersion(_) => write!(f, "peer protocol version mismatch"),
            Self::PeerServices(_) => write!(f, "peer doesn't have the required services"),
            Self::PeerHeight(_) => write!(f, "peer is too far behind"),
            Self::PeerMagic(magic) => write!(f, "received message with invalid magic: {}", magic),
            Self::PeerTimeout => write!(f, "peer timed out"),
            Self::SelfConnection => write!(f, "detected self-connection"),
            Self::ConnectionLimit => write!(f, "inbound connection limit reached"),
            Self::Command => write!(f, "received external command"),
        }
    }
}

mod message {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct Builder {
        magic: u32,
    }

    impl Builder {
        pub fn new(network: Network) -> Self {
            Builder {
                magic: network.magic(),
            }
        }

        pub fn message(&self, addr: net::SocketAddr, payload: NetworkMessage) -> Out {
            Out::Message(addr, self.raw(payload))
        }

        pub fn raw(&self, payload: NetworkMessage) -> RawNetworkMessage {
            RawNetworkMessage {
                payload,
                magic: self.magic,
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////

/// An instantiation of `Protocol`, for the Bitcoin P2P network. Parametrized over the
/// block-tree and compact filter store.
#[derive(Debug)]
pub struct Protocol<T, F> {
    /// Bitcoin network we're connecting to.
    network: network::Network,
    /// Our protocol version.
    protocol_version: u32,
    /// Consensus parameters.
    params: Params,
    /// Peer whitelist.
    whitelist: Whitelist,
    /// Peer address manager.
    addrmgr: AddressManager<Upstream>,
    /// Blockchain synchronization manager.
    syncmgr: SyncManager<T, Upstream>,
    /// Peer connection manager.
    connmgr: ConnectionManager<Upstream>,
    /// Ping manager.
    pingmgr: PingManager<Upstream>,
    /// SPV (Simply Payment Verification) manager.
    spvmgr: SpvManager<F, Upstream>,
    /// Peer manager.
    peermgr: PeerManager<Upstream>,
    /// Network-adjusted clock.
    clock: AdjustedTime<PeerId>,
    /// Informational name of this protocol instance. Used for logging purposes only.
    target: &'static str,
    /// Last time a "tick" was triggered.
    last_tick: LocalTime,
    /// Random number generator.
    rng: fastrand::Rng,
    /// Outbound channel. Used to communicate protocol events with a reactor.
    upstream: Upstream,
}

/// Protocol builder. Consume to build a new protocol instance.
#[derive(Clone)]
pub struct Builder<T, F> {
    /// Block cache.
    pub cache: T,
    /// Filter cache.
    pub filters: F,
    /// Clock.
    pub clock: AdjustedTime<PeerId>,
    /// RNG.
    pub rng: fastrand::Rng,
    /// Configuration.
    pub cfg: Config,
}

impl<T: BlockTree, F: Filters> Builder<T, F> {
    /// TODO
    pub fn build(self, upstream: chan::Sender<Out>) -> Protocol<T, F> {
        Protocol::new(
            self.cache,
            self.filters,
            self.clock,
            self.rng,
            self.cfg,
            upstream,
        )
    }
}

/// Protocol configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Bitcoin network we are connected to.
    pub network: network::Network,
    /// Addresses of peers to connect to.
    pub address_book: AddressBook,
    /// Services offered by our peer.
    pub services: ServiceFlags,
    /// Peer whitelist. Peers in this list are trusted by default.
    pub whitelist: Whitelist,
    /// Consensus parameters.
    pub params: Params,
    /// Our protocol version.
    pub protocol_version: u32,
    /// Our user agent.
    pub user_agent: &'static str,
    /// Target outbound peer connections.
    pub target_outbound_peers: usize,
    /// Maximum inbound peer connections.
    pub max_inbound_peers: usize,
    /// Log target.
    pub target: &'static str,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: network::Network::Mainnet,
            params: Params::new(network::Network::Mainnet.into()),
            address_book: AddressBook::default(),
            services: ServiceFlags::NONE,
            whitelist: Whitelist::default(),
            protocol_version: PROTOCOL_VERSION,
            target_outbound_peers: connmgr::TARGET_OUTBOUND_PEERS,
            max_inbound_peers: connmgr::MAX_INBOUND_PEERS,
            user_agent: USER_AGENT,
            target: "self",
        }
    }
}

impl Config {
    /// Construct a new configuration.
    pub fn from(
        target: &'static str,
        network: network::Network,
        address_book: AddressBook,
    ) -> Self {
        let params = Params::new(network.into());

        Self {
            network,
            address_book,
            target,
            params,
            ..Self::default()
        }
    }

    /// Get the listen port.
    pub fn port(&self) -> u16 {
        self.network.port()
    }
}

/// Peer whitelist.
#[derive(Debug, Clone)]
pub struct Whitelist {
    /// Trusted addresses.
    addr: HashSet<net::IpAddr>,
    /// Trusted user-agents.
    user_agent: HashSet<String>,
}

impl Default for Whitelist {
    fn default() -> Self {
        Whitelist {
            addr: HashSet::new(),
            user_agent: HashSet::new(),
        }
    }
}

impl Whitelist {
    fn contains(&self, addr: &net::IpAddr, user_agent: &str) -> bool {
        self.addr.contains(addr) || self.user_agent.contains(user_agent)
    }
}

impl<T: BlockTree, F: Filters> Protocol<T, F> {
    /// Construct a new protocol instance.
    pub fn new(
        tree: T,
        filters: F,
        clock: AdjustedTime<PeerId>,
        rng: fastrand::Rng,
        config: Config,
        upstream: chan::Sender<Out>,
    ) -> Self {
        let Config {
            network,
            address_book,
            services,
            whitelist,
            protocol_version,
            target_outbound_peers,
            max_inbound_peers,
            user_agent,
            target,
            params,
        } = config;

        let upstream = Upstream::new(network, protocol_version, target, upstream);

        let addrmgr = AddressManager::from(address_book, rng.clone(), upstream.clone());
        let syncmgr = SyncManager::new(
            tree,
            syncmgr::Config {
                max_message_headers: syncmgr::MAX_MESSAGE_HEADERS,
                request_timeout: syncmgr::REQUEST_TIMEOUT,
                params: params.clone(),
            },
            rng.clone(),
            upstream.clone(),
        );
        let connmgr = ConnectionManager::new(
            upstream.clone(),
            connmgr::Config {
                target_outbound_peers,
                max_inbound_peers,
            },
        );
        let pingmgr = PingManager::new(rng.clone(), upstream.clone());
        let spvmgr = SpvManager::new(
            spvmgr::Config::default(),
            rng.clone(),
            filters,
            upstream.clone(),
        );
        let peermgr = PeerManager::new(
            peermgr::Config {
                protocol_version: PROTOCOL_VERSION,
                whitelist: whitelist.clone(),
                services,
                user_agent,
            },
            rng.clone(),
            upstream.clone(),
        );

        Self {
            network,
            protocol_version,
            whitelist,
            target,
            params,
            clock,
            addrmgr,
            syncmgr,
            connmgr,
            pingmgr,
            spvmgr,
            peermgr,
            last_tick: LocalTime::default(),
            rng,
            upstream,
        }
    }

    /// Initialize the protocol. Called once before any event is sent to the state machine.
    pub fn initialize(&mut self, time: LocalTime) {
        self.clock.set_local_time(time);
        self.syncmgr.initialize(time);
        self.connmgr.initialize(time, &mut self.addrmgr);
        self.spvmgr.initialize(time, &self.syncmgr.tree);
    }

    /// Process the next input and advance the state machine by one step.
    pub fn step(&mut self, input: Input, local_time: LocalTime) {
        self.tick(local_time);

        match input {
            Input::Connected {
                addr,
                local_addr,
                link,
            } => {
                let height = self.syncmgr.tree.height();
                // This is usually not that useful, except when our local address is actually the
                // address our peers see.
                self.addrmgr.record_local_addr(local_addr);
                self.addrmgr.peer_connected(&addr, local_time);
                self.connmgr
                    .peer_connected(addr, local_addr, link, local_time);
                self.peermgr
                    .peer_connected(addr, local_addr, link, height, local_time);
            }
            Input::Disconnected(addr) => {
                self.spvmgr.peer_disconnected(&addr);
                self.syncmgr.peer_disconnected(&addr);
                self.addrmgr.peer_disconnected(&addr);
                self.connmgr.peer_disconnected(&addr, &self.addrmgr);
                self.pingmgr.peer_disconnected(&addr);
                self.peermgr.peer_disconnected(&addr);
            }
            Input::Received(addr, msg) => {
                self.upstream
                    .event(Event::Received(addr, msg.payload.clone()));
                self.receive(addr, msg);
            }
            Input::Sent(_addr, _msg) => {}
            Input::Command(cmd) => match cmd {
                Command::Connect(addr) => {
                    debug!(target: self.target, "Received command: Connect({})", addr);

                    self.whitelist.addr.insert(addr.ip());
                    self.connmgr.connect(&addr, &mut self.addrmgr, local_time);
                }
                Command::Disconnect(addr) => {
                    debug!(target: self.target, "Received command: Disconnect({})", addr);

                    self.disconnect(addr, DisconnectReason::Command);
                }
                Command::Query(msg, reply) => {
                    debug!(target: self.target, "Received command: Query({:?})", msg);

                    reply.send(self.query(msg)).ok();
                }
                Command::Broadcast(msg) => {
                    debug!(target: self.target, "Received command: Broadcast({:?})", msg);

                    for peer in self.peermgr.outbound() {
                        self.upstream.message(peer.address(), msg.clone());
                    }
                }
                Command::ImportHeaders(headers, reply) => {
                    debug!(target: self.target, "Received command: ImportHeaders(..)");

                    let result = self.syncmgr.import_blocks(headers.into_iter(), &self.clock);

                    match result {
                        Ok(import_result) => {
                            reply.send(Ok(import_result.clone())).ok();
                        }
                        Err(err) => {
                            reply.send(Err(err)).ok();
                        }
                    }
                }
                Command::GetTip(_) => todo!(),
                Command::GetFilters(range) => {
                    debug!(target: self.target,
                        "Received command: GetFilters({}..{})", range.start, range.end);

                    self.spvmgr.get_cfilters(range, &self.syncmgr.tree);
                }
                Command::GetBlock(hash) => {
                    self.query(NetworkMessage::GetBlocks(GetBlocksMessage {
                        locator_hashes: vec![],
                        stop_hash: hash,
                        version: self.protocol_version,
                    }));
                }
                Command::SubmitTransaction(tx) => {
                    debug!(target: self.target, "Received command: SubmitTransaction(..)");

                    self.query(NetworkMessage::Tx(tx));
                }
                Command::Shutdown => {
                    self.upstream.push(Out::Shutdown);
                }
            },
            Input::Timeout => {
                trace!(target: self.target, "Received timeout");

                self.connmgr.received_timeout(local_time, &self.addrmgr);
                self.syncmgr.received_timeout(local_time);
                self.pingmgr.received_timeout(local_time);
                self.addrmgr.received_timeout(local_time);
                self.peermgr.received_timeout(local_time);
                self.spvmgr.received_timeout(local_time, &self.syncmgr.tree);
            }
        };
    }

    /// Send a message to a random peer. Returns the peer id.
    fn query(&self, msg: NetworkMessage) -> Option<PeerId> {
        let peers = self.peermgr.outbound().collect::<Vec<_>>();

        match peers.len() {
            n if n > 0 => {
                let r = self.rng.usize(..n);
                let p = peers.get(r).unwrap();

                self.upstream.message(p.address(), msg);

                Some(p.address())
            }
            _ => None,
        }
    }

    fn tick(&mut self, local_time: LocalTime) {
        // The local time is set from outside the protocol.
        self.clock.set_local_time(local_time);

        if local_time - self.last_tick >= LocalDuration::from_secs(30) {
            let (tip, _) = self.syncmgr.tree.tip();
            let height = self.syncmgr.tree.height();
            let best = self.syncmgr.best_height();
            let sync = if best > 0 {
                height as f64 / best as f64 * 100.
            } else {
                0.
            };
            let peers = self.connmgr.outbound_peers().count();
            let target = self.connmgr.config.target_outbound_peers;

            // TODO: Add cache sizes on disk
            // TODO: Add protocol state(s)
            // TODO: Trim block hash
            // TODO: Add average headers/s or bandwidth

            log::info!(
                "tip = {tip}, height = {height}/{best} ({sync:.1}%), outbound = {peers}/{target}",
                tip = tip,
                height = height,
                best = best,
                sync = sync,
                peers = peers,
                target = target,
            );

            self.last_tick = local_time;
        }
    }

    fn receive(&mut self, addr: PeerId, msg: RawNetworkMessage) {
        let now = self.clock.local_time();
        let cmd = msg.cmd();

        if msg.magic != self.network.magic() {
            // TODO: Needs test.
            return self.disconnect(addr, DisconnectReason::PeerMagic(msg.magic));
        }

        if !self.peermgr.is_connected(&addr) {
            debug!(target: self.target, "Received {:?} from unknown peer {}", cmd, addr);
            return;
        };

        debug!(
            target: self.target, "{}: Received {:?}",
            addr, cmd
        );

        match msg.payload {
            NetworkMessage::Version(msg) => {
                let height = self.syncmgr.tree.height();

                self.peermgr
                    .received_version(&addr, msg, height, now, &mut self.addrmgr);
            }
            NetworkMessage::Verack => {
                if let Some(peer) = self.peermgr.received_verack(&addr, now) {
                    self.clock.record_offset(peer.address(), peer.time_offset);
                    self.addrmgr
                        .peer_negotiated(&addr, peer.services, peer.conn.link, now);
                    self.pingmgr.peer_negotiated(peer.address(), now);
                    self.spvmgr.peer_negotiated(
                        peer.address(),
                        peer.height,
                        peer.services,
                        peer.conn.link,
                        &self.clock,
                        &self.syncmgr.tree,
                    );
                    self.syncmgr.peer_negotiated(
                        peer.address(),
                        peer.height,
                        peer.services,
                        peer.conn.link,
                        &self.clock,
                    );
                }
            }
            NetworkMessage::Ping(nonce) => {
                self.pingmgr.received_ping(addr, nonce);
            }
            NetworkMessage::Pong(nonce) => {
                self.pingmgr.received_pong(addr, nonce, now);
            }
            NetworkMessage::Headers(headers) => {
                match self.syncmgr.received_headers(&addr, headers, &self.clock) {
                    Err(e) => log::error!("Error receiving headers: {}", e),
                    Ok(ImportResult::TipChanged(_, _, reverted)) if !reverted.is_empty() => {
                        // By rolling back the filter headers, we will trigger
                        // a re-download of the missing headers, which should result
                        // in us having the new headers.
                        self.spvmgr.rollback(reverted.len()).unwrap()
                    }
                    Ok(ImportResult::TipChanged(_, _, _)) => {
                        // Trigger an idle check, since we're going to have to catch up
                        // on the new block header(s). This is not required, but reduces
                        // latency.
                        self.spvmgr.idle(now, &self.syncmgr.tree);
                    }
                    _ => {}
                }
            }
            NetworkMessage::GetHeaders(GetHeadersMessage {
                locator_hashes,
                stop_hash,
                ..
            }) => {
                self.syncmgr
                    .received_getheaders(&addr, (locator_hashes, stop_hash));
            }
            NetworkMessage::Inv(inventory) => {
                // Receive an `inv` message. This will happen if we are out of sync with a
                // peer. And blocks are being announced. Otherwise, we expect to receive a
                // `headers` message.
                self.syncmgr.received_inv(addr, inventory, &self.clock);
            }
            NetworkMessage::CFHeaders(msg) => {
                self.spvmgr
                    .received_cfheaders(&addr, msg, &self.syncmgr.tree)
                    .unwrap();
            }
            NetworkMessage::GetCFHeaders(msg) => {
                self.spvmgr
                    .received_getcfheaders(&addr, msg, &self.syncmgr.tree)
                    .unwrap();
            }
            NetworkMessage::CFilter(msg) => {
                self.spvmgr
                    .received_cfilter(&addr, msg, &self.syncmgr.tree)
                    .unwrap();
            }
            NetworkMessage::GetCFilters(msg) => {
                self.spvmgr
                    .received_getcfilters(&addr, msg, &self.syncmgr.tree);
            }
            NetworkMessage::Addr(addrs) => {
                if addrs.is_empty() {
                    // Peer misbehaving, got empty message.
                    return;
                }
                self.addrmgr
                    .insert(addrs.into_iter(), addrmgr::Source::Peer(addr));
            }
            NetworkMessage::GetAddr => {
                self.addrmgr.received_getaddr(&addr);
            }
            _ => {
                debug!(target: self.target, "{}: Ignoring {:?}", addr, cmd);
            }
        }
    }

    fn disconnect(&mut self, addr: PeerId, reason: DisconnectReason) {
        debug!(target: self.target, "{}: Peer disconnecting..", addr);

        // TODO: Trigger disconnection everywhere, as if peer disconnected. This
        // avoids being in a state where we know a peer is about to get disconnected,
        // but we still process messages from it as normal.

        self.connmgr.disconnect(addr, reason);
    }
}
