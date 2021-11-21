//! Bitcoin protocol state machine.
#![warn(missing_docs)]
use crossbeam_channel as chan;
use log::*;

pub mod addrmgr;
pub mod cbfmgr;
pub mod channel;
pub mod event;
pub mod fees;
pub mod invmgr;
pub mod peermgr;
pub mod pingmgr;
pub mod syncmgr;

#[cfg(test)]
mod tests;

use addrmgr::AddressManager;
use cbfmgr::FilterManager;
use channel::{Channel, Disconnect as _};
use invmgr::InventoryManager;
use peermgr::PeerManager;
use pingmgr::PingManager;
use syncmgr::SyncManager;

use crate::stream;
use crate::traits;

pub use event::Event;

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::ops::{Bound, RangeInclusive};
use std::sync::Arc;
use std::{io, net};

use nakamoto_common::bitcoin::blockdata::block::BlockHeader;
use nakamoto_common::bitcoin::consensus::encode;
use nakamoto_common::bitcoin::consensus::params::Params;
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use nakamoto_common::bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use nakamoto_common::bitcoin::network::message_filter::GetCFilters;
use nakamoto_common::bitcoin::network::message_network::VersionMessage;
use nakamoto_common::bitcoin::network::Address;
use nakamoto_common::bitcoin::Script;

use nakamoto_common::block::filter::Filters;
use nakamoto_common::block::time::{AdjustedTime, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{self, BlockReader, BlockTree, ImportResult};
use nakamoto_common::block::{BlockHash, Height};
use nakamoto_common::block::{BlockTime, Transaction};
use nakamoto_common::network::{self, Network};
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_common::p2p::peer::AddressSource;
use nakamoto_common::p2p::{peer, Domain};

use thiserror::Error;

/// Peer-to-peer protocol version.
pub const PROTOCOL_VERSION: u32 = 70016;
/// Minimum supported peer protocol version.
/// This version includes support for the `sendheaders` feature.
pub const MIN_PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &str = "/nakamoto:0.2.0/";

/// Starting size of peer inbox buffer.
const INBOX_BUFFER_SIZE: usize = 1024 * 64;

/// Block locators. Consists of starting hashes and a stop hash.
type Locators = (Vec<BlockHash>, BlockHash);

/// Upstream communication channel. The protocol interacts with the peer network via this channel.
type Upstream = Channel;

/// Identifies a peer.
pub type PeerId = net::SocketAddr;

/// Reference counting virtual socket.
/// When there are no more references held, this peer can be dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Socket {
    /// Socket address.
    pub addr: net::SocketAddr,
    /// Reference counter.
    refs: Arc<()>,
}

impl Socket {
    /// Create a new virtual socket.
    pub fn new(addr: impl Into<net::SocketAddr>) -> Self {
        Self {
            addr: addr.into(),
            refs: Arc::new(()),
        }
    }

    /// Get the number of references to this virtual socket.
    pub fn refs(&self) -> usize {
        Arc::strong_count(&self.refs)
    }
}

impl From<net::SocketAddr> for Socket {
    fn from(addr: net::SocketAddr) -> Self {
        Self::new(addr)
    }
}

/// A remote peer.
#[derive(Debug, Clone)]
pub struct Peer {
    /// Peer address.
    pub addr: net::SocketAddr,
    /// Local peer address.
    pub local_addr: net::SocketAddr,
    /// Whether this is an inbound or outbound peer connection.
    pub link: Link,
    /// Connected since this time.
    pub since: LocalTime,
    /// The peer's best height.
    pub height: Height,
    /// The peer's services.
    pub services: ServiceFlags,
    /// Peer user agent string.
    pub user_agent: String,
    /// Whether this peer relays transactions.
    pub relay: bool,
}

impl Peer {
    /// Check if this is an outbound peer.
    pub fn is_outbound(&self) -> bool {
        self.link.is_outbound()
    }
}

impl From<(&peermgr::PeerInfo, &peermgr::Connection)> for Peer {
    fn from((peer, conn): (&peermgr::PeerInfo, &peermgr::Connection)) -> Self {
        Self {
            addr: conn.socket.addr,
            local_addr: conn.local_addr,
            link: conn.link,
            since: conn.since,
            height: peer.height,
            services: peer.services,
            user_agent: peer.user_agent.clone(),
            relay: peer.relay,
        }
    }
}

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

    /// Check whether the link is inbound.
    pub fn is_inbound(&self) -> bool {
        *self == Link::Inbound
    }
}

/// A command or request that can be sent to the protocol.
#[derive(Clone)]
pub enum Command {
    /// Get block header at height.
    GetBlockByHeight(Height, chan::Sender<Option<BlockHeader>>),
    /// Get connected peers.
    GetPeers(ServiceFlags, chan::Sender<Vec<Peer>>),
    /// Get the tip of the active chain.
    GetTip(chan::Sender<(Height, BlockHeader)>),
    /// Get a block from the active chain.
    GetBlock(BlockHash),
    /// Get block filters.
    GetFilters(
        RangeInclusive<Height>,
        chan::Sender<Result<(), GetFiltersError>>,
    ),
    /// Rescan the chain for matching scripts and addresses.
    Rescan {
        /// Start scan from this height. If unbounded, start at the current height.
        from: Bound<Height>,
        /// Stop scanning at this height. If unbounded, don't stop scanning.
        to: Bound<Height>,
        /// Scripts to match on.
        watch: Vec<Script>,
    },
    /// Broadcast to peers matching the predicate.
    Broadcast(NetworkMessage, fn(Peer) -> bool, chan::Sender<Vec<PeerId>>),
    /// Send a message to a random peer.
    Query(NetworkMessage, chan::Sender<Option<net::SocketAddr>>),
    /// Query the block tree.
    QueryTree(Arc<dyn Fn(&dyn BlockReader) + Send + Sync>),
    /// Connect to a peer.
    Connect(net::SocketAddr),
    /// Disconnect from a peer.
    Disconnect(net::SocketAddr),
    /// Import headers directly into the block store.
    ImportHeaders(
        Vec<BlockHeader>,
        chan::Sender<Result<ImportResult, tree::Error>>,
    ),
    /// Import addresses into the address book.
    ImportAddresses(Vec<Address>),
    /// Submit a transaction to the network.
    SubmitTransaction(
        Transaction,
        chan::Sender<Result<NonEmpty<PeerId>, CommandError>>,
    ),
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GetBlockByHeight(height, _) => write!(f, "GetBlockByHeight({})", height),
            Self::GetPeers(flags, _) => write!(f, "GetPeers({})", flags),
            Self::GetTip(_) => write!(f, "GetTip"),
            Self::GetBlock(hash) => write!(f, "GetBlock({})", hash),
            Self::GetFilters(range, _) => write!(f, "GetFilters({:?})", range),
            Self::Rescan { from, to, watch } => {
                write!(f, "Rescan({:?}, {:?}, {:?})", from, to, watch)
            }
            Self::Broadcast(msg, _, _) => write!(f, "Broadcast({})", msg.cmd()),
            Self::Query(msg, _) => write!(f, "Query({})", msg.cmd()),
            Self::QueryTree(_) => write!(f, "QueryTree"),
            Self::Connect(addr) => write!(f, "Connect({})", addr),
            Self::Disconnect(addr) => write!(f, "Disconnect({})", addr),
            Self::ImportHeaders(_headers, _) => write!(f, "ImportHeaders(..)"),
            Self::ImportAddresses(addrs) => write!(f, "ImportAddresses({:?})", addrs),
            Self::SubmitTransaction(tx, _) => write!(f, "SubmitTransaction({:?})", tx),
        }
    }
}

/// A generic error resulting from processing a [`Command`].
#[derive(Error, Debug)]
pub enum CommandError {
    /// Not connected to any peer with the required services.
    #[error("not connected to any peer with the required services")]
    NotConnected,
}

pub use cbfmgr::GetFiltersError;

/// Output of a state transition of the `Protocol` state machine.
#[derive(Debug)]
pub enum Io {
    /// There are some bytes ready to be sent to a peer.
    Write(PeerId),
    /// Connect to a peer.
    Connect(PeerId),
    /// Disconnect from a peer.
    Disconnect(PeerId, DisconnectReason),
    /// Ask for a wakeup in a specified amount of time.
    Wakeup(LocalDuration),
    /// Emit an event.
    Event(Event),
}

impl From<Event> for Io {
    fn from(event: Event) -> Self {
        Io::Event(event)
    }
}

/// Disconnect reason.
#[derive(Debug, Clone)]
pub enum DisconnectReason {
    /// Peer is misbehaving.
    PeerMisbehaving(&'static str),
    /// Peer protocol version is too old or too recent.
    PeerProtocolVersion(u32),
    /// Peer doesn't have the required services.
    PeerServices(ServiceFlags),
    /// Peer chain is too far behind.
    PeerHeight(Height),
    /// Peer magic is invalid.
    PeerMagic(u32),
    /// Peer timed out.
    PeerTimeout(&'static str),
    /// Peer was dropped by all sub-protocols.
    PeerDropped,
    /// Connection to self was detected.
    SelfConnection,
    /// Inbound connection limit reached.
    ConnectionLimit,
    /// Error with the underlying connection.
    ConnectionError(Arc<std::io::Error>),
    /// Error trying to decode incoming message.
    DecodeError(Arc<encode::Error>),
    /// Peer was forced to disconnect by external command.
    Command,
    /// Peer was disconnected for another reason.
    Other(&'static str),
}

impl DisconnectReason {
    /// Check whether the disconnect reason is transient, ie. may no longer be applicable
    /// after some time.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::ConnectionLimit
                | Self::PeerTimeout(_)
                | Self::PeerHeight(_)
                | Self::ConnectionError(_)
        )
    }
}

impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerMisbehaving(reason) => write!(f, "peer misbehaving: {}", reason),
            Self::PeerProtocolVersion(_) => write!(f, "peer protocol version mismatch"),
            Self::PeerServices(_) => write!(f, "peer doesn't have the required services"),
            Self::PeerHeight(_) => write!(f, "peer is too far behind"),
            Self::PeerMagic(magic) => write!(f, "received message with invalid magic: {}", magic),
            Self::PeerTimeout(s) => write!(f, "peer timed out: {:?}", s),
            Self::PeerDropped => write!(f, "peer dropped"),
            Self::SelfConnection => write!(f, "detected self-connection"),
            Self::ConnectionLimit => write!(f, "inbound connection limit reached"),
            Self::ConnectionError(err) => write!(f, "connection error: {}", err),
            Self::DecodeError(err) => write!(f, "message decode error: {}", err),
            Self::Command => write!(f, "received external command"),
            Self::Other(reason) => write!(f, "{}", reason),
        }
    }
}

mod message {
    use nakamoto_common::bitcoin::consensus::Encodable;

    use super::*;
    use std::io;

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

        pub fn write<W: io::Write>(
            &self,
            payload: NetworkMessage,
            writer: W,
        ) -> Result<usize, io::Error> {
            RawNetworkMessage {
                payload,
                magic: self.magic,
            }
            .consensus_encode(writer)
        }
    }
}

/// Holds functions that are used to hook into or alter protocol behavior.
#[derive(Clone)]
pub struct Hooks {
    /// Called when we receive a message from a peer.
    /// If an error is returned, the message is not further processed.
    pub on_message:
        Arc<dyn Fn(PeerId, &NetworkMessage, &Upstream) -> Result<(), &'static str> + Send + Sync>,
    /// Called when a `version` message is received.
    /// If an error is returned, the peer is dropped, and the error is logged.
    pub on_version: Arc<dyn Fn(PeerId, VersionMessage) -> Result<(), &'static str> + Send + Sync>,
    /// Called when a `getcfilters` message is received.
    pub on_getcfilters: Arc<dyn Fn(PeerId, GetCFilters, &Upstream) + Send + Sync>,
    /// Called when a `getdata` message is received.
    pub on_getdata: Arc<dyn Fn(PeerId, Vec<Inventory>, &Upstream) + Send + Sync>,
}

impl Default for Hooks {
    fn default() -> Self {
        Self {
            on_message: Arc::new(|_, _, _| Ok(())),
            on_version: Arc::new(|_, _| Ok(())),
            on_getcfilters: Arc::new(|_, _, _| {}),
            on_getdata: Arc::new(|_, _, _| {}),
        }
    }
}

impl fmt::Debug for Hooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hooks").finish()
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////

/// An instance of the Bitcoin P2P network protocol. Parametrized over the
/// block-tree and compact filter store.
#[derive(Debug)]
pub struct Protocol<T, F, P> {
    /// Block tree.
    tree: T,
    /// Bitcoin network we're connecting to.
    network: network::Network,
    /// Our protocol version.
    protocol_version: u32,
    /// Consensus parameters.
    params: Params,
    /// Peer message inboxes.
    inbox: HashMap<PeerId, stream::Decoder>,
    /// Peer address manager.
    addrmgr: AddressManager<P, Upstream>,
    /// Blockchain synchronization manager.
    syncmgr: SyncManager<Upstream>,
    /// Ping manager.
    pingmgr: PingManager<Upstream>,
    /// CBF (Compact Block Filter) manager.
    cbfmgr: FilterManager<F, Upstream>,
    /// Peer manager.
    peermgr: PeerManager<Upstream>,
    /// Inventory manager.
    invmgr: InventoryManager<Upstream>,
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
    /// Protocol event hooks.
    hooks: Hooks,
}

/// Protocol configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Bitcoin network we are connected to.
    pub network: network::Network,
    /// Peers to connect to.
    pub connect: Vec<net::SocketAddr>,
    /// Supported communication domains.
    pub domains: Vec<Domain>,
    /// Services offered by our peer.
    pub services: ServiceFlags,
    /// Required peer services.
    pub required_services: ServiceFlags,
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
    /// Ping timeout, after which remotes are disconnected.
    pub ping_timeout: LocalDuration,
    /// Size in bytes of the compact filter cache.
    pub filter_cache_size: usize,
    /// Log target.
    pub target: &'static str,
    /// Protocol event hooks.
    pub hooks: Hooks,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: network::Network::default(),
            params: Params::new(network::Network::default().into()),
            connect: Vec::new(),
            domains: Domain::all(),
            services: ServiceFlags::NONE,
            required_services: ServiceFlags::NETWORK,
            whitelist: Whitelist::default(),
            protocol_version: PROTOCOL_VERSION,
            target_outbound_peers: peermgr::TARGET_OUTBOUND_PEERS,
            max_inbound_peers: peermgr::MAX_INBOUND_PEERS,
            ping_timeout: pingmgr::PING_TIMEOUT,
            filter_cache_size: cbfmgr::DEFAULT_FILTER_CACHE_SIZE,
            user_agent: USER_AGENT,
            target: "self",
            hooks: Hooks::default(),
        }
    }
}

impl Config {
    /// Construct a new configuration.
    pub fn from(
        target: &'static str,
        network: network::Network,
        connect: Vec<net::SocketAddr>,
    ) -> Self {
        let params = Params::new(network.into());

        Self {
            network,
            connect,
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

impl<T: BlockTree, F: Filters, P: peer::Store> Protocol<T, F, P> {
    /// Construct a new protocol instance.
    pub fn new(
        tree: T,
        filters: F,
        peers: P,
        clock: AdjustedTime<PeerId>,
        rng: fastrand::Rng,
        config: Config,
    ) -> Self {
        let Config {
            network,
            connect,
            domains,
            services,
            whitelist,
            protocol_version,
            target_outbound_peers,
            max_inbound_peers,
            ping_timeout,
            filter_cache_size,
            user_agent,
            required_services,
            target,
            params,
            hooks,
        } = config;

        let upstream = Upstream::new(network, protocol_version, target);
        let inbox = HashMap::new();
        let syncmgr = SyncManager::new(
            syncmgr::Config {
                max_message_headers: syncmgr::MAX_MESSAGE_HEADERS,
                request_timeout: syncmgr::REQUEST_TIMEOUT,
                params: params.clone(),
            },
            rng.clone(),
            upstream.clone(),
        );
        let pingmgr = PingManager::new(ping_timeout, rng.clone(), upstream.clone());
        let cbfmgr = FilterManager::new(
            cbfmgr::Config {
                filter_cache_size,
                ..cbfmgr::Config::default()
            },
            rng.clone(),
            filters,
            upstream.clone(),
        );
        let peermgr = PeerManager::new(
            peermgr::Config {
                protocol_version: PROTOCOL_VERSION,
                whitelist,
                persistent: connect,
                domains: domains.clone(),
                target_outbound_peers,
                max_inbound_peers,
                retry_max_wait: LocalDuration::from_mins(60),
                retry_min_wait: LocalDuration::from_secs(1),
                required_services,
                preferred_services: syncmgr::REQUIRED_SERVICES | cbfmgr::REQUIRED_SERVICES,
                services,
                user_agent,
            },
            rng.clone(),
            hooks.clone(),
            upstream.clone(),
        );
        let addrmgr = AddressManager::new(
            addrmgr::Config {
                required_services,
                domains,
            },
            rng.clone(),
            peers,
            upstream.clone(),
        );
        let invmgr = InventoryManager::new(rng.clone(), upstream.clone());

        Self {
            tree,
            network,
            protocol_version,
            target,
            params,
            clock,
            inbox,
            addrmgr,
            syncmgr,
            pingmgr,
            cbfmgr,
            peermgr,
            invmgr,
            last_tick: LocalTime::default(),
            rng,
            upstream,
            hooks,
        }
    }

    fn received(&mut self, addr: &net::SocketAddr, msg: RawNetworkMessage) {
        let now = self.clock.local_time();
        let cmd = msg.cmd();
        let addr = *addr;

        if msg.magic != self.network.magic() {
            return self.disconnect(addr, DisconnectReason::PeerMagic(msg.magic));
        }

        if !self.peermgr.is_connected(&addr) {
            debug!(target: self.target, "Received {:?} from unknown peer {}", cmd, addr);
            return;
        }

        debug!(
            target: self.target, "{}: Received {:?}",
            addr, cmd
        );

        if let Err(err) = (self.hooks.on_message)(addr, &msg.payload, &self.upstream) {
            debug!(
                target: self.target,
                "{}: Message {:?} dropped by user hook: {}", addr, cmd, err
            );
            return;
        }

        match msg.payload {
            NetworkMessage::Version(msg) => {
                let height = self.tree.height();

                self.peermgr
                    .received_version(&addr, msg, height, now, &mut self.addrmgr);
            }
            NetworkMessage::Verack => {
                if let Some((peer, conn)) = self.peermgr.received_verack(&addr, now) {
                    self.clock.record_offset(conn.socket.addr, peer.time_offset);
                    self.addrmgr
                        .peer_negotiated(&addr, peer.services, conn.link, now);
                    self.pingmgr.peer_negotiated(conn.socket.addr, now);
                    self.cbfmgr.peer_negotiated(
                        conn.socket.clone(),
                        peer.height,
                        peer.services,
                        conn.link,
                        &self.clock,
                        &self.tree,
                    );
                    self.syncmgr.peer_negotiated(
                        conn.socket.clone(),
                        peer.height,
                        peer.services,
                        !peer.services.has(cbfmgr::REQUIRED_SERVICES),
                        conn.link,
                        &self.clock,
                        &self.tree,
                    );
                    self.invmgr.peer_negotiated(
                        conn.socket,
                        peer.services,
                        peer.relay,
                        peer.wtxidrelay,
                    );
                }
            }
            NetworkMessage::Ping(nonce) => {
                self.pingmgr.received_ping(addr, nonce);
            }
            NetworkMessage::Pong(nonce) => {
                if self.pingmgr.received_pong(addr, nonce, now) {
                    self.addrmgr.peer_active(addr, now);
                }
            }
            NetworkMessage::Headers(headers) => {
                match self
                    .syncmgr
                    .received_headers(&addr, headers, &self.clock, &mut self.tree)
                {
                    Err(e) => log::error!("Error receiving headers: {}", e),
                    Ok(ImportResult::TipChanged(_, _, _, reverted, _)) if !reverted.is_empty() => {
                        // By rolling back the filter headers, we will trigger
                        // a re-download of the missing headers, which should result
                        // in us having the new headers.
                        self.cbfmgr.rollback(reverted.len()).unwrap();

                        for (height, _) in reverted {
                            for tx in self.invmgr.block_reverted(height) {
                                self.cbfmgr.watch_transaction(&tx);
                            }
                        }
                    }
                    Ok(ImportResult::TipChanged { .. }) => {
                        // Trigger a filter sync, since we're going to have to catch up on the
                        // new block header(s). This is not required, but reduces latency.
                        self.cbfmgr.sync(&self.tree, now);
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
                    .received_getheaders(&addr, (locator_hashes, stop_hash), &self.tree);
            }
            NetworkMessage::Block(block) => {
                for confirmed in self.invmgr.received_block(&addr, block, &self.tree) {
                    self.cbfmgr.unwatch_transaction(&confirmed);
                }
            }
            NetworkMessage::Inv(inventory) => {
                self.syncmgr
                    .received_inv(addr, inventory, &self.clock, &self.tree);
                // TODO: invmgr: Update block availability for this peer.
            }
            NetworkMessage::CFHeaders(msg) => {
                match self.cbfmgr.received_cfheaders(&addr, msg, &self.tree, now) {
                    Err(cbfmgr::Error::InvalidMessage { reason, .. }) => {
                        self.disconnect(addr, DisconnectReason::PeerMisbehaving(reason))
                    }
                    _ => {}
                }
            }
            NetworkMessage::GetCFHeaders(msg) => {
                match self.cbfmgr.received_getcfheaders(&addr, msg, &self.tree) {
                    Err(cbfmgr::Error::InvalidMessage { reason, .. }) => {
                        self.disconnect(addr, DisconnectReason::PeerMisbehaving(reason))
                    }
                    _ => {}
                }
            }
            NetworkMessage::CFilter(msg) => {
                match self.cbfmgr.received_cfilter(&addr, msg, &self.tree) {
                    Ok(matches) => {
                        for (_, hash) in matches {
                            self.invmgr.get_block(hash);
                        }
                    }
                    Err(cbfmgr::Error::InvalidMessage { reason, .. }) => {
                        self.disconnect(addr, DisconnectReason::PeerMisbehaving(reason))
                    }
                    Err(cbfmgr::Error::Ignored { .. } | cbfmgr::Error::Filters { .. }) => {}
                }
            }
            NetworkMessage::GetCFilters(msg) => {
                (*self.hooks.on_getcfilters)(addr, msg, &self.upstream);
            }
            NetworkMessage::Addr(addrs) => {
                self.addrmgr.received_addr(addr, addrs);
                // TODO: Tick the peer manager, because we may have new addresses to connect to.
            }
            NetworkMessage::GetAddr => {
                self.addrmgr.received_getaddr(&addr);
            }
            NetworkMessage::GetData(invs) => {
                self.invmgr.received_getdata(addr, &invs);
                (*self.hooks.on_getdata)(addr, invs, &self.upstream);
            }
            NetworkMessage::WtxidRelay => {
                self.peermgr.received_wtxidrelay(&addr);
            }
            NetworkMessage::Unknown {
                command: ref cmd, ..
            } => {
                debug!(target: self.target, "{}: Ignoring unknown message {:?}", addr, cmd)
            }
            _ => {
                debug!(target: self.target, "{}: Ignoring {:?}", addr, cmd);
            }
        }
    }

    /// Send a message to a all peers matching the predicate.
    fn broadcast<Q>(&mut self, msg: NetworkMessage, predicate: Q) -> Vec<PeerId>
    where
        Q: Fn(&Peer) -> bool,
    {
        let mut peers = Vec::new();

        for peer in self.peermgr.peers().map(Peer::from) {
            if predicate(&peer) {
                peers.push(peer.addr);
                self.upstream.message(peer.addr, msg.clone());
            }
        }
        peers
    }

    /// Send a message to a random outbound peer. Returns the peer id.
    fn query<Q>(&mut self, msg: NetworkMessage, f: Q) -> Option<PeerId>
    where
        Q: Fn(&Peer) -> bool,
    {
        let peers = self
            .peermgr
            .negotiated(Link::Outbound)
            .map(Peer::from)
            .filter(f)
            .collect::<Vec<_>>();

        match peers.len() {
            n if n > 0 => {
                let r = self.rng.usize(..n);
                let p = peers.get(r).unwrap();

                self.upstream.message(p.addr, msg);

                Some(p.addr)
            }
            _ => None,
        }
    }

    fn disconnect(&mut self, addr: PeerId, reason: DisconnectReason) {
        // TODO: Trigger disconnection everywhere, as if peer disconnected. This
        // avoids being in a state where we know a peer is about to get disconnected,
        // but we still process messages from it as normal.

        self.peermgr.disconnect(addr, reason);
    }
}

impl<T: BlockTree, F: Filters, P: peer::Store> traits::Protocol for Protocol<T, F, P> {
    type Upstream = channel::Drain;

    fn initialize(&mut self, time: LocalTime) {
        self.clock.set_local_time(time);
        self.addrmgr.initialize(time);
        self.syncmgr.initialize(time, &self.tree);
        self.peermgr.initialize(time, &mut self.addrmgr);
        self.cbfmgr.initialize(time, &self.tree);
    }

    fn attempted(&mut self, addr: &net::SocketAddr) {
        self.addrmgr.peer_attempted(addr, self.clock.local_time());
        self.peermgr.peer_attempted(addr);
    }

    fn connected(&mut self, addr: net::SocketAddr, local_addr: &net::SocketAddr, link: Link) {
        let height = self.tree.height();
        let local_time = self.clock.local_time();

        self.addrmgr.record_local_address(*local_addr);
        self.addrmgr.peer_connected(&addr, local_time);
        self.peermgr
            .peer_connected(addr, *local_addr, link, height, local_time);
        self.inbox
            .insert(addr, stream::Decoder::new(INBOX_BUFFER_SIZE));
    }

    fn disconnected(&mut self, addr: &net::SocketAddr, reason: DisconnectReason) {
        let local_time = self.clock.local_time();

        info!(target: self.target, "[conn] {}: Disconnected: {}", addr, reason);

        self.cbfmgr.peer_disconnected(addr);
        self.syncmgr.peer_disconnected(addr);
        self.addrmgr.peer_disconnected(addr, reason.clone());
        self.pingmgr.peer_disconnected(addr);
        self.peermgr
            .peer_disconnected(addr, &mut self.addrmgr, local_time, reason);
        self.invmgr.peer_disconnected(addr);

        self.upstream.unregister(addr);
    }

    fn received_bytes(&mut self, addr: &net::SocketAddr, bytes: &[u8]) {
        if let Some(stream) = self.inbox.get_mut(addr) {
            stream.input(bytes);

            let mut msgs = Vec::with_capacity(1);

            loop {
                match stream.decode_next() {
                    Ok(Some(msg)) => msgs.push(msg),
                    Ok(None) => break,

                    Err(err) => {
                        self.upstream
                            .disconnect(*addr, DisconnectReason::DecodeError(Arc::new(err)));
                        return;
                    }
                }
            }
            for msg in msgs {
                self.received(addr, msg);
            }
        }
    }

    fn command(&mut self, cmd: Command) {
        debug!(target: self.target, "Received command: {:?}", cmd);

        match cmd {
            Command::QueryTree(query) => {
                query(&self.tree);
            }
            Command::GetBlockByHeight(height, reply) => {
                let header = self.tree.get_block_by_height(height).map(|h| h.to_owned());

                reply.send(header).ok();
            }
            Command::GetPeers(services, reply) => {
                let peers = self
                    .peermgr
                    .peers()
                    .filter(|(p, _)| p.is_negotiated())
                    .filter(|(p, _)| p.services.has(services))
                    .map(Peer::from)
                    .collect::<Vec<Peer>>();

                reply.send(peers).ok();
            }
            Command::Connect(addr) => {
                self.peermgr.whitelist(addr);
                self.peermgr.connect(&addr, self.clock.local_time());
            }
            Command::Disconnect(addr) => {
                self.disconnect(addr, DisconnectReason::Command);
            }
            Command::Query(msg, reply) => {
                reply.send(self.query(msg, |_| true)).ok();
            }
            Command::Broadcast(msg, predicate, reply) => {
                let peers = self.broadcast(msg, |p| predicate(p.clone()));
                reply.send(peers).ok();
            }
            Command::ImportHeaders(headers, reply) => {
                let result =
                    self.syncmgr
                        .import_blocks(headers.into_iter(), &self.clock, &mut self.tree);

                match result {
                    Ok(import_result) => {
                        reply.send(Ok(import_result)).ok();
                    }
                    Err(err) => {
                        reply.send(Err(err)).ok();
                    }
                }
            }
            Command::ImportAddresses(addrs) => {
                self.addrmgr.insert(
                    // Nb. For imported addresses, the time last active is not relevant.
                    addrs.into_iter().map(|a| (BlockTime::default(), a)),
                    peer::Source::Imported,
                );
            }
            Command::GetTip(reply) => {
                let (_, header) = self.tree.tip();
                let height = self.tree.height();

                reply.send((height, header)).ok();
            }
            Command::GetFilters(range, reply) => {
                let result = self.cbfmgr.get_cfilters(range, &self.tree);
                reply.send(result).ok();
            }
            Command::GetBlock(hash) => {
                self.invmgr.get_block(hash);
            }
            Command::SubmitTransaction(tx, reply) => {
                // Update local watchlist to track submitted transactions.
                //
                // Nb. This is currently non-optimal, as the cfilter matching is based on the
                // output scripts. This may trigger false-positives, since the same
                // invoice (address) can be re-used by multiple transactions, ie. outputs
                // can figure in more than one block.
                self.cbfmgr.watch_transaction(&tx);

                // TODO: For BIP 339 support, we can send a `WTx` inventory here.
                let peers = self.invmgr.announce(tx);

                if let Some(peers) = NonEmpty::from_vec(peers) {
                    reply.send(Ok(peers)).ok();
                } else {
                    reply.send(Err(CommandError::NotConnected)).ok();
                }
            }
            Command::Rescan { from, to, watch } => {
                // A rescan with a new watch list may return matches on cached filters.
                for (_, hash) in self.cbfmgr.rescan(from, to, watch, &self.tree) {
                    self.invmgr.get_block(hash);
                }
            }
        }
    }

    fn tick(&mut self, local_time: LocalTime) {
        trace!(target: self.target, "Received tick");

        self.clock.set_local_time(local_time);
    }

    fn tock(&mut self, local_time: LocalTime) {
        self.clock.set_local_time(local_time);

        trace!(target: self.target, "Received tock");

        self.invmgr.received_tick(local_time, &self.tree);
        self.syncmgr.received_tick(local_time, &self.tree);
        self.pingmgr.received_tick(local_time);
        self.addrmgr.received_tick(local_time);
        self.peermgr.received_tick(local_time, &mut self.addrmgr);
        self.cbfmgr.received_tick(local_time, &self.tree);

        #[cfg(not(test))]
        if local_time - self.last_tick >= LocalDuration::from_secs(10) {
            let (tip, _) = self.tree.tip();
            let height = self.tree.height();
            let best = self
                .syncmgr
                .best_height()
                .unwrap_or_else(|| self.tree.height());
            let sync = if best > 0 {
                height as f64 / best as f64 * 100.
            } else {
                0.
            };
            let outbound = self.peermgr.negotiated(Link::Outbound).count();
            let inbound = self.peermgr.negotiated(Link::Inbound).count();
            let connecting = self.peermgr.connecting().count();
            let target = self.peermgr.config.target_outbound_peers;
            let max_inbound = self.peermgr.config.max_inbound_peers;
            let addresses = self.addrmgr.len();
            let preferred = self
                .peermgr
                .negotiated(Link::Outbound)
                .filter(|(p, _)| p.services.has(self.peermgr.config.preferred_services))
                .count();

            // TODO: Add cache sizes on disk
            // TODO: Add protocol state(s)
            // TODO: Trim block hash
            // TODO: Add average headers/s or bandwidth

            let mut msg = Vec::new();

            msg.push(format!("tip = {}", tip));
            msg.push(format!("headers = {}/{} ({:.1}%)", height, best, sync));
            msg.push(format!(
                "cfheaders = {}/{}",
                self.cbfmgr.filters.height(),
                height
            ));
            msg.push(format!("inbound = {}/{}", inbound, max_inbound));
            msg.push(format!(
                "outbound = {}/{} ({})",
                outbound, target, preferred,
            ));
            msg.push(format!("connecting = {}/{}", connecting, target));
            msg.push(format!("addresses = {}", addresses));

            log::info!("{}", msg.join(", "));

            if self.cbfmgr.rescan.active {
                let rescan = &self.cbfmgr.rescan;
                log::info!(
                    "rescan current = {}, watch = {}, txs = {}, filter queue = {}, requested = {}",
                    rescan.current,
                    rescan.watch.len(),
                    rescan.transactions.len(),
                    rescan.received.len(),
                    rescan.requested.len()
                );
            }
            log::info!(
                "inventory block queue = {}, requested = {}, mempool = {}",
                self.invmgr.received.len(),
                self.invmgr.remaining.len(),
                self.invmgr.mempool.len(),
            );

            self.last_tick = local_time;
        }
    }

    fn drain(&mut self) -> channel::Drain {
        self.upstream.drain()
    }

    fn write<W: io::Write>(&mut self, addr: &net::SocketAddr, writer: W) -> io::Result<()> {
        self.upstream.write(addr, writer)
    }
}
