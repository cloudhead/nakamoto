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

use crate::event::Event;

use std::fmt::{self, Debug};
use std::net;
use std::ops::Range;
use std::sync::Arc;
use std::{collections::HashSet, net::SocketAddr};

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::network::message_filter::GetCFilters;
use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::Address;

use nakamoto_common::block::filter::Filters;
use nakamoto_common::block::time::{AdjustedTime, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{self, BlockTree, ImportResult};
use nakamoto_common::block::{BlockHash, Height};
use nakamoto_common::block::{BlockTime, Transaction};
use nakamoto_common::network::{self, Network};
use nakamoto_common::p2p::peer;

/// Peer-to-peer protocol version.
/// For now, we only support `70012`, due to lacking `sendcmpct` support.
pub const PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &str = "/nakamoto:0.2.0/";

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

    /// Check whether the link is inbound.
    pub fn is_inbound(&self) -> bool {
        *self == Link::Inbound
    }
}

/// A command or request that can be sent to the protocol.
#[derive(Debug, Clone)]
pub enum Command {
    /// Get block header at height.
    GetBlockByHeight(Height, chan::Sender<Option<BlockHeader>>),
    /// Get connected peers.
    GetPeers(ServiceFlags, chan::Sender<HashSet<SocketAddr>>),
    /// Get the tip of the active chain.
    GetTip(chan::Sender<(Height, BlockHeader)>),
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
    /// Import addresses into the address book.
    ImportAddresses(Vec<Address>),
    /// Submit a transaction to the network.
    SubmitTransaction(Transaction),
    /// Shutdown the protocol.
    Shutdown,
}

/// A protocol input event, parametrized over the network message type.
/// These are input events generated outside of the protocol.
#[derive(Debug, Clone)]
pub enum Input {
    /// Connection attempt underway.
    ///
    /// This input is only encountered when an outgoing connection attempt is made,
    /// and is always received before the `Connected` input.
    ///
    /// For incoming connections, the `Connected` input is received directly.
    Connecting {
        /// Remote peer address.
        addr: net::SocketAddr,
    },
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
    Disconnected(PeerId, DisconnectReason),
    /// Received a message from a remote peer.
    Received(PeerId, RawNetworkMessage),
    /// Sent a message to a remote peer, of the given size.
    Sent(PeerId, usize),
    /// An external command has been received.
    Command(Command),
    /// Used to advance the state machine after some wall time has passed.
    ///
    /// "a regular short, sharp sound, especially that made by a clock or watch, typically
    /// every second."
    Tick,
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
#[derive(Debug, Clone, Eq, PartialEq)]
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
    /// Connection to self was detected.
    SelfConnection,
    /// Inbound connection limit reached.
    ConnectionLimit,
    /// Error with the underlying connection.
    ConnectionError(String),
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
            Self::SelfConnection => write!(f, "detected self-connection"),
            Self::ConnectionLimit => write!(f, "inbound connection limit reached"),
            Self::ConnectionError(err) => write!(f, "connection error: {}", err),
            Self::Command => write!(f, "received external command"),
            Self::Other(reason) => write!(f, "{}", reason),
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
    pub on_getcfilters: Arc<dyn Fn(PeerId, GetCFilters) + Send + Sync>,
    /// Called when a `getdata` message is received.
    pub on_getdata: Arc<dyn Fn(PeerId, Vec<Inventory>) + Send + Sync>,
}

impl Default for Hooks {
    fn default() -> Self {
        Self {
            on_message: Arc::new(|_, _, _| Ok(())),
            on_version: Arc::new(|_, _| Ok(())),
            on_getcfilters: Arc::new(|_, _| {}),
            on_getdata: Arc::new(|_, _| {}),
        }
    }
}

impl fmt::Debug for Hooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hooks").finish()
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////

/// A generic state machine. The peer-to-peer protocol implements this trait.
pub trait Machine {
    /// Initialize the state machine. Called once before any event is sent to the state machine.
    fn initialize(&mut self, time: LocalTime);
    /// Process the next input and advance the state machine by one step.
    fn step(&mut self, input: Input, local_time: LocalTime);
}

/// An instantiation of `Protocol`, for the Bitcoin P2P network. Parametrized over the
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
    /// Peer address manager.
    addrmgr: AddressManager<P, Upstream>,
    /// Blockchain synchronization manager.
    syncmgr: SyncManager<Upstream>,
    /// Peer connection manager.
    connmgr: ConnectionManager<Upstream, AddressManager<P, Upstream>>,
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
    /// Log target.
    pub target: &'static str,
    /// Protocol event hooks.
    pub hooks: Hooks,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: network::Network::Mainnet,
            params: Params::new(network::Network::Mainnet.into()),
            connect: Vec::new(),
            services: ServiceFlags::NONE,
            required_services: ServiceFlags::NETWORK,
            whitelist: Whitelist::default(),
            protocol_version: PROTOCOL_VERSION,
            target_outbound_peers: connmgr::TARGET_OUTBOUND_PEERS,
            max_inbound_peers: connmgr::MAX_INBOUND_PEERS,
            ping_timeout: pingmgr::PING_TIMEOUT,
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
        upstream: chan::Sender<Out>,
    ) -> Self {
        let Config {
            network,
            connect,
            services,
            whitelist,
            protocol_version,
            target_outbound_peers,
            max_inbound_peers,
            ping_timeout,
            user_agent,
            required_services,
            target,
            params,
            hooks,
        } = config;

        let upstream = Upstream::new(network, protocol_version, target, upstream);

        let syncmgr = SyncManager::new(
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
                retry: connect,
                required_services,
                // Include services required by all sub-protocols.
                preferred_services: syncmgr::REQUIRED_SERVICES | spvmgr::REQUIRED_SERVICES,
            },
            rng.clone(),
        );
        let pingmgr = PingManager::new(ping_timeout, rng.clone(), upstream.clone());
        let spvmgr = SpvManager::new(
            spvmgr::Config::default(),
            rng.clone(),
            filters,
            upstream.clone(),
        );
        let peermgr = PeerManager::new(
            peermgr::Config {
                protocol_version: PROTOCOL_VERSION,
                whitelist,
                required_services,
                services,
                user_agent,
            },
            rng.clone(),
            hooks.clone(),
            upstream.clone(),
        );
        let addrmgr = AddressManager::new(
            addrmgr::Config { required_services },
            rng.clone(),
            peers,
            upstream.clone(),
        );

        Self {
            tree,
            network,
            protocol_version,
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
            hooks,
        }
    }

    /// Send a message to a random peer. Returns the peer id.
    fn query<Q>(&self, msg: NetworkMessage, mut f: Q) -> Option<PeerId>
    where
        Q: FnMut(&peermgr::Peer) -> bool,
    {
        let peers = self
            .peermgr
            .outbound()
            .filter(|p| f(*p))
            .collect::<Vec<_>>();

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

        #[cfg(not(test))]
        if local_time - self.last_tick >= LocalDuration::from_secs(30) {
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
            let peers = self.connmgr.outbound_peers().count();
            let target = self.connmgr.config.target_outbound_peers;

            // TODO: Add cache sizes on disk
            // TODO: Add protocol state(s)
            // TODO: Trim block hash
            // TODO: Add average headers/s or bandwidth
            // TODO: Add address book size
            // TODO: Add "connecting" peers
            // TODO: Add inbound peer count

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

        if !self.connmgr.is_connected(&addr) {
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
                if let Some(peer) = self.peermgr.received_verack(&addr, now) {
                    self.clock.record_offset(peer.address(), peer.time_offset);
                    self.addrmgr
                        .peer_negotiated(&addr, peer.services, peer.conn.link, now);
                    self.pingmgr.peer_negotiated(peer.address(), now);
                    self.connmgr.peer_negotiated(peer.address(), peer.services);
                    self.spvmgr.peer_negotiated(
                        peer.address(),
                        peer.height,
                        peer.services,
                        peer.conn.link,
                        &self.clock,
                        &self.tree,
                    );
                    self.syncmgr.peer_negotiated(
                        peer.address(),
                        peer.height,
                        peer.services,
                        peer.conn.link,
                        &self.clock,
                        &self.tree,
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
                match self
                    .syncmgr
                    .received_headers(&addr, headers, &self.clock, &mut self.tree)
                {
                    Err(e) => log::error!("Error receiving headers: {}", e),
                    Ok(ImportResult::TipChanged(_, _, _, reverted)) if !reverted.is_empty() => {
                        // By rolling back the filter headers, we will trigger
                        // a re-download of the missing headers, which should result
                        // in us having the new headers.
                        self.spvmgr.rollback(reverted.len()).unwrap();
                        self.spvmgr.sync(&self.tree, now);
                    }
                    Ok(ImportResult::TipChanged(_, _, _, _)) => {
                        if !self.syncmgr.is_syncing() {
                            // Trigger a filter sync, since we're going to have to catch up on the
                            // new block header(s). This is not required, but reduces latency.
                            // We only do this at the tip of the header chain.
                            self.spvmgr.sync(&self.tree, now);
                        }
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
                self.syncmgr.received_block(&addr, block, &self.tree);
            }
            NetworkMessage::Inv(inventory) => {
                // Receive an `inv` message. This will happen if we are out of sync with a
                // peer. And blocks are being announced. Otherwise, we expect to receive a
                // `headers` message.
                self.syncmgr
                    .received_inv(addr, inventory, &self.clock, &self.tree);
            }
            NetworkMessage::CFHeaders(msg) => {
                match self.spvmgr.received_cfheaders(&addr, msg, &self.tree, now) {
                    Err(spvmgr::Error::InvalidMessage { reason, .. }) => {
                        self.disconnect(addr, DisconnectReason::PeerMisbehaving(reason))
                    }
                    _ => {}
                }
            }
            NetworkMessage::GetCFHeaders(msg) => {
                match self.spvmgr.received_getcfheaders(&addr, msg, &self.tree) {
                    Err(spvmgr::Error::InvalidMessage { reason, .. }) => {
                        self.disconnect(addr, DisconnectReason::PeerMisbehaving(reason))
                    }
                    _ => {}
                }
            }
            NetworkMessage::CFilter(msg) => {
                match self.spvmgr.received_cfilter(&addr, msg, &self.tree) {
                    Err(spvmgr::Error::InvalidMessage { reason, .. }) => {
                        self.disconnect(addr, DisconnectReason::PeerMisbehaving(reason))
                    }
                    _ => {}
                }
            }
            NetworkMessage::GetCFilters(msg) => {
                (*self.hooks.on_getcfilters)(addr, msg);
            }
            NetworkMessage::Addr(addrs) => {
                self.addrmgr.received_addr(addr, addrs);
            }
            NetworkMessage::GetAddr => {
                self.addrmgr.received_getaddr(&addr);
            }
            NetworkMessage::GetData(inv) => {
                (*self.hooks.on_getdata)(addr, inv);
            }
            _ => {
                debug!(target: self.target, "{}: Ignoring {:?}", addr, cmd);
            }
        }
    }

    fn disconnect(&mut self, addr: PeerId, reason: DisconnectReason) {
        // TODO: Trigger disconnection everywhere, as if peer disconnected. This
        // avoids being in a state where we know a peer is about to get disconnected,
        // but we still process messages from it as normal.

        self.connmgr.disconnect(addr, reason);
    }
}

impl<T: BlockTree, F: Filters, P: peer::Store> Machine for Protocol<T, F, P> {
    /// Initialize the protocol. Called once before any event is sent to the state machine.
    fn initialize(&mut self, time: LocalTime) {
        self.clock.set_local_time(time);
        self.addrmgr.initialize(time);
        self.syncmgr.initialize(time, &self.tree);
        self.connmgr.initialize::<P>(time, &mut self.addrmgr);
        self.spvmgr.initialize(time, &self.tree);
    }

    /// Process the next input and advance the state machine by one step.
    fn step(&mut self, input: Input, local_time: LocalTime) {
        self.tick(local_time);

        match input {
            Input::Connecting { addr } => {
                self.addrmgr.peer_attempted(&addr, local_time);
                self.connmgr.peer_attempted(&addr);
            }
            Input::Connected {
                addr,
                local_addr,
                link,
            } => {
                let height = self.tree.height();
                // This is usually not that useful, except when our local address is actually the
                // address our peers see.
                self.addrmgr.record_local_addr(local_addr);
                self.addrmgr.peer_connected(&addr, local_time);
                self.connmgr
                    .peer_connected(addr, local_addr, link, local_time);
                self.peermgr
                    .peer_connected(addr, local_addr, link, height, local_time);
            }
            Input::Disconnected(addr, reason) => {
                info!(target: self.target, "{}: Disconnected: {}", addr, reason);

                self.spvmgr.peer_disconnected(&addr);
                self.syncmgr.peer_disconnected(&addr);
                self.addrmgr.peer_disconnected(&addr, reason);
                self.connmgr
                    .peer_disconnected::<P>(&addr, &mut self.addrmgr);
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
                Command::GetBlockByHeight(height, reply) => {
                    debug!(target: self.target, "Received command: GetBlockByHeight");

                    let header = self.tree.get_block_by_height(height).map(|h| h.to_owned());

                    reply.send(header).ok();
                }
                Command::GetPeers(services, reply) => {
                    debug!(target: self.target, "Received command: GetPeers");

                    let peers = self
                        .peermgr
                        .peers()
                        .filter(|f| f.is_negotiated())
                        .filter(|f| f.services.has(services))
                        .map(|f| f.address())
                        .collect::<HashSet<SocketAddr>>();

                    reply.send(peers).ok();
                }
                Command::Connect(addr) => {
                    debug!(target: self.target, "Received command: Connect({})", addr);

                    self.peermgr.whitelist(addr);
                    self.connmgr.connect::<P>(&addr);
                }
                Command::Disconnect(addr) => {
                    debug!(target: self.target, "Received command: Disconnect({})", addr);

                    self.disconnect(addr, DisconnectReason::Command);
                }
                Command::Query(msg, reply) => {
                    debug!(target: self.target, "Received command: Query({:?})", msg);

                    reply.send(self.query(msg, |_| true)).ok();
                }
                Command::Broadcast(msg) => {
                    debug!(target: self.target, "Received command: Broadcast({:?})", msg);

                    for peer in self.peermgr.outbound() {
                        self.upstream.message(peer.address(), msg.clone());
                    }
                }
                Command::ImportHeaders(headers, reply) => {
                    debug!(target: self.target, "Received command: ImportHeaders(..)");

                    let result = self.syncmgr.import_blocks(
                        headers.into_iter(),
                        &self.clock,
                        &mut self.tree,
                    );

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
                    debug!(target: self.target, "Received command: ImportAddresses(..)");

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
                Command::GetFilters(range) => {
                    debug!(target: self.target,
                        "Received command: GetFilters({}..{})", range.start, range.end);

                    self.spvmgr.get_cfilters(range, &self.tree);
                }
                Command::GetBlock(hash) => {
                    self.query(NetworkMessage::GetData(vec![Inventory::Block(hash)]), |p| {
                        p.services.has(ServiceFlags::NETWORK)
                    });
                }
                Command::SubmitTransaction(tx) => {
                    debug!(target: self.target, "Received command: SubmitTransaction(..)");

                    self.query(NetworkMessage::Tx(tx), |p| p.relay);
                }
                Command::Shutdown => {
                    self.upstream.push(Out::Shutdown);
                }
            },
            Input::Tick => {
                trace!(target: self.target, "Received tick");

                self.connmgr
                    .received_tick::<P>(local_time, &mut self.addrmgr);
                self.syncmgr.received_tick(local_time, &self.tree);
                self.pingmgr.received_tick(local_time);
                self.addrmgr.received_tick(local_time);
                self.peermgr.received_tick(local_time);
                self.spvmgr.received_tick(local_time, &self.tree);
            }
        };
    }
}
