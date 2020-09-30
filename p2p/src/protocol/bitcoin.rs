//! Bitcoin protocol state machine.
#![warn(missing_docs)]
use crossbeam_channel as chan;
use log::*;

pub mod addrmgr;
pub mod connmgr;
pub mod network;
pub mod pingmgr;
pub mod syncmgr;
pub use network::Network;
pub mod channel;

#[cfg(test)]
mod tests;

use addrmgr::AddressManager;
use channel::Channel;
use connmgr::ConnectionManager;
use pingmgr::PingManager;
use syncmgr::SyncManager;

use crate::address_book::AddressBook;
use crate::event::Event;
use crate::protocol::{self, Link, Message, Out, PeerId, Protocol, Timeout, TimeoutSource};

use std::collections::HashSet;
use std::fmt::Debug;
use std::net;

use bitcoin::consensus::params::Params;
use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::GetHeadersMessage;
use bitcoin::network::message_network::VersionMessage;

use nakamoto_common::block::time::{AdjustedTime, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{self, BlockTree, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::collections::HashMap;

/// Peer-to-peer protocol version.
/// For now, we only support `70012`, due to lacking `sendcmpct` support.
pub const PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &str = "/nakamoto:0.0.0/";

/// Maximum number of addresses to return when receiving a `getaddr` message.
const MAX_GETADDR_ADDRESSES: usize = 8;
/// Maximum height difference for a stale peer, to maintain the connection (2 weeks).
const MAX_STALE_HEIGHT_DIFFERENCE: Height = 2016;
/// Time to wait for response during peer handshake before disconnecting the peer.
const HANDSHAKE_TIMEOUT: LocalDuration = LocalDuration::from_secs(10);

/// A time offset, in seconds.
type TimeOffset = i64;

/// Block locators. Consists of starting hashes and a stop hash.
type Locators = (Vec<BlockHash>, BlockHash);

/// Input into the state machine.
type Input = protocol::Input<RawNetworkMessage, Command>;

/// A command or request that can be sent to the protocol.
#[derive(Debug, Clone)]
pub enum Command {
    /// Get the tip of the active chain.
    GetTip(chan::Sender<BlockHeader>),
    /// Get a block from the active chain.
    GetBlock(BlockHash),
    /// Broadcast to outbound peers.
    Broadcast(NetworkMessage),
    /// Send to message to a random peer.
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

impl Message for RawNetworkMessage {
    type Payload = NetworkMessage;

    fn from_parts(payload: NetworkMessage, magic: u32) -> Self {
        Self { payload, magic }
    }

    fn payload(&self) -> &Self::Payload {
        &self.payload
    }

    fn display(&self) -> &'static str {
        self.payload.cmd()
    }

    fn magic(&self) -> u32 {
        self.magic
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

        pub fn message<M: Message>(&self, addr: net::SocketAddr, payload: M::Payload) -> Out<M> {
            Out::Message(addr, self.raw(payload))
        }

        pub fn raw<M: Message>(&self, payload: M::Payload) -> M {
            M::from_parts(payload, self.magic)
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////

/// An instantiation of `Protocol`, for the Bitcoin P2P network. Parametrized over the
/// block-tree.
#[derive(Debug)]
pub struct Bitcoin<T> {
    /// Peer states.
    peers: HashMap<PeerId, Peer>,
    /// Bitcoin network we're connecting to.
    network: network::Network,
    /// Services offered by us.
    services: ServiceFlags,
    /// Our protocol version.
    protocol_version: u32,
    /// Our user agent.
    user_agent: &'static str,
    /// Block height of active chain.
    height: Height,
    /// Consensus parameters.
    params: Params,
    /// Peer whitelist.
    whitelist: Whitelist,
    /// Peer address manager.
    addrmgr: AddressManager<Channel<RawNetworkMessage>>,
    /// Blockchain synchronization manager.
    syncmgr: SyncManager<T, Channel<RawNetworkMessage>>,
    /// Peer connection manager.
    connmgr: ConnectionManager<Channel<RawNetworkMessage>>,
    /// Ping manager.
    pingmgr: PingManager<Channel<RawNetworkMessage>>,
    /// Network-adjusted clock.
    clock: AdjustedTime<PeerId>,
    /// Set of connected peers that have completed the handshake.
    ready: HashSet<PeerId>,
    /// Informational name of this protocol instance. Used for logging purposes only.
    target: &'static str,
    /// Random number generator.
    rng: fastrand::Rng,
    /// Outbound channel. Used to communicate protocol events with a reactor.
    outbound: Channel<RawNetworkMessage>,
}

/// Protocol builder. Consume to build a new protocol instance.
#[derive(Clone)]
pub struct Builder<T: BlockTree> {
    /// Block cache.
    pub cache: T,
    /// Clock.
    pub clock: AdjustedTime<PeerId>,
    /// RNG.
    pub rng: fastrand::Rng,
    /// Configuration.
    pub cfg: Config,
}

impl<T: BlockTree> protocol::ProtocolBuilder for Builder<T> {
    type Message = RawNetworkMessage;
    type Protocol = Bitcoin<T>;

    fn build(self, tx: chan::Sender<Out<RawNetworkMessage>>) -> Bitcoin<T> {
        Bitcoin::new(self.cache, self.clock, self.rng, self.cfg, tx)
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

impl<T: BlockTree> Bitcoin<T> {
    /// Construct a new Bitcoin state machine.
    pub fn new(
        tree: T,
        clock: AdjustedTime<PeerId>,
        rng: fastrand::Rng,
        config: Config,
        outbound: chan::Sender<Out<RawNetworkMessage>>,
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

        let height = tree.height();
        let outbound = Channel::new(network, protocol_version, target, outbound);

        let addrmgr = AddressManager::from(address_book, rng.clone(), outbound.clone());
        let syncmgr = SyncManager::new(
            tree,
            syncmgr::Config {
                max_message_headers: syncmgr::MAX_MESSAGE_HEADERS,
                request_timeout: syncmgr::REQUEST_TIMEOUT,
                params: params.clone(),
            },
            rng.clone(),
            outbound.clone(),
        );
        let connmgr = ConnectionManager::new(
            outbound.clone(),
            connmgr::Config {
                target_outbound_peers,
                max_inbound_peers,
            },
        );
        let pingmgr = PingManager::new(rng.clone(), outbound.clone());

        Self {
            peers: HashMap::with_hasher(rng.clone().into()),
            network,
            services,
            protocol_version,
            user_agent,
            whitelist,
            target,
            params,
            clock,
            height,
            addrmgr,
            syncmgr,
            connmgr,
            pingmgr,
            rng,
            outbound,
            ready: HashSet::new(),
        }
    }

    fn connected(&mut self, addr: PeerId, local_addr: net::SocketAddr, nonce: u64, link: Link) {
        let rng = self.rng.clone();

        // TODO: Keep negotiated peers in a different set with a user agent.
        // TODO: Handle case where peer already exists.
        self.peers.insert(
            addr,
            Peer::new(
                addr,
                local_addr,
                PeerState::Handshake(Handshake::default()),
                nonce,
                link,
                rng,
                self.target,
            ),
        );

        // Set a timeout for receiving the `version` message.
        self.outbound
            .set_timeout(TimeoutSource::Handshake(addr), HANDSHAKE_TIMEOUT);
    }
}

/// Handshake states.
///
/// The steps for an *outbound* handshake are:
///
///   1. Send "version" message.
///   2. Expect "version" message from remote.
///   3. Expect "verack" message from remote.
///   4. Send "verack" message.
///
/// The steps for an *inbound* handshake are:
///
///   1. Expect "version" message from remote.
///   2. Send "version" message.
///   3. Send "verack" message.
///   4. Expect "verack" message from remote.
///
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
enum Handshake {
    /// Waiting for "version" message from remote.
    AwaitingVersion,
    /// Waiting for "verack" message from remote.
    AwaitingVerack,
}

impl Default for Handshake {
    fn default() -> Self {
        Self::AwaitingVersion
    }
}

/// State of a remote peer.
#[derive(Debug, PartialEq, Eq)]
enum PeerState {
    /// Handshake is in progress.
    Handshake(Handshake),
    /// Handshake was completed successfully. This peer is ready to receive messages.
    Ready {
        /// Last time this peer was active. This is set every time we receive
        /// a message from this peer.
        last_active: LocalTime,
    },
    /// The peer is being disconnected.
    Disconnecting,
}

/// A remote peer.
/// TODO: It should be possible to statically enforce the state-machine rules.
/// Eg. `Peer<State>`.
#[derive(Debug)]
struct Peer {
    /// Remote peer address.
    address: net::SocketAddr,
    /// Local peer address.
    local_address: net::SocketAddr,
    /// The peer's best height.
    height: Height,
    /// The peer's services.
    services: ServiceFlags,
    /// An offset in seconds, between this peer's clock and ours.
    /// A positive offset means the peer's clock is ahead of ours.
    time_offset: TimeOffset,
    /// Whether this is an inbound or outbound peer connection.
    link: Link,
    /// Peer state.
    state: PeerState,
    /// Peer nonce. Used to detect self-connections.
    nonce: u64,
    /// Peer user agent string.
    user_agent: String,
    /// Random number generator.
    rng: fastrand::Rng,
    /// Informational context for this peer. Used for logging purposes only.
    ctx: &'static str,
}

impl Peer {
    /// Construct a new peer instance.
    fn new(
        address: net::SocketAddr,
        local_address: net::SocketAddr,
        state: PeerState,
        nonce: u64,
        link: Link,
        rng: fastrand::Rng,
        ctx: &'static str,
    ) -> Self {
        Self {
            address,
            local_address,
            height: 0,
            time_offset: 0,
            state,
            link,
            services: ServiceFlags::NONE,
            nonce,
            user_agent: String::default(),
            ctx,
            rng,
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self.state, PeerState::Ready { .. })
    }

    #[allow(dead_code)]
    fn is_inbound(&self) -> bool {
        self.link == Link::Inbound
    }

    fn is_outbound(&self) -> bool {
        self.link.is_outbound()
    }

    fn receive_verack(&mut self, time: LocalTime) {
        self.transition(PeerState::Ready { last_active: time });
    }

    fn transition(&mut self, state: PeerState) {
        if state == self.state {
            return;
        }
        debug!(target: self.ctx, "{}: {:?} -> {:?}", self.address, self.state, state);

        self.state = state;
    }
}

impl<T: BlockTree> Protocol<RawNetworkMessage> for Bitcoin<T> {
    type Command = self::Command;

    fn initialize(&mut self, time: LocalTime) {
        self.clock.set_local_time(time);
        self.syncmgr.initialize(time);
        self.connmgr.initialize(time, &mut self.addrmgr);
    }

    fn step(&mut self, input: Input, local_time: LocalTime) {
        // The local time is set from outside the protocol.
        self.clock.set_local_time(local_time);

        match input {
            Input::Connected {
                addr,
                local_addr,
                link,
            } => {
                // This is usually not that useful, except when our local address is actually the
                // address our peers see.
                self.addrmgr.record_local_addr(local_addr);
                self.addrmgr.peer_connected(&addr, local_time);
                self.connmgr
                    .peer_connected(addr, local_addr, link, local_time);

                match link {
                    Link::Inbound => {
                        self.connected(addr, local_addr, 0, link);
                    }
                    Link::Outbound => {
                        let nonce = self.rng.u64(..);

                        self.connected(addr, local_addr, nonce, link);
                        self.outbound
                            .message(addr, self.version(addr, local_addr, nonce, self.height));
                    }
                }
            }
            Input::Disconnected(addr) => {
                self.peers.remove(&addr);
                self.ready.remove(&addr);
                self.syncmgr.peer_disconnected(&addr);
                self.addrmgr.peer_disconnected(&addr);
                self.connmgr.peer_disconnected(&addr, &self.addrmgr);
                self.pingmgr.peer_disconnected(&addr);
            }
            Input::Received(addr, msg) => {
                self.outbound
                    .event(Event::Received(addr, msg.payload.clone()));
                self.receive(addr, msg);
            }
            Input::Sent(_addr, _msg) => {}
            Input::Command(cmd) => {
                match cmd {
                    Command::Connect(addr) => {
                        debug!(target: self.target, "Received command: Connect({})", addr);

                        self.whitelist.addr.insert(addr.ip());
                        self.connmgr.connect(&addr, &mut self.addrmgr, local_time);
                    }
                    Command::Disconnect(addr) => {
                        debug!(target: self.target, "Received command: Disconnect({})", addr);

                        self.disconnect(addr);
                    }
                    Command::Query(msg, reply) => {
                        debug!(target: self.target, "Received command: Query({:?})", msg);

                        let mut peers = self.outbound();

                        match peers.clone().count() {
                            n if n > 0 => {
                                // TODO: We should have a `sample` function that takes an iterator
                                // and picks a random item or returns `None` if it's empty.
                                let r = self.rng.usize(..n);
                                let p = peers.nth(r).unwrap();

                                self.outbound.message(p.address, msg);
                                reply.send(Some(p.address)).ok();
                            }
                            _ => {
                                reply.send(None).ok();
                            }
                        }
                    }
                    Command::Broadcast(msg) => {
                        debug!(target: self.target, "Received command: Broadcast({:?})", msg);

                        for peer in self.outbound() {
                            self.outbound.message(peer.address, msg.clone());
                        }
                    }
                    Command::ImportHeaders(headers, reply) => {
                        debug!(target: self.target, "Received command: ImportHeaders(..)");

                        let result = self.syncmgr.import_blocks(headers.into_iter(), &self.clock);

                        match result {
                            Ok(import_result) => {
                                reply.send(Ok(import_result.clone())).ok();

                                if let ImportResult::TipChanged(_, height, _) = import_result {
                                    self.height = height;
                                }
                            }
                            Err(err) => {
                                reply.send(Err(err)).ok();
                            }
                        }
                    }
                    Command::GetTip(_) => todo!(),
                    Command::GetBlock(_) => todo!(),
                    Command::SubmitTransaction(tx) => {
                        debug!(target: self.target, "Received command: SubmitTransaction(..)");

                        // FIXME: Consolidate with `Query`.
                        let ix = self.rng.usize(..self.ready.len());
                        let peer = *self.ready.iter().nth(ix).unwrap();

                        self.outbound.message(peer, NetworkMessage::Tx(tx));
                    }
                    Command::Shutdown => {
                        self.outbound.push(Out::Shutdown);
                    }
                }
            }
            Input::Timeout(source) => {
                trace!(target: self.target, "Received timeout for {:?}", source);

                // We may not have the peer anymore, if it was disconnected and remove in
                // the meantime.
                match source {
                    TimeoutSource::Connect => {
                        self.connmgr.received_timeout(local_time, &self.addrmgr);
                    }
                    TimeoutSource::Handshake(addr) => {
                        if let Some(peer) = self.peers.get_mut(&addr) {
                            match peer.state {
                                PeerState::Handshake(Handshake::AwaitingVerack)
                                | PeerState::Handshake(Handshake::AwaitingVersion) => {
                                    self.disconnect(addr);
                                }
                                _ => {}
                            }
                        } else {
                            debug!(target: self.target, "Peer {} no longer exists (ignoring)", addr);
                        }
                    }
                    TimeoutSource::Synch(addr) => {
                        self.syncmgr.received_timeout(addr, local_time);
                    }
                    TimeoutSource::Ping(addr) => {
                        self.pingmgr.received_timeout(addr, local_time);
                    }
                }
            }
        };
    }
}

impl<T: BlockTree> Bitcoin<T> {
    /// Get outbound & ready peers.
    fn outbound(&self) -> impl Iterator<Item = &Peer> + Clone {
        self.peers
            .values()
            .filter(|p| p.is_ready() && p.is_outbound())
    }

    fn receive(&mut self, addr: PeerId, msg: RawNetworkMessage) {
        let now = self.clock.local_time();
        let cmd = msg.cmd();

        if msg.magic != self.network.magic() {
            // TODO: Needs test.
            debug!(
                target: self.target, "{}: Received message with invalid magic: {}",
                addr, msg.magic
            );
            self.disconnect(addr);

            return;
        }

        let peer = if let Some(peer) = self.peers.get_mut(&addr) {
            peer
        } else {
            debug!(target: self.target, "Received {:?} from unknown peer {}", cmd, addr);
            return;
        };
        let local_addr = peer.local_address;

        debug!(
            target: self.target, "{}: Received {:?} ({:?})",
            addr, cmd, peer.state
        );

        if let PeerState::Ready {
            ref mut last_active,
            ..
        } = peer.state
        {
            *last_active = now;

            if let NetworkMessage::Ping(nonce) = msg.payload {
                return self.pingmgr.ping_received(addr, nonce);
            } else if let NetworkMessage::Pong(nonce) = msg.payload {
                return self.pingmgr.pong_received(addr, nonce, now);
            } else if let NetworkMessage::Addr(addrs) = msg.payload {
                if addrs.is_empty() {
                    // Peer misbehaving, got empty message.
                    return;
                }
                self.addrmgr
                    .insert(addrs.into_iter(), addrmgr::Source::Peer(addr));

                return;
            } else if let NetworkMessage::Headers(headers) = msg.payload {
                return self.syncmgr.received_headers(&addr, headers, &self.clock);
            }
        }

        // TODO: Make sure we handle all messages at all times in some way.
        match &peer.state {
            PeerState::Handshake(Handshake::AwaitingVersion) => {
                if let NetworkMessage::Version(VersionMessage {
                    // Peer's best height.
                    start_height,
                    // Peer's local time.
                    timestamp,
                    // Highest protocol version understood by the peer.
                    version,
                    // Services offered by this peer.
                    services,
                    // User agent.
                    user_agent,
                    // Peer nonce.
                    nonce,
                    // Our address, as seen by the remote peer.
                    receiver,
                    ..
                }) = msg.payload
                {
                    let height = self.height;
                    let whitelisted = self.whitelist.contains(&addr.ip(), &user_agent)
                        || addrmgr::is_local(&addr.ip());

                    info!(
                        target: self.target, "{}: Peer version = {}, height = {}, agent = {}, timestamp = {}",
                        addr, version, start_height, user_agent, timestamp
                    );

                    // Don't support peers with an older protocol than ours, we won't be
                    // able to handle it correctly.
                    if version < self.protocol_version {
                        debug!(
                            target: self.target, "{}: Disconnecting: peer protocol version is too old: {}",
                            addr, version
                        );
                        return self.disconnect(addr);
                    }
                    // Peers that don't advertise the `NETWORK` service are not full nodes.
                    // It's not so useful for us to connect to them, because they're likely
                    // to be less secure.
                    if peer.link.is_outbound()
                        && !services.has(ServiceFlags::NETWORK)
                        && !services.has(ServiceFlags::NETWORK_LIMITED)
                        && !whitelisted
                    {
                        debug!(
                            target: self.target, "{}: Disconnecting: peer doesn't have required services",
                            addr
                        );
                        return self.disconnect(addr);
                    }
                    // If the peer is too far behind, there's no use connecting to it, we'll
                    // have to wait for it to catch up.
                    if height.saturating_sub(start_height as Height) > MAX_STALE_HEIGHT_DIFFERENCE
                        && !whitelisted
                    {
                        debug!(
                            target: self.target, "{}: Disconnecting: peer is too far behind",
                            addr
                        );
                        return self.disconnect(addr);
                    }
                    // Check for self-connections. We only need to check one link direction,
                    // since in the case of a self-connection, we will see both link directions.
                    for (_, peer) in self.peers.iter() {
                        if peer.link.is_outbound() && peer.nonce == nonce {
                            debug!(
                                target: self.target, "{}: Disconnecting: detected self-connection",
                                addr
                            );
                            return self.disconnect(addr);
                        }
                    }

                    // Record the address this peer has of us.
                    if let Ok(addr) = receiver.socket_addr() {
                        self.addrmgr.record_local_addr(addr);
                    }

                    let mut peer = self
                        .peers
                        .get_mut(&addr)
                        .unwrap_or_else(|| panic!("peer {} is not known", addr));

                    peer.height = start_height as Height;
                    peer.time_offset = timestamp - now.block_time() as i64;
                    peer.services = services;
                    peer.user_agent = user_agent;
                    peer.transition(PeerState::Handshake(Handshake::AwaitingVerack));

                    match peer.link {
                        Link::Outbound => {
                            self.outbound
                                .set_timeout(TimeoutSource::Handshake(addr), HANDSHAKE_TIMEOUT);
                        }
                        Link::Inbound => {
                            let nonce = peer.nonce;

                            self.outbound
                                .message(addr, self.version(addr, local_addr, nonce, self.height))
                                .message(addr, NetworkMessage::Verack)
                                .set_timeout(TimeoutSource::Handshake(addr), HANDSHAKE_TIMEOUT);
                        }
                    }
                    return;
                } else {
                    // TODO: Include disconnect reason.
                    debug!(target: self.target, "{}: Peer misbehaving", addr);
                    return self.disconnect(addr);
                }
            }
            PeerState::Handshake(Handshake::AwaitingVerack) => {
                if msg.payload == NetworkMessage::Verack {
                    peer.receive_verack(now);

                    self.ready.insert(addr);
                    self.clock.record_offset(addr, peer.time_offset);
                    self.addrmgr
                        .peer_negotiated(&addr, peer.services, peer.link, now);

                    let link = peer.link;

                    match link {
                        Link::Outbound => {
                            self.outbound
                                .message(addr, NetworkMessage::Verack)
                                .message(addr, NetworkMessage::SendHeaders)
                                .message(addr, NetworkMessage::GetAddr);
                        }
                        Link::Inbound => {
                            self.outbound.message(addr, NetworkMessage::SendHeaders);
                        }
                    }

                    self.pingmgr.peer_negotiated(peer.address, now);
                    self.syncmgr.peer_negotiated(
                        peer.address,
                        peer.height,
                        peer.services,
                        peer.link,
                        &self.clock,
                    );

                    return;
                } else {
                    // TODO: Include disconnect reason.
                    debug!(target: self.target, "{}: Peer misbehaving", addr);
                    return self.disconnect(addr);
                }
            }
            PeerState::Ready { .. } => {
                if let NetworkMessage::GetHeaders(GetHeadersMessage {
                    locator_hashes,
                    stop_hash,
                    ..
                }) = msg.payload
                {
                    return self
                        .syncmgr
                        .received_getheaders(&addr, (locator_hashes, stop_hash));
                } else if let NetworkMessage::Inv(inventory) = msg.payload {
                    // Receive an `inv` message. This will happen if we are out of sync with a
                    // peer. And blocks are being announced. Otherwise, we expect to receive a
                    // `headers` message.
                    return self.syncmgr.received_inv(addr, inventory, &self.clock);
                } else if let NetworkMessage::GetAddr = msg.payload {
                    // TODO: Use `sample` here when it returns an iterator.
                    let addrs = self
                        .addrmgr
                        .iter()
                        // Don't send the peer their own address, nor non-TCP addresses.
                        .filter(|a| a.socket_addr().map_or(false, |s| s != addr))
                        .take(MAX_GETADDR_ADDRESSES)
                        // TODO: Return a non-zero time value.
                        .map(|a| (0, a.clone()))
                        .collect();

                    self.outbound.message(addr, NetworkMessage::Addr(addrs));
                    return;
                }
            }
            PeerState::Disconnecting => {
                debug!(
                    target: self.target, "Ignoring {} from peer {} (disconnecting)",
                    cmd, peer.address
                );
            }
        }

        debug!(target: self.target, "{}: Ignoring {:?}", peer.address, cmd);
    }

    fn version(
        &self,
        addr: net::SocketAddr,
        local_addr: net::SocketAddr,
        nonce: u64,
        start_height: Height,
    ) -> NetworkMessage {
        let start_height = start_height as i32;
        let timestamp = self.clock.local_time().block_time() as i64;

        NetworkMessage::Version(VersionMessage {
            // Our max supported protocol version.
            version: self.protocol_version,
            // Local services.
            services: self.services,
            // Local time.
            timestamp,
            // Receiver address, as perceived by us.
            receiver: Address::new(&addr, ServiceFlags::NETWORK | ServiceFlags::COMPACT_FILTERS),
            // Local address (unreliable) and local services (same as `services` field)
            sender: Address::new(&local_addr, self.services),
            // A nonce to detect connections to self.
            nonce,
            // Our user agent string.
            user_agent: self.user_agent.to_owned(),
            // Our best height.
            start_height,
            // Whether we want to receive transaction `inv` messages.
            relay: false,
        })
    }

    fn disconnect(&mut self, addr: PeerId) {
        let peer = self
            .peers
            .get_mut(&addr)
            .unwrap_or_else(|| panic!("peer {} is not known", addr));

        peer.transition(PeerState::Disconnecting);
        self.connmgr.disconnect(addr);
    }
}
