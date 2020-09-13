//! Bitcoin protocol state machine.
#![warn(missing_docs)]
use crossbeam_channel as chan;
use log::*;
use nonempty::NonEmpty;

pub mod addrmgr;
pub mod network;
pub mod syncmgr;
pub use network::Network;

#[cfg(test)]
mod tests;

use addrmgr::AddressManager;
use syncmgr::SyncManager;

use crate::address_book::AddressBook;
use crate::event::Event;
use crate::protocol::{self, Link, Message, Out, PeerId, Protocol, Timeout, TimeoutSource};

use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::net;

use bitcoin::consensus::params::Params;
use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
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
/// Maximum time adjustment between network and local time (70 minutes).
pub const MAX_TIME_ADJUSTMENT: TimeOffset = 70 * 60;
/// Maximum number of headers sent in a `headers` message.
pub const MAX_MESSAGE_HEADERS: usize = 2000;
/// Maximum number of addresses to return when receiving a `getaddr` message.
pub const MAX_GETADDR_ADDRESSES: usize = 8;
/// Maximum number of latencies recorded per peer.
pub const MAX_RECORDED_LATENCIES: usize = 64;
/// Maximum height difference for a stale peer, to maintain the connection (2 weeks).
pub const MAX_STALE_HEIGHT_DIFFERENCE: Height = 2016;
/// Time to wait for response during peer handshake before disconnecting the peer.
pub const HANDSHAKE_TIMEOUT: LocalDuration = LocalDuration::from_secs(10);
/// Time interval to wait between sent pings.
pub const PING_INTERVAL: LocalDuration = LocalDuration::from_mins(2);
/// Time to wait to receive a pong when sending a ping.
pub const PING_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);
/// Time to wait for a new connection.
pub const CONNECTION_TIMEOUT: LocalDuration = LocalDuration::from_secs(3);
/// Target number of concurrent outbound peer connections.
pub const TARGET_OUTBOUND_PEERS: usize = 8;
/// Maximum number of inbound peer connections.
pub const MAX_INBOUND_PEERS: usize = 16;

/// A time offset, in seconds.
pub type TimeOffset = i64;

/// Block locators. Consists of starting hashes and a stop hash.
pub type Locators = (Vec<BlockHash>, BlockHash);

/// Input into the state machine.
pub type Input = protocol::Input<RawNetworkMessage, Command>;

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

/// Output of the protocol state machine.
type Output = std::vec::IntoIter<Out<RawNetworkMessage>>;

/// Used to construct a protocol output.
pub struct OutputBuilder<M: Message> {
    /// Output queue.
    queue: Vec<Out<M>>,
    /// Network magic number.
    builder: message::Builder,
}

impl<M: Message> OutputBuilder<M> {
    /// Create a new output builder.
    pub fn new(network: Network) -> Self {
        Self {
            queue: Vec::new(),
            builder: message::Builder::new(network.magic()),
        }
    }

    /// Push an output to the queue.
    fn push(&mut self, output: Out<M>) {
        self.queue.push(output);
    }

    /// Push a request to the queue.
    fn request(&mut self, addr: PeerId, message: M, source: TimeoutSource, timeout: LocalDuration) {
        self.push(Out::Message(addr, message));
        self.push(Out::SetTimeout(source, timeout));
    }

    /// Push a message to the queue.
    fn message(&mut self, addr: PeerId, message: M::Payload) {
        self.push(self.builder.message(addr, message));
    }

    /// Push an event to the queue.
    fn event(&mut self, event: Event<M::Payload>) {
        self.push(Out::Event(event));
    }

    /// Extend the queue with a list of outputs.
    fn extend<T: IntoIterator<Item = Out<M>>>(&mut self, outputs: T) {
        self.queue.extend(outputs);
    }

    /// Consume the builder and return an iterator over the outputs.
    fn finish(self) -> std::vec::IntoIter<Out<M>> {
        self.queue.into_iter()
    }
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

    pub struct Builder {
        magic: u32,
    }

    impl Builder {
        pub fn new(magic: u32) -> Self {
            Builder { magic }
        }

        pub fn message<M: Message>(&self, addr: net::SocketAddr, payload: M::Payload) -> Out<M> {
            Out::Message(addr, M::from_parts(payload, self.magic))
        }
    }

    pub fn get_headers((locator_hashes, stop_hash): Locators, version: u32) -> NetworkMessage {
        NetworkMessage::GetHeaders(GetHeadersMessage {
            version,
            // Starting hashes, highest heights first.
            locator_hashes,
            // Using the zero hash means *fetch as many blocks as possible*.
            stop_hash,
        })
    }

    pub fn raw(payload: NetworkMessage, magic: u32) -> RawNetworkMessage {
        RawNetworkMessage { magic, payload }
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
    /// Target number of outbound peer connections.
    target_outbound_peers: usize,
    /// Maximum number of inbound peer connections.
    max_inbound_peers: usize,
    /// Peer address manager.
    addrmgr: AddressManager,
    /// Blockchain synchronization manager.
    syncmgr: SyncManager<T>,
    /// Network-adjusted clock.
    clock: AdjustedTime<PeerId>,
    /// Set of connected peers that have completed the handshake.
    ready: HashSet<PeerId>,
    /// Set of all connected peers.
    connected: HashSet<PeerId>,
    /// Set of disconnected peers.
    disconnected: HashSet<PeerId>,
    /// Informational name of this protocol instance. Used for logging purposes only.
    target: &'static str,
    /// Random number generator.
    rng: fastrand::Rng,
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
            target_outbound_peers: TARGET_OUTBOUND_PEERS,
            max_inbound_peers: MAX_INBOUND_PEERS,
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
    pub fn new(tree: T, clock: AdjustedTime<PeerId>, rng: fastrand::Rng, config: Config) -> Self {
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

        let addrmgr = AddressManager::from(address_book, rng.clone());
        let syncmgr = SyncManager::new(
            tree,
            syncmgr::Config {
                max_message_headers: MAX_MESSAGE_HEADERS,
                request_timeout: syncmgr::REQUEST_TIMEOUT,
                params: params.clone(),
            },
            rng.clone(),
        );

        Self {
            peers: HashMap::with_hasher(rng.clone().into()),
            network,
            services,
            protocol_version,
            target_outbound_peers,
            max_inbound_peers,
            user_agent,
            whitelist,
            target,
            params,
            clock,
            height,
            addrmgr,
            syncmgr,
            rng,
            ready: HashSet::new(),
            connected: HashSet::new(),
            disconnected: HashSet::new(),
        }
    }

    fn connected(
        &mut self,
        addr: PeerId,
        local_addr: net::SocketAddr,
        nonce: u64,
        link: Link,
    ) -> Out<RawNetworkMessage> {
        self.connected.insert(addr);
        self.disconnected.remove(&addr);
        self.addrmgr.peer_connected(&addr);

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
        Out::SetTimeout(TimeoutSource::Handshake(addr), HANDSHAKE_TIMEOUT)
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
    /// The peer's best block.
    tip: BlockHash,
    /// An offset in seconds, between this peer's clock and ours.
    /// A positive offset means the peer's clock is ahead of ours.
    time_offset: TimeOffset,
    /// Whether this is an inbound or outbound peer connection.
    link: Link,
    /// Peer state.
    state: PeerState,
    /// Nonce and time the last ping was sent to this peer.
    last_ping: Option<(u64, LocalTime)>,
    /// Observed round-trip latencies for this peer.
    latencies: VecDeque<LocalDuration>,
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
            tip: BlockHash::default(),
            time_offset: 0,
            state,
            link,
            last_ping: None,
            latencies: VecDeque::new(),
            services: ServiceFlags::NONE,
            user_agent: String::default(),
            ctx,
            nonce,
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
        self.link == Link::Outbound
    }

    fn ping(&mut self, local_time: LocalTime) -> NetworkMessage {
        let nonce = self.rng.u64(..);
        self.last_ping = Some((nonce, local_time));

        NetworkMessage::Ping(nonce)
    }

    /// Calculate the average latency of this peer.
    #[allow(dead_code)]
    fn latency(&self) -> LocalDuration {
        let sum: LocalDuration = self.latencies.iter().sum();

        sum / self.latencies.len() as u32
    }

    fn record_latency(&mut self, sample: LocalDuration) {
        self.latencies.push_front(sample);
        self.latencies.truncate(MAX_RECORDED_LATENCIES);
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
    const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_mins(10);

    type Command = self::Command;
    type Output = self::Output;

    fn initialize(&mut self, time: LocalTime) -> Self::Output {
        let mut out = OutputBuilder::new(self.network);

        self.clock.set_local_time(time);
        self.syncmgr.initialize(time);

        self.drain_sync_requests(&mut out);

        // FIXME: Should be random
        let addrs = self
            .addrmgr
            .iter()
            .take(self.target_outbound_peers)
            .map(|a| a.socket_addr().ok())
            .flatten()
            .collect::<Vec<_>>();

        for addr in addrs {
            self.connect(&addr, &mut out);
        }

        out.push(Out::SetTimeout(TimeoutSource::Global, Self::IDLE_TIMEOUT));
        out.finish()
    }

    fn step(&mut self, input: Input, local_time: LocalTime) -> Self::Output {
        let mut out = OutputBuilder::new(self.network);

        // The local time is set from outside the protocol.
        self.clock.set_local_time(local_time);

        // Generate `Event` outputs from the input.
        self.generate_events(&input, &mut out);

        match input {
            Input::Connected {
                addr,
                local_addr,
                link,
            } => {
                info!(target: self.target, "{}: Peer connected ({:?})", &addr, link);
                debug_assert!(!self.connected.contains(&addr));

                // This is usually not that useful, except when our local address is actually the
                // address our peers see.
                self.addrmgr.record_local_addr(local_addr);

                match link {
                    Link::Inbound if self.connected.len() >= self.max_inbound_peers => {
                        // Don't allow inbound connections beyond the configured limit.
                        debug!(
                            target: self.target,
                            "{}: Disconnecting: reached target peer connections ({})",
                            addr, self.target_outbound_peers
                        );
                        out.push(Out::Disconnect(addr));
                    }
                    Link::Inbound => out.push(self.connected(addr, local_addr, 0, link)),
                    Link::Outbound => {
                        let nonce = self.rng.u64(..);

                        out.push(self.connected(addr, local_addr, nonce, link));
                        out.message(addr, self.version(addr, local_addr, nonce, self.height));
                    }
                };
            }
            Input::Disconnected(addr) => {
                debug!(target: self.target, "Disconnected from {}", &addr);
                debug_assert!(self.connected.contains(&addr));
                debug_assert!(!self.disconnected.contains(&addr));

                let peer = self
                    .peers
                    .get(&addr)
                    .expect("disconnected peers should be known");

                let link = peer.link;

                self.peers.remove(&addr);
                self.ready.remove(&addr);
                self.connected.remove(&addr);
                self.disconnected.insert(addr);
                self.syncmgr.peer_disconnected(&addr);
                self.addrmgr.peer_disconnected(&addr);

                // If an outbound peer disconnected, we should make sure to maintain
                // our target outbound connection count.
                if link == Link::Outbound {
                    self.maintain_outbound_peers(&mut out);
                }
            }
            Input::Received(addr, msg) => {
                out.extend(self.receive(addr, msg));
            }
            Input::Sent(_addr, _msg) => {}
            Input::Command(cmd) => {
                match cmd {
                    Command::Connect(addr) => {
                        debug!(target: self.target, "Received command: Connect({})", addr);

                        self.whitelist.addr.insert(addr.ip());

                        if !self.connected.contains(&addr) {
                            self.connect(&addr, &mut out);
                        }
                    }
                    Command::Disconnect(addr) => {
                        debug!(target: self.target, "Received command: Disconnect({})", addr);

                        if self.connected.contains(&addr) {
                            out.push(Out::Disconnect(addr));
                        }
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

                                out.message(p.address, msg);
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
                            out.message(peer.address, msg.clone());
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

                        out.message(peer, NetworkMessage::Tx(tx));
                    }
                    Command::Shutdown => {
                        out.push(Out::Shutdown);
                    }
                }
            }
            Input::Timeout(source) => {
                trace!(target: self.target, "Received timeout for {:?}", source);

                // We may not have the peer anymore, if it was disconnected and remove in
                // the meantime.
                match source {
                    TimeoutSource::Connect(addr) => {
                        debug_assert!(!self.peers.contains_key(&addr));
                        debug_assert!(!self.connected.contains(&addr));

                        self.maintain_outbound_peers(&mut out);
                    }
                    TimeoutSource::Handshake(addr) => {
                        if let Some(peer) = self.peers.get_mut(&addr) {
                            match peer.state {
                                PeerState::Handshake(Handshake::AwaitingVerack)
                                | PeerState::Handshake(Handshake::AwaitingVersion) => {
                                    out.push(Out::Disconnect(addr));
                                }
                                _ => {}
                            }
                        } else {
                            debug!(target: self.target, "Peer {} no longer exists (ignoring)", addr);
                        }
                    }
                    TimeoutSource::Synch(addr) => {
                        let timeout = self.syncmgr.received_timeout(addr, local_time);

                        if let syncmgr::OnTimeout::Disconnect = timeout {
                            out.push(Out::Disconnect(addr));
                        }
                    }
                    TimeoutSource::Ping(addr) => {
                        if let Some(peer) = self.peers.get_mut(&addr) {
                            if let PeerState::Ready { last_active, .. } = peer.state {
                                let now = self.clock.local_time();

                                if let Some((_, last_ping)) = peer.last_ping {
                                    // A ping was sent and we're waiting for a `pong`. If too much
                                    // time has passed, we consider this peer dead, and disconnect
                                    // from them.
                                    if now - last_ping >= PING_TIMEOUT {
                                        let address = peer.address;

                                        log::debug!(
                                            target: self.target, "{}: Disconnecting (ping timeout)",

                                            address
                                        );
                                        self.disconnect(&address, &mut out);
                                    }
                                } else if now - last_active >= PING_INTERVAL {
                                    let ping = peer.ping(now);

                                    out.push(Out::Message(
                                        peer.address,
                                        RawNetworkMessage {
                                            magic: self.network.magic(),
                                            payload: ping,
                                        },
                                    ));
                                    out.push(Out::SetTimeout(
                                        TimeoutSource::Ping(addr),
                                        PING_TIMEOUT,
                                    ));
                                    out.push(Out::SetTimeout(
                                        TimeoutSource::Ping(addr),
                                        PING_INTERVAL,
                                    ));
                                }
                            }
                        }
                    }
                    TimeoutSource::Global => {
                        debug!(target: self.target, "Tick: local_time = {}", local_time);

                        self.syncmgr.tick(local_time);

                        out.push(Out::SetTimeout(TimeoutSource::Global, Self::IDLE_TIMEOUT));
                    }
                }
            }
        };
        self.drain_sync_events(&mut out);
        self.drain_sync_requests(&mut out);
        self.drain_sync_responses(&mut out);

        self.drain_addr_events(&mut out);

        out.finish()
    }
}

impl<T: BlockTree> Bitcoin<T> {
    fn drain_sync_requests(&mut self, out: &mut OutputBuilder<RawNetworkMessage>) {
        let magic = self.network.magic();

        for req in self.syncmgr.requests() {
            let syncmgr::GetHeaders {
                addr,
                locators,
                timeout,
                ..
            } = req;

            out.request(
                addr,
                message::raw(message::get_headers(locators, self.protocol_version), magic),
                TimeoutSource::Synch(addr),
                timeout,
            );
        }
    }

    fn drain_sync_responses(&mut self, out: &mut OutputBuilder<RawNetworkMessage>) {
        for resp in self.syncmgr.responses() {
            let syncmgr::SendHeaders { addrs, headers } = resp;

            if !addrs.is_empty() {
                debug!(
                    target: self.target,
                    "Sending {} header(s) to {} peer(s)..",
                    headers.len(),
                    addrs.len()
                );
            }

            for addr in addrs {
                out.message(addr, NetworkMessage::Headers(headers.clone()));
            }
        }
    }

    /// Send a `getaddr` message to all our outbound peers.
    fn get_addresses(&self) -> Output {
        let mut out = OutputBuilder::new(self.network);

        for p in self.outbound() {
            out.message(p.address, NetworkMessage::GetAddr);
        }
        out.finish()
    }

    /// Get outbound & ready peers.
    fn outbound(&self) -> impl Iterator<Item = &Peer> + Clone {
        self.peers
            .values()
            .filter(|p| p.is_ready() && p.is_outbound())
    }

    /// Attempt to maintain a certain number of outbound peers.
    fn maintain_outbound_peers(&mut self, out: &mut OutputBuilder<RawNetworkMessage>) {
        let current = self.outbound().count();

        if current < self.target_outbound_peers {
            // FIXME: This debug statement shouldn't be here. Listen on events instead.
            debug!(
                target: self.target, "Peer outbound connections ({}) below target ({})",
                current, self.target_outbound_peers
            );
            out.event(Event::Connecting(current, self.target_outbound_peers));

            if let Some(addr) = self.addrmgr.sample() {
                if let Ok(sockaddr) = addr.socket_addr() {
                    debug_assert!(!self.connected.contains(&sockaddr));

                    self.connect(&sockaddr, out);
                } else {
                    // TODO: Perhaps the address manager should just return addresses
                    // that can be converted to socket addresses?
                    // The only ones that cannot are Tor addresses.
                    todo!();
                }
            } else {
                debug!(target: self.target, "Address book exhausted, asking peers..");

                // We're out of addresses, ask for more!
                out.extend(self.get_addresses());
            }
        }
    }

    fn receive(&mut self, addr: PeerId, msg: RawNetworkMessage) -> Vec<Out<RawNetworkMessage>> {
        let builder = message::Builder::new(self.network.magic());
        let now = self.clock.local_time();
        let cmd = msg.cmd();

        if msg.magic != self.network.magic() {
            // TODO: Needs test.
            debug!(
                target: self.target, "{}: Received message with invalid magic: {}",
                addr, msg.magic
            );
            return vec![Out::Disconnect(addr)];
        }

        let mut peer = if let Some(peer) = self.peers.get_mut(&addr) {
            peer
        } else {
            debug!(target: self.target, "Received {:?} from unknown peer {}", cmd, addr);
            return vec![];
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
        }

        if peer.is_ready() {
            if let NetworkMessage::Ping(nonce) = msg.payload {
                return vec![builder.message(addr, NetworkMessage::Pong(nonce))];
            } else if let NetworkMessage::Pong(nonce) = msg.payload {
                match peer.last_ping {
                    Some((last_nonce, last_time)) if nonce == last_nonce => {
                        peer.record_latency(now - last_time);
                        peer.last_ping = None;
                    }
                    // Unsolicited or redundant `pong`. Ignore.
                    _ => {}
                }
                return vec![];
            } else if let NetworkMessage::Addr(addrs) = msg.payload {
                if addrs.is_empty() {
                    // Peer misbehaving, got empty message.
                    return vec![];
                }
                let mut out = OutputBuilder::new(self.network);

                if self
                    .addrmgr
                    .insert(addrs.into_iter(), addrmgr::Source::Peer(addr))
                {
                    self.maintain_outbound_peers(&mut out);
                }
                return out.finish().collect();
            } else if let NetworkMessage::Headers(headers) = msg.payload {
                return self.receive_headers(addr, headers);
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
                        return vec![Out::Disconnect(addr)];
                    }
                    // Peers that don't advertise the `NETWORK` service are not full nodes.
                    // It's not so useful for us to connect to them, because they're likely
                    // to be less secure.
                    if peer.link == Link::Outbound
                        && !services.has(ServiceFlags::NETWORK)
                        && !services.has(ServiceFlags::NETWORK_LIMITED)
                        && !whitelisted
                    {
                        debug!(
                            target: self.target, "{}: Disconnecting: peer doesn't have required services",
                            addr
                        );
                        return vec![Out::Disconnect(addr)];
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
                        return vec![Out::Disconnect(addr)];
                    }
                    // Check for self-connections. We only need to check one link direction,
                    // since in the case of a self-connection, we will see both link directions.
                    for (_, peer) in self.peers.iter() {
                        if peer.link == Link::Outbound && peer.nonce == nonce {
                            debug!(
                                target: self.target, "{}: Disconnecting: detected self-connection",
                                addr
                            );
                            return vec![Out::Disconnect(addr)];
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
                            return vec![Out::SetTimeout(
                                TimeoutSource::Handshake(addr),
                                HANDSHAKE_TIMEOUT,
                            )]
                        }
                        Link::Inbound => {
                            let nonce = peer.nonce;

                            return vec![
                                builder.message(
                                    addr,
                                    self.version(addr, local_addr, nonce, self.height),
                                ),
                                builder.message(addr, NetworkMessage::Verack),
                                Out::SetTimeout(TimeoutSource::Handshake(addr), HANDSHAKE_TIMEOUT),
                            ];
                        }
                    }
                } else {
                    // TODO: Include disconnect reason.
                    debug!(target: self.target, "{}: Peer misbehaving", addr);
                    return vec![Out::Disconnect(addr)];
                }
            }
            PeerState::Handshake(Handshake::AwaitingVerack) => {
                if msg.payload == NetworkMessage::Verack {
                    peer.receive_verack(now);

                    self.ready.insert(addr);
                    self.clock.record_offset(addr, peer.time_offset);
                    self.addrmgr.peer_negotiated(&addr, peer.services, now);

                    let ping = peer.ping(now);
                    let link = peer.link;

                    let mut out = Vec::new();

                    out.extend(match link {
                        Link::Outbound => vec![
                            builder.message(addr, NetworkMessage::Verack),
                            builder.message(addr, NetworkMessage::SendHeaders),
                            builder.message(addr, NetworkMessage::GetAddr),
                            builder.message(addr, ping),
                        ],
                        Link::Inbound => vec![
                            builder.message(addr, NetworkMessage::SendHeaders),
                            builder.message(addr, ping),
                        ],
                    });

                    self.syncmgr.peer_negotiated(
                        peer.address,
                        peer.height,
                        peer.tip,
                        peer.services,
                        peer.link,
                        &self.clock,
                    );

                    return out;
                } else {
                    // TODO: Include disconnect reason.
                    debug!(target: self.target, "{}: Peer misbehaving", addr);
                    return vec![Out::Disconnect(addr)];
                }
            }
            PeerState::Ready { .. } => {
                if let NetworkMessage::GetHeaders(GetHeadersMessage {
                    locator_hashes,
                    stop_hash,
                    ..
                }) = msg.payload
                {
                    if let Some(syncmgr::SendHeaders { addrs, headers }) =
                        self.syncmgr.received_getheaders(
                            &addr,
                            (locator_hashes, stop_hash),
                            MAX_MESSAGE_HEADERS,
                        )
                    {
                        return addrs
                            .into_iter()
                            .map(|a| builder.message(a, NetworkMessage::Headers(headers.clone())))
                            .collect::<Vec<_>>();
                    }
                } else if let NetworkMessage::Inv(inventory) = msg.payload {
                    return self.receive_inv(addr, inventory);
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
                    return vec![builder.message(addr, NetworkMessage::Addr(addrs))];
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

        vec![]
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

    /// Receive an `inv` message. This will happen if we are out of sync with a peer. And blocks
    /// are being announced. Otherwise, we expect to receive a `headers` message.
    fn receive_inv(&mut self, addr: PeerId, inv: Vec<Inventory>) -> Vec<Out<RawNetworkMessage>> {
        self.syncmgr.received_inv(addr, inv, &self.clock);
        return vec![];
    }

    fn receive_headers(
        &mut self,
        addr: PeerId,
        headers: Vec<BlockHeader>,
    ) -> Vec<Out<RawNetworkMessage>> {
        let headers = if let Some(headers) = NonEmpty::from_vec(headers) {
            headers
        } else {
            return vec![];
        };

        debug!(
            target: self.target, "{}: Received {} header(s)",

            addr,
            headers.len()
        );
        self.syncmgr.received_headers(&addr, headers, &self.clock);

        vec![]
    }

    fn drain_sync_events(&mut self, outbound: &mut OutputBuilder<RawNetworkMessage>) {
        for event in self.syncmgr.events() {
            debug!(target: self.target, "[sync] {}", &event);

            match &event {
                syncmgr::Event::ReceivedInvalidHeaders(_, _) => {
                    // TODO: Bad blocks received!
                    // TODO: Disconnect peer.
                }
                syncmgr::Event::HeadersImported(import_result) => {
                    debug!(target: self.target, "Import result: {:?}", &import_result);

                    if let ImportResult::TipChanged(tip, height, _) = import_result {
                        info!(target: self.target, "Chain height = {}, tip = {}", height, tip);
                    }
                }
                _ => {}
            }
            outbound.push(Out::Event(Event::SyncManager(event)));
        }
    }

    fn drain_addr_events(&mut self, outbound: &mut OutputBuilder<RawNetworkMessage>) {
        for event in self.addrmgr.events() {
            outbound.push(Out::Event(Event::AddrManager(event)));
        }
    }

    fn disconnect(&mut self, addr: &PeerId, out: &mut OutputBuilder<RawNetworkMessage>) {
        let peer = self
            .peers
            .get_mut(&addr)
            .unwrap_or_else(|| panic!("peer {} is not known", addr));

        peer.transition(PeerState::Disconnecting);
        out.push(Out::Disconnect(*addr));
    }

    fn connect(&mut self, addr: &PeerId, out: &mut OutputBuilder<RawNetworkMessage>) {
        out.push(Out::Connect(*addr, CONNECTION_TIMEOUT));

        self.addrmgr.peer_attempted(&addr, self.clock.local_time());
    }

    fn generate_events(&self, input: &Input, out: &mut OutputBuilder<RawNetworkMessage>) {
        match input {
            Input::Connected { addr, link, .. } => {
                out.push(Out::Event(Event::Connected(*addr, *link)));
            }
            Input::Disconnected(addr) => {
                out.push(Out::Event(Event::Disconnected(*addr)));
            }
            Input::Received(addr, msg) => {
                out.push(Out::Event(Event::Received(*addr, msg.payload.clone())));
            }
            _ => {}
        }
    }
}
