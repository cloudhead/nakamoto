use crossbeam_channel as chan;
use log::*;

pub mod addrmgr;
pub mod network;
pub use network::Network;

use addrmgr::AddressManager;

use crate::address_book::AddressBook;
use crate::event::Event;
use crate::protocol::{Input, Link, Message, Output, PeerId, Protocol};

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::net;

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::network::message_network::VersionMessage;
use bitcoin::util::hash::BitcoinHash;

use nakamoto_chain::block::time::{AdjustedTime, LocalDuration, LocalTime};
use nakamoto_chain::block::tree::{self, BlockTree, ImportResult};
use nakamoto_chain::block::{BlockHash, BlockHeader, Height};

/// Peer-to-peer protocol version.
/// For now, we only support `70012`, due to lacking `sendcmpct` support.
pub const PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &str = "/nakamoto:0.0.0/";
/// Minimum number of peers to be connected to.
pub const PEER_CONNECTION_THRESHOLD: usize = 1;
/// Maximum time adjustment between network and local time (70 minutes).
pub const MAX_TIME_ADJUSTMENT: TimeOffset = 70 * 60;
/// Maximum number of headers sent in a `headers` message.
pub const MAX_MESSAGE_HEADERS: usize = 2000;
/// Maximum number of latencies recorded per peer.
pub const MAX_RECORDED_LATENCIES: usize = 64;
/// Maximum height difference for a stale peer, to maintain the connection (2 weeks).
pub const MAX_STALE_HEIGHT_DIFFERENCE: Height = 2016;

/// A time offset, in seconds.
pub type TimeOffset = i64;

/// A command or request that can be sent to the protocol.
#[derive(Debug, Clone)]
pub enum Command {
    GetTip(chan::Sender<BlockHeader>),
    GetBlock(BlockHash),
    Connect(net::SocketAddr),
    Receive(net::SocketAddr, NetworkMessage),
    ImportHeaders(
        Vec<BlockHeader>,
        chan::Sender<Result<ImportResult, tree::Error>>,
    ),
}

impl Message for RawNetworkMessage {
    type Payload = NetworkMessage;

    fn payload(&self) -> &Self::Payload {
        &self.payload
    }

    fn display(&self) -> &'static str {
        self.payload.cmd()
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////

/// An instantiation of `Protocol`, for the Bitcoin P2P network. Parametrized over the
/// block-tree.
#[derive(Debug)]
pub struct Bitcoin<T> {
    /// Peer states.
    pub peers: HashMap<PeerId, Peer>,
    /// Protocol state.
    pub state: State,
    /// Block tree.
    pub tree: T,
    /// Bitcoin network we're connecting to.
    pub network: network::Network,
    /// Services offered by us.
    pub services: ServiceFlags,
    /// Our protocol version.
    pub protocol_version: u32,
    /// Whether or not we should relay transactions.
    pub relay: bool,
    /// Our user agent.
    pub user_agent: &'static str,

    /// Peer address manager.
    addrmgr: AddressManager,
    /// Network-adjusted clock.
    clock: AdjustedTime<PeerId>,
    /// Set of connected peers that have completed the handshake.
    ready: HashSet<PeerId>,
    /// Set of all connected peers.
    connected: HashSet<PeerId>,
    /// Set of disconnected peers.
    disconnected: HashSet<PeerId>,
    /// Informational name of this protocol instance. Used for logging purposes only.
    name: &'static str,
}

/// Protocol state.
#[derive(Debug, PartialEq, Eq)]
pub enum State {
    /// Connecting to the network. Syncing hasn't started yet.
    Connecting,
    /// We're out of sync, and syncing with the designated peer.
    Syncing(PeerId),
    /// We're in sync.
    Synced,
}

/// Peer config.
#[derive(Debug, Clone)]
pub struct Config {
    pub network: network::Network,
    pub address_book: AddressBook,
    pub services: ServiceFlags,
    pub protocol_version: u32,
    pub relay: bool,
    pub user_agent: &'static str,
    pub name: &'static str,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: network::Network::Mainnet,
            address_book: AddressBook::default(),
            services: ServiceFlags::NONE,
            protocol_version: PROTOCOL_VERSION,
            relay: false,
            user_agent: USER_AGENT,
            name: "self",
        }
    }
}

impl Config {
    pub fn from(name: &'static str, network: network::Network, address_book: AddressBook) -> Self {
        Self {
            network,
            address_book,
            name,
            ..Self::default()
        }
    }

    pub fn port(&self) -> u16 {
        self.network.port()
    }
}

impl<T: BlockTree> Bitcoin<T> {
    const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);

    pub fn new(tree: T, clock: AdjustedTime<PeerId>, config: Config) -> Self {
        let Config {
            network,
            address_book,
            services,
            protocol_version,
            relay,
            user_agent,
            name,
        } = config;

        let addrmgr = AddressManager::from(address_book);

        Self {
            peers: HashMap::new(),
            state: State::Connecting,
            tree,
            network,
            services,
            protocol_version,
            relay,
            user_agent,
            name,
            clock,
            addrmgr,
            ready: HashSet::new(),
            connected: HashSet::new(),
            disconnected: HashSet::new(),
        }
    }

    fn connected(&mut self, addr: PeerId, local_addr: net::SocketAddr, link: Link) -> u64 {
        self.connected.insert(addr);
        self.disconnected.remove(&addr);

        let nonce = fastrand::u64(..);

        // TODO: Handle case where peer already exists.
        self.peers.insert(
            addr,
            Peer::new(
                addr,
                local_addr,
                PeerState::Handshake(Handshake::default()),
                nonce,
                link,
                self.name,
            ),
        );

        nonce
    }

    /// Start syncing with the given peer.
    pub fn sync(&mut self, addr: PeerId, local_time: LocalTime) -> Vec<Output<RawNetworkMessage>> {
        let mut outs = self
            .transition(State::Syncing(addr))
            .into_iter()
            .map(Output::from)
            .collect::<Vec<Output<_>>>();

        let locator_hashes = self.locator_hashes();
        let get_headers = self.message(
            addr,
            self.get_headers(locator_hashes.clone(), BlockHash::default()),
        );
        let peer = self.peers.get_mut(&addr).unwrap();

        peer.syncing(Syncing::AwaitingHeaders(locator_hashes, local_time));

        outs.push(get_headers);
        outs
    }

    /// Check whether or not we are in sync with the network.
    /// TODO: Should return the minimum peer height, so that we can
    /// keep track of it in our state, while syncing to it.
    pub fn is_synced(&self) -> bool {
        let height = self.tree.height();

        // TODO: Check actual block hashes once we are caught up on height.
        if let Some(peer_height) = self
            .peers
            .values()
            .filter(|p| p.is_ready())
            .map(|p| p.height)
            .min()
        {
            height >= peer_height
        } else {
            true
        }
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
pub enum Handshake {
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

/// Synchronization states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Syncing {
    /// Not currently syncing. This is usually the starting and end state.
    Idle,
    /// Syncing. A `getheaders` message was sent and we are expecting a response.
    AwaitingHeaders(Vec<BlockHash>, LocalTime),
}

/// State of a remote peer.
#[derive(Debug, PartialEq, Eq)]
pub enum PeerState {
    /// Handshake is in progress.
    Handshake(Handshake),
    /// Handshake was completed successfully. This peer is ready to receive messages.
    Ready {
        /// Last time this peer was active. This is set every time we receive
        /// a message from this peer.
        last_active: LocalTime,
        /// Syncing state.
        syncing: Syncing,
    },
    /// The peer is being disconnected.
    Disconnecting,
}

/// A remote peer.
/// TODO: It should be possible to statically enforce the state-machine rules.
/// Eg. `Peer<State>`.
#[derive(Debug)]
pub struct Peer {
    /// Remote peer address.
    pub address: net::SocketAddr,
    /// Local peer address.
    pub local_address: net::SocketAddr,
    /// The peer's best height.
    pub height: Height,
    /// The peer's best block.
    pub tip: BlockHash,
    /// An offset in seconds, between this peer's clock and ours.
    /// A positive offset means the peer's clock is ahead of ours.
    pub time_offset: TimeOffset,
    /// Whether this is an inbound or outbound peer connection.
    pub link: Link,
    /// Peer state.
    pub state: PeerState,
    /// Nonce and time the last ping was sent to this peer.
    pub last_ping: Option<(u64, LocalTime)>,
    /// Observed round-trip latencies for this peer.
    pub latencies: VecDeque<LocalDuration>,
    /// Peer nonce. Used to detect self-connections.
    pub nonce: u64,
    /// Informational context for this peer. Used for logging purposes only.
    pub ctx: &'static str,
}

impl Peer {
    pub fn new(
        address: net::SocketAddr,
        local_address: net::SocketAddr,
        state: PeerState,
        nonce: u64,
        link: Link,
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
            ctx,
            nonce,
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self.state, PeerState::Ready { .. })
    }

    fn is_idle(&self) -> bool {
        matches!(self.state, PeerState::Ready { syncing: Syncing::Idle, .. })
    }

    fn is_inbound(&self) -> bool {
        self.link == Link::Inbound
    }

    #[allow(dead_code)]
    fn is_outbound(&self) -> bool {
        self.link == Link::Outbound
    }

    fn ping(&mut self, local_time: LocalTime) -> NetworkMessage {
        let nonce = fastrand::u64(..);
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
        self.transition(PeerState::Ready {
            last_active: time,
            syncing: Syncing::Idle,
        });
    }

    fn transition(&mut self, state: PeerState) {
        if state == self.state {
            return;
        }
        debug!(
            "[{}] {}: {:?} -> {:?}",
            self.ctx, self.address, self.state, state
        );

        self.state = state;
    }

    fn syncing(&mut self, state: Syncing) {
        if let PeerState::Ready {
            ref mut syncing, ..
        } = self.state
        {
            if syncing == &state {
                return;
            }
            debug!(
                "[{}] {}: Syncing: {:?} -> {:?}",
                self.ctx, self.address, syncing, state
            );

            *syncing = state;
        }
    }
}

impl<T: BlockTree> Protocol<RawNetworkMessage> for Bitcoin<T> {
    const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_secs(60 * 5);
    const PING_INTERVAL: LocalDuration = LocalDuration::from_secs(60);

    type Command = self::Command;

    fn initialize(&mut self, time: LocalTime) -> Vec<Output<RawNetworkMessage>> {
        self.clock.set_local_time(time);

        let mut outbound = Vec::new();

        // FIXME: We should fetch multiple addresses.
        if let Some(addr) = self.addrmgr.sample() {
            if let Ok(addr) = addr.socket_addr() {
                outbound.push(Output::Connect(addr));
            }
        }
        outbound
    }

    fn step(
        &mut self,
        input: Input<RawNetworkMessage, Command>,
        time: LocalTime,
    ) -> Vec<Output<RawNetworkMessage>> {
        let mut outbound = Vec::new();

        // The local time is set from outside the protocol.
        self.clock.set_local_time(time);
        // Generate `Event` outputs from the input.
        self.generate_events(&input, &mut outbound);

        match input {
            Input::Connected {
                addr,
                local_addr,
                link,
            } => {
                let nonce = self.connected(addr, local_addr, link);

                match link {
                    Link::Outbound => {
                        info!("[{}] {}: Peer connected (outbound)", self.name, &addr);

                        outbound.push(self.message(
                            addr,
                            self.version(addr, local_addr, nonce, self.tree.height()),
                        ));
                    }
                    Link::Inbound => {
                        info!("[{}] {}: Peer connected (inbound)", self.name, &addr);
                    } // Wait to receive remote version.
                }
            }
            Input::Disconnected(addr) => {
                debug!("[{}] Disconnected from {}", self.name, &addr);

                self.peers.remove(&addr);
                self.ready.remove(&addr);
                self.connected.remove(&addr);
                self.disconnected.insert(addr);
            }
            Input::Received(addr, msg) => {
                for out in self.receive(addr, msg) {
                    outbound.push(out);
                }
            }
            Input::Sent(_addr, _msg) => {}
            Input::Command(cmd) => match cmd {
                Command::Connect(addr) => {
                    debug!("[{}] Received command: Connect({})", self.name, addr);

                    if !self.connected.contains(&addr) {
                        outbound.push(Output::Connect(addr));
                    }
                }
                Command::Receive(addr, msg) => {
                    debug!(
                        "[{}] Received command: Receive({}, {:?})",
                        self.name, addr, msg
                    );

                    if self.connected.contains(&addr) {
                        for out in self.receive(
                            addr,
                            RawNetworkMessage {
                                magic: self.network.magic(),
                                payload: msg,
                            },
                        ) {
                            outbound.push(out);
                        }
                    }
                }
                Command::ImportHeaders(headers, reply) => {
                    debug!("[{}] Received command: ImportHeaders(..)", self.name);
                    let result = self.tree.import_blocks(headers.into_iter(), &self.clock);

                    debug!("[{}] Import result: {:?}", self.name, &result);

                    reply.send(result).ok();
                    outbound.extend(self.broadcast_tip());
                }
                Command::GetTip(_) => todo!(),
                Command::GetBlock(_) => todo!(),
            },
            Input::Timeout(_addr) => {
                todo!();
            }
            Input::Idle => {
                let now = self.clock.local_time();
                let mut disconnect = Vec::new();

                debug!("[{}] Idle: local_time = {}", self.name, now);

                for (_, peer) in self.peers.iter_mut() {
                    if let PeerState::Ready { last_active, .. } = peer.state {
                        // Check if we've been waiting too long to receive headers from
                        // this peer.
                        if let PeerState::Ready {
                            syncing: Syncing::AwaitingHeaders(_, since),
                            ..
                        } = peer.state
                        {
                            if now - since >= Self::REQUEST_TIMEOUT {
                                disconnect.push(peer.address);
                                continue;
                            }
                        }

                        if let Some((_, last_ping)) = peer.last_ping {
                            // A ping was sent and we're waiting for a `pong`. If too much
                            // time has passed, we consider this peer dead, and disconnect
                            // from them.
                            if now - last_ping >= Self::IDLE_TIMEOUT {
                                disconnect.push(peer.address);
                            }
                        } else if now - last_active >= Self::PING_INTERVAL {
                            let ping = peer.ping(now);

                            outbound.push(Output::Message(
                                peer.address,
                                RawNetworkMessage {
                                    magic: self.network.magic(),
                                    payload: ping,
                                },
                            ));
                        }
                    }
                }

                for addr in &disconnect {
                    self.disconnect(&addr, &mut outbound);
                }
            }
        };

        // TODO: Should be triggered when idle, potentially by an `Idle` input.
        match self.state {
            State::Syncing(_) => {}
            State::Connecting | State::Synced => {
                // Wait for a certain connection threshold to make sure we choose the best
                // peer to sync from. For now, we choose a random peer.
                // TODO: Threshold should be a parameter.
                // TODO: Peer should be picked amongst lowest latency ones.
                if self.ready.len() >= PEER_CONNECTION_THRESHOLD {
                    debug_assert!(!self.ready.is_empty());

                    if self.is_synced() {
                        let events = self.transition(State::Synced);
                        outbound.extend(events.into_iter().map(Output::from));
                    } else {
                        // TODO: Pick a peer whos `height` is high enough.
                        let ix = fastrand::usize(..self.ready.len());
                        let p = *self.ready.iter().nth(ix).unwrap();

                        let outs = self.sync(p, self.clock.local_time());
                        outbound.extend(outs);
                    }
                }
            }
        }

        if log::max_level() >= log::Level::Debug {
            outbound.iter().for_each(|o| match o {
                Output::Message(addr, msg) => {
                    debug!("[{}] {}: Sending {:?}", self.name, addr, msg.display());
                }
                _ => {}
            });
        }

        outbound
    }
}

impl<T: BlockTree> Bitcoin<T> {
    pub fn message(&self, addr: PeerId, payload: NetworkMessage) -> Output<RawNetworkMessage> {
        Output::Message(
            addr,
            RawNetworkMessage {
                magic: self.network.magic(),
                payload,
            },
        )
    }

    pub fn transition(&mut self, state: State) -> Option<Event<NetworkMessage>> {
        if state == self.state {
            return None;
        }
        debug!("[{}] state: {:?} -> {:?}", self.name, self.state, state);

        self.state = state;

        match self.state {
            State::Connecting => Some(Event::Connecting),
            State::Syncing(_) => Some(Event::Syncing),
            State::Synced => Some(Event::Synced),
        }
    }

    pub fn receive(
        &mut self,
        addr: PeerId,
        msg: RawNetworkMessage,
    ) -> Vec<Output<RawNetworkMessage>> {
        let now = self.clock.local_time();

        debug!("[{}] {}: Received {:?}", self.name, addr, msg.cmd());

        if msg.magic != self.network.magic() {
            // TODO: Needs test.
            debug!(
                "[{}] {}: Received message with invalid magic: {}",
                self.name, addr, msg.magic
            );
            return vec![Output::Disconnect(addr)];
        }

        let mut peer = self
            .peers
            .get_mut(&addr)
            .unwrap_or_else(|| panic!("peer {} is not known", addr));
        let local_addr = peer.local_address;

        if let PeerState::Ready {
            ref mut last_active,
            ..
        } = peer.state
        {
            *last_active = now;
        }

        if let NetworkMessage::Ping(nonce) = msg.payload {
            if peer.is_ready() {
                return vec![self.message(addr, NetworkMessage::Pong(nonce))];
            }
        } else if let NetworkMessage::Pong(nonce) = msg.payload {
            match peer.last_ping {
                Some((last_nonce, last_time)) if nonce == last_nonce => {
                    peer.record_latency(now - last_time);
                    peer.last_ping = None;
                }
                // Unsolicited or redundant `pong`. Ignore.
                _ => return vec![],
            }
        } else if let NetworkMessage::Addr(addrs) = msg.payload {
            self.addrmgr
                .insert(addrs.into_iter(), addrmgr::Source::Peer(addr));

            return vec![];
        } else if let NetworkMessage::Headers(headers) = msg.payload {
            return self.receive_headers(addr, headers);
        } else if let NetworkMessage::Inv(inventory) = msg.payload {
            return self.receive_inv(addr, inventory);
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
                    ..
                }) = msg.payload
                {
                    let height = self.tree.height();

                    info!(
                        "[{}] {}: Peer version = {}, height = {}, agent = {}, timestamp = {}",
                        self.name, addr, version, start_height, user_agent, timestamp
                    );

                    // Don't support peers with an older protocol than ours, we won't be
                    // able to handle it correctly.
                    if version < self.protocol_version {
                        debug!(
                            "[{}] {}: Disconnecting: peer protocol version is too old: {}",
                            self.name, addr, version
                        );
                        return vec![Output::Disconnect(addr)];
                    }
                    // Peers that don't advertise the `NETWORK` service are not full nodes.
                    // It's not so useful for us to connect to them, because they're likely
                    // to be less secure.
                    if !services.has(ServiceFlags::NETWORK)
                        && !services.has(ServiceFlags::NETWORK_LIMITED)
                        && !user_agent.starts_with("/nakamoto:")
                    {
                        debug!(
                            "[{}] {}: Disconnecting: peer doesn't have required services",
                            self.name, addr
                        );
                        return vec![Output::Disconnect(addr)];
                    }
                    // If the peer is too far behind, there's no use connecting to it, we'll
                    // have to wait for it to catch up.
                    if height.saturating_sub(start_height as Height) > MAX_STALE_HEIGHT_DIFFERENCE {
                        debug!(
                            "[{}] {}: Disconnecting: peer is too far behind",
                            self.name, addr
                        );
                        return vec![Output::Disconnect(addr)];
                    }
                    // Check for self-connections. We only need to check one link direction,
                    // since in the case of a self-connection, we will see both link directions.
                    for (_, peer) in self.peers.iter() {
                        if peer.link == Link::Outbound && peer.nonce == nonce {
                            debug!(
                                "[{}] {}: Disconnecting: detected self-connection",
                                self.name, addr
                            );
                            return vec![Output::Disconnect(addr)];
                        }
                    }

                    let mut peer = self
                        .peers
                        .get_mut(&addr)
                        .unwrap_or_else(|| panic!("peer {} is not known", addr));

                    peer.height = start_height as Height;
                    peer.time_offset = timestamp - now.as_secs() as i64;
                    peer.transition(PeerState::Handshake(Handshake::AwaitingVerack));

                    match peer.link {
                        Link::Outbound => {}
                        Link::Inbound => {
                            let nonce = peer.nonce;

                            return vec![
                                self.message(
                                    addr,
                                    self.version(addr, local_addr, nonce, self.tree.height()),
                                ),
                                self.message(addr, NetworkMessage::Verack),
                            ];
                        }
                    }
                }
            }
            PeerState::Handshake(Handshake::AwaitingVerack) => {
                if msg.payload == NetworkMessage::Verack {
                    peer.receive_verack(now);

                    self.ready.insert(addr);
                    self.clock.record_offset(addr, peer.time_offset);

                    let ping = peer.ping(now);

                    return match peer.link {
                        Link::Outbound => vec![
                            self.message(addr, NetworkMessage::Verack),
                            self.message(addr, NetworkMessage::SendHeaders),
                            self.message(addr, ping),
                        ],
                        Link::Inbound => vec![
                            self.message(addr, NetworkMessage::SendHeaders),
                            self.message(addr, ping),
                        ],
                    };
                }
            }
            PeerState::Ready {
                syncing: Syncing::Idle,
                ..
            } if self.state == State::Synced => {
                if let NetworkMessage::GetHeaders(GetHeadersMessage { locator_hashes, .. }) =
                    msg.payload
                {
                    let tree = &self.tree;

                    // Start from the highest locator hash that is on our active chain.
                    // We don't respond with anything if none of the locators were found. Sorry!
                    if let Some(hash) = locator_hashes.iter().find(|h| tree.contains(h)) {
                        let (start_height, _) = self.tree.get_block(hash).unwrap();

                        // TODO: Set this to highest locator hash. We can assume that the peer
                        // is at this height if they know this hash.
                        // TODO: If the height is higher than the previous peer height, also
                        // set the peer tip.
                        peer.height = start_height;

                        let start = start_height + 1;
                        let end =
                            Height::min(start + MAX_MESSAGE_HEADERS as Height, tree.height() + 1);
                        let headers = tree.range(start..end).collect();

                        return vec![self.message(addr, NetworkMessage::Headers(headers))];
                    }
                } else if let NetworkMessage::Inv(inventory) = msg.payload {
                    return self.receive_inv(addr, inventory);
                }
            }
            PeerState::Ready { .. } => {
                debug!(
                    "[{}] Ignoring {} from peer {}",
                    self.name,
                    msg.cmd(),
                    peer.address
                );
            }
            PeerState::Disconnecting => {
                debug!(
                    "[{}] Ignoring {} from peer {} (disconnecting)",
                    self.name,
                    msg.cmd(),
                    peer.address
                );
            }
        }

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
        let timestamp = self.clock.local_time().as_secs() as i64;

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
            relay: self.relay,
        })
    }

    fn get_headers(&self, locator_hashes: Vec<BlockHash>, stop_hash: BlockHash) -> NetworkMessage {
        NetworkMessage::GetHeaders(GetHeadersMessage {
            version: self.protocol_version,
            // Starting hashes, highest heights first.
            locator_hashes,
            // Using the zero hash means *fetch as many blocks as possible*.
            stop_hash,
        })
    }

    /// Receive an `inv` message. This will happen if we are out of sync with a peer. And blocks
    /// are being announced. Otherwise, we expect to receive a `headers` message.
    fn receive_inv(&mut self, addr: PeerId, inv: Vec<Inventory>) -> Vec<Output<RawNetworkMessage>> {
        let mut best_block = None;

        for i in &inv {
            if let Inventory::Block(hash) = i {
                // TODO: Update block availability for this peer.
                if !self.tree.is_known(hash) {
                    debug!("[{}] {}: Discovered new block: {}", self.name, addr, &hash);
                    // The final block hash in the inventory should be the highest. Use
                    // that one for a `getheaders` call.
                    best_block = Some(hash);
                }
            }
        }

        if let Some(stop_hash) = best_block {
            vec![self.message(addr, self.get_headers(self.locator_hashes(), *stop_hash))]
        } else {
            vec![]
        }
    }

    fn receive_headers(
        &mut self,
        addr: PeerId,
        headers: Vec<BlockHeader>,
    ) -> Vec<Output<RawNetworkMessage>> {
        let mut outbound = Vec::new();
        let length = headers.len();
        let peer = self
            .peers
            .get_mut(&addr)
            .unwrap_or_else(|| panic!("peer {} is not known", addr));

        let best = if let Some(last) = headers.last() {
            last.bitcoin_hash()
        } else {
            return vec![];
        };
        debug!("[{}] {}: Received {} header(s)", self.name, addr, length);

        if self.tree.contains(&best) {
            debug!(
                "[{}] {}: Received header(s) already part of active chain",
                self.name, addr,
            );
            return vec![];
        }

        // TODO: Before importing, we could check the headers against known checkpoints.
        // TODO: Check that headers form a chain.
        match self.tree.import_blocks(headers.into_iter(), &self.clock) {
            Ok(ImportResult::TipUnchanged) => {
                // Try to find a common ancestor.
                let locators = self.tree.locators_hashes(self.tree.height());

                // TODO: What if we already knew these headers?
                peer.syncing(Syncing::AwaitingHeaders(
                    locators.clone(),
                    self.clock.local_time(),
                ));
                outbound.push(self.message(addr, self.get_headers(locators, BlockHash::default())));
            }
            Ok(ImportResult::TipChanged(tip, height, _)) => {
                info!(
                    "[{}] Imported {} header(s) from {}",
                    self.name, length, addr
                );
                info!("[{}] Chain height = {}, tip = {}", self.name, height, tip);

                peer.tip = tip;
                peer.height = height;

                // TODO: Check that the headers received match the headers awaited.
                if let PeerState::Ready {
                    syncing: Syncing::AwaitingHeaders(_locators, _local_time),
                    ..
                } = &peer.state
                {
                    // If we received less than the maximum number of headers, we must be in sync.
                    // Otherwise, ask for the next batch of headers.
                    if length < MAX_MESSAGE_HEADERS {
                        // If these headers were unsolicited, we may already be ready/synced.
                        // Otherwise, we're finally in sync.
                        peer.syncing(Syncing::Idle);

                        outbound
                            .extend(self.transition(State::Synced).into_iter().map(Output::from));
                    } else {
                        // TODO: If we're already in the state of asking for this header, don't
                        // ask again.
                        let locators = vec![tip];

                        peer.syncing(Syncing::AwaitingHeaders(
                            locators.clone(),
                            self.clock.local_time(),
                        ));

                        outbound.push(
                            self.message(addr, self.get_headers(locators, BlockHash::default())),
                        );
                    }
                }

                // Broadcast the new tip, since we were able to verify the header chain up to it.
                outbound.extend(self.broadcast_tip());
            }
            Err(err) => {
                // TODO: Bad blocks received!
                // TODO: Disconnect peer.
                error!("[{}] Error importing headers: {}", self.name, err);
            }
        }
        outbound
    }

    /// Broadcast our best block header to connected peers who don't have it.
    fn broadcast_tip(&self) -> Vec<Output<RawNetworkMessage>> {
        let (height, best) = self.tree.best_block();
        let mut out = Vec::new();

        for (addr, peer) in &self.peers {
            if peer.is_inbound() && peer.is_idle() && height > peer.height {
                out.push(self.message(*addr, NetworkMessage::Headers(vec![*best])));
                // TODO: Update peer inventory?
            }
        }

        if !out.is_empty() {
            debug!(
                "[{}] Broadcasting tip at height {} to {} peer(s)..",
                self.name,
                height,
                out.len()
            );
        }
        out
    }

    #[allow(dead_code)]
    fn sample_headers(&self, outbound: &mut Vec<(PeerId, NetworkMessage)>) {
        let height = self.tree.height();
        let (tip, _) = self.tree.tip();

        if let Some(parent) = self.tree.get_block_by_height(height - 1) {
            let locators = vec![parent.bitcoin_hash()];

            for (addr, peer) in self.peers.iter() {
                if peer.is_ready() && peer.link == Link::Outbound {
                    outbound.push((*addr, self.get_headers(locators.clone(), tip)));
                }
            }
        }
    }

    fn disconnect(&mut self, addr: &PeerId, outbound: &mut Vec<Output<RawNetworkMessage>>) {
        let peer = self
            .peers
            .get_mut(&addr)
            .unwrap_or_else(|| panic!("peer {} is not known", addr));

        peer.transition(PeerState::Disconnecting);
        outbound.push(Output::Disconnect(*addr));
    }

    fn locator_hashes(&self) -> Vec<BlockHash> {
        let (hash, _) = self.tree.tip();

        vec![hash]
    }

    fn generate_events(
        &self,
        input: &Input<RawNetworkMessage, Command>,
        outbound: &mut Vec<Output<RawNetworkMessage>>,
    ) {
        match input {
            Input::Connected { addr, link, .. } => {
                outbound.push(Output::Event(Event::Connected(*addr, *link)));
            }
            Input::Disconnected(addr) => {
                outbound.push(Output::Event(Event::Disconnected(*addr)));
            }
            Input::Received(addr, msg) => {
                outbound.push(Output::Event(Event::Received(*addr, msg.payload.clone())));
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::time::SystemTime;

    use nakamoto_chain::block::cache::model;
    use nakamoto_chain::block::BlockHeader;
    use nakamoto_test::logger;
    use nakamoto_test::TREE;

    /// A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
    mod simulator {
        use super::*;

        pub fn handshake<T: BlockTree>(
            alice: &mut Bitcoin<T>,
            alice_addr: net::SocketAddr,
            bob: &mut Bitcoin<T>,
            bob_addr: net::SocketAddr,
            local_time: LocalTime,
        ) {
            self::run(
                vec![
                    (
                        alice_addr,
                        alice,
                        vec![Input::Connected {
                            addr: bob_addr,
                            local_addr: alice_addr,
                            link: Link::Outbound,
                        }],
                    ),
                    (
                        bob_addr,
                        bob,
                        vec![Input::Connected {
                            addr: alice_addr,
                            local_addr: bob_addr,
                            link: Link::Inbound,
                        }],
                    ),
                ],
                local_time,
            );

            assert!(alice.peers.values().all(|p| p.is_ready()));
            assert!(bob.peers.values().all(|p| p.is_ready()));
        }

        pub fn run<P: Protocol<M, Command = C>, M: Message + Debug, C: Debug>(
            peers: Vec<(PeerId, &mut P, Vec<Input<M, C>>)>,
            local_time: LocalTime,
        ) {
            let mut sim: HashMap<PeerId, (&mut P, VecDeque<Input<M, C>>)> = HashMap::new();
            let mut events = Vec::new();

            logger::init(log::Level::Debug);

            // Add peers to simulator.
            for (addr, proto, evs) in peers.into_iter() {
                sim.insert(addr, (proto, VecDeque::new()));

                for e in evs.into_iter() {
                    events.push((addr, e));
                }
            }

            while !events.is_empty() || sim.values().any(|(_, q)| !q.is_empty()) {
                // Prepare event queues.
                for (receiver, event) in events.drain(..) {
                    let (_, q) = sim.get_mut(&receiver).unwrap();
                    q.push_back(event);
                }

                for (peer, (proto, queue)) in sim.iter_mut() {
                    if let Some(event) = queue.pop_front() {
                        let outs = proto.step(event, local_time);

                        for out in outs.into_iter() {
                            match out {
                                Output::Message(receiver, msg) => {
                                    debug!("(sim) {} -> {}: {:?}", peer, receiver, msg);
                                    events.push((receiver, Input::Received(*peer, msg)))
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    }

    /// Test protocol config.
    const CONFIG: Config = Config {
        network: network::Network::Mainnet,
        address_book: AddressBook::new(),
        // Pretend that we're a full-node, to fool connections
        // between instances of this protocol in tests.
        services: ServiceFlags::NETWORK,
        protocol_version: PROTOCOL_VERSION,
        user_agent: USER_AGENT,
        relay: false,
        name: "self",
    };

    #[test]
    fn test_handshake() {
        let genesis = BlockHeader {
            version: 1,
            prev_blockhash: Default::default(),
            merkle_root: Default::default(),
            nonce: 0,
            time: 0,
            bits: 0,
        };
        let tree = model::Cache::new(genesis);
        let clock = AdjustedTime::default();
        let local_time = LocalTime::from(SystemTime::now());

        let alice_addr = ([127, 0, 0, 1], 8333).into();
        let bob_addr = ([127, 0, 0, 2], 8333).into();

        let mut alice = Bitcoin::new(tree.clone(), clock.clone(), CONFIG);
        let mut bob = Bitcoin::new(tree, clock, CONFIG);

        simulator::run(
            vec![
                (
                    alice_addr,
                    &mut alice,
                    vec![Input::Connected {
                        addr: bob_addr,
                        local_addr: alice_addr,
                        link: Link::Outbound,
                    }],
                ),
                (
                    bob_addr,
                    &mut bob,
                    vec![Input::Connected {
                        addr: alice_addr,
                        local_addr: bob_addr,
                        link: Link::Inbound,
                    }],
                ),
            ],
            local_time,
        );

        assert!(
            alice.peers.values().all(|p| p.is_ready()),
            "alice: {:#?}",
            alice.peers
        );

        assert!(
            bob.peers.values().all(|p| p.is_ready()),
            "bob: {:#?}",
            bob.peers
        );
    }

    #[test]
    fn test_initial_sync() {
        let clock = AdjustedTime::default();
        let local_time = LocalTime::from(SystemTime::now());

        let alice_addr: PeerId = ([127, 0, 0, 1], 8333).into();
        let bob_addr: PeerId = ([127, 0, 0, 2], 8333).into();

        // Blockchain height we're going to be working with. Making it larger
        // than the threshold ensures a sync happens.
        let height = 144;

        // Let's test Bob trying to sync with Alice from genesis.
        let mut alice_tree = TREE.clone();
        let bob_tree = model::Cache::new(*alice_tree.genesis());

        // Truncate chain to test height.
        alice_tree.rollback(height).unwrap();

        let mut alice = Bitcoin::new(alice_tree, clock.clone(), CONFIG);
        let mut bob = Bitcoin::new(bob_tree, clock, CONFIG);

        simulator::handshake(&mut alice, alice_addr, &mut bob, bob_addr, local_time);

        assert_eq!(alice.tree.height(), height);
        assert_eq!(bob.tree.height(), height);
    }

    /// Test what happens when a peer is idle for too long.
    #[test]
    fn test_idle() {
        let genesis = BlockHeader {
            version: 1,
            prev_blockhash: Default::default(),
            merkle_root: Default::default(),
            nonce: 0,
            time: 0,
            bits: 0,
        };
        let tree = model::Cache::new(genesis);
        let clock = AdjustedTime::default();
        let local_time = LocalTime::from(SystemTime::now());
        let idle_timeout = Bitcoin::<model::Cache>::IDLE_TIMEOUT;

        let alice_addr: PeerId = ([127, 0, 0, 1], 8333).into();
        let bob_addr: PeerId = ([127, 0, 0, 2], 8333).into();

        let mut alice = Bitcoin::new(
            tree.clone(),
            clock.clone(),
            Config {
                name: "alice",
                ..CONFIG
            },
        );
        let mut bob = Bitcoin::new(
            tree,
            clock,
            Config {
                name: "bob",
                ..CONFIG
            },
        );

        simulator::handshake(&mut alice, alice_addr, &mut bob, bob_addr, local_time);

        // Let a certain amount of time pass.
        let mut out = alice
            .step(Input::Idle, local_time + idle_timeout)
            .into_iter();

        assert!(
            matches!(
            out.next(),
            Some(Output::Message(
                addr,
                RawNetworkMessage {
                    payload: NetworkMessage::Ping(_), ..
                },
            )) if addr == bob_addr
        ),
            "Alice pings Bob"
        );

        // More time passes, and Bob doesn't `pong` back.
        let mut out = alice
            .step(Input::Idle, local_time + idle_timeout + idle_timeout)
            .into_iter();

        // Alice now decides to disconnect Bob.
        assert!(alice
            .peers
            .values()
            .all(|p| p.state == PeerState::Disconnecting));
        assert_eq!(out.next(), Some(Output::Disconnect(bob_addr)));
    }
}
