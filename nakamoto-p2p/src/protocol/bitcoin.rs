use log::*;

pub mod network;
pub use network::Network;

use crate::address_book::AddressBook;
use crate::protocol::{Event, Link, Output, PeerId, Protocol};

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
use nakamoto_chain::block::tree::BlockTree;
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

///////////////////////////////////////////////////////////////////////////////////////////////

/// An instantiation of `Protocol`, for the Bitcoin P2P network. Parametrized over the
/// block-tree.
#[derive(Debug)]
pub struct Bitcoin<T> {
    /// Peer states.
    pub peers: HashMap<PeerId, Peer>,
    /// Peer configuration.
    pub config: Config,
    /// Protocol state.
    pub state: State,
    /// Block tree.
    pub tree: T,

    /// Address book of peers.
    address_book: AddressBook,
    /// Network-adjusted clock.
    clock: AdjustedTime<PeerId>,
    /// Set of connected peers that have completed the handshake.
    ready: HashSet<PeerId>,
    /// Set of all connected peers.
    connected: HashSet<PeerId>,
    /// Set of disconnected peers.
    disconnected: HashSet<PeerId>,
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
#[derive(Debug, Copy, Clone)]
pub struct Config {
    pub network: network::Network,
    pub services: ServiceFlags,
    pub protocol_version: u32,
    pub relay: bool,
    pub user_agent: &'static str,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: network::Network::Mainnet,
            services: ServiceFlags::NONE,
            protocol_version: PROTOCOL_VERSION,
            relay: false,
            user_agent: USER_AGENT,
        }
    }
}

impl Config {
    pub fn from(network: network::Network) -> Self {
        Self {
            network,
            ..Self::default()
        }
    }

    pub fn port(&self) -> u16 {
        self.network.port()
    }
}

impl<T: BlockTree> Bitcoin<T> {
    const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);

    pub fn new(
        tree: T,
        address_book: AddressBook,
        clock: AdjustedTime<PeerId>,
        config: Config,
    ) -> Self {
        Self {
            peers: HashMap::new(),
            config,
            state: State::Connecting,
            tree,
            clock,
            address_book,
            ready: HashSet::new(),
            connected: HashSet::new(),
            disconnected: HashSet::new(),
        }
    }

    fn connected(&mut self, addr: PeerId, local_addr: net::SocketAddr, link: Link) -> bool {
        self.connected.insert(addr);
        self.disconnected.remove(&addr);

        self.peers
            .insert(
                addr,
                Peer::new(
                    addr,
                    local_addr,
                    PeerState::Handshake(Handshake::default()),
                    link,
                ),
            )
            .is_none()
    }

    /// Start syncing with the given peer.
    pub fn sync(&mut self, addr: PeerId, local_time: LocalTime) -> Vec<(PeerId, NetworkMessage)> {
        self.transition(State::Syncing(addr));

        let locator_hashes = self.locator_hashes();
        let peer = self.peers.get_mut(&addr).unwrap();

        peer.syncing(Syncing::AwaitingHeaders(locator_hashes.clone(), local_time));

        vec![(addr, self.get_headers(locator_hashes, BlockHash::default()))]
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
}

impl Peer {
    pub fn new(
        address: net::SocketAddr,
        local_address: net::SocketAddr,
        state: PeerState,
        link: Link,
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
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self.state, PeerState::Ready { .. })
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
        debug!("{}: {:?} -> {:?}", self.address, self.state, state);

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
            debug!("{}: Syncing: {:?} -> {:?}", self.address, syncing, state);

            *syncing = state;
        }
    }
}

impl<T: BlockTree> Protocol<RawNetworkMessage> for Bitcoin<T> {
    const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_secs(60 * 5);
    const PING_INTERVAL: LocalDuration = LocalDuration::from_secs(60);

    fn initialize(&mut self, time: LocalTime) -> Vec<Output<RawNetworkMessage>> {
        self.clock.set_local_time(time);

        let mut outbound = Vec::new();

        for peer in self.address_book.iter() {
            outbound.push(Output::Connect(*peer));
        }
        outbound
    }

    fn step(
        &mut self,
        event: Event<RawNetworkMessage>,
        time: LocalTime,
    ) -> Vec<Output<RawNetworkMessage>> {
        let mut outbound = Vec::new();

        self.clock.set_local_time(time);

        match event {
            Event::Connected {
                addr,
                local_addr,
                link,
            } => {
                self.connected(addr, local_addr, link);

                match link {
                    Link::Outbound => {
                        info!("{}: Peer connected (outbound)", &addr);

                        outbound.push(Output::Message(
                            addr,
                            self.version(addr, local_addr, self.tree.height()),
                        ));
                    }
                    Link::Inbound => {
                        info!("{}: Peer connected (inbound)", &addr);
                    } // Wait to receive remote version.
                }
            }
            Event::Disconnected(addr) => {
                debug!("Disconnected from {}", &addr);

                self.peers.remove(&addr);
                self.ready.remove(&addr);
                self.connected.remove(&addr);
                self.disconnected.insert(addr);
            }
            Event::Received(addr, msg) => {
                for out in self.receive(addr, msg) {
                    outbound.push(out);
                }
            }
            Event::Sent(_addr, _msg) => {}
            Event::Error(addr, err) => {
                error!("{}: Error: {}", addr, err);

                self.ready.remove(&addr);
                self.connected.remove(&addr);
                self.disconnected.insert(addr);
                // TODO: Protocol shouldn't handle socket and io errors directly, because it
                // needs to understand all kinds of socket errors then, even though it's agnostic
                // to the transport. This doesn't make sense. What should happen is that
                // transport errors should be handled at the transport (or reactor) layer. The "protocol"
                // doesn't decide on what to do about transport errors. It _may_ receive a higher
                // level event like `Disconnected`, or an opaque `Error`, just to keep track of
                // peer errors, scores etc.
            }
            Event::Idle => {
                let now = self.clock.local_time();
                let mut disconnect = Vec::new();

                debug!("Idle: local_time = {}", now);

                for (_, peer) in self.peers.iter_mut() {
                    if let PeerState::Ready { last_active, .. } = peer.state {
                        if let Some((_, last_ping)) = peer.last_ping {
                            // A ping was sent and we're waiting for a `pong`. If too much
                            // time has passed, we consider this peer dead, and disconnect
                            // from them.
                            if now - last_ping >= Self::IDLE_TIMEOUT {
                                disconnect.push(peer.address);
                            }
                        } else if now - last_active >= Self::PING_INTERVAL {
                            outbound.push(Output::Message(peer.address, peer.ping(now)));
                        }

                        // Check if we've been waiting too long to receive headers from
                        // this peer.
                        if let PeerState::Ready {
                            syncing: Syncing::AwaitingHeaders(_, since),
                            ..
                        } = peer.state
                        {
                            if now - since >= Self::REQUEST_TIMEOUT {
                                disconnect.push(peer.address);
                            }
                        }
                    }
                }

                for addr in &disconnect {
                    self.disconnect(&addr, &mut outbound);
                }
            }
        };

        // TODO: Should be triggered when idle, potentially by an `Idle` event.
        match self.state {
            State::Syncing(_) => {}
            State::Connecting | State::Synced => {
                // Wait for a certain connection threshold to make sure we choose the best
                // peer to sync from. For now, we choose a random peer.
                // TODO: Threshold should be a parameter.
                // TODO: Peer should be picked amongst lowest latency ones.
                if self.ready.len() >= PEER_CONNECTION_THRESHOLD {
                    if self.is_synced() {
                        self.transition(State::Synced);
                    } else {
                        // TODO: Pick a peer whos `height` is high enough.
                        let ix = fastrand::usize(..self.ready.len());
                        let p = *self.ready.iter().nth(ix).unwrap();

                        for (addr, msg) in self.sync(p, self.clock.local_time()) {
                            outbound.push(Output::Message(addr, msg));
                        }
                    }
                }
            }
        }

        outbound
            .into_iter()
            .map(|out| match out {
                Output::Message(addr, msg) => {
                    debug!("{}: Sending {:?}", addr, msg.cmd());

                    Output::Message(
                        addr,
                        RawNetworkMessage {
                            magic: self.config.network.magic(),
                            payload: msg,
                        },
                    )
                }
                Output::Connect(addr) => {
                    debug!("{}: Connecting..", addr);

                    Output::Connect(addr)
                }
                Output::Disconnect(addr) => {
                    debug!("{}: Disconnecting..", addr);

                    Output::Disconnect(addr)
                }
            })
            .collect()
    }
}

impl<T: BlockTree> Bitcoin<T> {
    pub fn transition(&mut self, state: State) {
        if state == self.state {
            return;
        }
        debug!("state: {:?} -> {:?}", self.state, state);

        self.state = state;
    }

    pub fn receive(&mut self, addr: PeerId, msg: RawNetworkMessage) -> Vec<Output<NetworkMessage>> {
        let now = self.clock.local_time();

        debug!("{}: Received {:?}", addr, msg.cmd());

        if msg.magic != self.config.network.magic() {
            // TODO: Needs test.
            debug!(
                "{}: Received message with invalid magic: {}",
                addr, msg.magic
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
                return vec![Output::Message(addr, NetworkMessage::Pong(nonce))];
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
            for _addr in &addrs {
                todo!();
            }
            return vec![];
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
                    ..
                }) = msg.payload
                {
                    let height = self.tree.height();

                    info!(
                        "{}: Peer version = {}, height = {}, agent = {} timestamp = {}",
                        addr, version, start_height, user_agent, timestamp
                    );

                    // Don't support peers with an older protocol than ours, we won't be
                    // able to handle it correctly.
                    if version < self.config.protocol_version {
                        debug!(
                            "{}: Disconnecting: peer protocol version is too old: {}",
                            addr, version
                        );
                        return vec![Output::Disconnect(addr)];
                    }
                    // Peers that don't advertise the `NETWORK` service are not full nodes.
                    // It's not so useful for us to connect to them, because they're likely
                    // to be less secure.
                    if !services.has(ServiceFlags::NETWORK) {
                        debug!(
                            "{}: Disconnecting: peer doesn't have required services",
                            addr
                        );
                        return vec![Output::Disconnect(addr)];
                    }
                    // If the peer is too far behind, there's no use connecting to it, we'll
                    // have to wait for it to catch up.
                    if height.saturating_sub(start_height as Height) > MAX_STALE_HEIGHT_DIFFERENCE {
                        debug!("{}: Disconnecting: peer is too far behind", addr);
                        return vec![Output::Disconnect(addr)];
                    }

                    peer.height = start_height as Height;
                    peer.time_offset = timestamp - now.as_secs() as i64;
                    peer.transition(PeerState::Handshake(Handshake::AwaitingVerack));

                    match peer.link {
                        Link::Outbound => {}
                        Link::Inbound => {
                            return vec![
                                Output::Message(
                                    addr,
                                    self.version(addr, local_addr, self.tree.height()),
                                ),
                                Output::Message(addr, NetworkMessage::Verack),
                            ]
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
                            Output::Message(addr, NetworkMessage::Verack),
                            Output::Message(addr, NetworkMessage::SendHeaders),
                            Output::Message(addr, ping),
                        ],
                        Link::Inbound => vec![
                            Output::Message(addr, NetworkMessage::SendHeaders),
                            Output::Message(addr, ping),
                        ],
                    };
                }
            }
            PeerState::Ready {
                syncing: Syncing::AwaitingHeaders(_locators, _local_time),
                ..
            } => {
                // TODO: These two handlers are duplicated when the state is `Synced`.
                if let NetworkMessage::Headers(headers) = msg.payload {
                    // TODO: Check that the headers received match the headers awaited.
                    return self.receive_headers(addr, headers);
                } else if let NetworkMessage::Inv(inventory) = msg.payload {
                    return self.receive_inv(addr, inventory);
                }
            }
            PeerState::Ready {
                syncing: Syncing::Idle,
                ..
            } if self.state == State::Synced => {
                if let NetworkMessage::GetHeaders(GetHeadersMessage { locator_hashes, .. }) =
                    msg.payload
                {
                    assert!(locator_hashes.len() == 1);

                    let start_hash = locator_hashes[0];
                    let (start_height, _) = self.tree.get_block(&start_hash).unwrap();

                    // TODO: Set this to highest locator hash. We can assume that the peer
                    // is at this height if they know this hash.
                    // TODO: If the height is higher than the previous peer height, also
                    // set the peer tip.
                    peer.height = start_height;

                    let start = start_height + 1;
                    let end = Height::min(
                        start + MAX_MESSAGE_HEADERS as Height,
                        self.tree.height() + 1,
                    );
                    let headers = self.tree.range(start..end).collect();

                    return vec![Output::Message(addr, NetworkMessage::Headers(headers))];
                } else if let NetworkMessage::Headers(headers) = msg.payload {
                    return self.receive_headers(addr, headers);
                } else if let NetworkMessage::Inv(inventory) = msg.payload {
                    return self.receive_inv(addr, inventory);
                }
            }
            PeerState::Ready { .. } => {
                debug!("Ignoring {} from peer {}", msg.cmd(), peer.address);
            }
            PeerState::Disconnecting => {
                debug!(
                    "Ignoring {} from peer {} (disconnecting)",
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
        start_height: Height,
    ) -> NetworkMessage {
        let start_height = start_height as i32;
        let timestamp = self.clock.local_time().as_secs() as i64;

        NetworkMessage::Version(VersionMessage {
            // Our max supported protocol version.
            version: self.config.protocol_version,
            // Local services.
            services: self.config.services,
            // Local time.
            timestamp,
            // Receiver address, as perceived by us.
            receiver: Address::new(&addr, ServiceFlags::NETWORK | ServiceFlags::COMPACT_FILTERS),
            // Local address (unreliable) and local services (same as `services` field)
            sender: Address::new(&local_addr, self.config.services),
            // A nonce to detect connections to self (FIXME)
            nonce: 0,
            // Our user agent string.
            user_agent: self.config.user_agent.to_owned(),
            // Our best height.
            start_height,
            // Whether we want to receive transaction `inv` messages.
            relay: self.config.relay,
        })
    }

    fn get_headers(&self, locator_hashes: Vec<BlockHash>, stop_hash: BlockHash) -> NetworkMessage {
        NetworkMessage::GetHeaders(GetHeadersMessage {
            version: self.config.protocol_version,
            // Starting hashes, highest heights first.
            locator_hashes,
            // Using the zero hash means *fetch as many blocks as possible*.
            stop_hash,
        })
    }

    /// Receive an `inv` message. This will happen if we are out of sync with a peer. And blocks
    /// are being announced. Otherwise, we expect to receive a `headers` message.
    fn receive_inv(&mut self, addr: PeerId, inv: Vec<Inventory>) -> Vec<Output<NetworkMessage>> {
        let mut best_block = None;

        for i in &inv {
            match i {
                Inventory::Block(hash) => {
                    // TODO: Update block availability for this peer.
                    if !self.tree.is_known(hash) {
                        debug!("{}: Discovered new block: {}", addr, &hash);
                        // The final block hash in the inventory should be the highest. Use
                        // that one for a `getheaders` call.
                        best_block = Some(hash);
                    }
                }
                _ => {}
            }
        }
        if let Some(stop_hash) = best_block {
            vec![Output::Message(
                addr,
                self.get_headers(self.locator_hashes(), *stop_hash),
            )]
        } else {
            vec![]
        }
    }

    fn receive_headers(
        &mut self,
        addr: PeerId,
        headers: Vec<BlockHeader>,
    ) -> Vec<Output<NetworkMessage>> {
        let length = headers.len();
        let peer = self
            .peers
            .get_mut(&addr)
            .unwrap_or_else(|| panic!("peer {} is not known", addr));

        if headers.is_empty() {
            return vec![];
        }
        debug!("{}: Received {} header(s)", addr, length);

        // TODO: Before importing, we could check the headers against known checkpoints.
        match self.tree.import_blocks(headers.into_iter(), &self.clock) {
            Ok((tip, height)) => {
                info!("Imported {} header(s) from {}", length, addr);
                info!("Chain height = {}, tip = {}", height, tip);

                peer.tip = tip;
                peer.height = height;

                // If we received less than the maximum number of headers, we must be in sync.
                // Otherwise, ask for the next batch of headers.
                if length < MAX_MESSAGE_HEADERS {
                    info!("{}: Finished synchronizing", addr);

                    // If these headers were unsolicited, we may already be ready/synced.
                    // Otherwise, we're finally in sync.
                    peer.syncing(Syncing::Idle);
                    self.transition(State::Synced);

                    vec![]
                } else {
                    // TODO: If we're already in the state of asking for this header, don't
                    // ask again.
                    let locators = vec![tip];

                    peer.syncing(Syncing::AwaitingHeaders(
                        locators.clone(),
                        self.clock.local_time(),
                    ));

                    vec![Output::Message(
                        addr,
                        self.get_headers(locators, BlockHash::default()),
                    )]
                }
            }
            Err(err) => {
                // TODO: Bad blocks received!
                error!("Error importing headers: {}", err);

                vec![]
            }
        }
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

    fn disconnect(&mut self, addr: &PeerId, outbound: &mut Vec<Output<NetworkMessage>>) {
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
                        vec![Event::Connected {
                            addr: bob_addr,
                            local_addr: alice_addr,
                            link: Link::Outbound,
                        }],
                    ),
                    (
                        bob_addr,
                        bob,
                        vec![Event::Connected {
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

        pub fn run<P: Protocol<M>, M: Debug>(
            peers: Vec<(PeerId, &mut P, Vec<Event<M>>)>,
            local_time: LocalTime,
        ) {
            let mut sim: HashMap<PeerId, (&mut P, VecDeque<Event<M>>)> = HashMap::new();
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
                                    events.push((receiver, Event::Received(*peer, msg)))
                                }
                                _ => todo!(),
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
        // Pretend that we're a full-node, to fool connections
        // between instances of this protocol in tests.
        services: ServiceFlags::NETWORK,
        protocol_version: PROTOCOL_VERSION,
        user_agent: USER_AGENT,
        relay: false,
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
        let book = AddressBook::new();

        let alice_addr = ([127, 0, 0, 1], 8333).into();
        let bob_addr = ([127, 0, 0, 2], 8333).into();

        let mut alice = Bitcoin::new(tree.clone(), book.clone(), clock.clone(), CONFIG);
        let mut bob = Bitcoin::new(tree, book, clock, CONFIG);

        simulator::run(
            vec![
                (
                    alice_addr,
                    &mut alice,
                    vec![Event::Connected {
                        addr: bob_addr,
                        local_addr: alice_addr,
                        link: Link::Outbound,
                    }],
                ),
                (
                    bob_addr,
                    &mut bob,
                    vec![Event::Connected {
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
        let book = AddressBook::new();

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

        let mut alice = Bitcoin::new(alice_tree, book.clone(), clock.clone(), CONFIG);
        let mut bob = Bitcoin::new(bob_tree, book, clock, CONFIG);

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
        let book = AddressBook::new();
        let idle_timeout = Bitcoin::<model::Cache>::IDLE_TIMEOUT;

        let alice_addr: PeerId = ([127, 0, 0, 1], 8333).into();
        let bob_addr: PeerId = ([127, 0, 0, 2], 8333).into();

        let mut alice = Bitcoin::new(tree.clone(), book.clone(), clock.clone(), CONFIG);
        let mut bob = Bitcoin::new(tree, book, clock.clone(), CONFIG);

        simulator::handshake(&mut alice, alice_addr, &mut bob, bob_addr, local_time);

        // Let a certain amount of time pass.
        let mut out = alice
            .step(Event::Idle, local_time + idle_timeout)
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
            .step(Event::Idle, local_time + idle_timeout + idle_timeout)
            .into_iter();

        // Alice now decides to disconnect Bob.
        assert!(alice
            .peers
            .values()
            .all(|p| p.state == PeerState::Disconnecting));
        assert_eq!(out.next(), Some(Output::Disconnect(bob_addr)));
    }
}
