use log::*;

pub mod network;
pub use network::Network;

use crate::protocol::{Event, Link, PeerId, Protocol};

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::net;
use std::time::{self, SystemTime, UNIX_EPOCH};

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::network::message_network::VersionMessage;
use bitcoin::util::hash::BitcoinHash;

use nakamoto_chain::block::time::AdjustedTime;
use nakamoto_chain::block::tree::BlockTree;
use nakamoto_chain::block::{BlockHash, BlockHeader, Height};

/// Peer-to-peer protocol version.
/// For now, we only support `70012`, due to lacking `sendcmpct` support.
pub const PROTOCOL_VERSION: u32 = 70012;
/// User agent included in `version` messages.
pub const USER_AGENT: &str = "/nakamoto:0.0.0/";
/// Number of blocks out of sync we have to be to trigger an initial sync.
pub const SYNC_THRESHOLD: Height = 144;
/// Minimum number of peers to be connected to.
pub const PEER_CONNECTION_THRESHOLD: usize = 1;
/// Maximum time adjustment between network and local time (70 minutes).
pub const MAX_TIME_ADJUSTMENT: TimeOffset = 70 * 60;

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
    /// Initial syncing (IBD) has started with the designated peer.
    InitialSync(PeerId),
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
    pub sync: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: network::Network::Mainnet,
            services: ServiceFlags::NONE,
            protocol_version: PROTOCOL_VERSION,
            relay: false,
            sync: true,
        }
    }
}

impl Config {
    pub fn port(&self) -> u16 {
        self.network.port()
    }
}

impl<T: BlockTree> Bitcoin<T> {
    pub fn new(tree: T, clock: AdjustedTime<PeerId>, config: Config) -> Self {
        Self {
            peers: HashMap::new(),
            config,
            state: State::Connecting,
            tree,
            clock,
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

    /// Start initial block header sync.
    pub fn initial_sync(&mut self, addr: PeerId) -> Vec<(PeerId, NetworkMessage)> {
        self.transition(State::InitialSync(addr));

        let locator_hashes = self.locator_hashes();
        let peer = self.peers.get_mut(&addr).unwrap();

        peer.transition(PeerState::Syncing(Syncing::AwaitingHeaders(
            locator_hashes.clone(),
        )));

        vec![(addr, self.get_headers(locator_hashes, BlockHash::default()))]
    }

    /// Check whether or not we are in sync with the network.
    /// TODO: Should return the minimum peer height, so that we can
    /// keep track of it in our state, while syncing to it.
    // FIXME: Currently if you connect and realize you're < 144 blocks behind, you do
    // nothing. This should change.
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
            height >= peer_height || peer_height - height <= SYNC_THRESHOLD
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
#[derive(Clone, Debug)]
pub enum Syncing {
    // TODO: This should keep track of what hash we requested.
    AwaitingHeaders(Vec<BlockHash>),
}

/// State of a remote peer.
#[derive(Debug)]
pub enum PeerState {
    Handshake(Handshake),
    Ready,
    Syncing(Syncing),
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
    /// An offset in seconds, between this peer's clock and ours.
    /// A positive offset means the peer's clock is ahead of ours.
    pub time_offset: TimeOffset,
    /// Whether this is an inbound or outbound peer connection.
    pub link: Link,
    /// Peer state.
    pub state: PeerState,
    /// Last time we heard from this peer.
    pub last_active: Option<time::Instant>,
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
            time_offset: 0,
            last_active: None,
            state,
            link,
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self.state, PeerState::Ready)
    }

    fn receive_version(
        &mut self,
        VersionMessage {
            start_height,
            timestamp,
            ..
        }: VersionMessage,
    ) {
        let height = 0;
        // TODO: I'm not sure we should be getting the system time here.
        // It may be a better idea _not_ to store the time offset locally,
        // and instead send the remote time back to the network controller.
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        if height > start_height as Height + 1 {
            // We're ahead of this peer by more than one block. Don't use it
            // for IBD.
            todo!();
        }
        // TODO: Check version
        // TODO: Check services
        // TODO: Check start_height
        self.height = start_height as Height;
        self.time_offset = timestamp - local_time;

        self.transition(PeerState::Handshake(Handshake::AwaitingVerack));
    }

    fn receive_verack(&mut self) {
        self.transition(PeerState::Ready);
    }

    fn transition(&mut self, state: PeerState) {
        debug!("{}: {:?} -> {:?}", self.address, self.state, state);

        self.state = state;
    }
}

impl<T: BlockTree> Protocol<RawNetworkMessage> for Bitcoin<T> {
    const IDLE_TIMEOUT: time::Duration = time::Duration::from_secs(60 * 5);
    const PING_INTERVAL: time::Duration = time::Duration::from_secs(60);

    fn step(&mut self, event: Event<RawNetworkMessage>) -> Vec<(PeerId, RawNetworkMessage)> {
        let mut outbound = match event {
            Event::Connected {
                addr,
                local_addr,
                link,
            } => {
                self.connected(addr, local_addr, link);

                match link {
                    Link::Outbound => {
                        vec![(addr, self.version(addr, local_addr, self.tree.height()))]
                    }
                    Link::Inbound => vec![], // Wait to receive remote version.
                }
            }
            Event::Received(addr, msg) => self.receive(addr, msg),
            Event::Sent(_addr, _msg) => vec![],
            Event::Error(addr, err) => {
                debug!("Disconnected from {}", &addr);
                debug!("error: {}: {}", addr, err);

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
                // TODO: If this is a disconnect, then we need to send Command::Quit to the
                // connection somehow. Maybe not directly here, but perhaps this should return
                // not just messages but also the ability to drop a peer?
                // TODO: The other option is that error events (Event::Error) and disconnects
                // are handled one layer above. But this means the protocol can't decide on
                // these things, but instead it is the reactor that does.
                vec![]
            }
        };

        // TODO: Should be triggered when idle, potentially by an `Idle` event.
        if self.config.sync && self.ready.len() >= PEER_CONNECTION_THRESHOLD {
            if self.is_synced() {
                if matches!(self.state, State::Connecting) {
                    self.transition(State::Synced);
                }
            } else {
                // TODO: Pick a peer whos `height` is high enough.
                let ix = fastrand::usize(..self.ready.len());
                let p = *self.ready.iter().nth(ix).unwrap();

                let sync_msgs = self.initial_sync(p);
                outbound.extend_from_slice(&sync_msgs);
            }
        }

        outbound
            .into_iter()
            .map(|(addr, msg)| {
                debug!("{}: Sending {:?}", addr, msg.cmd());
                (
                    addr,
                    RawNetworkMessage {
                        magic: self.config.network.magic(),
                        payload: msg,
                    },
                )
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

    pub fn receive(
        &mut self,
        addr: PeerId,
        msg: RawNetworkMessage,
    ) -> Vec<(PeerId, NetworkMessage)> {
        debug!("{}: Received {:?}", addr, msg.cmd());

        if msg.magic != self.config.network.magic() {
            // TODO: Send rejection messsage to peer and close connection.
            todo!();
        }
        let mut peer = self
            .peers
            .get_mut(&addr)
            .unwrap_or_else(|| panic!("peer {} is not known", addr));
        let local_addr = peer.local_address;

        peer.last_active = Some(time::Instant::now());

        // TODO: Make sure we handle all messages at all times in some way.
        match &peer.state {
            PeerState::Handshake(Handshake::AwaitingVersion) => {
                if let NetworkMessage::Version(version) = msg.payload {
                    peer.receive_version(version);

                    match peer.link {
                        Link::Outbound => {}
                        Link::Inbound => {
                            return vec![
                                (addr, self.version(addr, local_addr, self.tree.height())),
                                (addr, NetworkMessage::Verack),
                            ]
                        }
                    }
                }
            }
            PeerState::Handshake(Handshake::AwaitingVerack) => {
                if msg.payload == NetworkMessage::Verack {
                    peer.receive_verack();

                    self.ready.insert(addr);
                    self.clock.add_sample(addr, peer.time_offset);

                    return match peer.link {
                        Link::Outbound => vec![
                            (addr, NetworkMessage::Verack),
                            (addr, NetworkMessage::SendHeaders),
                        ],
                        Link::Inbound => vec![(addr, NetworkMessage::SendHeaders)],
                    };
                }
            }
            PeerState::Syncing(Syncing::AwaitingHeaders(_locators)) => {
                assert_eq!(self.state, State::InitialSync(peer.address));

                if let NetworkMessage::Headers(headers) = msg.payload {
                    // TODO: Check that the headers received match the headers awaited.
                    return self.receive_headers(addr, headers);
                }
            }
            PeerState::Ready if self.state == State::Synced => {
                if let NetworkMessage::GetHeaders(GetHeadersMessage { locator_hashes, .. }) =
                    msg.payload
                {
                    assert!(locator_hashes.len() == 1);

                    let start_hash = locator_hashes[0];
                    let (start_height, _) = self.tree.get_block(&start_hash).unwrap();

                    // TODO: Set this to highest locator hash. We can assume that the peer
                    // is at this height if they know this hash.
                    peer.height = start_height;

                    let headers = self
                        .tree
                        .range(start_height + 1..self.tree.height() + 1)
                        .collect();

                    return vec![(addr, NetworkMessage::Headers(headers))];
                } else if let NetworkMessage::Headers(headers) = msg.payload {
                    return self.receive_headers(addr, headers);
                } else if let NetworkMessage::Inv(inventory) = msg.payload {
                    return self.receive_inv(addr, inventory);
                }
            }
            PeerState::Ready => {
                debug!("Ignoring {} from peer {}", msg.cmd(), peer.address);
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
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        NetworkMessage::Version(VersionMessage {
            version: self.config.protocol_version,
            services: self.config.services,
            timestamp,
            receiver: Address::new(&addr, ServiceFlags::NETWORK | ServiceFlags::COMPACT_FILTERS),
            sender: Address::new(&local_addr, ServiceFlags::NONE),
            nonce: 0,
            user_agent: USER_AGENT.to_owned(),
            start_height,
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
    fn receive_inv(&mut self, addr: PeerId, inv: Vec<Inventory>) -> Vec<(PeerId, NetworkMessage)> {
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
            vec![(addr, self.get_headers(self.locator_hashes(), *stop_hash))]
        } else {
            vec![]
        }
    }

    fn receive_headers(
        &mut self,
        addr: PeerId,
        headers: Vec<BlockHeader>,
    ) -> Vec<(PeerId, NetworkMessage)> {
        let length = headers.len();
        let peer = self
            .peers
            .get_mut(&addr)
            .unwrap_or_else(|| panic!("peer {} is not known", addr));

        debug!("{}: Received {} header(s)", addr, length);

        if let (Some(first), Some(last)) = (headers.first(), headers.last()) {
            debug!(
                "{}: Range = {}..{}",
                addr,
                first.bitcoin_hash(),
                last.bitcoin_hash()
            );
        } else {
            info!("{}: Finished synchronizing", addr);

            peer.transition(PeerState::Ready);
            self.transition(State::Synced);

            return vec![];
        }

        match self.tree.import_blocks(headers.into_iter(), &self.clock) {
            Ok((tip, height)) => {
                info!("Imported {} header(s) from {}", length, addr);
                info!("Chain height = {}, tip = {}", height, tip);

                let locators = vec![tip];

                peer.transition(PeerState::Syncing(Syncing::AwaitingHeaders(
                    locators.clone(),
                )));

                // TODO: We can break here if we've received less than 2'000 headers.
                return vec![(addr, self.get_headers(locators, BlockHash::default()))];
            }
            Err(err) => {
                // TODO: Bad blocks received!
                error!("Error importing headers: {}", err);

                vec![]
            }
        }
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

    use nakamoto_chain::block::cache::model;
    use nakamoto_chain::block::BlockHeader;
    use nakamoto_test::TREE;

    /// A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
    mod simulator {
        use super::*;

        pub fn run<P: Protocol<M>, M>(peers: Vec<(PeerId, &mut P, Vec<Event<M>>)>) {
            let mut sim: HashMap<PeerId, (&mut P, VecDeque<Event<M>>)> = HashMap::new();
            let mut events = Vec::new();

            // logging().apply().unwrap();

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
                        let out = proto.step(event);

                        for (receiver, msg) in out.into_iter() {
                            events.push((receiver, Event::Received(*peer, msg)));
                        }
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    fn logging() -> fern::Dispatch {
        fern::Dispatch::new()
            .format(move |out, message, record| {
                out.finish(format_args!(
                    "{:5} [{}] {}",
                    record.level(),
                    record.target(),
                    message
                ))
            })
            .level(log::LevelFilter::Trace)
            .chain(std::io::stderr())
    }

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
        let config = Config {
            sync: false,
            ..Config::default()
        };
        let clock = AdjustedTime::new();

        let alice_addr = ([127, 0, 0, 1], 8333).into();
        let bob_addr = ([127, 0, 0, 2], 8333).into();

        let mut alice = Bitcoin::new(tree.clone(), clock.clone(), config);
        let mut bob = Bitcoin::new(tree, clock, config);

        simulator::run(vec![
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
        ]);

        assert!(
            alice
                .peers
                .values()
                .all(|p| matches!(p.state, PeerState::Ready)),
            "alice: {:#?}",
            alice.peers
        );

        assert!(
            bob.peers
                .values()
                .all(|p| matches!(p.state, PeerState::Ready)),
            "bob: {:#?}",
            bob.peers
        );
    }

    #[test]
    fn test_initial_sync() {
        let config = Config::default();
        let clock = AdjustedTime::new();

        let alice_addr: PeerId = ([127, 0, 0, 1], 8333).into();
        let bob_addr: PeerId = ([127, 0, 0, 2], 8333).into();

        // Blockchain height we're going to be working with. Making it larger
        // than the threshold ensures a sync happens.
        let height = SYNC_THRESHOLD + 1;

        // Let's test Bob trying to sync with Alice from genesis.
        let mut alice_tree = TREE.clone();
        let bob_tree = model::Cache::new(*alice_tree.genesis());

        // Truncate chain to test height.
        alice_tree.rollback(height).unwrap();

        let mut alice = Bitcoin::new(alice_tree, clock.clone(), config);
        let mut bob = Bitcoin::new(bob_tree, clock, config);

        simulator::run(vec![
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
        ]);

        assert_eq!(alice.tree.height(), height);
        assert_eq!(bob.tree.height(), height);
    }
}
