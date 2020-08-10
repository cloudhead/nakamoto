use crossbeam_channel as chan;
use log::*;
use nonempty::NonEmpty;

pub mod addrmgr;
pub mod network;
pub mod syncmgr;
pub use network::Network;

use addrmgr::AddressManager;
use syncmgr::SyncManager;

use crate::address_book::AddressBook;
use crate::event::Event;
use crate::protocol::{Component, Input, Link, Message, Output, PeerId, Protocol};

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::net;

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use bitcoin::network::message_network::VersionMessage;

use nakamoto_common::block::time::{AdjustedTime, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{self, BlockTree, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height, Transaction};

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
/// Time to wait for response during peer handshake before disconnecting the peer.
pub const HANDSHAKE_TIMEOUT: LocalDuration = LocalDuration::from_secs(10);
/// Time interval to wait between sent pings.
pub const PING_INTERVAL: LocalDuration = LocalDuration::from_secs(60);

/// A time offset, in seconds.
pub type TimeOffset = i64;

/// Block locators. Consists of starting hashes and a stop hash.
pub type Locators = (Vec<BlockHash>, BlockHash);

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
    SubmitTransaction(Transaction),
    Shutdown,
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

mod message {
    use super::*;

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

    pub struct Builder {
        #[allow(dead_code)]
        version: u32,
        magic: u32,
    }

    impl Builder {
        pub fn new(version: u32, magic: u32) -> Self {
            Builder { version, magic }
        }

        pub fn build(
            &self,
            addr: net::SocketAddr,
            payload: NetworkMessage,
        ) -> Output<RawNetworkMessage> {
            Output::Message(addr, message::raw(payload, self.magic))
        }

        #[allow(dead_code)]
        pub fn get_headers(&self, locators: Locators) -> NetworkMessage {
            message::get_headers(locators, self.version)
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////

/// An instantiation of `Protocol`, for the Bitcoin P2P network. Parametrized over the
/// block-tree.
#[derive(Debug)]
pub struct Bitcoin<T> {
    /// Peer states.
    pub peers: HashMap<PeerId, Peer>,
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
    /// Block height of active chain.
    pub height: Height,

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
    name: &'static str,
    /// Random number generator.
    rng: fastrand::Rng,
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
    pub fn new(tree: T, clock: AdjustedTime<PeerId>, rng: fastrand::Rng, config: Config) -> Self {
        let Config {
            network,
            address_book,
            services,
            protocol_version,
            relay,
            user_agent,
            name,
        } = config;

        let height = tree.height();

        let addrmgr = AddressManager::from(address_book, rng.clone());
        let syncmgr = SyncManager::new(
            tree,
            syncmgr::Config {
                max_message_headers: MAX_MESSAGE_HEADERS,
                request_timeout: syncmgr::REQUEST_TIMEOUT,
            },
            rng.clone(),
        );

        Self {
            peers: HashMap::new(),
            network,
            services,
            protocol_version,
            relay,
            user_agent,
            name,
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

    fn connected(&mut self, addr: PeerId, local_addr: net::SocketAddr, link: Link) -> u64 {
        self.connected.insert(addr);
        self.disconnected.remove(&addr);

        let nonce = self.rng.u64(..);
        let rng = self.rng.clone();

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
                self.name,
            ),
        );

        nonce
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
    /// The peer's services.
    pub services: ServiceFlags,
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
    /// Random number generator.
    pub rng: fastrand::Rng,
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

    #[allow(dead_code)]
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
        debug!(
            "[{}] {}: {:?} -> {:?}",
            self.ctx, self.address, self.state, state
        );

        self.state = state;
    }
}

impl<T: BlockTree> Protocol<RawNetworkMessage> for Bitcoin<T> {
    const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_secs(60 * 5);

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

    // FIXME: This function shouldn't take a time input: what if it gets a timeout
    // which doesn't match the input time?
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

                        outbound.push(
                            self.message(addr, self.version(addr, local_addr, nonce, self.height)),
                        );
                    }
                    Link::Inbound => {
                        info!("[{}] {}: Peer connected (inbound)", self.name, &addr);
                    } // Wait to receive remote version.
                }
                // Set a timeout for receiving the `version` message.
                outbound.push(Output::SetTimeout(
                    addr,
                    Component::HandshakeManager,
                    HANDSHAKE_TIMEOUT,
                ));
            }
            Input::Disconnected(addr) => {
                debug!("[{}] Disconnected from {}", self.name, &addr);

                self.peers.remove(&addr);
                self.ready.remove(&addr);
                self.connected.remove(&addr);
                self.disconnected.insert(addr);
                self.syncmgr.peer_disconnected(&addr);
            }
            Input::Received(addr, msg) => {
                for out in self.receive(addr, msg) {
                    outbound.push(out);
                }
            }
            Input::Sent(_addr, _msg) => {}
            Input::Command(cmd) => {
                match cmd {
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

                        let result = self.syncmgr.import_blocks(headers.into_iter(), &self.clock);

                        match result {
                            Ok((import_result, send)) => {
                                reply.send(Ok(import_result.clone())).ok();

                                if let ImportResult::TipChanged(_, height, _) = import_result {
                                    self.height = height;
                                }

                                if let Some(syncmgr::SendHeaders { addrs, headers }) = send {
                                    for addr in addrs {
                                        outbound.push(self.message(
                                            addr,
                                            NetworkMessage::Headers(headers.clone()),
                                        ));
                                    }
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
                        debug!("[{}] Received command: SubmitTransaction(..)", self.name);

                        // TODO: Factor this out when we have a `peermgr`.
                        let ix = self.rng.usize(..self.ready.len());
                        let peer = *self.ready.iter().nth(ix).unwrap();

                        outbound.push(self.message(peer, NetworkMessage::Tx(tx)));
                    }
                    Command::Shutdown => {
                        outbound.push(Output::Shutdown);
                    }
                }
            }
            Input::Timeout(addr, component) => {
                debug!(
                    "[{}] Received timeout for {} @ {:?}",
                    self.name, addr, component
                );

                // We may not have the peer anymore, if it was disconnected and remove in
                // the meantime.
                if let Some(peer) = self.peers.get(&addr) {
                    match component {
                        Component::HandshakeManager => match peer.state {
                            PeerState::Handshake(Handshake::AwaitingVerack)
                            | PeerState::Handshake(Handshake::AwaitingVersion) => {
                                outbound.push(Output::Disconnect(addr));
                            }
                            _ => {}
                        },
                        Component::SyncManager => {
                            if let Some(syncmgr::PeerTimeout) = self.syncmgr.handle_timeout(addr) {
                                outbound.push(Output::Disconnect(addr));
                            }
                        }
                        _ => {
                            warn!("[{}] Unhandled timeout: {:?}", self.name, (addr, component));
                        }
                    }
                } else {
                    debug!("[{}] Peer {} no longer exists (ignoring)", self.name, addr);
                }
            }
            Input::Idle => {
                let now = self.clock.local_time();
                let mut disconnect = Vec::new();

                debug!("[{}] Idle: local_time = {}", self.name, now);

                for (_, peer) in self.peers.iter_mut() {
                    if let PeerState::Ready { last_active, .. } = peer.state {
                        if let Some((_, last_ping)) = peer.last_ping {
                            // A ping was sent and we're waiting for a `pong`. If too much
                            // time has passed, we consider this peer dead, and disconnect
                            // from them.
                            if now - last_ping >= Self::IDLE_TIMEOUT {
                                disconnect.push(peer.address);
                            }
                        } else if now - last_active >= PING_INTERVAL {
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
        self.drain_sync_events(&mut outbound);

        if log::max_level() >= log::Level::Debug {
            outbound.iter().for_each(|o| match o {
                Output::Message(addr, msg) => {
                    debug!("[{}] {}: Sending {:?}", self.name, addr, msg.display());
                }
                _ => debug!("[{}] Output: {:?}", self.name, o),
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

    pub fn request(
        &self,
        addr: PeerId,
        message: NetworkMessage,
        timeout: LocalDuration,
        component: Component,
        out: &mut Vec<Output<RawNetworkMessage>>,
    ) {
        out.push(self.message(addr, message));
        out.push(Output::SetTimeout(addr, component, timeout));
    }

    fn get_headers(&self, request: syncmgr::GetHeaders) -> Vec<Output<RawNetworkMessage>> {
        let mut out = Vec::new();
        let syncmgr::GetHeaders {
            addr,
            locators,
            timeout,
        } = request;

        self.request(
            addr,
            message::get_headers(locators, self.protocol_version),
            timeout,
            Component::SyncManager,
            &mut out,
        );
        out
    }

    pub fn receive(
        &mut self,
        addr: PeerId,
        msg: RawNetworkMessage,
    ) -> Vec<Output<RawNetworkMessage>> {
        let now = self.clock.local_time();
        let cmd = msg.cmd();

        debug!("[{}] {}: Received {:?}", self.name, addr, cmd);

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
                    let height = self.height;

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
                    peer.services = services;
                    peer.transition(PeerState::Handshake(Handshake::AwaitingVerack));

                    match peer.link {
                        Link::Outbound => {
                            return vec![Output::SetTimeout(
                                addr,
                                Component::HandshakeManager,
                                HANDSHAKE_TIMEOUT,
                            )]
                        }
                        Link::Inbound => {
                            let nonce = peer.nonce;

                            return vec![
                                self.message(
                                    addr,
                                    self.version(addr, local_addr, nonce, self.height),
                                ),
                                self.message(addr, NetworkMessage::Verack),
                                Output::SetTimeout(
                                    addr,
                                    Component::HandshakeManager,
                                    HANDSHAKE_TIMEOUT,
                                ),
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
                    let link = peer.link;

                    let mut out = Vec::new();

                    let msg = message::Builder::new(self.protocol_version, self.network.magic());

                    out.extend(match link {
                        Link::Outbound => vec![
                            msg.build(addr, NetworkMessage::Verack),
                            msg.build(addr, NetworkMessage::SendHeaders),
                            msg.build(addr, ping),
                        ],
                        Link::Inbound => vec![
                            msg.build(addr, NetworkMessage::SendHeaders),
                            msg.build(addr, ping),
                        ],
                    });
                    out.extend(
                        self.syncmgr
                            .peer_connected(
                                peer.address,
                                peer.height,
                                peer.tip,
                                peer.services,
                                peer.link,
                            )
                            .map_or(vec![], |req| self.get_headers(req)),
                    );

                    return out;
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
                        self.syncmgr.receive_get_headers(
                            &addr,
                            (locator_hashes, stop_hash),
                            MAX_MESSAGE_HEADERS,
                        )
                    {
                        return addrs
                            .into_iter()
                            .map(|a| self.message(a, NetworkMessage::Headers(headers.clone())))
                            .collect::<Vec<_>>();
                    }
                } else if let NetworkMessage::Inv(inventory) = msg.payload {
                    return self
                        .syncmgr
                        .receive_inv(addr, inventory)
                        .map_or(vec![], |req| self.get_headers(req));
                }
            }
            PeerState::Disconnecting => {
                debug!(
                    "[{}] Ignoring {} from peer {} (disconnecting)",
                    self.name, cmd, peer.address
                );
            }
        }

        debug!(
            "[{}] {}: Ignoring {:?} (state = {:?})",
            self.name, peer.address, cmd, peer.state,
        );

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

    /// Receive an `inv` message. This will happen if we are out of sync with a peer. And blocks
    /// are being announced. Otherwise, we expect to receive a `headers` message.
    fn receive_inv(&mut self, addr: PeerId, inv: Vec<Inventory>) -> Vec<Output<RawNetworkMessage>> {
        self.syncmgr
            .receive_inv(addr, inv)
            .map_or(vec![], |req| self.get_headers(req))
    }

    fn receive_headers(
        &mut self,
        addr: PeerId,
        headers: Vec<BlockHeader>,
    ) -> Vec<Output<RawNetworkMessage>> {
        let headers = if let Some(headers) = NonEmpty::from_vec(headers) {
            headers
        } else {
            return vec![];
        };

        debug!(
            "[{}] {}: Received {} header(s)",
            self.name,
            addr,
            headers.len()
        );
        let mut outbound = Vec::new();

        match self.syncmgr.receive_headers(&addr, headers, &self.clock) {
            syncmgr::SyncResult::GetHeaders(req) => {
                outbound.extend(self.get_headers(req));
            }
            syncmgr::SyncResult::SendHeaders(syncmgr::SendHeaders { addrs, headers }) => {
                if !addrs.is_empty() {
                    debug!(
                        "[{}] Sending {} header(s) to {} peer(s)..",
                        self.name,
                        headers.len(),
                        addrs.len()
                    );
                }

                for addr in addrs {
                    outbound.push(self.message(addr, NetworkMessage::Headers(headers.clone())));
                }
            }
            syncmgr::SyncResult::Okay => {}
        }
        outbound
    }

    fn drain_sync_events(&mut self, outbound: &mut Vec<Output<RawNetworkMessage>>) {
        for event in self.syncmgr.events() {
            match event {
                syncmgr::Event::ReceivedInvalidHeaders(addr, error) => {
                    // TODO: Bad blocks received!
                    // TODO: Disconnect peer.
                    debug!(
                        "[{}] {}: Received invalid headers: {}",
                        self.name, addr, error
                    );
                }
                syncmgr::Event::HeadersImported(import_result) => {
                    debug!("[{}] Import result: {:?}", self.name, &import_result);

                    if let ImportResult::TipChanged(tip, height, _) = import_result {
                        info!("[{}] Chain height = {}, tip = {}", self.name, height, tip);
                    }
                    outbound.push(Output::Event(Event::HeadersImported(import_result)));
                }
                syncmgr::Event::Synced(_, _) => {
                    outbound.push(Output::Event(Event::Synced));
                }
                syncmgr::Event::Syncing(_addr) => {
                    outbound.push(Output::Event(Event::Syncing));
                }
                syncmgr::Event::WaitingForPeers => {
                    // TODO: Connect to peers!
                    debug!("[{}] syncmgr: Waiting for peers..", self.name);

                    outbound.push(Output::Event(Event::Connecting));
                }
                syncmgr::Event::BlockDiscovered(from, hash) => {
                    debug!("[{}] {}: Discovered new block: {}", self.name, from, &hash);
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
    use bitcoin_hashes::hex::FromHex;
    use std::collections::VecDeque;
    use std::time::SystemTime;

    use nakamoto_common::block::BlockHeader;

    use nakamoto_test::block::cache::model;
    use nakamoto_test::logger;
    use nakamoto_test::TREE;

    fn payload<M: Message>(o: &Output<M>) -> Option<&M::Payload> {
        match o {
            Output::Message(_, m) => Some(m.payload()),
            _ => None,
        }
    }

    mod setup {
        use super::*;

        /// Test protocol config.
        pub const CONFIG: Config = Config {
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

        pub fn singleton(network: Network) -> (Bitcoin<model::Cache>, LocalTime) {
            use bitcoin::blockdata::constants;

            let genesis = constants::genesis_block(network.into()).header;
            let tree = model::Cache::new(genesis);
            let time = LocalTime::from_secs(genesis.time as u64);
            let clock = AdjustedTime::new(time);

            (
                Bitcoin::new(tree, clock, fastrand::Rng::new(), CONFIG),
                time,
            )
        }

        pub fn pair(
            network: Network,
        ) -> (
            (Bitcoin<model::Cache>, PeerId),
            (Bitcoin<model::Cache>, PeerId),
            LocalTime,
        ) {
            use bitcoin::blockdata::constants;

            let genesis = constants::genesis_block(network.into()).header;
            let tree = model::Cache::new(genesis);
            let time = LocalTime::from_secs(genesis.time as u64);
            let clock = AdjustedTime::new(time);

            let mut alice = Bitcoin::new(tree.clone(), clock.clone(), fastrand::Rng::new(), CONFIG);
            let mut bob = Bitcoin::new(tree, clock, fastrand::Rng::new(), CONFIG);

            alice.initialize(time);
            bob.initialize(time);

            let alice_addr = ([152, 168, 3, 33], 3333).into();
            let bob_addr = ([152, 168, 7, 77], 7777).into();

            simulator::handshake(&mut alice, alice_addr, &mut bob, bob_addr, time);

            ((alice, alice_addr), (bob, bob_addr), time)
        }
    }

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

        let mut alice = Bitcoin::new(
            tree.clone(),
            clock.clone(),
            fastrand::Rng::new(),
            setup::CONFIG,
        );
        let mut bob = Bitcoin::new(tree, clock, fastrand::Rng::new(), setup::CONFIG);

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
        use fastrand::Rng;

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

        let mut alice = Bitcoin::new(alice_tree, clock.clone(), Rng::new(), setup::CONFIG);

        // Bob connects to Alice.
        {
            let mut bob = Bitcoin::new(bob_tree.clone(), clock.clone(), Rng::new(), setup::CONFIG);

            simulator::handshake(&mut bob, bob_addr, &mut alice, alice_addr, local_time);

            assert_eq!(alice.syncmgr.tree.height(), height);
            assert_eq!(bob.syncmgr.tree.height(), height);
        }
        // Alice connects to Bob.
        {
            let mut bob = Bitcoin::new(bob_tree, clock, Rng::new(), setup::CONFIG);

            simulator::handshake(&mut alice, alice_addr, &mut bob, bob_addr, local_time);

            assert_eq!(alice.syncmgr.tree.height(), height);
            assert_eq!(bob.syncmgr.tree.height(), height);
        }
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
            fastrand::Rng::new(),
            Config {
                name: "alice",
                ..setup::CONFIG
            },
        );
        let mut bob = Bitcoin::new(
            tree,
            clock,
            fastrand::Rng::new(),
            Config {
                name: "bob",
                ..setup::CONFIG
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

    #[test]
    fn test_getheaders_timeout() {
        logger::init(log::Level::Debug);

        let network = Network::Mainnet;
        // TODO: Protocol should try different peers if it can't get the headers from the first
        // peer. It should keep trying until it succeeds.
        let ((mut local, _), (_, remote_addr), time) = setup::pair(network);
        // Some hash for a nonexistent block.
        let hash =
            BlockHash::from_hex("0000000000b7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
                .unwrap();

        let out = local.step(
            Input::Received(
                remote_addr,
                message::raw(
                    NetworkMessage::Inv(vec![Inventory::Block(hash)]),
                    network.magic(),
                ),
            ),
            time,
        );
        out.iter()
            .find(|o| matches!(payload(o), Some(NetworkMessage::GetHeaders(_))))
            .expect("a `getheaders` message should be returned");
        out.iter()
            .find(|o| matches!(o, Output::SetTimeout(addr, _, _) if addr == &remote_addr))
            .expect("a timer should be returned");

        local
            .step(Input::Timeout(remote_addr, Component::SyncManager), time)
            .iter()
            .find(|o| matches!(o, Output::Disconnect(addr) if addr == &remote_addr))
            .expect("the unresponsive peer should be disconnected");
    }

    #[test]
    fn test_handshake_version_timeout() {
        let network = Network::Mainnet;
        let (mut instance, time) = setup::singleton(network);

        let remote = ([131, 31, 11, 33], 11111).into();
        let local = ([0, 0, 0, 0], 0).into();

        for link in &[Link::Outbound, Link::Inbound] {
            let out = instance.step(
                Input::Connected {
                    addr: remote,
                    local_addr: local,
                    link: *link,
                },
                time,
            );
            out.iter()
                .find(|o| matches!(o, Output::SetTimeout(addr, _, _) if addr == &remote))
                .expect("a timer should be returned");

            let out = instance.step(Input::Timeout(remote, Component::HandshakeManager), time);
            assert!(out
                .iter()
                .any(|o| matches!(o, Output::Disconnect(a) if a == &remote)));
        }
    }

    #[test]
    fn test_handshake_verack_timeout() {
        let network = Network::Mainnet;
        let (mut instance, time) = setup::singleton(network);

        let remote = ([131, 31, 11, 33], 11111).into();
        let local = ([0, 0, 0, 0], 0).into();

        for link in &[Link::Outbound, Link::Inbound] {
            instance.step(
                Input::Connected {
                    addr: remote,
                    local_addr: local,
                    link: *link,
                },
                time,
            );

            let out = instance.step(
                Input::Received(
                    remote,
                    RawNetworkMessage {
                        magic: network.magic(),
                        payload: instance.version(local, remote, 0, 0),
                    },
                ),
                time,
            );
            out.iter()
                .find(|o| matches!(o, Output::SetTimeout(addr, _, _) if *addr == remote))
                .expect("a timer should be returned");

            let out = instance.step(Input::Timeout(remote, Component::HandshakeManager), time);
            assert!(out
                .iter()
                .any(|o| matches!(o, Output::Disconnect(a) if *a == remote)));
        }
    }
}
