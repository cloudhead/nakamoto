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
use crate::protocol::{self, Component, Link, Message, Output, PeerId, Protocol};

use std::collections::{HashSet, VecDeque};
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
/// Maximum number of latencies recorded per peer.
pub const MAX_RECORDED_LATENCIES: usize = 64;
/// Maximum height difference for a stale peer, to maintain the connection (2 weeks).
pub const MAX_STALE_HEIGHT_DIFFERENCE: Height = 2016;
/// Time to wait for response during peer handshake before disconnecting the peer.
pub const HANDSHAKE_TIMEOUT: LocalDuration = LocalDuration::from_secs(10);
/// Time interval to wait between sent pings.
pub const PING_INTERVAL: LocalDuration = LocalDuration::from_secs(60);
/// Time to wait to receive a pong when sending a ping.
pub const PING_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);
/// Target number of concurrent peer connections.
pub const TARGET_PEER_CONNECTIONS: usize = 32;

/// A time offset, in seconds.
pub type TimeOffset = i64;

/// Block locators. Consists of starting hashes and a stop hash.
pub type Locators = (Vec<BlockHash>, BlockHash);

/// Input into the state machine.
pub type Input = protocol::Input<RawNetworkMessage, Command>;

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
    /// Target number of peer connections.
    pub target_peers: usize,

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
    pub target_peers: usize,
    pub name: &'static str,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: network::Network::Mainnet,
            address_book: AddressBook::default(),
            services: ServiceFlags::NONE,
            protocol_version: PROTOCOL_VERSION,
            target_peers: TARGET_PEER_CONNECTIONS,
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
            target_peers,
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
            peers: HashMap::with_hasher(rng.clone().into()),
            network,
            services,
            protocol_version,
            target_peers,
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
        self.addrmgr.peer_connected(&addr);

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
    const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_secs(6);

    type Command = self::Command;

    fn initialize(&mut self, time: LocalTime) -> Vec<Output<RawNetworkMessage>> {
        self.clock.set_local_time(time);

        let mut outbound = Vec::new();

        for addr in self.addrmgr.iter().take(self.target_peers) {
            if let Ok(addr) = addr.socket_addr() {
                outbound.push(Output::Connect(addr));
            }
        }
        outbound
    }

    fn step(&mut self, input: Input) -> Vec<Output<RawNetworkMessage>> {
        let mut outbound = Vec::new();

        // Generate `Event` outputs from the input.
        self.generate_events(&input, &mut outbound);

        match input {
            Input::Tick(time) => {
                debug!("[{}] Tick: local_time = {}", self.name, time);

                // The local time is set from outside the protocol.
                self.clock.set_local_time(time);
            }
            Input::Connected {
                addr,
                local_addr,
                link,
            } => {
                // TODO: Should maintain a target for outbound peers only?
                if self.ready.len() >= self.target_peers {
                    match link {
                        Link::Inbound => {
                            // Don't allow inbound connections beyond the configured limit.
                            debug!(
                                "[{}] {}: Disconnecting: reached target peer connections ({})",
                                self.name, addr, self.target_peers
                            );
                            outbound.push(Output::Disconnect(addr));
                        }
                        Link::Outbound => {
                            // TODO: If we've initiated an outbound connection despite having
                            // reached our target, or if an inbound connection has been initiated
                            // while we were attempting to establish an outbound connection,
                            // we drop one of our existing connections to allow this new connection
                            // to be established.
                        }
                    }
                } else {
                    let nonce = self.connected(addr, local_addr, link);

                    match link {
                        Link::Outbound => {
                            info!("[{}] {}: Peer connected (outbound)", self.name, &addr);

                            outbound.push(
                                self.message(
                                    addr,
                                    self.version(addr, local_addr, nonce, self.height),
                                ),
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
            }
            Input::Disconnected(addr) => {
                debug!("[{}] Disconnected from {}", self.name, &addr);

                self.peers.remove(&addr);
                self.ready.remove(&addr);
                self.connected.remove(&addr);
                self.disconnected.insert(addr);
                self.syncmgr.peer_disconnected(&addr);
                self.addrmgr.peer_disconnected(&addr);

                if self.ready.len() < self.target_peers {
                    if let Some(addr) = self.addrmgr.sample() {
                        if let Ok(sockaddr) = addr.socket_addr() {
                            outbound.push(Output::Connect(sockaddr));
                        } else {
                            // TODO: Perhaps the address manager should just return addresses
                            // that can be converted to socket addresses?
                            // The only ones that cannot are Tor addresses.
                            todo!();
                        }
                    } else {
                        // TODO: Out of addresses, ask for more!
                    }
                }
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
                if let Some(peer) = self.peers.get_mut(&addr) {
                    match component {
                        Component::HandshakeManager => match peer.state {
                            PeerState::Handshake(Handshake::AwaitingVerack)
                            | PeerState::Handshake(Handshake::AwaitingVersion) => {
                                outbound.push(Output::Disconnect(addr));
                            }
                            _ => {}
                        },
                        Component::SyncManager => {
                            let (timeout, get_headers) = self.syncmgr.receive_timeout(addr);

                            if let Some(syncmgr::PeerTimeout) = timeout {
                                outbound.push(Output::Disconnect(addr));
                            }
                            if let Some(req) = get_headers {
                                outbound.extend(self.get_headers(req));
                            }
                        }
                        Component::PingManager => {
                            if let PeerState::Ready { last_active, .. } = peer.state {
                                let now = self.clock.local_time();

                                if let Some((_, last_ping)) = peer.last_ping {
                                    // A ping was sent and we're waiting for a `pong`. If too much
                                    // time has passed, we consider this peer dead, and disconnect
                                    // from them.
                                    if now - last_ping >= PING_TIMEOUT {
                                        let address = peer.address;

                                        log::debug!(
                                            "[{}] {}: Disconnecting (ping timeout)",
                                            self.name,
                                            address
                                        );
                                        self.disconnect(&address, &mut outbound);
                                    }
                                } else if now - last_active >= PING_INTERVAL {
                                    let ping = peer.ping(now);

                                    outbound.extend(vec![
                                        Output::Message(
                                            peer.address,
                                            RawNetworkMessage {
                                                magic: self.network.magic(),
                                                payload: ping,
                                            },
                                        ),
                                        Output::SetTimeout(
                                            addr,
                                            Component::PingManager,
                                            PING_TIMEOUT,
                                        ),
                                        Output::SetTimeout(
                                            addr,
                                            Component::PingManager,
                                            PING_INTERVAL,
                                        ),
                                    ]);
                                }
                            }
                        }
                    }
                } else {
                    debug!("[{}] Peer {} no longer exists (ignoring)", self.name, addr);
                }
            }
        };
        self.drain_sync_events(&mut outbound);

        if log::max_level() >= log::Level::Debug {
            outbound.iter().for_each(|o| match o {
                Output::Message(addr, msg) => {
                    debug!("[{}] {}: Sending {:?}", self.name, addr, msg.display());
                }
                Output::Event(Event::Received(_, _)) => {}
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

        if msg.magic != self.network.magic() {
            // TODO: Needs test.
            debug!(
                "[{}] {}: Received message with invalid magic: {}",
                self.name, addr, msg.magic
            );
            return vec![Output::Disconnect(addr)];
        }

        let mut peer = if let Some(peer) = self.peers.get_mut(&addr) {
            peer
        } else {
            debug!("[{}] Peer {} is not known", self.name, addr);
            return vec![];
        };
        let local_addr = peer.local_address;

        debug!(
            "[{}] {}: Received {:?} ({:?})",
            self.name, addr, cmd, peer.state
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
                return vec![self.message(addr, NetworkMessage::Pong(nonce))];
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
                self.addrmgr
                    .insert(addrs.into_iter(), addrmgr::Source::Peer(addr));
                return vec![];
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
                } else {
                    // TODO: Include disconnect reason.
                    debug!("[{}] {}: Peer misbehaving", self.name, addr);
                    return vec![Output::Disconnect(addr)];
                }
            }
            PeerState::Handshake(Handshake::AwaitingVerack) => {
                if msg.payload == NetworkMessage::Verack {
                    peer.receive_verack(now);

                    self.ready.insert(addr);
                    self.clock.record_offset(addr, peer.time_offset);
                    self.addrmgr.peer_negotiated(&addr, peer.services.clone());

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
                            .peer_negotiated(
                                peer.address,
                                peer.height,
                                peer.tip,
                                peer.services,
                                peer.link,
                            )
                            .map_or(vec![], |req| self.get_headers(req)),
                    );

                    return out;
                } else {
                    // TODO: Include disconnect reason.
                    debug!("[{}] {}: Peer misbehaving", self.name, addr);
                    return vec![Output::Disconnect(addr)];
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
                    return self.receive_inv(addr, inventory);
                }
            }
            PeerState::Disconnecting => {
                debug!(
                    "[{}] Ignoring {} from peer {} (disconnecting)",
                    self.name, cmd, peer.address
                );
            }
        }

        debug!("[{}] {}: Ignoring {:?}", self.name, peer.address, cmd,);

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

    fn generate_events(&self, input: &Input, outbound: &mut Vec<Output<RawNetworkMessage>>) {
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
