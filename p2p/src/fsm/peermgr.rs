//! Peer manager. Handles peer negotiation (handshake).
//!
//! The steps for an *outbound* handshake are:
//!
//!   1. Send `version` message.
//!   2. Expect `version` message from remote.
//!   3. Expect `verack` message from remote.
//!   4. Send `verack` message.
//!
//! The steps for an *inbound* handshake are:
//!
//!   1. Expect `version` message from remote.
//!   2. Send `version` message.
//!   3. Send `verack` message.
//!   4. Expect `verack` message from remote.
//!
use std::net;
use std::sync::Arc;

use nakamoto_common::bitcoin::p2p::message_network::VersionMessage;
use nakamoto_common::bitcoin::p2p::{Address, ServiceFlags};
use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::Height;
use nakamoto_common::collections::{HashMap, HashSet};
use nakamoto_common::p2p::peer::{AddressSource, Source};
use nakamoto_common::p2p::Domain;
use nakamoto_common::source;
use nakamoto_net as network;

use crate::fsm::addrmgr;
use crate::fsm::DisconnectReason;

use super::output::{Connect, Disconnect, SetTimer, Wire};
use super::{Hooks, Link, PeerId, Socket, Whitelist};

/// Time to wait for response during peer handshake before disconnecting the peer.
pub const HANDSHAKE_TIMEOUT: LocalDuration = LocalDuration::from_secs(12);
/// Time to wait for a new connection.
/// TODO: Should be in config.
pub const CONNECTION_TIMEOUT: LocalDuration = LocalDuration::from_secs(6);
/// Time to wait until idle.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);
/// Target number of concurrent outbound peer connections.
pub const TARGET_OUTBOUND_PEERS: usize = 8;
/// Maximum number of inbound peer connections.
pub const MAX_INBOUND_PEERS: usize = 16;

/// Maximum height difference for a stale peer, to maintain the connection (2 weeks).
const MAX_STALE_HEIGHT_DIFFERENCE: Height = 2016;

/// A time offset, in seconds.
type TimeOffset = i64;

/// An event originating in the peer manager.
#[derive(Debug, Clone)]
pub enum Event {
    /// The `version` message was received from a peer.
    VersionReceived {
        /// The peer's id.
        addr: PeerId,
        /// The version message.
        msg: VersionMessage,
    },
    /// A peer has successfully negotiated (handshaked).
    Negotiated {
        /// The peer's id.
        addr: PeerId,
        /// Connection link.
        link: Link,
        /// Services offered by negotiated peer.
        services: ServiceFlags,
        /// Peer user agent.
        user_agent: String,
        /// Peer height.
        height: Height,
        /// Protocol version.
        version: u32,
    },
    /// Connecting to a peer found from the specified source.
    Connecting(PeerId, Source, ServiceFlags),
    /// Connection attempt failed.
    ConnectionFailed(PeerId, Arc<std::io::Error>),
    /// A new peer has connected and is ready to accept messages.
    /// This event is triggered *before* the peer handshake
    /// has successfully completed.
    Connected(PeerId, Link),
    /// A peer has been disconnected.
    Disconnected(PeerId, network::Disconnect<DisconnectReason>),
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VersionReceived { addr, msg } => write!(
                fmt,
                "Peer address = {}, version = {}, height = {}, agent = {}, services = {}, timestamp = {}",
                addr, msg.version, msg.start_height, msg.user_agent, msg.services, msg.timestamp
            ),
            Self::Negotiated {
                addr,
                height,
                services,
                ..
            } => write!(
                fmt,
                "{}: Peer negotiated with services {} and height {}..",
                addr, services, height
            ),
            Self::Connecting(addr, source, services) => {
                write!(
                    fmt,
                    "Connecting to peer {} from source `{}` with {}",
                    addr, source, services
                )
            }
            Self::Connected(addr, link) => write!(fmt, "{}: Peer connected ({:?})", &addr, link),
            Self::ConnectionFailed(addr, err) => {
                write!(fmt, "{}: Peer connection attempt failed: {}", &addr, err)
            }
            Self::Disconnected(addr, reason) => {
                write!(fmt, "Disconnected from {} ({})", &addr, reason)
            }
        }
    }
}

/// Peer manager configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Protocol version.
    pub protocol_version: u32,
    /// Peer whitelist.
    pub whitelist: Whitelist,
    /// Services offered by this implementation.
    pub services: ServiceFlags,
    /// Peer addresses to persist connections with.
    pub persistent: Vec<net::SocketAddr>,
    /// Services required by peers.
    pub required_services: ServiceFlags,
    /// Peer services preferred. We try to maintain as many
    /// connections to peers with these services.
    pub preferred_services: ServiceFlags,
    /// Target number of outbound peer connections.
    pub target_outbound_peers: usize,
    /// Maximum number of inbound peer connections.
    pub max_inbound_peers: usize,
    /// Maximum time to wait between reconnection attempts.
    pub retry_max_wait: LocalDuration,
    /// Minimum time to wait between reconnection attempts.
    pub retry_min_wait: LocalDuration,
    /// Our user agent.
    pub user_agent: &'static str,
    /// Supported communication domains.
    pub domains: Vec<Domain>,
}

/// Peer negotiation (handshake) state.
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
enum HandshakeState {
    /// Received "version" and waiting for "verack" message from remote.
    ReceivedVersion { since: LocalTime },
    /// Received "verack". Handshake is complete.
    ReceivedVerack { since: LocalTime },
}

/// A peer connection. Peers that haven't yet sent their `version` message are stored as
/// connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    /// Remote peer socket.
    pub socket: Socket,
    /// Local peer address.
    pub local_addr: net::SocketAddr,
    /// Whether this is an inbound or outbound peer connection.
    pub link: Link,
    /// Connected since this time.
    pub since: LocalTime,
}

/// Peer state.
#[derive(Debug, Clone)]
pub enum Peer {
    /// A connection is being attempted.
    Connecting {
        /// Time the connection was attempted.
        time: LocalTime,
    },
    /// A connection is established.
    Connected {
        /// Connection.
        conn: Connection,
        /// Peer information, if a `version` message was received.
        peer: Option<PeerInfo>,
    },
    /// The connection is being closed.
    Disconnecting,
}

/// A peer with protocol information.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// The peer's best height.
    pub height: Height,
    /// The peer's services.
    pub services: ServiceFlags,
    /// Peer user agent string.
    pub user_agent: String,
    /// An offset in seconds, between this peer's clock and ours.
    /// A positive offset means the peer's clock is ahead of ours.
    pub time_offset: TimeOffset,
    /// Whether this peer relays transactions.
    pub relay: bool,
    /// Whether this peer supports BIP-339.
    pub wtxidrelay: bool,
    /// The max protocol version supported by both the peer and nakamoto.
    pub version: u32,
    /// Whether this is a persistent peer.
    pub persistent: bool,

    /// Peer nonce. Used to detect self-connections.
    nonce: u64,
    /// Peer handshake state.
    state: HandshakeState,
}

impl PeerInfo {
    /// Check whether the peer has finished negotiating and received our `version`.
    pub fn is_negotiated(&self) -> bool {
        matches!(self.state, HandshakeState::ReceivedVerack { .. })
    }
}

/// Manages peer connections and handshake.
#[derive(Debug)]
pub struct PeerManager<U, C> {
    /// Peer manager configuration.
    pub config: Config,

    /// Last time we were idle.
    last_idle: Option<LocalTime>,
    /// Connection states.
    peers: HashMap<net::SocketAddr, Peer>,
    /// Peers that have been disconnected and a retry attempt is scheduled.
    disconnected: HashMap<net::SocketAddr, (Option<LocalTime>, usize)>,
    upstream: U,
    rng: fastrand::Rng,
    hooks: Hooks,
    clock: C,
}

impl<U: Wire<Event> + SetTimer + Connect + Disconnect, C: Clock> PeerManager<U, C> {
    /// Create a new peer manager.
    pub fn new(config: Config, rng: fastrand::Rng, hooks: Hooks, upstream: U, clock: C) -> Self {
        let peers = HashMap::with_hasher(rng.clone().into());
        let disconnected = HashMap::with_hasher(rng.clone().into());

        Self {
            config,
            last_idle: None,
            peers,
            disconnected,
            upstream,
            rng,
            hooks,
            clock,
        }
    }

    /// Initialize the peer manager. Must be called once.
    pub fn initialize<A: AddressSource>(&mut self, addrs: &mut A) {
        let peers = self.config.persistent.clone();

        for addr in peers {
            if !self.connect(&addr) {
                // TODO: Return error here, or send event.
                panic!(
                    "{}: unable to connect to persistent peer: {}",
                    source!(),
                    addr
                );
            }
        }
        self.upstream.set_timer(IDLE_TIMEOUT);
        self.maintain_connections(addrs);
    }

    /// A persistent peer has been disconnected.
    fn persistent_disconnected(&mut self, addr: &net::SocketAddr, local_time: LocalTime) {
        let (retry_at, attempts) = self.disconnected.entry(*addr).or_default();
        let delay = LocalDuration::from_secs(2u64.saturating_pow(*attempts as u32))
            .clamp(self.config.retry_min_wait, self.config.retry_max_wait);

        *retry_at = Some(local_time + delay);
        *attempts += 1;

        self.upstream.set_timer(delay);
    }

    /// Maintain persistent peer connections.
    fn maintain_persistent(&mut self) {
        let local_time = self.clock.local_time();
        let mut reconnect = Vec::new();

        for (addr, (retry_at, _)) in &mut self.disconnected {
            if let Some(t) = retry_at {
                if *t <= local_time {
                    *retry_at = None;
                    reconnect.push(*addr);
                }
            }
        }

        for addr in reconnect {
            if !self.connect(&addr) {
                log::error!(target: "p2p", "Couldn't establish connection with {addr}");
            }
        }
    }

    /// Called when a peer connected.
    pub fn peer_connected(
        &mut self,
        addr: net::SocketAddr,
        local_addr: net::SocketAddr,
        link: Link,
        height: Height,
    ) {
        let local_time = self.clock.local_time();

        #[cfg(debug_assertions)]
        if link.is_outbound() {
            debug_assert!(self.is_connecting(&addr), "{} is not connecting", addr)
        }
        debug_assert!(!self.is_connected(&addr), "{} is already connected", addr);

        // TODO: There is a chance that we simultaneously connect to a peer that is connecting
        // to us. This would create two connections to the same peer, one outbound and one
        // inbound. To prevent this, we could look at IPs when receiving inbound connections,
        // to check whether we are already connected to the peer.

        self.peers.insert(
            addr,
            Peer::Connected {
                conn: Connection {
                    socket: Socket::new(addr),
                    local_addr,
                    link,
                    since: local_time,
                },
                peer: None,
            },
        );
        self.disconnected.remove(&addr);

        match link {
            Link::Inbound => {
                if self.connected().filter(|c| c.link.is_inbound()).count()
                    >= self.config.max_inbound_peers
                {
                    // TODO: Test this branch.
                    // Don't allow inbound connections beyond the configured limit.
                    self._disconnect(addr, DisconnectReason::ConnectionLimit);
                } else {
                    // Wait for their version message..
                }
            }
            Link::Outbound => {
                let nonce = self.rng.u64(..);
                self.upstream.version(
                    addr,
                    self.version(addr, local_addr, nonce, height, local_time),
                );
            }
        }
        // Set a timeout for receiving the `version` message.
        self.upstream.set_timer(HANDSHAKE_TIMEOUT);
        self.upstream.event(Event::Connected(addr, link));
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected<A: AddressSource>(
        &mut self,
        addr: &net::SocketAddr,
        addrs: &mut A,
        reason: network::Disconnect<DisconnectReason>,
    ) {
        let local_time = self.clock.local_time();

        debug_assert!(self.peers.contains_key(addr));
        debug_assert!(!self.is_disconnected(addr));

        if self.is_disconnecting(addr) || self.is_connected(addr) {
            self.upstream.event(Event::Disconnected(*addr, reason));
        } else if self.is_connecting(addr) {
            // If we haven't yet established a connection, the disconnect reason
            // should always be a `ConnectionError`.
            if let network::Disconnect::ConnectionError(err) = reason {
                self.upstream.event(Event::ConnectionFailed(*addr, err));
            }
        }
        self.peers.remove(addr);

        if self.config.persistent.contains(addr) {
            self.persistent_disconnected(addr, local_time);
        } else {
            // If an outbound peer disconnected, we should make sure to maintain
            // our target outbound connection count.
            self.maintain_connections(addrs);
        }
    }

    /// Called when a `wtxidrelay` message was received.
    pub fn received_wtxidrelay(&mut self, addr: &PeerId) {
        if let Some(Peer::Connected {
            peer: Some(peer),
            conn: _,
        }) = self.peers.get_mut(addr)
        {
            match peer.state {
                HandshakeState::ReceivedVersion { .. } => peer.wtxidrelay = true,
                _ => self.disconnect(
                    *addr,
                    DisconnectReason::PeerMisbehaving(
                        "`wtxidrelay` must be received before `verack`",
                    ),
                ),
            }
        }
    }

    /// Called when a `version` message was received.
    pub fn received_version<A: AddressSource>(
        &mut self,
        addr: &PeerId,
        msg: VersionMessage,
        height: Height,
        addrs: &mut A,
    ) {
        if let Err(reason) = self.handle_version(addr, msg, height, addrs) {
            self._disconnect(*addr, reason);
        }
    }

    fn handle_version<A: AddressSource>(
        &mut self,
        addr: &PeerId,
        msg: VersionMessage,
        height: Height,
        addrs: &mut A,
    ) -> Result<(), DisconnectReason> {
        let now = self.clock.local_time();

        if let Some(Peer::Connected { conn, .. }) = self.peers.get(addr) {
            self.upstream.event(Event::VersionReceived {
                addr: *addr,
                msg: msg.clone(),
            });

            let VersionMessage {
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
                // Relay node.
                relay,
                ..
            } = msg.clone();

            let target = self.config.target_outbound_peers;
            let preferred = self.config.preferred_services;
            let trusted = self.config.whitelist.contains(&addr.ip(), &user_agent)
                || addrmgr::is_local(&addr.ip());

            // Don't support peers with too old of a protocol version.
            if version < super::MIN_PROTOCOL_VERSION {
                return Err(DisconnectReason::PeerProtocolVersion(version));
            }

            // Peers that don't advertise the `NETWORK` service are not full nodes.
            // It's not so useful for us to connect to them, because they're likely
            // to be less secure.
            if conn.link.is_outbound() && !services.has(self.config.required_services) && !trusted {
                return Err(DisconnectReason::PeerServices(services));
            }
            // If the peer is too far behind, there's no use connecting to it, we'll
            // have to wait for it to catch up.
            if conn.link.is_outbound()
                && height.saturating_sub(start_height as Height) > MAX_STALE_HEIGHT_DIFFERENCE
                && !trusted
            {
                return Err(DisconnectReason::PeerHeight(start_height as Height));
            }
            // Check for self-connections. We only need to check one link direction,
            // since in the case of a self-connection, we will see both link directions.
            for (peer, conn) in self.peers() {
                if conn.link.is_outbound() && peer.nonce == nonce {
                    return Err(DisconnectReason::SelfConnection);
                }
            }

            // If this peer doesn't have the preferred services, and we already have enough peers,
            // disconnect this peer.
            if conn.link.is_outbound()
                && !services.has(preferred)
                && self.negotiated(Link::Outbound).count() >= target
            {
                return Err(DisconnectReason::ConnectionLimit);
            }

            // Call the user-provided version hook and disconnect if asked.
            if let Err(reason) = (*self.hooks.on_version)(*addr, msg) {
                return Err(DisconnectReason::Other(reason));
            }

            // Record the address this peer has of us.
            if let Ok(addr) = receiver.socket_addr() {
                addrs.record_local_address(addr);
            }

            match conn.link {
                Link::Inbound => {
                    self.upstream
                        .version(
                            conn.socket.addr,
                            self.version(conn.socket.addr, conn.local_addr, nonce, height, now),
                        )
                        .wtxid_relay(conn.socket.addr)
                        .verack(conn.socket.addr)
                        .send_headers(conn.socket.addr)
                        .set_timer(HANDSHAKE_TIMEOUT);
                }
                Link::Outbound => {
                    self.upstream
                        .wtxid_relay(conn.socket.addr)
                        .verack(conn.socket.addr)
                        .send_headers(conn.socket.addr)
                        .set_timer(HANDSHAKE_TIMEOUT);
                }
            }
            let conn = conn.clone();
            let persistent = self.config.persistent.contains(&conn.socket.addr);

            self.peers.insert(
                conn.socket.addr,
                Peer::Connected {
                    conn,
                    peer: Some(PeerInfo {
                        nonce,
                        height: start_height as Height,
                        time_offset: timestamp - now.block_time() as i64,
                        services,
                        persistent,
                        user_agent,
                        state: HandshakeState::ReceivedVersion { since: now },
                        relay,
                        wtxidrelay: false,
                        version: u32::min(self.config.protocol_version, version),
                    }),
                },
            );
        }

        Ok(())
    }

    /// Called when a `verack` message was received.
    pub fn received_verack(
        &mut self,
        addr: &PeerId,
        local_time: LocalTime,
    ) -> Option<(PeerInfo, Connection)> {
        if let Some(Peer::Connected {
            peer: Some(peer),
            conn,
        }) = self.peers.get_mut(addr)
        {
            if let HandshakeState::ReceivedVersion { .. } = peer.state {
                self.upstream.event(Event::Negotiated {
                    addr: *addr,
                    link: conn.link,
                    services: peer.services,
                    user_agent: peer.user_agent.clone(),
                    height: peer.height,
                    version: peer.version,
                });

                peer.state = HandshakeState::ReceivedVerack { since: local_time };

                return Some((peer.clone(), conn.clone()));
            } else {
                self._disconnect(
                    *addr,
                    DisconnectReason::PeerMisbehaving("unexpected `verack` message received"),
                );
            }
        }
        None
    }

    /// Called when a tick was received.
    pub fn received_wake<A: AddressSource>(&mut self, addrs: &mut A) {
        let mut timed_out = Vec::new();
        let local_time = self.clock.local_time();

        // Time out all peers that have been idle in a "connecting" state for too long.
        for addr in self.idle_peers(local_time).collect::<Vec<_>>() {
            timed_out.push((addr, "connection"));
        }
        // Time out peers that haven't sent a `verack` quickly enough.
        for (peer, conn) in self.peers() {
            match peer.state {
                HandshakeState::ReceivedVersion { since } => {
                    if local_time - since >= HANDSHAKE_TIMEOUT {
                        timed_out.push((conn.socket.addr, "handshake"));
                    }
                }
                HandshakeState::ReceivedVerack { .. } => {}
            }
        }
        // Time out peers that haven't sent a `version` quickly enough.
        for connected in self.peers.values().filter_map(|c| match c {
            Peer::Connected { conn, peer: None } => Some(conn),
            _ => None,
        }) {
            if local_time - connected.since >= HANDSHAKE_TIMEOUT {
                timed_out.push((connected.socket.addr, "handshake"));
            }
        }
        // Disconnect all timed out peers.
        for (addr, reason) in timed_out {
            self._disconnect(addr, DisconnectReason::PeerTimeout(reason));
        }

        // Disconnect peers that have been dropped from all other sub-protocols.
        // Since the job of the peer manager is simply to establish connections, if a peer is
        // dropped from all other sub-protocols and we are holding on to the last reference,
        // there is no use in keeping this peer around.
        let dropped = self
            .negotiated(Link::Outbound)
            .filter(|(_, c)| c.socket.refs() == 1)
            .map(|(_, c)| c.socket.addr)
            .collect::<Vec<_>>();
        for addr in dropped {
            self._disconnect(addr, DisconnectReason::PeerDropped);
        }

        if local_time - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            self.maintain_connections(addrs);
            self.upstream.set_timer(IDLE_TIMEOUT);
            self.last_idle = Some(local_time);
        }
        self.maintain_persistent();
    }

    /// Whitelist a peer.
    pub fn whitelist(&mut self, addr: net::SocketAddr) -> bool {
        self.config.whitelist.addr.insert(addr.ip())
    }

    /// Create a `version` message for this peer.
    pub fn version(
        &self,
        addr: net::SocketAddr,
        local_addr: net::SocketAddr,
        nonce: u64,
        start_height: Height,
        local_time: LocalTime,
    ) -> VersionMessage {
        let start_height = start_height as i32;
        let timestamp = local_time.block_time() as i64;

        VersionMessage {
            // Our max supported protocol version.
            version: self.config.protocol_version,
            // Local services.
            services: self.config.services,
            // Local time.
            timestamp,
            // Receiver address and services, as perceived by us.
            receiver: Address::new(&addr, ServiceFlags::NONE),
            // Local address (unreliable) and local services (same as `services` field)
            sender: Address::new(&local_addr, self.config.services),
            // A nonce to detect connections to self.
            nonce,
            // Our user agent string.
            user_agent: self.config.user_agent.to_owned(),
            // Our best height.
            start_height,
            // Whether we want to receive transaction `inv` messages.
            relay: false,
        }
    }
}

/// Connection management functions.
impl<U: Connect + Disconnect + SetTimer + Wire<Event>, C: Clock> PeerManager<U, C> {
    /// Called when a peer is being connected to.
    pub fn peer_attempted(&mut self, addr: &net::SocketAddr) {
        // Since all "attempts" are made from this module, we expect that when a peer is
        // attempted, we know about it already.
        //
        // It's possible that as we were attempting to connect to a peer, that peer in the
        // meantime connected to us. Hence we also account for an already-connected *inbound*
        // peer.
        debug_assert!(self.is_connecting(addr) || self.is_inbound(addr));
    }

    /// Check whether a peer is connected via an inbound link.
    pub fn is_inbound(&self, addr: &PeerId) -> bool {
        self.peers.get(addr).map_or(
            false,
            |c| matches!(c, Peer::Connected { conn, .. } if conn.link.is_inbound()),
        )
    }

    /// Check whether a peer is connecting.
    pub fn is_connecting(&self, addr: &PeerId) -> bool {
        self.peers
            .get(addr)
            .map_or(false, |c| matches!(c, Peer::Connecting { .. }))
    }

    /// Check whether a peer is connected.
    pub fn is_connected(&self, addr: &PeerId) -> bool {
        self.peers
            .get(addr)
            .map_or(false, |c| matches!(c, Peer::Connected { .. }))
    }

    /// Check whether a peer is disconnected.
    pub fn is_disconnected(&self, addr: &PeerId) -> bool {
        !self.peers.contains_key(addr)
    }

    /// Check whether a peer is being disconnected.
    pub fn is_disconnecting(&self, addr: &PeerId) -> bool {
        matches!(self.peers.get(addr), Some(Peer::Disconnecting))
    }

    /// Iterator over peers that have at least sent their `version` message.
    pub fn peers(&self) -> impl Iterator<Item = (&PeerInfo, &Connection)> + Clone {
        self.peers.values().filter_map(move |c| match c {
            Peer::Connected {
                conn,
                peer: Some(peer),
            } => Some((peer, conn)),
            _ => None,
        })
    }

    /// Returns connecting peers.
    pub fn connecting(&self) -> impl Iterator<Item = &PeerId> {
        self.peers
            .iter()
            .filter(|(_, p)| matches!(p, Peer::Connecting { .. }))
            .map(|(addr, _)| addr)
    }

    /// Iterator over peers in a *connected* state..
    pub fn connected(&self) -> impl Iterator<Item = &Connection> + Clone {
        self.peers.values().filter_map(|c| match c {
            Peer::Connected { conn, .. } => Some(conn),
            _ => None,
        })
    }

    /// Iterator over fully negotiated peers.
    pub fn negotiated(&self, link: Link) -> impl Iterator<Item = (&PeerInfo, &Connection)> + Clone {
        self.peers()
            .filter(move |(p, c)| p.is_negotiated() && c.link == link)
    }

    /// Connect to a peer.
    pub fn connect(&mut self, addr: &PeerId) -> bool {
        let time = self.clock.local_time();

        if self.is_connected(addr) || self.is_connecting(addr) {
            return true;
        }
        if !self.is_disconnected(addr) && !self.is_disconnecting(addr) {
            return false;
        }
        // Don't allow connections to unsupported domains.
        if !self.config.domains.contains(&Domain::for_address(addr)) {
            return false;
        }
        self.peers.insert(*addr, Peer::Connecting { time });
        self.upstream.connect(*addr, CONNECTION_TIMEOUT);

        true
    }

    /// Disconnect from a peer.
    pub fn disconnect(&mut self, addr: PeerId, reason: DisconnectReason) {
        if self.is_connected(&addr) {
            self._disconnect(addr, reason);
        }
    }

    /// Disconnect a peer (internal).
    fn _disconnect(&mut self, addr: PeerId, reason: DisconnectReason) {
        self.upstream.disconnect(addr, reason);
        self.peers.insert(addr, Peer::Disconnecting);
    }

    /// Given the current peer state and targets, calculate how many new connections we should
    /// make.
    fn delta(&self) -> usize {
        // Peers with our preferred services.
        let primary = self
            .negotiated(Link::Outbound)
            .filter(|(p, _)| p.services.has(self.config.preferred_services))
            .count();
        // Peers only with required services, which we'd eventually want to drop in favor of peers
        // that have all services.
        let secondary = self.negotiated(Link::Outbound).count() - primary;
        // Connected peers that have not yet completed handshake.
        let connected = self.connected().count() - primary - secondary;
        // Connecting peers.
        let connecting = self.connecting().count();

        // We connect up to the target number of peers plus an extra margin equal to the number of
        // target divided by two. This ensures we have *some* connections to
        // primary peers, even if that means exceeding our target. When a secondary peer is
        // dropped, if we have our target number of primary peers connected, there is no need
        // to replace the connection.
        //
        // Above the target count, all peer connections without the preferred services are
        // automatically dropped. This ensures we never have more than the target of secondary
        // peers.
        let target = self.config.target_outbound_peers;
        let unknown = connecting + connected;
        let total = primary + secondary + unknown;
        let max = target + target / 2;

        // If we are somehow connected to more peers than the target or maximum,
        // don't attempt to connect to more. This can happen if the client has been
        // requesting connections to specific peers.
        if total > max || primary + unknown > target {
            return 0;
        }

        usize::min(max - total, target - (primary + unknown))
    }

    /// Attempt to maintain a certain number of outbound peers.
    fn maintain_connections<A: AddressSource>(&mut self, addrs: &mut A) {
        // If we have persistent peers configured, we don't use this mechanism for maintaining
        // connections. Instead, we retry the configured peers.
        if !self.config.persistent.is_empty() {
            return;
        }

        let delta = self.delta();
        let negotiated = self.negotiated(Link::Outbound).count();
        let target = self.config.target_outbound_peers;

        // Keep track of new addresses we're connecting to, and loop until
        // we've connected to enough addresses.
        let mut connecting = HashSet::with_hasher(self.rng.clone().into());

        while connecting.len() < delta {
            if let Some((addr, source)) =
                addrs.sample(self.config.preferred_services).or_else(|| {
                    // Only try to connect to non-preferred peers if we are below our target.
                    if negotiated < target {
                        addrs
                            .sample(self.config.required_services)
                            // If we can't find peers with any kind of useful services, then
                            // perhaps we should connect to peers that may know of such peers. This
                            // is especially important when doing an initial DNS sync, since DNS
                            // addresses don't come with service information. This will draw from
                            // that pool.
                            .or_else(|| addrs.sample(ServiceFlags::NONE))
                    } else {
                        None
                    }
                })
            {
                if let Ok(sockaddr) = addr.socket_addr() {
                    // TODO: Remove this assertion once address manager no longer cares about
                    // connections.
                    debug_assert!(!self.is_connected(&sockaddr));

                    if self.connect(&sockaddr) {
                        connecting.insert(sockaddr);
                        self.upstream
                            .event(Event::Connecting(sockaddr, source, addr.services));
                    }
                }
            } else {
                // We're completely out of addresses, give up.
                // TODO: Fetch from DNS seeds. Make sure we're able to add to address book
                // even though address manager doesn't like peers with no services if `insert`
                // is used.
                break;
            }
        }
    }

    /// Peers that have been idle longer than [`CONNECTION_TIMEOUT`].
    fn idle_peers(&self, now: LocalTime) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.iter().filter_map(move |(addr, c)| {
            if let Peer::Connecting { time } = c {
                if now - *time >= CONNECTION_TIMEOUT {
                    return Some(*addr);
                }
            }
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    use nakamoto_common::bitcoin::network::address::Address;
    use nakamoto_common::block::time::RefClock;
    use nakamoto_test::assert_matches;

    mod util {
        use super::*;

        pub fn config() -> Config {
            Config {
                protocol_version: crate::fsm::PROTOCOL_VERSION,
                target_outbound_peers: TARGET_OUTBOUND_PEERS,
                max_inbound_peers: MAX_INBOUND_PEERS,
                domains: Domain::all(),
                user_agent: crate::fsm::USER_AGENT,
                persistent: vec![],
                retry_max_wait: LocalDuration::from_mins(60),
                retry_min_wait: LocalDuration::from_secs(1),
                services: ServiceFlags::NONE,
                preferred_services: ServiceFlags::COMPACT_FILTERS | ServiceFlags::NETWORK,
                required_services: ServiceFlags::NETWORK,
                whitelist: Whitelist::default(),
            }
        }
    }

    #[test]
    fn test_persistent_client_reconnect() {
        let rng = fastrand::Rng::with_seed(1);
        let time = RefClock::from(LocalTime::now());
        let height = 144;

        let local = ([99, 99, 99, 99], 9999).into();
        let remote = ([124, 43, 110, 1], 8333).into();

        let mut addrs = VecDeque::new();
        let cfg = Config {
            persistent: vec![remote],
            ..util::config()
        };
        let mut peermgr = PeerManager::new(cfg, rng, Hooks::default(), (), time.clone());

        peermgr.initialize(&mut addrs);
        assert_eq!(peermgr.connecting().next(), Some(&remote));

        peermgr.peer_connected(remote, local, Link::Outbound, height);
        assert_eq!(
            peermgr.connected().map(|c| &c.socket.addr).next(),
            Some(&remote)
        );

        // Confirm first attempt
        peermgr.peer_disconnected(
            &remote,
            &mut addrs,
            DisconnectReason::PeerTimeout("").into(),
        );
        assert!(peermgr.is_disconnected(&remote));
        assert_eq!(peermgr.connected().next(), None);

        time.elapse(LocalDuration::from_secs(1));
        peermgr.received_wake(&mut addrs);
        assert_eq!(peermgr.connecting().next(), Some(&remote));

        // Confirm exponential backoff after failed first attempt
        peermgr.peer_disconnected(
            &remote,
            &mut addrs,
            DisconnectReason::PeerTimeout("").into(),
        );
        assert!(peermgr.is_disconnected(&remote));
        assert_eq!(peermgr.connecting().next(), None);

        time.elapse(LocalDuration::from_secs(1));
        peermgr.received_wake(&mut addrs);
        assert_eq!(peermgr.connecting().next(), None);

        time.elapse(LocalDuration::from_secs(1));
        peermgr.received_wake(&mut addrs);
        assert_eq!(peermgr.connecting().next(), Some(&remote));
    }

    #[test]
    fn test_wtxidrelay_outbound() {
        let rng = fastrand::Rng::with_seed(1);
        let time = LocalTime::now();

        let mut addrs = VecDeque::new();
        let mut peermgr = PeerManager::new(util::config(), rng.clone(), Hooks::default(), (), time);

        let height = 144;
        let local = ([99, 99, 99, 99], 9999).into();
        let remote = ([124, 43, 110, 1], 8333).into();
        let version = VersionMessage {
            services: ServiceFlags::NETWORK,
            ..peermgr.version(local, remote, rng.u64(..), height, time)
        };

        peermgr.initialize(&mut addrs);
        peermgr.connect(&remote);
        peermgr.peer_connected(remote, local, Link::Outbound, height);
        peermgr.received_version(&remote, version, height, &mut addrs);

        assert_matches!(
            peermgr.peers.get(&remote),
            Some(Peer::Connected{peer: Some(p), ..}) if !p.wtxidrelay
        );

        peermgr.received_wtxidrelay(&remote);
        peermgr.received_verack(&remote, time);

        assert_matches!(
            peermgr.peers.get(&remote),
            Some(Peer::Connected{peer: Some(p), ..}) if p.wtxidrelay
        );
    }

    #[test]
    fn test_wtxidrelay_misbehavior() {
        let rng = fastrand::Rng::with_seed(1);
        let time = LocalTime::now();

        let mut addrs = VecDeque::new();
        let mut peermgr = PeerManager::new(util::config(), rng.clone(), Hooks::default(), (), time);

        let height = 144;
        let local = ([99, 99, 99, 99], 9999).into();
        let remote = ([124, 43, 110, 1], 8333).into();
        let version = VersionMessage {
            services: ServiceFlags::NETWORK,
            ..peermgr.version(local, remote, rng.u64(..), height, time)
        };

        peermgr.initialize(&mut addrs);
        peermgr.connect(&remote);
        peermgr.peer_connected(remote, local, Link::Outbound, height);
        peermgr.received_version(&remote, version, height, &mut addrs);
        peermgr.received_verack(&remote, time);
        peermgr.received_wtxidrelay(&remote);

        assert_matches!(peermgr.peers.get(&remote), Some(Peer::Disconnecting));
    }

    #[test]
    fn test_connect_timeout() {
        let rng = fastrand::Rng::with_seed(1);
        let time = RefClock::from(LocalTime::now());

        let remote = ([124, 43, 110, 1], 8333).into();

        let mut addrs = VecDeque::new();
        let mut peermgr = PeerManager::new(util::config(), rng, Hooks::default(), (), time.clone());

        peermgr.initialize(&mut addrs);
        peermgr.connect(&remote);

        assert_eq!(peermgr.connecting().next(), Some(&remote));
        assert_eq!(peermgr.connecting().count(), 1);

        time.elapse(LocalDuration::from_secs(1));
        peermgr.received_wake(&mut addrs);

        assert_eq!(peermgr.connecting().next(), Some(&remote));

        // After the timeout has elapsed, the peer should be disconnected.
        time.elapse(CONNECTION_TIMEOUT);
        peermgr.received_wake(&mut addrs);

        assert_eq!(peermgr.connecting().next(), None);
        assert!(matches!(
            peermgr.peers.get(&remote),
            Some(Peer::Disconnecting)
        ));
    }

    #[test]
    fn test_peer_dropped() {
        let rng = fastrand::Rng::with_seed(1);
        let time = LocalTime::now();
        let mut addrs = VecDeque::new();
        let mut peermgr = PeerManager::new(util::config(), rng.clone(), Hooks::default(), (), time);

        let height = 144;
        let local = ([99, 99, 99, 99], 9999).into();
        let remote = ([124, 43, 110, 1], 8333).into();
        let version = VersionMessage {
            services: ServiceFlags::NETWORK,
            ..peermgr.version(local, remote, rng.u64(..), height, time)
        };

        peermgr.initialize(&mut addrs);
        peermgr.connect(&remote);
        peermgr.peer_connected(remote, local, Link::Outbound, height);
        peermgr.received_version(&remote, version, height, &mut addrs);

        let (_, conn) = peermgr.received_verack(&remote, time).unwrap();
        let socket = conn.socket;
        assert_eq!(socket.refs(), 2);

        peermgr
            .negotiated(Link::Outbound)
            .find(|(_, c)| c.socket.addr == remote)
            .unwrap();

        peermgr.received_wake(&mut addrs);
        assert!(!peermgr.is_disconnecting(&remote));
        assert_eq!(socket.refs(), 2);

        drop(socket);

        peermgr.received_wake(&mut addrs);
        assert!(peermgr.is_disconnecting(&remote));
    }

    #[test]
    fn test_disconnects() {
        let rng = fastrand::Rng::with_seed(1);
        let time = LocalTime::now();
        let height = 144;

        let services = ServiceFlags::NETWORK;
        let local = ([99, 99, 99, 99], 9999).into();
        let remote1 = ([124, 43, 110, 1], 8333).into();
        let remote2 = ([124, 43, 110, 2], 8333).into();
        let remote3 = ([124, 43, 110, 3], 8333).into();
        let remote4 = ([124, 43, 110, 4], 8333).into();
        let reason: network::Disconnect<DisconnectReason> =
            DisconnectReason::PeerTimeout("timeout").into();

        let mut addrs = VecDeque::new();
        let mut peermgr = PeerManager::new(util::config(), rng, Hooks::default(), (), time);

        peermgr.initialize(&mut addrs);
        peermgr.connect(&remote1);

        assert_eq!(peermgr.connecting().next(), Some(&remote1));
        assert_eq!(peermgr.connected().next(), None);

        peermgr.peer_connected(remote1, local, Link::Outbound, height);

        assert_eq!(peermgr.connecting().next(), None);
        assert_eq!(
            peermgr.connected().map(|c| &c.socket.addr).next(),
            Some(&remote1)
        );

        // Disconnect remote#1 after it has connected.
        addrs.push_back((Address::new(&remote2, services), Source::Dns));
        peermgr.peer_disconnected(&remote1, &mut addrs, reason.clone());

        assert!(peermgr.is_disconnected(&remote1));
        assert_eq!(peermgr.connected().next(), None);
        assert_eq!(
            peermgr.connecting().next(),
            Some(&remote2),
            "Disconnection triggers a new connection to remote#2"
        );

        // Disconnect remote#2 while still connecting.
        addrs.push_back((Address::new(&remote3, services), Source::Dns));
        peermgr.peer_disconnected(&remote2, &mut addrs, reason.clone());

        assert!(peermgr.is_disconnected(&remote2));
        assert_eq!(
            peermgr.connecting().next(),
            Some(&remote3),
            "Disconnection triggers a new connection to remote#3"
        );

        // Connect, then disconnect remote#3.
        addrs.push_back((Address::new(&remote4, services), Source::Dns));

        peermgr.peer_connected(remote3, local, Link::Outbound, height);
        peermgr.disconnect(remote3, DisconnectReason::Command);
        peermgr.peer_disconnected(&remote3, &mut addrs, reason);

        assert!(peermgr.is_disconnected(&remote3));
        assert_eq!(
            peermgr.connecting().next(),
            Some(&remote4),
            "Disconnection triggers a new connection to remote#4"
        );
    }

    #[test]
    fn test_connection_delta() {
        let target_outbound_peers = 4;
        let height = 144;
        let cfg = Config {
            target_outbound_peers,
            ..util::config()
        };
        let rng = fastrand::Rng::with_seed(1);
        let time = LocalTime::now();
        let local = ([99, 99, 99, 99], 9999).into();

        let cases: Vec<((usize, usize, usize, usize), usize)> = vec![
            // outbound = 0/4 (0), connecting = 0/4
            ((0, 0, 0, 0), target_outbound_peers),
            // outbound = 0/4 (0), connecting = 1/4
            ((1, 0, 0, 0), target_outbound_peers - 1),
            // outbound = 0/4 (0), connecting = 3/4
            ((1, 2, 0, 0), target_outbound_peers - 3),
            // outbound = 1/4 (0), connecting = 2/4
            ((1, 1, 1, 0), 2),
            // outbound = 2/4 (1), connecting = 2/4
            ((1, 1, 1, 1), 1),
            // outbound = 3/4 (1), connecting = 1/4
            ((0, 1, 2, 1), 2),
            // outbound = 4/4 (1), connecting = 0/4, extra = 2
            ((0, 0, 3, 1), 2),
            // outbound = 6/4 (3), connecting = 0/4
            ((0, 0, 3, 3), 0),
            // outbound = 4/4 (4), connecting = 0/4
            ((0, 0, 0, target_outbound_peers), 0),
            // outbound = 6/4 (2), connecting = 0/4
            ((0, 0, 4, 2), 0),
            // outbound = 6/4 (3), connecting = 0/4
            ((0, 0, 2, 4), 0),
            // outbound = 5/4 (2), connecting = 0/4, extra = 1
            ((0, 0, 3, 2), 1),
            // outbound = 0/4 (0), connecting = 4/4
            ((4, 0, 0, 0), 0),
            // outbound = 4/4 (0), connecting = 0/4, extra = 2
            ((0, 0, 4, 0), 2),
            // outbound = 5/4 (3), connecting = 0/4, extra = 1
            ((0, 0, 2, 3), 1),
            // outbound = 5/4 (3), connecting = 1/4, extra = 0
            ((1, 0, 2, 3), 0),
            // outbound = 5/4 (3), connecting = 1/4, extra = 0
            ((0, 1, 2, 3), 0),
        ];

        for (case, delta) in cases {
            let (connecting, connected, required, preferred) = case;

            let mut addrs = VecDeque::new();
            let mut peermgr =
                PeerManager::new(cfg.clone(), rng.clone(), Hooks::default(), (), time);

            peermgr.initialize(&mut addrs);

            for i in 0..connecting {
                let remote = ([44, 44, 44, i as u8], 8333).into();
                peermgr.connect(&remote);
                assert!(peermgr.peers.contains_key(&remote));
            }
            for i in 0..connected {
                let remote = ([55, 55, 55, i as u8], 8333).into();
                peermgr.connect(&remote);
                peermgr.peer_connected(remote, local, Link::Outbound, height);
                assert!(peermgr.peers.contains_key(&remote));
            }
            for i in 0..required {
                let remote = ([66, 66, 66, i as u8], 8333).into();
                let version = VersionMessage {
                    services: cfg.required_services,
                    ..peermgr.version(local, remote, rng.u64(..), height, time)
                };

                peermgr.connect(&remote);
                peermgr.peer_connected(remote, local, Link::Outbound, height);
                assert!(peermgr.peers.contains_key(&remote));

                peermgr.received_version(&remote, version, height, &mut addrs);
                assert!(peermgr.peers.contains_key(&remote));

                peermgr.received_verack(&remote, time).unwrap();
                assert_matches!(
                    peermgr.peers.get(&remote).unwrap(),
                    Peer::Connected { peer: Some(p), .. } if p.is_negotiated()
                );
            }
            for i in 0..preferred {
                let remote = ([77, 77, 77, i as u8], 8333).into();
                let version = VersionMessage {
                    services: cfg.preferred_services,
                    ..peermgr.version(local, remote, rng.u64(..), height, time)
                };

                peermgr.connect(&remote);
                peermgr.peer_connected(remote, local, Link::Outbound, height);
                assert!(peermgr.peers.contains_key(&remote));

                peermgr.received_version(&remote, version, height, &mut addrs);
                assert!(peermgr.peers.contains_key(&remote));

                peermgr.received_verack(&remote, time).unwrap();
                assert_matches!(
                    peermgr.peers.get(&remote).unwrap(),
                    Peer::Connected { peer: Some(p), .. } if p.is_negotiated()
                );
            }
            assert_eq!(peermgr.delta(), delta, "{:?}", case);
        }
    }
}
