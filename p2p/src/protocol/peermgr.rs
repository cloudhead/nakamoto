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

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_network::VersionMessage;

use nakamoto_common::p2p::peer::{AddressSource, Source};
use nakamoto_common::p2p::Domain;

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::block::Height;
use nakamoto_common::collections::{HashMap, HashSet};

use crate::protocol::addrmgr;

use super::{
    channel::{Disconnect, SetTimeout},
    DisconnectReason, Timeout,
};
use super::{Hooks, Link, PeerId, Whitelist};

/// Time to wait for response during peer handshake before disconnecting the peer.
pub const HANDSHAKE_TIMEOUT: LocalDuration = LocalDuration::from_secs(10);
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
        /// Services offered by negotiated peer.
        services: ServiceFlags,
    },
    /// Connecting to a peer found from the specified source.
    Connecting(PeerId, Source),
    /// A new peer has connected and is ready to accept messages.
    /// This event is triggered *after* the peer handshake
    /// has successfully completed.
    Connected(PeerId, Link),
    /// A peer has been disconnected.
    Disconnected(PeerId),
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VersionReceived { addr, msg } => write!(
                fmt,
                "{}: Peer version = {}, height = {}, agent = {}, services = {}, timestamp = {}",
                addr, msg.version, msg.start_height, msg.user_agent, msg.services, msg.timestamp
            ),
            Self::Negotiated { addr, services } => write!(
                fmt,
                "{}: Peer negotiated with services {}..",
                addr, services
            ),
            Self::Connecting(addr, source) => {
                write!(fmt, "Connecting to peer {} from source `{}`", addr, source)
            }
            Self::Connected(addr, link) => write!(fmt, "{}: Peer connected ({:?})", &addr, link),
            Self::Disconnected(addr) => write!(fmt, "Disconnected from {}", &addr),
        }
    }
}

/// The ability to negotiate protocols between peers.
pub trait Handshake {
    /// Send a `version` message.
    fn version(&self, addr: PeerId, msg: VersionMessage) -> &Self;
    /// Send a `verack` message.
    fn verack(&self, addr: PeerId) -> &Self;
}

/// Ability to connect to peers.
pub trait Connect {
    /// Connect to peer.
    fn connect(&self, addr: net::SocketAddr, timeout: Timeout);
}

/// The ability to emit peer related events.
pub trait Events {
    /// Emit a peer-related event.
    fn event(&self, event: Event);
}

/// Peer manager configuration.
#[derive(Debug)]
pub struct Config {
    /// Protocol version.
    pub protocol_version: u32,
    /// Peer whitelist.
    pub whitelist: Whitelist,
    /// Services offered by this implementation.
    pub services: ServiceFlags,
    /// Peer addresses that should always be retried.
    pub retry: Vec<net::SocketAddr>,
    /// Services required by peers.
    pub required_services: ServiceFlags,
    /// Peer services preferred. We try to maintain as many
    /// connections to peers with these services.
    pub preferred_services: ServiceFlags,
    /// Target number of outbound peer connections.
    pub target_outbound_peers: usize,
    /// Maximum number of inbound peer connections.
    pub max_inbound_peers: usize,
    /// Our user agent.
    pub user_agent: &'static str,

    /// Supported communication domains.
    pub domains: Vec<Domain>,
}

/// Peer states.
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
enum PeerState {
    /// Waiting for "verack" message from remote.
    AwaitingVerack {
        since: LocalTime,
    },
    Negotiated {
        since: LocalTime,
    },
}

/// A peer connection. Peers that haven't yet sent their `version` message are stored as
/// connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connected {
    /// Remote peer address.
    pub addr: net::SocketAddr,
    /// Local peer address.
    pub local_addr: net::SocketAddr,
    /// Whether this is an inbound or outbound peer connection.
    pub link: Link,
    /// Connected since this time.
    pub since: LocalTime,
}

/// Peer connection state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Connection {
    /// A connection is being attempted.
    Connecting {
        /// Time the connection was attempted.
        time: LocalTime,
    },
    /// A connection is established.
    Connected(Connected),
    /// The connection is being closed.
    Disconnecting,
}

/// A peer with connection and protocol information.
#[derive(Debug, Clone)]
pub struct Peer {
    /// Connection information.
    pub conn: Connected,
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

    /// Peer nonce. Used to detect self-connections.
    nonce: u64,
    /// Peer state.
    state: PeerState,
}

impl Peer {
    /// Get the peer's address.
    pub fn address(&self) -> net::SocketAddr {
        self.conn.addr
    }

    /// Check whether the peer has successfully negotiated.
    pub fn is_negotiated(&self) -> bool {
        matches!(self.state, PeerState::Negotiated { .. })
    }

    /// Check whether the peer is outbound.
    pub fn is_outbound(&self) -> bool {
        self.conn.link.is_outbound()
    }
}

/// Manages peers and peer negotiation.
#[derive(Debug)]
pub struct PeerManager<U> {
    /// Peer manager configuration.
    pub config: Config,

    /// Last time we were idle.
    last_idle: Option<LocalTime>,
    /// Connection states.
    connections: HashMap<net::SocketAddr, Connection>,
    /// Peers for which we have received at least the `version` message.
    peers: HashMap<PeerId, Peer>,
    upstream: U,
    rng: fastrand::Rng,
    hooks: Hooks,
}

impl<U: Handshake + SetTimeout + Connect + Disconnect + Events> PeerManager<U> {
    /// Create a new peer manager.
    pub fn new(config: Config, rng: fastrand::Rng, hooks: Hooks, upstream: U) -> Self {
        let connections = HashMap::with_hasher(rng.clone().into());
        let peers = HashMap::with_hasher(rng.clone().into());

        Self {
            config,
            last_idle: None,
            connections,
            peers,
            upstream,
            rng,
            hooks,
        }
    }

    /// Initialize the peer manager. Must be called once.
    pub fn initialize<A: AddressSource>(&mut self, time: LocalTime, addrs: &mut A) {
        let retry = self
            .config
            .retry
            .iter()
            .take(self.config.target_outbound_peers)
            .cloned()
            .collect::<Vec<_>>();

        for addr in retry {
            self.connect(&addr, time);
        }
        self.upstream.set_timeout(IDLE_TIMEOUT);
        self.maintain_connections(addrs, time);
    }

    /// Iterator over inbound peers.
    pub fn negotiated(&self, link: Link) -> impl Iterator<Item = &Peer> + Clone {
        self.peers
            .values()
            .filter(move |p| p.is_negotiated() && p.conn.link == link)
    }

    /// Iterator over peers that have at least sent their `version` message.
    pub fn peers(&self) -> impl Iterator<Item = &Peer> + Clone {
        self.peers.values()
    }

    /// Called when a peer connected.
    pub fn peer_connected(
        &mut self,
        addr: net::SocketAddr,
        local_addr: net::SocketAddr,
        link: Link,
        height: Height,
        local_time: LocalTime,
    ) {
        #[cfg(debug_assertions)]
        if link.is_outbound() {
            debug_assert!(self.is_connecting(&addr))
        }
        debug_assert!(!self.is_connected(&addr));

        // TODO: There is a chance that we simultaneously connect to a peer that is connecting
        // to us. This would create two connections to the same peer, one outbound and one
        // inbound. To prevent this, we could look at IPs when receiving inbound connections,
        // to check whether we are already connected to the peer.

        self.connections.insert(
            addr,
            Connection::Connected(Connected {
                addr,
                local_addr,
                link,
                since: local_time,
            }),
        );

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
        self.upstream.set_timeout(HANDSHAKE_TIMEOUT);
        self.upstream.event(Event::Connected(addr, link));
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected<A: AddressSource>(
        &mut self,
        addr: &net::SocketAddr,
        addrs: &mut A,
        local_time: LocalTime,
    ) {
        debug_assert!(self.connections.contains_key(addr));
        debug_assert!(!self.is_disconnected(addr));

        self.peers.remove(addr);
        self.connections.remove(addr);
        // If an outbound peer disconnected, we should make sure to maintain
        // our target outbound connection count.
        self.maintain_connections(addrs, local_time);

        Events::event(&self.upstream, Event::Disconnected(*addr));
    }

    /// Called when a `version` message was received.
    pub fn received_version<S, T>(
        &mut self,
        addr: &PeerId,
        msg: VersionMessage,
        height: Height,
        now: LocalTime,
        addrs: &mut addrmgr::AddressManager<S, T>,
    ) {
        if let Some(Connection::Connected(conn)) = self.connections.get(addr) {
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

            let trusted = self.config.whitelist.contains(&addr.ip(), &user_agent)
                || addrmgr::is_local(&addr.ip());

            // Don't support peers with an older protocol than ours, we won't be
            // able to handle it correctly.
            if version < self.config.protocol_version {
                return self
                    .upstream
                    .disconnect(*addr, DisconnectReason::PeerProtocolVersion(version));
            }

            // Peers that don't advertise the `NETWORK` service are not full nodes.
            // It's not so useful for us to connect to them, because they're likely
            // to be less secure.
            if conn.link.is_outbound() && !services.has(self.config.required_services) && !trusted {
                return self
                    .upstream
                    .disconnect(*addr, DisconnectReason::PeerServices(services));
            }
            // If the peer is too far behind, there's no use connecting to it, we'll
            // have to wait for it to catch up.
            if conn.link.is_outbound()
                && height.saturating_sub(start_height as Height) > MAX_STALE_HEIGHT_DIFFERENCE
                && !trusted
            {
                return self
                    .upstream
                    .disconnect(*addr, DisconnectReason::PeerHeight(start_height as Height));
            }
            // Check for self-connections. We only need to check one link direction,
            // since in the case of a self-connection, we will see both link directions.
            for (_, peer) in self.peers.iter() {
                if conn.link.is_outbound() && peer.nonce == nonce {
                    return self
                        .upstream
                        .disconnect(*addr, DisconnectReason::SelfConnection);
                }
            }

            // Call the user-provided version hook and disconnect if asked.
            if let Err(reason) = (*self.hooks.on_version)(*addr, msg) {
                return self
                    .upstream
                    .disconnect(*addr, DisconnectReason::Other(reason));
            }

            // Record the address this peer has of us.
            if let Ok(addr) = receiver.socket_addr() {
                addrs.record_local_addr(addr);
            }

            match conn.link {
                Link::Outbound => {
                    self.upstream
                        .verack(conn.addr)
                        .set_timeout(HANDSHAKE_TIMEOUT);
                }
                Link::Inbound => {
                    self.upstream
                        .version(
                            conn.addr,
                            self.version(conn.addr, conn.local_addr, nonce, height, now),
                        )
                        .verack(conn.addr)
                        .set_timeout(HANDSHAKE_TIMEOUT);
                }
            }

            self.peers.insert(
                conn.addr,
                Peer {
                    nonce,
                    conn: conn.clone(),
                    height: start_height as Height,
                    time_offset: timestamp - now.block_time() as i64,
                    services,
                    user_agent,
                    state: PeerState::AwaitingVerack { since: now },
                    relay,
                },
            );
        }
    }

    /// Called when a `verack` message was received.
    pub fn received_verack(&mut self, addr: &PeerId, local_time: LocalTime) -> Option<&Peer> {
        if let Some(peer) = self.peers.get_mut(addr) {
            if let PeerState::AwaitingVerack { .. } = peer.state {
                self.upstream.event(Event::Negotiated {
                    addr: *addr,
                    services: peer.services,
                });

                peer.state = PeerState::Negotiated { since: local_time };

                return Some(peer);
            } else {
                self.upstream.disconnect(
                    *addr,
                    DisconnectReason::PeerMisbehaving("unexpected `verack` message received"),
                );
            }
        }
        None
    }

    /// Called when a tick was received.
    pub fn received_tick<A: AddressSource>(&mut self, local_time: LocalTime, addrs: &mut A) {
        let mut timed_out = Vec::new();

        // Disconnect all peers that have been idle in a "connecting" state for too long.
        for addr in self.idle_peers(local_time).collect::<Vec<_>>() {
            timed_out.push((addr, "connection"));
        }

        for (addr, peer) in self.peers.iter() {
            match peer.state {
                PeerState::AwaitingVerack { since } => {
                    if local_time - since >= HANDSHAKE_TIMEOUT {
                        timed_out.push((*addr, "handshake"));
                    }
                }
                PeerState::Negotiated { .. } => {}
            }
        }
        for connected in self
            .connected()
            .filter(|c| !self.peers.contains_key(&c.addr))
        {
            if local_time - connected.since >= HANDSHAKE_TIMEOUT {
                timed_out.push((connected.addr, "handshake"));
            }
        }

        for (addr, reason) in timed_out {
            self._disconnect(addr, DisconnectReason::PeerTimeout(reason));
        }

        if local_time - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            self.maintain_connections(addrs, local_time);
            self.upstream.set_timeout(IDLE_TIMEOUT);
            self.last_idle = Some(local_time);
        }
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
impl<U: Connect + SetTimeout + Disconnect + Events> PeerManager<U> {
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
        self.connections.get(addr).map_or(
            false,
            |c| matches!(c, Connection::Connected(conn) if conn.link.is_inbound()),
        )
    }

    /// Check whether a peer is connecting.
    pub fn is_connecting(&self, addr: &PeerId) -> bool {
        self.connections
            .get(addr)
            .map_or(false, |c| matches!(c, Connection::Connecting { .. }))
    }

    /// Check whether a peer is connected.
    pub fn is_connected(&self, addr: &PeerId) -> bool {
        self.connections
            .get(addr)
            .map_or(false, |c| matches!(c, Connection::Connected { .. }))
    }

    /// Check whether a peer is disconnected.
    pub fn is_disconnected(&self, addr: &PeerId) -> bool {
        !self.connections.contains_key(addr)
    }

    /// Check whether a peer is being disconnected.
    pub fn is_disconnecting(&self, addr: &PeerId) -> bool {
        matches!(self.connections.get(addr), Some(Connection::Disconnecting))
    }

    /// Returns connecting peers.
    pub fn connecting(&self) -> impl Iterator<Item = &PeerId> {
        self.connections
            .iter()
            .filter(|(_, p)| matches!(p, Connection::Connecting { .. }))
            .map(|(addr, _)| addr)
    }

    /// Iterator over connections in a *connected* state..
    pub fn connected(&self) -> impl Iterator<Item = &Connected> + Clone {
        self.connections.values().filter_map(|c| {
            if let Connection::Connected(conn) = c {
                Some(conn)
            } else {
                None
            }
        })
    }

    /// Connect to a peer.
    pub fn connect(&mut self, addr: &PeerId, time: LocalTime) -> bool {
        if !self.is_disconnected(addr) && !self.is_disconnecting(addr) {
            return false;
        }
        // Don't allow connections to unsupported domains.
        if !self.config.domains.contains(&Domain::for_address(addr)) {
            return false;
        }
        self.connections
            .insert(*addr, Connection::Connecting { time });
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
        self.connections.insert(addr, Connection::Disconnecting);
    }

    /// Attempt to maintain a certain number of outbound peers.
    fn maintain_connections<A: AddressSource>(&mut self, addrs: &mut A, local_time: LocalTime) {
        let outbound = self.connected().filter(|c| c.link.is_outbound());
        let current = outbound.count() + self.connecting().count();
        let target = self.config.target_outbound_peers;
        let delta = target - current;

        // Keep track of new addresses we're connecting to, and loop until
        // we've connected to enough addresses.
        let mut connecting = HashSet::with_hasher(self.rng.clone().into());

        while connecting.len() < delta {
            if let Some((addr, source)) = addrs
                .sample(self.config.preferred_services)
                .or_else(|| addrs.sample(self.config.required_services))
            {
                if let Ok(sockaddr) = addr.socket_addr() {
                    // TODO: Remove this assertion once address manager no longer cares about
                    // connections.
                    debug_assert!(!self.is_connected(&sockaddr));

                    if self.connect(&sockaddr, local_time) {
                        connecting.insert(sockaddr);
                        self.upstream.event(Event::Connecting(sockaddr, source));
                    }
                }
            } else {
                // We're completely out of addresses, give up.
                break;
            }
        }
    }

    /// Peers that have been idle longer than [`CONNECTION_TIMEOUT`].
    fn idle_peers(&self, now: LocalTime) -> impl Iterator<Item = PeerId> + '_ {
        self.connections.iter().filter_map(move |(addr, c)| {
            if let Connection::Connecting { time } = c {
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
    use bitcoin::network::address::Address;
    use std::collections::VecDeque;

    fn config() -> Config {
        Config {
            protocol_version: crate::protocol::PROTOCOL_VERSION,
            target_outbound_peers: TARGET_OUTBOUND_PEERS,
            max_inbound_peers: MAX_INBOUND_PEERS,
            domains: Domain::all(),
            user_agent: crate::protocol::USER_AGENT,
            retry: vec![],
            services: ServiceFlags::NONE,
            preferred_services: ServiceFlags::COMPACT_FILTERS | ServiceFlags::NETWORK,
            required_services: ServiceFlags::NETWORK,
            whitelist: Whitelist::default(),
        }
    }

    #[test]
    fn test_connect_timeout() {
        let rng = fastrand::Rng::with_seed(1);
        let mut time = LocalTime::now();

        let remote = ([124, 43, 110, 1], 8333).into();

        let mut addrs = VecDeque::new();
        let mut peermgr = PeerManager::new(config(), rng, Hooks::default(), ());

        peermgr.initialize(time, &mut addrs);
        peermgr.connect(&remote, time);

        assert_eq!(peermgr.connecting().next(), Some(&remote));
        assert_eq!(peermgr.connecting().count(), 1);

        time.elapse(LocalDuration::from_secs(1));
        peermgr.received_tick(time, &mut addrs);

        assert_eq!(peermgr.connecting().next(), Some(&remote));

        // After the timeout has elapsed, the peer should be disconnected.
        time.elapse(CONNECTION_TIMEOUT);
        peermgr.received_tick(time, &mut addrs);

        assert_eq!(peermgr.connecting().next(), None);
        assert!(matches!(
            peermgr.connections.get(&remote),
            Some(Connection::Disconnecting)
        ));
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

        let mut addrs = VecDeque::new();
        let mut peermgr = PeerManager::new(config(), rng, Hooks::default(), ());

        peermgr.initialize(time, &mut addrs);
        peermgr.connect(&remote1, time);

        assert_eq!(peermgr.connecting().next(), Some(&remote1));
        assert_eq!(peermgr.connected().next(), None);

        peermgr.peer_connected(remote1, local, Link::Outbound, height, time);

        assert_eq!(peermgr.connecting().next(), None);
        assert_eq!(peermgr.connected().map(|c| &c.addr).next(), Some(&remote1));

        // Disconnect remote#1 after it has connected.
        addrs.push_back((Address::new(&remote2, services), Source::Dns));
        peermgr.peer_disconnected(&remote1, &mut addrs, time);

        assert!(peermgr.is_disconnected(&remote1));
        assert_eq!(peermgr.connected().next(), None);
        assert_eq!(
            peermgr.connecting().next(),
            Some(&remote2),
            "Disconnection triggers a new connection to remote#2"
        );

        // Disconnect remote#2 while still connecting.
        addrs.push_back((Address::new(&remote3, services), Source::Dns));
        peermgr.peer_disconnected(&remote2, &mut addrs, time);

        assert!(peermgr.is_disconnected(&remote2));
        assert_eq!(
            peermgr.connecting().next(),
            Some(&remote3),
            "Disconnection triggers a new connection to remote#3"
        );

        // Connect, then disconnect remote#3.
        addrs.push_back((Address::new(&remote4, services), Source::Dns));

        peermgr.peer_connected(remote3, local, Link::Outbound, height, time);
        peermgr.disconnect(remote3, DisconnectReason::Command);
        peermgr.peer_disconnected(&remote3, &mut addrs, time);

        assert!(peermgr.is_disconnected(&remote3));
        assert_eq!(
            peermgr.connecting().next(),
            Some(&remote4),
            "Disconnection triggers a new connection to remote#4"
        );
    }
}
