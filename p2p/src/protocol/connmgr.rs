//! Peer connection manager.

use std::marker::PhantomData;
use std::net;

use bitcoin::network::constants::ServiceFlags;

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::collections::{HashMap, HashSet};
use nakamoto_common::p2p::peer::{AddressSource, Source};
use nakamoto_common::p2p::Domain;

use super::channel::{Disconnect, SetTimeout};
use crate::protocol::{DisconnectReason, Link, PeerId, Timeout};

/// Time to wait for a new connection.
/// TODO: Should be in config.
pub const CONNECTION_TIMEOUT: LocalDuration = LocalDuration::from_secs(6);
/// Time to wait until idle.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);
/// Target number of concurrent outbound peer connections.
pub const TARGET_OUTBOUND_PEERS: usize = 8;
/// Maximum number of inbound peer connections.
pub const MAX_INBOUND_PEERS: usize = 16;

/// Ability to connect to peers.
pub trait Connect {
    /// Connect to peer.
    fn connect(&self, addr: net::SocketAddr, timeout: Timeout);
}

/// Ability to emit events.
pub trait Events {
    /// Emit event.
    fn event(&self, event: Event);
}

/// A connection-related event.
#[derive(Debug, Clone)]
pub enum Event {
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
            Event::Connecting(addr, source) => {
                write!(fmt, "Connecting to peer {} from source `{}`", addr, source)
            }
            Event::Connected(addr, link) => write!(fmt, "{}: Peer connected ({:?})", &addr, link),
            Event::Disconnected(addr) => write!(fmt, "Disconnected from {}", &addr),
        }
    }
}

/// Connection manager configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Target number of outbound peer connections.
    pub target_outbound_peers: usize,
    /// Maximum number of inbound peer connections.
    pub max_inbound_peers: usize,
    /// Peer addresses that should always be retried.
    pub retry: Vec<net::SocketAddr>,
    /// Supported communication domains.
    pub domains: Vec<Domain>,
    /// Peer services required.
    pub required_services: ServiceFlags,
    /// Peer services preferred. We try to maintain as many
    /// connections to peers with these services.
    pub preferred_services: ServiceFlags,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            target_outbound_peers: TARGET_OUTBOUND_PEERS,
            max_inbound_peers: MAX_INBOUND_PEERS,
            retry: vec![],
            domains: Domain::all(),
            required_services: ServiceFlags::NONE,
            preferred_services: ServiceFlags::NONE,
        }
    }
}

/// A connected peer.
#[derive(Debug)]
enum Peer {
    Connecting {
        /// Time the connection was attempted.
        time: LocalTime,
    },
    Connected {
        /// Remote peer address.
        address: net::SocketAddr,
        /// Local peer address.
        local_address: net::SocketAddr,
        /// Whether this is an inbound or outbound peer connection.
        link: Link,
        /// Services offered.
        services: ServiceFlags,
        /// Time connected.
        time: LocalTime,
    },
    Disconnecting,
    Disconnected, // TODO: Keep track of when the peer was disconnected, so we can prune.
}

/// Manages peer connections.
#[derive(Debug)]
pub struct ConnectionManager<U, A> {
    /// Configuration.
    pub config: Config,
    /// Set of outbound peers being connected to.
    peers: HashMap<PeerId, Peer>,
    /// Last time we were idle.
    last_idle: Option<LocalTime>,
    /// Channel to the network.
    upstream: U,
    /// Type witness for address source.
    addresses: PhantomData<A>,
    /// RNG.
    rng: fastrand::Rng,
}

impl<U: Connect + Disconnect + Events + SetTimeout, A: AddressSource> ConnectionManager<U, A> {
    /// Create a new connection manager.
    pub fn new(upstream: U, config: Config, rng: fastrand::Rng) -> Self {
        Self {
            peers: HashMap::with_hasher(rng.clone().into()),
            last_idle: None,
            config,
            upstream,
            addresses: PhantomData,
            rng,
        }
    }

    /// Initialize the connection manager. Must be called once.
    pub fn initialize(&mut self, time: LocalTime, addrs: &mut A) {
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

    /// Check whether a peer is connected.
    pub fn is_connected(&self, addr: &PeerId) -> bool {
        self.peers
            .get(addr)
            .map_or(false, |p| matches!(p, Peer::Connected { .. }))
    }

    /// Check whether a peer is disconnected.
    pub fn is_disconnected(&self, addr: &PeerId) -> bool {
        self.peers
            .get(addr)
            .map_or(true, |p| matches!(p, Peer::Disconnected))
    }

    /// Check whether a peer is connecting.
    pub fn is_connecting(&self, addr: &PeerId) -> bool {
        self.peers
            .get(addr)
            .map_or(false, |p| matches!(p, Peer::Connecting { .. }))
    }

    /// Check whether a peer is connected via an inbound link.
    pub fn is_inbound(&self, addr: &PeerId) -> bool {
        self.peers.get(addr).map_or(
            false,
            |p| matches!(p, Peer::Connected { link, .. } if link.is_inbound()),
        )
    }

    /// Connect to a peer.
    pub fn connect(&mut self, addr: &PeerId, time: LocalTime) -> bool {
        if !self.is_disconnected(addr) {
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

    /// Called when a peer connected.
    pub fn peer_connected(
        &mut self,
        address: net::SocketAddr,
        local_address: net::SocketAddr,
        link: Link,
        time: LocalTime,
    ) {
        debug_assert!(!self.is_connected(&address));

        Events::event(&self.upstream, Event::Connected(address, link));

        // TODO: There is a chance that we simultaneously connect to a peer that is connecting
        // to us. This would create two connections to the same peer, one outbound and one
        // inbound. To prevent this, we could look at IPs when receiving inbound connections,
        // to check whether we are already connected to the peer.

        match link {
            Link::Inbound if self.inbound_peers().count() >= self.config.max_inbound_peers => {
                // TODO: Test this branch.
                // Don't allow inbound connections beyond the configured limit.
                self._disconnect(address, DisconnectReason::ConnectionLimit);
            }
            _ => {
                self.peers.insert(
                    address,
                    Peer::Connected {
                        address,
                        local_address,
                        services: ServiceFlags::NONE,
                        link,
                        time,
                    },
                );
            }
        }
    }

    /// Call when a peer negotiated.
    pub fn peer_negotiated(&mut self, address: net::SocketAddr, flags: ServiceFlags) {
        if let Some(Peer::Connected {
            ref mut services, ..
        }) = self.peers.get_mut(&address)
        {
            *services = flags;
        } else {
            panic!(
                "ConnectionManager::peer_negotiated: negotiated peers should be connected first"
            );
        }
    }

    /// Call when a peer disconnected.
    pub fn peer_disconnected(
        &mut self,
        addr: &net::SocketAddr,
        addrs: &mut A,
        local_time: LocalTime,
    ) {
        debug_assert!(self.peers.contains_key(addr));
        debug_assert!(!self.is_disconnected(addr));

        Events::event(&self.upstream, Event::Disconnected(*addr));

        // If an outbound peer disconnected, we should make sure to maintain
        // our target outbound connection count.
        let previous = self.peers.insert(*addr, Peer::Disconnected);
        match previous {
            Some(Peer::Connected { link, .. }) if link.is_outbound() => {
                self.maintain_connections(addrs, local_time);
            }
            Some(Peer::Connecting { .. } | Peer::Disconnecting { .. }) => {
                self.maintain_connections(addrs, local_time);
            }
            _ => {}
        }
    }

    /// Call when we recevied a tick.
    pub fn received_tick(&mut self, now: LocalTime, addrs: &mut A) {
        // Disconnect all peers that have been idle for too long.
        for addr in self.idle_peers(now).collect::<Vec<_>>() {
            self._disconnect(addr, DisconnectReason::PeerTimeout("connection"));
        }

        if now - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            self.maintain_connections(addrs, now);
            self.upstream.set_timeout(IDLE_TIMEOUT);
            self.last_idle = Some(now);
        }
    }

    /// Returns outbound peer addresses.
    pub fn outbound_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peers
            .iter()
            .filter(|(_, p)| matches!(p, Peer::Connected { link, .. } if link.is_outbound()))
            .map(|(addr, _)| addr)
    }

    /// Returns inbound peer addresses.
    pub fn inbound_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peers
            .iter()
            .filter(|(_, p)| matches!(p, Peer::Connected { link, .. } if link.is_inbound()))
            .map(|(addr, _)| addr)
    }

    /// Returns connecting peers.
    pub fn connecting_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peers
            .iter()
            .filter(|(_, p)| matches!(p, Peer::Connecting { .. }))
            .map(|(addr, _)| addr)
    }

    /// Attempt to maintain a certain number of outbound peers.
    fn maintain_connections(&mut self, addrs: &mut A, local_time: LocalTime) {
        let current = self.outbound().count() + self.connecting_peers().count();
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

    /// Get outbound peers.
    fn outbound(&self) -> impl Iterator<Item = &Peer> + Clone {
        self.peers
            .values()
            .filter(|p| matches!(p, Peer::Connected { link, .. } if link.is_outbound()))
    }

    /// Disconnect a peer (internal).
    fn _disconnect(&mut self, addr: PeerId, reason: DisconnectReason) {
        self.peers.insert(addr, Peer::Disconnecting);
        self.upstream.disconnect(addr, reason);
    }

    /// Peers that have been idle longer than [`CONNECTION_TIMEOUT`].
    fn idle_peers(&self, now: LocalTime) -> impl Iterator<Item = PeerId> + '_ {
        self.peers.iter().filter_map(move |(addr, p)| {
            if let Peer::Connecting { time } = p {
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

    #[test]
    fn test_connect_timeout() {
        let cfg = Config::default();
        let rng = fastrand::Rng::with_seed(1);
        let mut time = LocalTime::now();

        let remote = ([124, 43, 110, 1], 8333).into();

        let mut addrs = VecDeque::new();
        let mut connmgr: ConnectionManager<_, VecDeque<_>> = ConnectionManager::new((), cfg, rng);

        connmgr.initialize(time, &mut addrs);
        connmgr.connect(&remote, time);

        assert_eq!(connmgr.connecting_peers().next(), Some(&remote));
        assert_eq!(connmgr.connecting_peers().count(), 1);

        time.elapse(LocalDuration::from_secs(1));
        connmgr.received_tick(time, &mut addrs);

        assert_eq!(connmgr.connecting_peers().next(), Some(&remote));

        // After the timeout has elapsed, the peer should be disconnected.
        time.elapse(CONNECTION_TIMEOUT);
        connmgr.received_tick(time, &mut addrs);

        assert_eq!(connmgr.connecting_peers().next(), None);
        assert!(matches!(
            connmgr.peers.get(&remote),
            Some(Peer::Disconnecting)
        ));
    }

    #[test]
    fn test_disconnects() {
        let cfg = Config::default();
        let rng = fastrand::Rng::with_seed(1);
        let time = LocalTime::now();

        let services = ServiceFlags::NETWORK;
        let local = ([99, 99, 99, 99], 9999).into();
        let remote1 = ([124, 43, 110, 1], 8333).into();
        let remote2 = ([124, 43, 110, 2], 8333).into();
        let remote3 = ([124, 43, 110, 3], 8333).into();
        let remote4 = ([124, 43, 110, 4], 8333).into();

        let mut addrs = VecDeque::new();
        let mut connmgr: ConnectionManager<_, VecDeque<_>> = ConnectionManager::new((), cfg, rng);

        connmgr.initialize(time, &mut addrs);
        connmgr.connect(&remote1, time);

        assert_eq!(connmgr.connecting_peers().next(), Some(&remote1));
        assert_eq!(connmgr.outbound_peers().next(), None);

        connmgr.peer_connected(remote1, local, Link::Outbound, time);

        assert_eq!(connmgr.connecting_peers().next(), None);
        assert_eq!(connmgr.outbound_peers().next(), Some(&remote1));

        // Disconnect remote#1 after it has connected.
        addrs.push_back((Address::new(&remote2, services), Source::Dns));
        connmgr.peer_disconnected(&remote1, &mut addrs, time);

        assert!(connmgr.is_disconnected(&remote1));
        assert_eq!(connmgr.outbound_peers().next(), None);
        assert_eq!(
            connmgr.connecting_peers().next(),
            Some(&remote2),
            "Disconnection triggers a new connection to remote#2"
        );

        // Disconnect remote#2 while still connecting.
        addrs.push_back((Address::new(&remote3, services), Source::Dns));
        connmgr.peer_disconnected(&remote2, &mut addrs, time);

        assert!(connmgr.is_disconnected(&remote2));
        assert_eq!(
            connmgr.connecting_peers().next(),
            Some(&remote3),
            "Disconnection triggers a new connection to remote#3"
        );

        // Connect, then disconnect remote#3.
        addrs.push_back((Address::new(&remote4, services), Source::Dns));

        connmgr.peer_connected(remote3, local, Link::Outbound, time);
        connmgr.disconnect(remote3, DisconnectReason::Command);
        connmgr.peer_disconnected(&remote3, &mut addrs, time);

        assert!(connmgr.is_disconnected(&remote3));
        assert_eq!(
            connmgr.connecting_peers().next(),
            Some(&remote4),
            "Disconnection triggers a new connection to remote#4"
        );
    }
}
