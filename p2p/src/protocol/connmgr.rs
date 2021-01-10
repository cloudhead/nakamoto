//! Peer connection manager.

use std::collections::{HashMap, HashSet};
use std::net;

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::p2p::peer::{self, AddressSource, Source};

use super::channel::{Disconnect, SetTimeout};
use crate::protocol::{DisconnectReason, Link, PeerId, Timeout};

/// Time to wait for a new connection.
/// TODO: Should be in config.
pub const CONNECTION_TIMEOUT: LocalDuration = LocalDuration::from_secs(3);
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
    /// Address book exhausted when trying to connect.
    AddressBookExhausted,
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::Connecting(addr, source) => {
                write!(fmt, "Connecting to peer {} from source `{}`", addr, source)
            }
            Event::Connected(addr, link) => write!(fmt, "{}: Peer connected ({:?})", &addr, link),
            Event::Disconnected(addr) => write!(fmt, "Disconnected from {}", &addr),
            Event::AddressBookExhausted => {
                write!(fmt, "Address book exhausted when attempting to connect..")
            }
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
}

/// A connected peer.
#[derive(Debug)]
struct Peer {
    /// Remote peer address.
    address: net::SocketAddr,
    /// Local peer address.
    local_address: net::SocketAddr,
    /// Whether this is an inbound or outbound peer connection.
    link: Link,
    /// Time connected.
    time: LocalTime,
}

/// Manages peer connections.
#[derive(Debug)]
pub struct ConnectionManager<U> {
    /// Configuration.
    pub config: Config,
    /// Set of peers being connected to.
    connecting: HashSet<PeerId>,
    /// Set of all connected peers.
    connected: HashMap<PeerId, Peer>,
    /// Set of disconnected peers.
    disconnected: HashSet<PeerId>,
    /// Last time we were idle.
    last_idle: Option<LocalTime>,
    /// Channel to the network.
    upstream: U,
}

impl<U: Connect + Disconnect + Events + SetTimeout> ConnectionManager<U> {
    /// Create a new connection manager.
    pub fn new(upstream: U, config: Config) -> Self {
        Self {
            connecting: HashSet::new(),
            connected: HashMap::new(),
            disconnected: HashSet::new(),
            last_idle: None,
            config,
            upstream,
        }
    }

    /// Initialize the connection manager. Must be called once.
    pub fn initialize<S: peer::Store, A: AddressSource>(
        &mut self,
        _time: LocalTime,
        addrs: &mut A,
    ) {
        let retry = self
            .config
            .retry
            .iter()
            .take(self.config.target_outbound_peers)
            .cloned()
            .collect::<Vec<_>>();

        for addr in retry {
            self.connect::<S, A>(&addr);
        }
        self.upstream.set_timeout(IDLE_TIMEOUT);
        self.maintain_connections::<S, A>(addrs);
    }

    /// Connect to a peer.
    pub fn connect<S: peer::Store, A: AddressSource>(&mut self, addr: &PeerId) -> bool {
        if self.connected.contains_key(&addr) || self.connecting.contains(addr) {
            return false;
        }
        self.connecting.insert(*addr);
        self.upstream.connect(*addr, CONNECTION_TIMEOUT);

        true
    }

    /// Disconnect from a peer.
    pub fn disconnect(&mut self, addr: PeerId, reason: DisconnectReason) {
        if self.connected.contains_key(&addr) {
            debug_assert!(!self.disconnected.contains(&addr));

            self.upstream.disconnect(addr, reason);
        }
    }

    /// Call when a peer connected.
    pub fn peer_connected(
        &mut self,
        address: net::SocketAddr,
        local_address: net::SocketAddr,
        link: Link,
        time: LocalTime,
    ) {
        debug_assert!(!self.connected.contains_key(&address));

        Events::event(&self.upstream, Event::Connected(address, link));

        match link {
            Link::Inbound if self.connected.len() >= self.config.max_inbound_peers => {
                // Don't allow inbound connections beyond the configured limit.
                self.upstream
                    .disconnect(address, DisconnectReason::ConnectionLimit);
            }
            _ => {
                self.disconnected.remove(&address);
                self.connecting.remove(&address);
                self.connected.insert(
                    address,
                    Peer {
                        address,
                        local_address,
                        link,
                        time,
                    },
                );
            }
        }
    }

    /// Call when a peer disconnected.
    pub fn peer_disconnected<S: peer::Store, A: AddressSource>(
        &mut self,
        addr: &net::SocketAddr,
        addrs: &A,
    ) {
        debug_assert!(self.connected.contains_key(&addr));
        debug_assert!(!self.disconnected.contains(&addr));

        Events::event(&self.upstream, Event::Disconnected(*addr));

        self.disconnected.insert(*addr);

        if let Some(peer) = self.connected.remove(&addr) {
            // If an outbound peer disconnected, we should make sure to maintain
            // our target outbound connection count.
            if peer.link.is_outbound() {
                self.maintain_connections::<S, A>(addrs);
            }
        } else {
            self.connecting.remove(&addr);
        }
    }

    /// Call when we recevied a timeout.
    pub fn received_timeout<S: peer::Store, A: AddressSource>(
        &mut self,
        local_time: LocalTime,
        addrs: &A,
    ) {
        if local_time - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            self.maintain_connections::<S, A>(addrs);
            self.upstream.set_timeout(IDLE_TIMEOUT);
            self.last_idle = Some(local_time);
        }
    }

    /// Returns outbound peer addresses.
    pub fn outbound_peers(&self) -> impl Iterator<Item = &PeerId> {
        self.connected
            .iter()
            .filter(|(_, p)| p.link.is_outbound())
            .map(|(addr, _)| addr)
    }

    /// Attempt to maintain a certain number of outbound peers.
    fn maintain_connections<S: peer::Store, A: AddressSource>(&mut self, addrs: &A) {
        while self.outbound().count() + self.connecting.len() < self.config.target_outbound_peers {
            if let Some((addr, source)) = addrs.sample() {
                // TODO: Support Tor?
                if let Ok(sockaddr) = addr.socket_addr() {
                    // TODO: Remove this assertion once address manager no longer cares about
                    // connections.
                    debug_assert!(!self.connected.contains_key(&sockaddr));

                    if self.connect::<S, A>(&sockaddr) {
                        self.upstream.event(Event::Connecting(sockaddr, source));
                        break;
                    }
                }
            } else {
                // We're out of addresses. We don't need to do anything here, the address manager
                // will eventually find new addresses.
                Events::event(&self.upstream, Event::AddressBookExhausted);
                break;
            }
        }
    }

    /// Get outbound peers.
    fn outbound(&self) -> impl Iterator<Item = &Peer> + Clone {
        self.connected.values().filter(|p| p.link.is_outbound())
    }
}
