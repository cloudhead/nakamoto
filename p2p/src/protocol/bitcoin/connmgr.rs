// TODO
#![allow(missing_docs)]
use std::collections::{HashMap, HashSet};
use std::net;

use nakamoto_common::block::time::{LocalDuration, LocalTime};

use super::addrmgr::{self, AddressManager};
use crate::protocol::{Link, PeerId, Timeout};

/// Time to wait for a new connection.
/// TODO: Should be in config.
pub const CONNECTION_TIMEOUT: LocalDuration = LocalDuration::from_secs(3);

pub trait Connect {
    fn connect(&self, addr: net::SocketAddr, timeout: Timeout);
}

pub trait Disconnect {
    fn disconnect(&self, addr: net::SocketAddr);
}

pub trait Events {
    fn event(&self, event: Event);
}

#[derive(Debug)]
pub enum Event {
    /// The node is connecting to peers.
    Connecting(usize, usize),
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
            Event::Connecting(current, target) => write!(
                fmt,
                "Peer outbound connections ({}) below target ({})",
                current, target,
            ),
            Event::Connected(addr, link) => write!(fmt, "{}: Peer connected ({:?})", &addr, link),
            Event::Disconnected(addr) => write!(fmt, "Disconnected from {}", &addr),
            Event::AddressBookExhausted => {
                write!(fmt, "Address book exhausted when attempting to connect..")
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Target number of outbound peer connections.
    pub target_outbound_peers: usize,
    /// Maximum number of inbound peer connections.
    pub max_inbound_peers: usize,
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
}

#[derive(Debug)]
pub struct ConnectionManager<U> {
    /// Configuration.
    pub config: Config,

    /// Set of all connected peers.
    connected: HashMap<PeerId, Peer>,
    /// Set of disconnected peers.
    disconnected: HashSet<PeerId>,

    upstream: U,
}

impl<U: Connect + Disconnect + Events + addrmgr::GetAddresses + addrmgr::Events>
    ConnectionManager<U>
{
    pub fn initialize(&mut self, time: LocalTime, addrmgr: &mut AddressManager<U>) {
        // FIXME: Should be random
        let addrs = addrmgr
            .iter()
            .take(self.config.target_outbound_peers)
            .map(|a| a.socket_addr().ok())
            .flatten()
            .collect::<Vec<_>>();

        for addr in addrs {
            self.connect(&addr, addrmgr, time);
        }
    }

    pub fn connect(
        &mut self,
        addr: &PeerId,
        addrmgr: &mut AddressManager<U>,
        local_time: LocalTime,
    ) {
        if !self.connected.contains_key(&addr) {
            self.upstream.connect(*addr, CONNECTION_TIMEOUT);
            addrmgr.peer_attempted(&addr, local_time);
        }
    }

    pub fn disconnect(&mut self, addr: PeerId) {
        if self.connected.contains_key(&addr) {
            debug_assert!(!self.disconnected.contains(&addr));

            self.upstream.disconnect(addr);
        }
    }

    // TODO: Rename to `idle` when it's called periodically.
    pub fn tick(&mut self, addrmgr: &AddressManager<U>) {
        self.maintain_connections(addrmgr);
    }

    /// Attempt to maintain a certain number of outbound peers.
    fn maintain_connections(&mut self, addrmgr: &AddressManager<U>) {
        let current = self.outbound().count();

        if current < self.config.target_outbound_peers {
            Events::event(
                &self.upstream,
                Event::Connecting(current, self.config.target_outbound_peers),
            );

            if let Some(addr) = addrmgr.sample() {
                if let Ok(sockaddr) = addr.socket_addr() {
                    debug_assert!(!self.connected.contains_key(&sockaddr));

                    self.upstream.connect(sockaddr, CONNECTION_TIMEOUT);
                } else {
                    // TODO: Perhaps the address manager should just return addresses
                    // that can be converted to socket addresses?
                    // The only ones that cannot are Tor addresses.
                    todo!();
                }
            } else {
                // TODO: Don't ask for addresses many times in a row.
                Events::event(&self.upstream, Event::AddressBookExhausted);

                // We're out of addresses, ask for more!
                addrmgr.get_addresses();
            }
        }
    }

    pub fn peer_disconnected(&mut self, addr: &net::SocketAddr, addrmgr: &AddressManager<U>) {
        debug_assert!(self.connected.contains_key(&addr));
        debug_assert!(!self.disconnected.contains(&addr));

        Events::event(&self.upstream, Event::Disconnected(*addr));

        self.disconnected.insert(*addr);

        if let Some(peer) = self.connected.remove(&addr) {
            // If an outbound peer disconnected, we should make sure to maintain
            // our target outbound connection count.
            if peer.link.is_outbound() {
                self.maintain_connections(addrmgr);
            }
        }
    }

    pub fn received_timeout(
        &mut self,
        addr: PeerId,
        _local_time: LocalTime,
        addrmgr: &AddressManager<U>,
    ) {
        debug_assert!(!self.connected.contains_key(&addr));

        self.maintain_connections(addrmgr);
    }
}

impl<U: Connect + Disconnect + Events> ConnectionManager<U> {
    pub fn new(upstream: U, config: Config) -> Self {
        Self {
            connected: HashMap::new(),
            disconnected: HashSet::new(),
            config,
            upstream,
        }
    }

    pub fn peer_connected(
        &mut self,
        address: net::SocketAddr,
        local_address: net::SocketAddr,
        link: Link,
    ) {
        debug_assert!(!self.connected.contains_key(&address));

        self.upstream.event(Event::Connected(address, link));

        match link {
            Link::Inbound if self.connected.len() >= self.config.max_inbound_peers => {
                // Don't allow inbound connections beyond the configured limit.
                // TODO: Include disconnect reason.
                self.upstream.disconnect(address);
            }
            _ => {
                self.disconnected.remove(&address);
                self.connected.insert(
                    address,
                    Peer {
                        address,
                        local_address,
                        link,
                    },
                );
            }
        }
    }

    /// Get outbound peers.
    fn outbound(&self) -> impl Iterator<Item = &Peer> + Clone {
        self.connected.values().filter(|p| p.link.is_outbound())
    }
}
