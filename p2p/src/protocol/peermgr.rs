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

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::block::Height;
use nakamoto_common::collections::HashMap;

use crate::protocol::addrmgr;

use super::{
    channel::{Disconnect, SetTimeout},
    DisconnectReason,
};
use super::{Link, PeerId, Whitelist};

/// Time to wait for response during peer handshake before disconnecting the peer.
pub const HANDSHAKE_TIMEOUT: LocalDuration = LocalDuration::from_secs(10);

/// Maximum height difference for a stale peer, to maintain the connection (2 weeks).
const MAX_STALE_HEIGHT_DIFFERENCE: Height = 2016;

/// A time offset, in seconds.
type TimeOffset = i64;

/// An event originating in the SPV manager.
#[derive(Debug, Clone)]
pub enum Event {
    /// The `version` message was received from a peer.
    PeerVersionReceived {
        /// The peer's id.
        addr: PeerId,
        /// The version message.
        msg: VersionMessage,
    },
    /// A peer has successfully negotiated (handshaked).
    PeerNegotiated {
        /// The peer's id.
        addr: PeerId,
        /// Services by negotiated peer
        services: ServiceFlags,
    },
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PeerVersionReceived { addr, msg } => write!(
                fmt,
                "{}: Peer version = {}, height = {}, agent = {}, services = {}, timestamp = {}",
                addr, msg.version, msg.start_height, msg.user_agent, msg.services, msg.timestamp
            ),
            Self::PeerNegotiated { addr, services } => write!(
                fmt,
                "{}: Peer negotiated with services {}..",
                addr, services
            ),
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
    /// Services required by peers.
    pub required_services: ServiceFlags,
    /// Our user agent.
    pub user_agent: &'static str,
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
#[derive(Debug)]
pub struct Connection {
    /// Remote peer address.
    pub addr: net::SocketAddr,
    /// Local peer address.
    pub local_addr: net::SocketAddr,
    /// Whether this is an inbound or outbound peer connection.
    pub link: Link,
    /// Connected since this time.
    pub since: LocalTime,
}

/// A peer with connection and protocol information.
#[derive(Debug)]
pub struct Peer {
    /// Connection information.
    pub conn: Connection,
    // last_active: LocalTime,
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
}

/// Manages peers and peer negotiation.
#[derive(Debug)]
pub struct PeerManager<U> {
    /// Peer manager configuration.
    pub config: Config,

    connections: HashMap<net::SocketAddr, Connection>,
    peers: HashMap<PeerId, Peer>,
    upstream: U,
    rng: fastrand::Rng,
}

impl<U: Handshake + SetTimeout + Disconnect + Events> PeerManager<U> {
    /// Create a new peer manager.
    pub fn new(config: Config, rng: fastrand::Rng, upstream: U) -> Self {
        let connections = HashMap::with_hasher(rng.clone().into());
        let peers = HashMap::with_hasher(rng.clone().into());

        Self {
            config,
            connections,
            peers,
            upstream,
            rng,
        }
    }

    /// Iterator over outbound, negotiated peers.
    pub fn outbound(&self) -> impl Iterator<Item = &Peer> + Clone {
        self.peers
            .values()
            .filter(|p| p.is_negotiated() && p.conn.link.is_outbound())
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
        self.connections.insert(
            addr,
            Connection {
                addr,
                local_addr,
                link,
                since: local_time,
            },
        );

        match link {
            Link::Inbound => { /* Wait for their version message.. */ }
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
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, addr: &net::SocketAddr) {
        self.peers.remove(&addr);
        self.connections.remove(&addr);
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
        if let Some(conn) = self.connections.remove(addr) {
            self.upstream.event(Event::PeerVersionReceived {
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
            } = msg;

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
                    conn,
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
                self.upstream.event(Event::PeerNegotiated {
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
    pub fn received_tick(&mut self, local_time: LocalTime) {
        let mut timed_out = Vec::new();

        for (addr, peer) in self.peers.iter() {
            match peer.state {
                PeerState::AwaitingVerack { since } => {
                    if local_time - since >= HANDSHAKE_TIMEOUT {
                        timed_out.push(*addr);
                    }
                }
                PeerState::Negotiated { .. } => {}
            }
        }
        for (addr, conn) in self.connections.iter() {
            if local_time - conn.since >= HANDSHAKE_TIMEOUT {
                timed_out.push(*addr);
            }
        }

        for addr in timed_out {
            self.upstream
                .disconnect(addr, DisconnectReason::PeerTimeout("handshake"));
        }
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
