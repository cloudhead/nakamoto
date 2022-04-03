//! Ping manager.
//!
//! Detects dead peer connections and responds to peer `ping` messages.
//!
//! *Implementation of BIP 0031.*
//!
use std::collections::VecDeque;
use std::net;

use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::collections::HashMap;

use crate::protocol::PeerId;

use super::{
    output::{Disconnect, Wakeup},
    DisconnectReason,
};

/// Time interval to wait between sent pings.
pub const PING_INTERVAL: LocalDuration = LocalDuration::from_mins(2);
/// Time to wait to receive a pong when sending a ping.
pub const PING_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);

/// Maximum number of latencies recorded per peer.
const MAX_RECORDED_LATENCIES: usize = 64;

/// The ability to send `ping` and `pong` messages.
pub trait Ping {
    /// Send a `ping` message.
    fn ping(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self;
    /// Send a `pong` message.
    fn pong(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self;
}

#[derive(Debug)]
enum State {
    AwaitingPong { nonce: u64, since: LocalTime },
    Idle { since: LocalTime },
}

#[derive(Debug)]
struct Peer {
    address: net::SocketAddr,
    state: State,
    /// Observed round-trip latencies for this peer.
    latencies: VecDeque<LocalDuration>,
}

impl Peer {
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
}

/// Detects dead peer connections.
#[derive(Debug)]
pub struct PingManager<U, C> {
    peers: HashMap<PeerId, Peer>,
    ping_timeout: LocalDuration,
    /// Random number generator.
    rng: fastrand::Rng,
    upstream: U,
    clock: C,
}

impl<U: Ping + Wakeup + Disconnect, C: Clock> PingManager<U, C> {
    /// Create a new ping manager.
    pub fn new(ping_timeout: LocalDuration, rng: fastrand::Rng, upstream: U, clock: C) -> Self {
        let peers = HashMap::with_hasher(rng.clone().into());

        Self {
            peers,
            ping_timeout,
            rng,
            upstream,
            clock,
        }
    }

    /// Called when a peer is negotiated.
    pub fn peer_negotiated(&mut self, address: PeerId) {
        let nonce = self.rng.u64(..);
        let now = self.clock.local_time();

        self.upstream.ping(address, nonce);
        self.peers.insert(
            address,
            Peer {
                address,
                state: State::AwaitingPong { nonce, since: now },
                latencies: VecDeque::new(),
            },
        );
    }

    /// Called when a peer is disconnected.
    pub fn peer_disconnected(&mut self, addr: &PeerId) {
        self.peers.remove(addr);
    }

    /// Called when a tick is received.
    pub fn received_wake(&mut self) {
        let now = self.clock.local_time();

        for peer in self.peers.values_mut() {
            match peer.state {
                State::AwaitingPong { since, .. } => {
                    // TODO: By using nonces we should be able to overlap ping messages.
                    // This would allow us to only disconnect a peer after N ping messages
                    // are sent in a row with no reply.
                    //
                    // A ping was sent and we're waiting for a `pong`. If too much
                    // time has passed, we consider this peer dead, and disconnect
                    // from them.
                    if now - since >= self.ping_timeout {
                        self.upstream
                            .disconnect(peer.address, DisconnectReason::PeerTimeout("ping"));
                    }
                }
                State::Idle { since } => {
                    // We aren't waiting for any `pong`. Check whether enough time has passed since we
                    // received the last `pong`, and if so, send a new `ping`.
                    if now - since >= PING_INTERVAL {
                        let nonce = self.rng.u64(..);

                        self.upstream
                            .ping(peer.address, nonce)
                            .wakeup(self.ping_timeout)
                            .wakeup(PING_INTERVAL);

                        peer.state = State::AwaitingPong { nonce, since: now };
                    }
                }
            }
        }
    }

    /// Called when a `ping` is received.
    pub fn received_ping(&mut self, addr: PeerId, nonce: u64) -> bool {
        if self.peers.contains_key(&addr) {
            self.upstream.pong(addr, nonce);

            return true;
        }
        false
    }

    /// Called when a `pong` is received.
    pub fn received_pong(&mut self, addr: PeerId, nonce: u64, now: LocalTime) -> bool {
        if let Some(peer) = self.peers.get_mut(&addr) {
            match peer.state {
                State::AwaitingPong {
                    nonce: last_nonce,
                    since,
                } => {
                    if nonce == last_nonce {
                        peer.record_latency(now - since);
                        peer.state = State::Idle { since: now };

                        return true;
                    }
                }
                // Unsolicited or redundant `pong`. Ignore.
                State::Idle { .. } => {}
            }
        }
        false
    }
}
