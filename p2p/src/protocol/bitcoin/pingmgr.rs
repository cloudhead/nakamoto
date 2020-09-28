#![allow(missing_docs)]
use std::collections::VecDeque;
use std::net;

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::collections::HashMap;

use crate::protocol::{PeerId, Timeout};

use super::connmgr;

/// Time interval to wait between sent pings.
pub const PING_INTERVAL: LocalDuration = LocalDuration::from_mins(2);
/// Time to wait to receive a pong when sending a ping.
pub const PING_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);

/// Maximum number of latencies recorded per peer.
const MAX_RECORDED_LATENCIES: usize = 64;

pub trait Ping {
    fn ping(&self, addr: net::SocketAddr, nonce: u64) -> &Self;
    fn pong(&self, addr: net::SocketAddr, nonce: u64) -> &Self;
    fn set_timeout(&self, addr: net::SocketAddr, timeout: Timeout) -> &Self;
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

#[derive(Debug)]
pub struct PingManager<U> {
    peers: HashMap<PeerId, Peer>,
    /// Random number generator.
    rng: fastrand::Rng,
    upstream: U,
}

impl<U: Ping + connmgr::Disconnect> PingManager<U> {
    pub fn new(rng: fastrand::Rng, upstream: U) -> Self {
        let peers = HashMap::with_hasher(rng.clone().into());

        Self {
            peers,
            rng,
            upstream,
        }
    }

    pub fn peer_negotiated(&mut self, address: PeerId, now: LocalTime) {
        let nonce = self.rng.u64(..);

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

    pub fn peer_disconnected(&mut self, addr: &PeerId) {
        self.peers.remove(addr);
    }

    pub fn received_timeout(&mut self, addr: PeerId, now: LocalTime) {
        if let Some(peer) = self.peers.get_mut(&addr) {
            match peer.state {
                State::AwaitingPong { since, .. } => {
                    // A ping was sent and we're waiting for a `pong`. If too much
                    // time has passed, we consider this peer dead, and disconnect
                    // from them.
                    if now - since >= PING_TIMEOUT {
                        // TODO: Include reason for disconnect (ping timeout).
                        self.upstream.disconnect(peer.address);
                    }
                }
                State::Idle { since } => {
                    // We aren't waiting for any `pong`. Check whether enough time has passed since we
                    // received the last `pong`, and if so, send a new `ping`.
                    if now - since >= PING_INTERVAL {
                        let nonce = self.rng.u64(..);

                        self.upstream
                            .ping(peer.address, nonce)
                            .set_timeout(addr, PING_TIMEOUT)
                            .set_timeout(addr, PING_INTERVAL);

                        peer.state = State::AwaitingPong { nonce, since: now };
                    }
                }
            }
        }
    }

    pub fn ping_received(&mut self, addr: PeerId, nonce: u64) {
        self.upstream.pong(addr, nonce);
    }

    pub fn pong_received(&mut self, addr: PeerId, nonce: u64, now: LocalTime) {
        if let Some(peer) = self.peers.get_mut(&addr) {
            match peer.state {
                State::AwaitingPong {
                    nonce: last_nonce,
                    since,
                } => {
                    if nonce == last_nonce {
                        peer.record_latency(now - since);
                        peer.state = State::Idle { since: now };
                    }
                }
                // Unsolicited or redundant `pong`. Ignore.
                State::Idle { .. } => {}
            }
        }
    }
}
