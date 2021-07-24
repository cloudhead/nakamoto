//! Inventory manager.
//! Takes care of sending and fetching inventories.
use bitcoin::network::{constants::ServiceFlags, message_blockdata::Inventory};
use bitcoin::{Transaction, Txid};

// TODO: Timeout should be configurable
// TODO: Add exponential back-off
// TODO: Do we need to handle re-orgs?

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::collections::{HashMap, HashSet};
use nakamoto_common::source;

use super::channel::{Disconnect, SetTimeout};
use super::{DisconnectReason, Mempool, PeerId};

/// Time between re-broadcasts of inventories.
pub const REBROADCAST_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);

/// Maximum number of attempts to send inventories to a peer.
pub const MAX_ATTEMPTS: usize = 3;

/// Time between idles.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);

/// The ability to send and receive inventory data.
pub trait Inventories {
    /// Sends an `inv` message to a peer.
    fn inv(&self, addr: PeerId, inventories: Vec<Inventory>);
    /// Sends a `getdata` message to a peer.
    fn getdata(&self, addr: PeerId, inventories: Vec<Inventory>);
    /// Sends a `tx` message to a peer.
    fn tx(&self, addr: PeerId, tx: Transaction);
}

/// Inventory manager peer.
#[derive(Debug)]
pub struct Peer {
    /// Is this peer a transaction relay?
    pub relay: bool,
    /// Peer announced services.
    pub services: ServiceFlags,

    /// Inventories we are attempting to send to this peer.
    outbox: HashSet<Txid>,
    /// Number of times we attempted to send inventories to this peer.
    attempts: usize,
    /// Last time we attempted to send inventories to this peer.
    last_attempt: Option<LocalTime>,
}

impl Peer {
    fn attempted(&mut self, time: LocalTime) {
        self.last_attempt = Some(time);
        self.attempts += 1;
    }

    fn reset(&mut self) {
        self.last_attempt = None;
        self.attempts = 0;
    }
}

/// Inventory manager state.
#[derive(Debug)]
pub struct InventoryManager<U> {
    /// Peer map.
    peers: HashMap<PeerId, Peer>,
    /// Timeout used for retrying broadcasts.
    timeout: LocalDuration,
    /// Inventories.
    inventories: HashMap<Txid, Inventory>,

    last_tick: Option<LocalTime>,
    rng: fastrand::Rng,
    upstream: U,
}

impl<U: Inventories + SetTimeout + Disconnect> InventoryManager<U> {
    /// Create a new inventory manager.
    pub fn new(rng: fastrand::Rng, upstream: U) -> Self {
        Self {
            peers: HashMap::with_hasher(rng.clone().into()),
            inventories: HashMap::with_hasher(rng.clone().into()),
            timeout: REBROADCAST_TIMEOUT,
            last_tick: None,
            rng,
            upstream,
        }
    }

    /// Check whether the inventory is empty.
    pub fn is_empty(&self) -> bool {
        self.inventories.is_empty()
    }

    /// Check if the inventory contains the given transaction.
    pub fn contains(&self, txid: &Txid) -> bool {
        self.inventories.contains_key(txid)
    }

    /// Called when a peer is negotiated.
    pub fn peer_negotiated(&mut self, addr: PeerId, services: ServiceFlags, relay: bool) {
        // Add existing inventories to this peer's outbox so that they are announced.
        let mut outbox = HashSet::with_hasher(self.rng.clone().into());
        for inv in self.inventories.keys() {
            outbox.insert(*inv);
        }
        self.schedule_tick();
        self.peers.insert(
            addr,
            Peer {
                services,
                attempts: 0,
                relay,
                outbox,
                last_attempt: None,
            },
        );
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.peers.remove(id);
    }

    /// Called when we receive a tick.
    pub fn received_tick(&mut self, time: LocalTime, mempool: &Mempool) {
        // Rate-limit how much we run this code.
        if time - self.last_tick.unwrap_or_default() >= IDLE_TIMEOUT {
            self.last_tick = Some(time);

            // Prune inventories. Anything that isn't in the mempool should no longer be tracked
            // by the inventory manager.
            let mut remove = Vec::new();

            for txid in self.inventories.keys() {
                if !mempool.contains(txid) {
                    remove.push(*txid);
                }
            }

            for txid in remove {
                self.inventories.remove(&txid);

                for peer in self.peers.values_mut() {
                    peer.outbox.remove(&txid);
                }
            }
        }

        let mut disconnect = Vec::new();
        for (addr, peer) in &mut self.peers {
            let elapsed = time - peer.last_attempt.unwrap_or_default();

            // Peer timeout.
            if elapsed >= self.timeout && !peer.outbox.is_empty() {
                // If we've already reached the maximum number of attempts, just disconnect
                // the peer and move on to the next.
                if peer.attempts >= MAX_ATTEMPTS {
                    disconnect.push(*addr);
                    continue;
                }

                // ... Another attempt ...

                peer.attempted(time);

                let mut invs = Vec::with_capacity(peer.outbox.len());
                for inv in &peer.outbox {
                    invs.push(self.inventories[inv]);
                }
                self.upstream.inv(*addr, invs);
                self.upstream.set_timeout(self.timeout);
            }
        }

        for addr in disconnect {
            self.peers.remove(&addr);
            self.upstream
                .disconnect(addr, DisconnectReason::PeerTimeout("inv"));
        }
    }

    /// Called when a `getdata` is received from a peer.
    pub fn received_getdata(&mut self, addr: PeerId, invs: &[Inventory], mempool: &Mempool) {
        for inv in invs {
            match inv {
                // NOTE: Normally, we would handle non-witness inventory requests differently
                // than witness inventories, but the `bitcoin` crate doesn't allow us to
                // omit the witness data, hence we treat them equally here.
                Inventory::Transaction(txid) | Inventory::WitnessTransaction(txid) => {
                    if let Some(tx) = mempool.txs.get(txid) {
                        debug_assert!(self.inventories.contains_key(txid));

                        self.upstream.tx(addr, tx.clone());
                    }
                    // Since we received a `getdata` from the peer, it means it received our
                    // inventory broadcast and we no longer need to send it.
                    if let Some(peer) = self.peers.get_mut(&addr) {
                        peer.outbox.remove(txid);

                        if peer.outbox.is_empty() {
                            // Reset retry state.
                            peer.reset();
                        }
                    }
                }
                Inventory::WTx(_wtxid) => {
                    // TODO: This should be filled in as part of BIP 339 support.
                }
                _ => {}
            }
        }
    }

    /// Announce inventories to all matching peers. Retries if necessary.
    pub fn announce(&mut self, inv: Vec<Inventory>) -> Vec<PeerId> {
        // All peers we are sending inventories to.
        let mut addrs = Vec::new();

        // Insert each inventory into the peer outboxes and keep a local copy for re-broadcasting
        // later.
        for i in inv.iter().cloned() {
            match i {
                Inventory::Transaction(txid) => {
                    self.inventories.insert(txid, i);

                    for (addr, peer) in self.peers.iter_mut().filter(|(_, p)| p.relay) {
                        peer.outbox.insert(txid);
                        addrs.push(*addr);
                    }
                }
                Inventory::WTx(_) => {
                    panic!("{}: BIP 339 is not yet supported", source!());
                }
                Inventory::WitnessTransaction(_) => {
                    panic!(
                        "{}: witness transaction inventories are only used in `getdata`",
                        source!()
                    );
                }
                other => {
                    panic!("{}: {:?} is not supported", source!(), other);
                }
            }
        }
        self.schedule_tick();

        addrs
    }

    /// Get data from one of the matching peers. Retries if necessary.
    pub fn get<P>(&mut self, inventory: Inventory, predicate: P) -> Option<PeerId>
    where
        P: Fn(&Peer) -> bool,
    {
        let peers = self
            .peers
            .iter()
            .filter_map(|(a, p)| if predicate(p) { Some(*a) } else { None })
            .collect::<Vec<_>>();

        match peers.len() {
            n if n > 0 => {
                let r = self.rng.usize(..n);
                let a = peers[r];

                self.upstream.getdata(a, vec![inventory]);

                Some(a)
            }
            _ => None,
        }
    }

    ////////////////////////////////////////////////////////////////////////////

    fn schedule_tick(&self) {
        self.upstream.set_timeout(LocalDuration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
    use crate::protocol::channel::{chan, Channel};
    use crate::protocol::{Network, Out, PROTOCOL_VERSION};

    use nakamoto_test::block::gen;

    #[test]
    fn test_rebroadcast_timeout() {
        let network = Network::Mainnet;
        let (sender, receiver) = chan::unbounded::<Out>();
        let upstream = Channel::new(network, PROTOCOL_VERSION, "test", sender);

        let mut rng = fastrand::Rng::with_seed(1);
        let mut mempool = Mempool::new();

        let time = LocalTime::now();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, upstream);

        mempool.insert(vec![(tx.txid(), tx.clone())]);

        invmgr.peer_negotiated(([88, 88, 88, 88], 8333).into(), ServiceFlags::NETWORK, true);
        invmgr.announce(vec![Inventory::Transaction(tx.txid())]);
        invmgr.received_tick(time, &mempool);

        assert_eq!(
            receiver
                .try_iter()
                .filter(|o| matches!(o, Out::Message(_, _)))
                .count(),
            1
        );

        invmgr.received_tick(time, &mempool);
        assert_eq!(receiver.try_iter().count(), 0, "Timeout hasn't lapsed");
    }

    #[test]
    fn test_max_attemps() {
        let network = Network::Mainnet;
        let (sender, receiver) = chan::unbounded::<Out>();
        let upstream = Channel::new(network, PROTOCOL_VERSION, "test", sender);

        let mut rng = fastrand::Rng::with_seed(1);
        let mut time = LocalTime::now();
        let mut mempool = Mempool::new();

        let remote = ([88, 88, 88, 88], 8333).into();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, upstream);

        mempool.insert(vec![(tx.txid(), tx.clone())]);

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true);
        invmgr.announce(vec![Inventory::Transaction(tx.txid())]);

        // We attempt to broadcast up to `MAX_ATTEMPTS` times.
        for _ in 0..MAX_ATTEMPTS {
            invmgr.received_tick(time, &mempool);
            receiver
                .try_iter()
                .find(|o| {
                    matches!(
                        o,
                        Out::Message(
                            _,
                            RawNetworkMessage {
                                payload: NetworkMessage::Inv(_),
                                ..
                            }
                        )
                    )
                })
                .expect("Inventory is announced");

            time.elapse(REBROADCAST_TIMEOUT);
        }

        // The next time we time out, we disconnect the peer.
        invmgr.received_tick(time, &mempool);
        receiver
            .try_iter()
            .find(|o| matches!(o, Out::Disconnect(addr, _) if addr == &remote))
            .expect("Peer is disconnected");

        assert!(invmgr.contains(&tx.txid()));
        assert!(invmgr.peers.is_empty());
    }
}
