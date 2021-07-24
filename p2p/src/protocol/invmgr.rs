//! Inventory manager.
//! Takes care of sending and fetching inventories.
use bitcoin::network::{constants::ServiceFlags, message_blockdata::Inventory};
use bitcoin::{Transaction, Txid};

// TODO: Timeout should be configurable
// TODO: Add max retries
// TODO: Add exponential back-off

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::collections::{HashMap, HashSet};
use nakamoto_common::source;

use super::channel::SetTimeout;
use super::{Mempool, PeerId};

/// Time between re-broadcasts of inventories.
pub const REBROADCAST_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);

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
    /// Last time we attempted to send inventories to this peer.
    last_attempt: Option<LocalTime>,
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

impl<U: Inventories + SetTimeout> InventoryManager<U> {
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

        for (addr, peer) in &mut self.peers {
            let elapsed = time - peer.last_attempt.unwrap_or_default();

            if elapsed >= self.timeout && !peer.outbox.is_empty() {
                // TODO: Test this - we shouldn't be sending too many inventories even if
                // we tick often.
                peer.last_attempt = Some(time);

                let mut invs = Vec::with_capacity(peer.outbox.len());
                for inv in &peer.outbox {
                    invs.push(self.inventories[inv]);
                }
                self.upstream.inv(*addr, invs);
                self.upstream.set_timeout(self.timeout);
            }
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
                            peer.last_attempt = None;
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
