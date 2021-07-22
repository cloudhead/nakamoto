//! Inventory manager.
//! Takes care of sending and fetching inventories.
use bitcoin::network::{constants::ServiceFlags, message_blockdata::Inventory};
use bitcoin::Transaction;

use nakamoto_common::collections::HashMap;

use super::channel::SetTimeout;
use super::{Mempool, PeerId};

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
}

/// Inventory manager state.
#[derive(Debug)]
pub struct InventoryManager<U> {
    peers: HashMap<PeerId, Peer>,
    rng: fastrand::Rng,
    upstream: U,
}

impl<U: Inventories + SetTimeout> InventoryManager<U> {
    /// Create a new inventory manager.
    pub fn new(rng: fastrand::Rng, upstream: U) -> Self {
        Self {
            peers: HashMap::with_hasher(rng.clone().into()),
            rng,
            upstream,
        }
    }

    /// Called when a peer is negotiated.
    pub fn peer_negotiated(&mut self, id: PeerId, services: ServiceFlags, relay: bool) {
        self.peers.insert(id, Peer { services, relay });
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.peers.remove(id);
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
                        self.upstream.tx(addr, tx.clone());
                    }
                }
                Inventory::WTx(_wtxid) => {
                    // TODO: This should be filled in as part of BIP 339 support.
                }
                _ => {}
            }
        }
    }

    /// Broadcast inventories to all matching peers. Retries if necessary.
    pub fn broadcast<P>(&mut self, inv: Vec<Inventory>, predicate: P) -> Vec<PeerId>
    where
        P: Fn(&Peer) -> bool,
    {
        let mut peers = Vec::new();

        for (addr, peer) in &self.peers {
            if predicate(peer) {
                peers.push(*addr);
                self.upstream.inv(*addr, inv.clone());
            }
        }
        peers
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
}
