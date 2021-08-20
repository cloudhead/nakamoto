//! Inventory manager.
//! Takes care of sending and fetching inventories.
//!
//! ## Handling of reverted blocks
//!
//! When a block is reverted, the inventory manager is notified, via the
//! [`InventoryManager::block_reverted`] function. Since confirmed transactions are held
//! for some time in memory, the transactions that were confirmed in the reverted block
//! can be matched and the user can be notified via a [`Event::Reverted`] event. These transactions
//! are then placed back into the local mempool, to ensure that they get re-broadcast and
//! eventually included in a new block.
//!
//! To ensure that any new and/or conflicting block that may contain the transaction is matched,
//! the filter manager is told to re-watch all reverted transactions. Thus, the inventory manager
//! can expect to receive the new block that contains the transaction that was reverted, via
//! the [`InventoryManager::received_block`] event.
//!
//! To keep only the smallest set of confirmed transactions in memory, we prune the set every time
//! the [`InventoryManager::received_tick`] function is called. Confirmed transactions are removed
//! after they are burried at a certain depth.
//!
use std::collections::BTreeMap;
use std::iter;

use bitcoin::network::{constants::ServiceFlags, message_blockdata::Inventory};
use bitcoin::{Block, BlockHash, Transaction, Txid};

// TODO: Timeout should be configurable
// TODO: Add exponential back-off

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::block::tree::BlockTree;
use nakamoto_common::collections::{AddressBook, HashMap, HashSet};

use super::channel::{Disconnect, SetTimeout};
use super::{DisconnectReason, Height, PeerId};

/// Time between re-broadcasts of inventories.
pub const REBROADCAST_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);

/// Time between request retries.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);

/// Maximum number of attempts to send inventories to a peer.
pub const MAX_ATTEMPTS: usize = 3;

/// Time between idles.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);

/// Block depth at which confirmed transactions are pruned and no longer reverted after a re-org.
pub const TRANSACTION_PRUNE_DEPTH: Height = 12;

/// The ability to send and receive inventory data.
pub trait Inventories {
    /// Sends an `inv` message to a peer.
    fn inv(&self, addr: PeerId, inventories: Vec<Inventory>);
    /// Sends a `getdata` message to a peer.
    fn getdata(&self, addr: PeerId, inventories: Vec<Inventory>);
    /// Sends a `tx` message to a peer.
    fn tx(&self, addr: PeerId, tx: Transaction);
    /// Fire an event.
    fn event(&self, event: Event);
}

/// An event emitted by the inventory manager.
#[derive(Debug, Clone)]
pub enum Event {
    /// Block received.
    BlockReceived {
        /// Sender.
        from: PeerId,
        /// Block height.
        height: Height,
    },
    /// Block received.
    BlockProcessed {
        /// Block.
        block: Block,
        /// Block height.
        height: Height,
    },
    /// A peer acknowledged one of our transaction inventories.
    Acknowledged {
        /// The acknowledged transaction ID.
        txid: Txid,
        /// The acknowledging peer.
        peer: PeerId,
    },
    /// A transaction was confirmed.
    Confirmed {
        /// The confirmed transaction.
        transaction: Transaction, // TODO: Just the txid?
        /// The height at which it was confirmed.
        height: Height,
        /// The block in which it was confirmed.
        block: BlockHash,
    },
    /// A transaction was reverted.
    Reverted {
        /// The reverted transaction.
        transaction: Transaction, // TODO: Just the txid?
    },
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::BlockReceived { from, height, .. } => {
                write!(fmt, "{}: Received block at height {}", from, height)
            }
            Event::BlockProcessed { height, .. } => {
                write!(fmt, "Processed block at height {}", height)
            }
            Event::Acknowledged { txid, peer } => {
                write!(
                    fmt,
                    "Transaction {} was acknowledged by peer {}",
                    txid, peer
                )
            }
            Event::Confirmed {
                transaction,
                height,
                block,
            } => write!(
                fmt,
                "Transaction {} was included in block {} at height {}",
                transaction.txid(),
                block,
                height
            ),
            Event::Reverted { transaction, .. } => {
                write!(fmt, "Transaction {} was reverted", transaction.txid(),)
            }
        }
    }
}

/// Transaction Mempool.
///
/// Keeps track of unconfirmed transactions.
///
/// The mempool is shared between the client and the protocol.  The client's responsibility is to
/// remove transactions that have been included in blocks, while the protocol adds transactions
/// that were submitted by the client.
///
/// The protocol also uses the mempool to respond to `getdata` messages received from peers.
#[derive(Debug)]
pub struct Mempool {
    txs: BTreeMap<Txid, Transaction>,
}

impl Mempool {
    /// Create a new, empty mempool.
    fn new() -> Self {
        Self {
            txs: BTreeMap::new(),
        }
    }

    /// Remove a transaction from the mempool. This should be used when the transaction is
    /// confirmed.
    fn remove(&mut self, txid: &Txid) -> Option<Transaction> {
        self.txs.remove(txid)
    }

    /// Check if the mempool contains a specific transaction.
    fn contains(&self, txid: &Txid) -> bool {
        self.txs.contains_key(txid)
    }

    /// Remove all confirmed transactions in the given block from the mempool.
    fn process_block(&mut self, block: &Block) -> Vec<Transaction> {
        let mut confirmed = Vec::new();

        for tx in &block.txdata {
            let txid = tx.txid();

            // Attempt to remove confirmed transaction from mempool.
            // TODO: If this block becomes stale, this tx should go back in the
            // mempool.
            if let Some(tx) = self.remove(&txid) {
                confirmed.push(tx);
            }
        }
        confirmed
    }

    /// Add an unconfirmed transaction to the mempool.
    fn insert(&mut self, txs: impl IntoIterator<Item = (Txid, Transaction)>) {
        for (k, v) in txs {
            self.txs.insert(k, v);
        }
    }
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

    /// Number of times a certain block was requested.
    #[allow(dead_code)]
    requests: HashMap<BlockHash, usize>,
}

impl Peer {
    fn attempted(&mut self, time: LocalTime) {
        self.last_attempt = Some(time);
        self.attempts += 1;
    }

    #[allow(dead_code)]
    fn requested(&mut self, hash: BlockHash) {
        *self.requests.entry(hash).or_default() += 1;
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
    peers: AddressBook<PeerId, Peer>,
    /// Timeout used for retrying broadcasts.
    timeout: LocalDuration,
    /// Transaction mempool. Stores unconfirmed transactions sent to the network.
    mempool: Mempool,
    /// Confirmed transactions by block height.
    /// Pruned after a certain depth.
    confirmed: HashMap<Height, Vec<Transaction>>,

    /// Inventories requested and the time at which they were last requested.
    /// Only blocks are requested currently.
    remaining: HashMap<BlockHash, Option<LocalTime>>,
    /// Blocks received, waiting to be processed.
    received: HashMap<Height, Block>,

    last_tick: Option<LocalTime>,
    rng: fastrand::Rng,
    upstream: U,
}

impl<U: Inventories + SetTimeout + Disconnect> InventoryManager<U> {
    /// Create a new inventory manager.
    pub fn new(rng: fastrand::Rng, upstream: U) -> Self {
        Self {
            peers: AddressBook::new(rng.clone()),
            mempool: Mempool::new(),
            confirmed: HashMap::with_hasher(rng.clone().into()),
            remaining: HashMap::with_hasher(rng.clone().into()),
            received: HashMap::with_hasher(rng.clone().into()),
            timeout: REBROADCAST_TIMEOUT,
            last_tick: None,
            rng,
            upstream,
        }
    }

    /// Check whether the inventory is empty.
    pub fn is_empty(&self) -> bool {
        self.mempool.txs.is_empty()
    }

    /// Check if the inventory contains the given transaction.
    pub fn contains(&self, txid: &Txid) -> bool {
        self.mempool.contains(txid)
    }

    /// Called when a peer is negotiated.
    pub fn peer_negotiated(&mut self, addr: PeerId, services: ServiceFlags, relay: bool) {
        // Add existing inventories to this peer's outbox so that they are announced.
        let mut outbox = HashSet::with_hasher(self.rng.clone().into());
        for txid in self.mempool.txs.keys() {
            outbox.insert(*txid);
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
                requests: HashMap::with_hasher(self.rng.clone().into()),
            },
        );
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.peers.remove(id);
    }

    /// Called when a block is reverted.
    pub fn block_reverted(&mut self, height: Height) -> Vec<Transaction> {
        if let Some(transactions) = self.confirmed.remove(&height) {
            self.announce(transactions.clone());

            for transaction in transactions.iter().cloned() {
                self.upstream.event(Event::Reverted { transaction });
            }
            transactions
        } else {
            Vec::new()
        }
    }

    /// Called when we receive a tick.
    pub fn received_tick<T: BlockTree>(&mut self, now: LocalTime, tree: &T) {
        // Rate-limit how much we run this function.
        if now - self.last_tick.unwrap_or_default() >= IDLE_TIMEOUT {
            self.last_tick = Some(now);
        } else {
            return;
        }

        {
            // Prune confirmed transactions burried passed a certain depth.
            let height = tree.height();
            self.confirmed
                .retain(|h, _| height - h <= TRANSACTION_PRUNE_DEPTH);
        }

        // Handle retries annd disconnects.
        let mut requests = Vec::new();
        let mut disconnect = Vec::new();

        for (addr, peer) in &mut *self.peers {
            // TODO: Disconnect peers from which we requested blocks many times, and who haven't
            // responded, or at least don't retry the same peer too many times.

            // Schedule inventory requests.
            let queue = self
                .remaining
                .iter_mut()
                .filter(|(_, t)| now - t.unwrap_or_default() >= REQUEST_TIMEOUT);

            for (block_hash, last_request) in queue {
                *last_request = Some(now);
                requests.push(*block_hash);
            }

            // Peer inventory announce timeout.
            if !peer.outbox.is_empty() {
                let elapsed = now - peer.last_attempt.unwrap_or_default();
                if elapsed < self.timeout {
                    continue;
                }

                // If we've already reached the maximum number of attempts, just disconnect
                // the peer and move on to the next.
                if peer.attempts >= MAX_ATTEMPTS {
                    disconnect.push(*addr);
                    continue;
                }

                // ... Another attempt ...

                peer.attempted(now);

                let mut invs = Vec::with_capacity(peer.outbox.len());
                for inv in &peer.outbox {
                    // TODO: Should we send a WitnessTransaction?
                    invs.push(Inventory::Transaction(self.mempool.txs[inv].txid()));
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
        for block_hash in requests {
            if let Some(_peer) = self.request(block_hash) {
                // TODO: Fire event.
            }
        }
    }

    /// Called when a `getdata` is received from a peer.
    pub fn received_getdata(&mut self, addr: PeerId, invs: &[Inventory]) {
        for inv in invs {
            match inv {
                // NOTE: Normally, we would handle non-witness inventory requests differently
                // than witness inventories, but the `bitcoin` crate doesn't allow us to
                // omit the witness data, hence we treat them equally here.
                Inventory::Transaction(txid) | Inventory::WitnessTransaction(txid) => {
                    if let Some(tx) = self.mempool.txs.get(txid) {
                        debug_assert!(self.mempool.contains(txid));

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
                        self.upstream.event(Event::Acknowledged {
                            peer: addr,
                            txid: *txid,
                        });
                    }
                }
                Inventory::WTx(_wtxid) => {
                    // TODO: This should be filled in as part of BIP 339 support.
                }
                _ => {}
            }
        }
    }

    /// Called when a block is received from a peer.
    /// Returns the list of confirmed [`Txid`].
    ///
    /// Note that the confirmed transactions don't necessarily pertain to this block.
    pub fn received_block<T: BlockTree>(
        &mut self,
        from: &PeerId,
        block: Block,
        tree: &T,
    ) -> Vec<Txid> {
        let hash = block.block_hash();
        let from = *from;

        if self.remaining.remove(&hash).is_none() {
            // Nb. The remote isn't necessarily sending an unsolicited block here.
            // We often have to ask multiple peers to get a response, so we may
            // have already received this block once.
            return vec![];
        }

        // We're done requesting this block.
        for peer in self.peers.values_mut() {
            peer.requests.remove(&hash);
        }

        // Find the block height, otherwise we've somehow requested a block which
        // isn't part of the active chain. This could happen in the case of a re-org
        // and a delayed block arrival.
        let height = if let Some((height, _)) = tree.get_block(&hash) {
            height
        } else {
            return vec![];
        };

        // Add to processing queue. Blocks are processed in-order only.
        self.received.insert(height, block);
        self.upstream.event(Event::BlockReceived { from, height });

        // If there are still blocks remaining to download, don't process any of the
        // received queue yet.
        if !self.remaining.is_empty() {
            return vec![];
        }

        // Now that all blocks to be processed are downloaded, we can start
        // processing them in order.
        let mut confirmed = Vec::new();

        while let Some((height, block)) = self
            .received
            .keys()
            .min()
            .cloned()
            .and_then(|h| self.received.remove(&h).map(|b| (h, b)))
        {
            let txs = self.mempool.process_block(&block);
            let hash = block.block_hash();

            for transaction in txs {
                let txid = transaction.txid();

                // Transactions that have been confirmed no longer need to be announced.
                for peer in self.peers.values_mut() {
                    peer.outbox.remove(&txid);
                }
                confirmed.push(txid);

                self.confirmed
                    .entry(height)
                    .or_default()
                    .push(transaction.clone());

                self.upstream.event(Event::Confirmed {
                    transaction,
                    block: hash,
                    height,
                });
            }
            self.upstream.event(Event::BlockProcessed { block, height });
        }
        confirmed
    }

    /// Announce inventories to all matching peers. Retries if necessary.
    pub fn announce(&mut self, txs: Vec<Transaction>) -> Vec<PeerId> {
        // All peers we are sending inventories to.
        let mut addrs = Vec::new();

        // Insert each inventory into the peer outboxes and keep a local copy for re-broadcasting
        // later.
        for tx in txs {
            let txid = tx.txid();
            self.mempool.insert(iter::once((txid, tx)));

            for (addr, peer) in self.peers.iter_mut().filter(|(_, p)| p.relay) {
                peer.outbox.insert(txid);
                addrs.push(*addr);
            }
        }
        self.schedule_tick();

        addrs
    }

    /// Attempt to get a block from the network. Retries if necessary.
    pub fn get_block(&mut self, hash: BlockHash) {
        self.remaining.entry(hash).or_insert(None);
        self.schedule_tick();
    }

    ////////////////////////////////////////////////////////////////////////////

    fn schedule_tick(&mut self) {
        self.last_tick = None; // Disable rate-limiting for the next tick.
        self.upstream.set_timeout(LocalDuration::from_secs(1));
    }

    /// Request a block from a random peer.
    fn request(&self, block: BlockHash) -> Option<PeerId> {
        self.peers
            .sample_with(|_, p| p.services.has(ServiceFlags::NETWORK))
            .map(|(addr, _)| {
                log::debug!("Requesting block {} from {}", block, addr);

                self.upstream.getdata(*addr, vec![Inventory::Block(block)]);
                *addr
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
    use crate::protocol;
    use crate::protocol::channel::{chan, Channel};
    use crate::protocol::{Network, Out, PROTOCOL_VERSION};

    use nakamoto_common::nonempty::NonEmpty;
    use nakamoto_test::block::cache::model;
    use nakamoto_test::block::gen;
    use nakamoto_test::logger;

    fn messages(
        receiver: &chan::Receiver<Out>,
    ) -> impl Iterator<Item = (PeerId, NetworkMessage)> + '_ {
        receiver.try_iter().filter_map(|o| match o {
            Out::Message(a, m) => Some((a, m.payload)),
            _ => None,
        })
    }

    fn events(receiver: &chan::Receiver<Out>) -> impl Iterator<Item = Event> + '_ {
        receiver.try_iter().filter_map(|o| match o {
            Out::Event(protocol::Event::InventoryManager(e)) => Some(e),
            _ => None,
        })
    }

    #[test]
    fn test_get_block() {
        logger::init(log::Level::Debug);

        let network = Network::Regtest;
        let (sender, receiver) = chan::unbounded::<Out>();
        let upstream = Channel::new(network, PROTOCOL_VERSION, "test", sender);

        let mut rng = fastrand::Rng::new();
        let mut time = LocalTime::now();

        let genesis = network.genesis_block();
        let chain = gen::blockchain(genesis, 16, &mut rng);
        let headers = NonEmpty::from_vec(chain.iter().map(|b| b.header).collect()).unwrap();
        let tree = model::Cache::from(headers);
        let header = tree.get_block_by_height(6).unwrap();
        let hash = header.block_hash();
        let inv = vec![Inventory::Block(hash)];
        let block = chain.iter().find(|b| b.block_hash() == hash).unwrap();

        let mut invmgr = InventoryManager::new(rng.clone(), upstream);

        invmgr.peer_negotiated(([66, 66, 66, 66], 8333).into(), ServiceFlags::NETWORK, true);
        invmgr.peer_negotiated(([77, 77, 77, 77], 8333).into(), ServiceFlags::NETWORK, true);
        invmgr.peer_negotiated(([88, 88, 88, 88], 8333).into(), ServiceFlags::NETWORK, true);
        invmgr.peer_negotiated(([99, 99, 99, 99], 8333).into(), ServiceFlags::NETWORK, true);

        invmgr.get_block(hash);

        let mut requested = HashSet::with_hasher(rng.clone().into());
        let mut last_request = LocalTime::default();

        loop {
            time.elapse(LocalDuration::from_secs(rng.u64(10..30)));
            invmgr.received_tick(time, &tree);
            assert!(!invmgr.remaining.is_empty());

            if let Some((addr, _)) = messages(&receiver)
                .find(|(_, m)| matches!(m, NetworkMessage::GetData(i) if i == &inv))
            {
                assert!(
                    time - last_request >= REQUEST_TIMEOUT,
                    "Requests are never made within the request timeout"
                );
                last_request = time;

                requested.insert(addr);
                if requested.len() < invmgr.peers.len() {
                    // We're not done until we've requested all peers.
                    continue;
                }
                invmgr.received_block(&addr, block.clone(), &tree);

                assert!(invmgr.remaining.is_empty(), "No more blocks to remaining");
                events(&receiver)
                    .find(|e| matches!(e, Event::BlockReceived { .. }))
                    .expect("An event is emitted when a block is received");

                break;
            }
        }
        invmgr.received_tick(time + REQUEST_TIMEOUT, &tree);
        assert_eq!(messages(&receiver).count(), 0, "No more requests are sent");
    }

    #[test]
    fn test_rebroadcast_timeout() {
        let network = Network::Mainnet;
        let (sender, receiver) = chan::unbounded::<Out>();
        let upstream = Channel::new(network, PROTOCOL_VERSION, "test", sender);
        let tree = model::Cache::from(NonEmpty::new(network.genesis()));
        let remote = ([88, 88, 88, 88], 8333).into();
        let mut rng = fastrand::Rng::with_seed(1);

        let time = LocalTime::now();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, upstream);

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true);
        invmgr.announce(vec![tx]);
        invmgr.received_tick(time, &tree);

        assert_eq!(
            messages(&receiver)
                .filter(|(a, m)| matches!(m, NetworkMessage::Inv(_)) && a == &remote)
                .count(),
            1
        );

        invmgr.received_tick(time, &tree);
        assert_eq!(receiver.try_iter().count(), 0, "Timeout hasn't lapsed");

        invmgr.received_tick(time + REBROADCAST_TIMEOUT, &tree);
        assert_eq!(
            messages(&receiver)
                .filter(|(a, m)| matches!(m, NetworkMessage::Inv(_)) && a == &remote)
                .count(),
            1
        );
    }

    #[test]
    fn test_max_attemps() {
        let network = Network::Mainnet;
        let (sender, receiver) = chan::unbounded::<Out>();
        let upstream = Channel::new(network, PROTOCOL_VERSION, "test", sender);
        let tree = model::Cache::from(NonEmpty::new(network.genesis()));

        let mut rng = fastrand::Rng::with_seed(1);
        let mut time = LocalTime::now();

        let remote = ([88, 88, 88, 88], 8333).into();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, upstream);

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true);
        invmgr.announce(vec![tx.clone()]);

        // We attempt to broadcast up to `MAX_ATTEMPTS` times.
        for _ in 0..MAX_ATTEMPTS {
            invmgr.received_tick(time, &tree);
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
        invmgr.received_tick(time, &tree);
        receiver
            .try_iter()
            .find(|o| matches!(o, Out::Disconnect(addr, _) if addr == &remote))
            .expect("Peer is disconnected");

        assert!(invmgr.contains(&tx.txid()));
        assert!(invmgr.peers.is_empty());
    }

    #[test]
    fn test_block_reverted() {
        let network = Network::Regtest;
        let remote = ([88, 88, 88, 88], 8333).into();
        let (sender, receiver) = chan::unbounded::<Out>();
        let mut rng = fastrand::Rng::new();

        let mut main = gen::blockchain(network.genesis_block(), 16, &mut rng);
        let tip = main.last().header;
        let tx = gen::transaction(&mut rng);
        let main_block1 = gen::block_with(&tip, vec![tx.clone()], &mut rng);

        main.push(main_block1.clone());

        let height = main.len() as Height - 1;
        let headers = NonEmpty::from_vec(main.iter().map(|b| b.header).collect()).unwrap();

        let fork_block1 = gen::block_with(&tip, vec![tx.clone()], &mut rng);
        let fork_block2 = gen::block(&fork_block1.header, &mut rng);

        let upstream = Channel::new(network, PROTOCOL_VERSION, "test", sender);
        let time = LocalTime::now();

        let mut tree = model::Cache::from(headers);
        let mut invmgr = InventoryManager::new(rng, upstream);

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true);
        invmgr.announce(vec![tx.clone()]);
        invmgr.get_block(main_block1.block_hash());
        invmgr.received_block(&remote, main_block1, &tree);

        assert!(!invmgr.contains(&tx.txid()));

        let mut events = events(&receiver);

        events
            .find(|e| {
                matches! {
                    e, Event::Confirmed { transaction, .. }
                    if transaction.txid() == tx.txid()
                }
            })
            .unwrap();

        tree.import_blocks(
            vec![fork_block1.header, fork_block2.header].into_iter(),
            &time,
        )
        .unwrap();

        invmgr.block_reverted(height);
        assert!(invmgr.contains(&tx.txid()));

        events
            .find(|e| {
                matches! {
                    e, Event::Reverted { transaction }
                    if transaction.txid() == tx.txid()
                }
            })
            .unwrap();

        invmgr.get_block(fork_block1.block_hash());
        invmgr.received_block(&remote, fork_block1.clone(), &tree);

        events
            .find(|e| {
                matches! {
                    e, Event::Confirmed { transaction, block: b, .. }
                    if transaction.txid() == tx.txid() && b == &fork_block1.block_hash()
                }
            })
            .unwrap();
    }
}
