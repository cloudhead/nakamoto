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
//! the [`InventoryManager::received_wake`] function is called. Confirmed transactions are removed
//! after they are burried at a certain depth.
//!
use std::collections::BTreeMap;

use nakamoto_common::bitcoin::network::{constants::ServiceFlags, message_blockdata::Inventory};
use nakamoto_common::bitcoin::{Block, BlockHash, Transaction, Txid, Wtxid};

// TODO: Timeout should be configurable
// TODO: Add exponential back-off

use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::BlockReader;
use nakamoto_common::collections::{AddressBook, HashMap};

use super::fees::{FeeEstimate, FeeEstimator};
use super::output::{SetTimer, Wire};
use super::{Height, PeerId, Socket};

/// Time between re-broadcasts of inventories.
pub const REBROADCAST_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);

/// Time between request retries.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(15);

/// Maximum number of attempts to send inventories to a peer.
pub const MAX_ATTEMPTS: usize = 3;

/// Time between idles.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);

/// Block depth at which confirmed transactions are pruned and no longer reverted after a re-org.
pub const TRANSACTION_PRUNE_DEPTH: Height = 12;

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
    /// Block processed.
    BlockProcessed {
        /// Block.
        block: Block, // TODO: Just the block hash?
        /// Block height.
        height: Height,
        /// Block tx fee estimate.
        fees: Option<FeeEstimate>,
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
    /// A request timed out.
    TimedOut {
        /// Peer who timed out.
        peer: PeerId,
    },
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::BlockReceived { from, height, .. } => {
                write!(fmt, "{}: Received block #{}", from, height)
            }
            Event::BlockProcessed { height, .. } => {
                write!(fmt, "Processed block #{}", height)
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
                "Transaction {} was included in block #{} ({})",
                transaction.txid(),
                height,
                block,
            ),
            Event::Reverted { transaction, .. } => {
                write!(fmt, "Transaction {} was reverted", transaction.txid(),)
            }
            Event::TimedOut { peer } => write!(fmt, "Peer {} timed out", peer),
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
    /// Does this peer use BIP-339?
    pub wtxidrelay: bool,

    /// Inventories we are attempting to send to this peer.
    outbox: HashMap<Wtxid, Txid>,
    /// Number of times we attempted to send inventories to this peer.
    attempts: usize,
    /// Last time we attempted to send inventories to this peer.
    last_attempt: Option<LocalTime>,

    /// Number of times a certain block was requested.
    #[allow(dead_code)]
    requests: HashMap<BlockHash, usize>,

    /// Peer socket.
    _socket: Socket,
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
pub struct InventoryManager<U, C> {
    /// Peer map.
    peers: AddressBook<PeerId, Peer>,
    /// Timeout used for retrying broadcasts.
    timeout: LocalDuration,
    /// Confirmed transactions by block height.
    /// Pruned after a certain depth.
    confirmed: HashMap<Height, Vec<Transaction>>,

    /// Transaction fee estimator.
    estimator: FeeEstimator,

    /// Transaction mempool. Stores unconfirmed transactions sent to the network.
    pub mempool: BTreeMap<Wtxid, Transaction>,
    /// Blocks requested and the time at which they were last requested.
    pub remaining: HashMap<BlockHash, Option<LocalTime>>,
    /// Blocks received, waiting to be processed.
    pub received: HashMap<Height, Block>,

    last_tick: Option<LocalTime>,
    rng: fastrand::Rng,
    upstream: U,
    clock: C,
}

impl<U: Wire<Event> + SetTimer, C: Clock> InventoryManager<U, C> {
    /// Create a new inventory manager.
    pub fn new(rng: fastrand::Rng, upstream: U, clock: C) -> Self {
        Self {
            peers: AddressBook::new(rng.clone()),
            mempool: BTreeMap::new(),
            estimator: FeeEstimator::default(),
            confirmed: HashMap::with_hasher(rng.clone().into()),
            remaining: HashMap::with_hasher(rng.clone().into()),
            received: HashMap::with_hasher(rng.clone().into()),
            timeout: REBROADCAST_TIMEOUT,
            last_tick: None,
            rng,
            upstream,
            clock,
        }
    }

    #[cfg(test)]
    /// Check whether the inventory is empty.
    pub fn is_empty(&self) -> bool {
        self.mempool.is_empty()
    }

    #[cfg(test)]
    /// Check if the inventory contains the given transaction.
    pub fn contains(&self, wtxid: &Wtxid) -> bool {
        self.mempool.contains_key(wtxid)
    }

    /// Called when a peer is negotiated.
    pub fn peer_negotiated(
        &mut self,
        socket: Socket,
        services: ServiceFlags,
        relay: bool,
        wtxidrelay: bool,
    ) {
        // Add existing inventories to this peer's outbox so that they are announced.
        let mut outbox = HashMap::with_hasher(self.rng.clone().into());
        for (wtxid, tx) in self.mempool.iter() {
            outbox.insert(*wtxid, tx.txid());
        }
        self.schedule_tick();
        self.peers.insert(
            socket.addr,
            Peer {
                services,
                attempts: 0,
                relay,
                wtxidrelay,
                outbox,
                last_attempt: None,
                requests: HashMap::with_hasher(self.rng.clone().into()),
                _socket: socket,
            },
        );
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.peers.remove(id);
    }

    /// Called when a block is reverted.
    pub fn block_reverted(&mut self, height: Height) -> Vec<Transaction> {
        self.estimator.rollback(height - 1);

        if let Some(transactions) = self.confirmed.remove(&height) {
            for tx in transactions.iter().cloned() {
                self.announce(tx);
            }
            for transaction in transactions.iter().cloned() {
                self.upstream.event(Event::Reverted { transaction });
            }
            transactions
        } else {
            Vec::new()
        }
    }

    /// Lookup a submitted transaction in the local mempool.
    pub fn get_submitted_tx(&mut self, txid: &Txid) -> Option<Transaction> {
        self.mempool.values().find(|tx| tx.txid() == *txid).cloned()
    }

    /// Called when we receive a tick.
    pub fn received_wake<T: BlockReader>(&mut self, tree: &T) {
        let now = self.clock.local_time();
        if now - self.last_tick.unwrap_or_default() >= IDLE_TIMEOUT {
            self.last_tick = Some(now);
            self.upstream.set_timer(IDLE_TIMEOUT);
        }

        {
            // Prune confirmed transactions burried passed a certain depth.
            let height = tree.height();
            self.confirmed
                .retain(|h, _| height - h <= TRANSACTION_PRUNE_DEPTH);
        }

        // Handle retries annd disconnects.
        let mut disconnect = Vec::new();

        for (addr, peer) in &mut *self.peers {
            // TODO: Disconnect peers from which we requested blocks many times, and who haven't
            // responded, or at least don't retry the same peer too many times.

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
                if peer.wtxidrelay {
                    for wtxid in peer.outbox.keys() {
                        invs.push(Inventory::WTx(self.mempool[wtxid].wtxid()));
                    }
                } else {
                    // TODO: Should we send a WitnessTransaction?
                    for wtxid in peer.outbox.keys() {
                        invs.push(Inventory::Transaction(self.mempool[wtxid].txid()));
                    }
                }
                self.upstream.inv(*addr, invs);
                self.upstream.set_timer(self.timeout);
            }
        }

        for addr in disconnect {
            self.peers.remove(&addr);
            self.upstream.event(Event::TimedOut { peer: addr });
        }

        // Handle block request queue.
        let queue = self
            .remaining
            .iter_mut()
            .filter(|(_, t)| now - t.unwrap_or_default() >= REQUEST_TIMEOUT);

        for (block_hash, last_request) in queue {
            if let Some((addr, _)) = self
                .peers
                .sample_with(|_, p| p.services.has(ServiceFlags::NETWORK))
            {
                log::debug!("Requesting block {} from {}", block_hash, addr);

                self.upstream
                    .get_data(*addr, vec![Inventory::Block(*block_hash)]);
                self.upstream.set_timer(REQUEST_TIMEOUT);

                *last_request = Some(now);
            } else {
                log::debug!(
                    "No peers with required services to request block {} from",
                    block_hash
                );
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
                    if let Some(tx) = self.mempool.values().find(|tx| tx.txid() == *txid) {
                        let wtxid = tx.wtxid();
                        debug_assert!(self.mempool.contains_key(&wtxid));
                        self.upstream.tx(addr, tx.clone());

                        // Since we received a `getdata` from the peer, it means it received our
                        // inventory broadcast and we no longer need to send it.
                        if let Some(peer) = self.peers.get_mut(&addr) {
                            if peer.outbox.remove(&wtxid).is_some() {
                                // Reset retry state.
                                peer.reset();

                                if peer.outbox.is_empty() {
                                    log::debug!("Peer {} transaction outbox is empty", &addr);
                                }
                                self.upstream.event(Event::Acknowledged {
                                    peer: addr,
                                    txid: *txid,
                                });
                            }
                        }
                    }
                }
                Inventory::WTx(wtxid) => {
                    if let Some(tx) = self.mempool.get(wtxid) {
                        self.upstream.tx(addr, tx.clone());
                    }

                    // Since we received a `getdata` from the peer, it means it received our
                    // inventory broadcast and we no longer need to send it.
                    if let Some(peer) = self.peers.get_mut(&addr) {
                        if let Some(txid) = peer.outbox.remove(wtxid) {
                            // Reset retry state.
                            peer.reset();

                            if peer.outbox.is_empty() {
                                log::debug!("Peer {} transaction outbox is empty", &addr);
                            }
                            self.upstream
                                .event(Event::Acknowledged { peer: addr, txid });
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Called when a block is received from a peer.
    /// Returns the list of confirmed [`Txid`].
    ///
    /// Note that the confirmed transactions don't necessarily pertain to this block.
    pub fn received_block<T: BlockReader>(
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
            let hash = block.block_hash();

            for tx in &block.txdata {
                let wtxid = tx.wtxid();

                // Attempt to remove confirmed transaction from mempool.
                if let Some(transaction) = self.mempool.remove(&wtxid) {
                    confirmed.push(tx.txid());

                    // Transactions that have been confirmed no longer need to be announced.
                    for peer in self.peers.values_mut() {
                        peer.outbox.remove(&wtxid);
                    }

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
            }
            // Process block through fee estimator.
            let fees = self.estimator.process(block.clone(), height);

            self.upstream.event(Event::BlockProcessed {
                block,
                height,
                fees,
            });
        }
        confirmed
    }

    /// Announce inventories to all matching peers. Retries if necessary.
    pub fn announce(&mut self, tx: Transaction) -> Vec<PeerId> {
        // All peers we are sending inventories to.
        let mut addrs = Vec::new();

        let txid = tx.txid();
        let wtxid = tx.wtxid();

        // Insert transaction into the peer outboxes and keep a local copy for re-broadcasting later.
        self.mempool.insert(wtxid, tx);

        for (addr, peer) in self.peers.iter_mut().filter(|(_, p)| p.relay) {
            peer.outbox.insert(wtxid, txid);
            addrs.push(*addr);
        }
        self.schedule_tick();

        addrs
    }

    /// Attempt to get a block from the network. Retries if necessary.
    pub fn get_block(&mut self, hash: BlockHash) {
        log::debug!("Queueing block {hash} to be requested");

        self.remaining.entry(hash).or_insert(None);
        self.schedule_tick();
    }

    ////////////////////////////////////////////////////////////////////////////

    fn schedule_tick(&mut self) {
        self.last_tick = None; // Disable rate-limiting for the next tick.
        self.upstream.set_timer(LocalDuration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net;

    use crate::fsm;
    use crate::fsm::network::Network;
    use crate::fsm::output::{self, Outbox};
    use crate::fsm::{Io, PROTOCOL_VERSION};

    use nakamoto_common::bitcoin::network::message::NetworkMessage;
    use nakamoto_common::block::time::RefClock;
    use nakamoto_common::block::tree::BlockTree as _;
    use nakamoto_common::collections::HashSet;
    use nakamoto_common::nonempty::NonEmpty;
    use nakamoto_test::block::cache::model;
    use nakamoto_test::block::gen;
    use nakamoto_test::{assert_matches, logger};

    fn events(outputs: impl Iterator<Item = Io>) -> impl Iterator<Item = Event> {
        outputs.filter_map(|o| match o {
            Io::Event(fsm::Event::Inventory(e)) => Some(e),
            _ => None,
        })
    }

    #[test]
    fn test_get_block() {
        logger::init(log::Level::Debug);

        let network = Network::Regtest;

        let mut upstream = Outbox::new(network, PROTOCOL_VERSION);
        let mut rng = fastrand::Rng::new();
        let clock = RefClock::from(LocalTime::now());

        let genesis = network.genesis_block();
        let chain = gen::blockchain(genesis, 16, &mut rng);
        let headers = NonEmpty::from_vec(chain.iter().map(|b| b.header).collect()).unwrap();
        let tree = model::Cache::from(headers);
        let header = tree.get_block_by_height(6).unwrap();
        let hash = header.block_hash();
        let inv = vec![Inventory::Block(hash)];
        let block = chain.iter().find(|b| b.block_hash() == hash).unwrap();

        let mut invmgr = InventoryManager::new(rng.clone(), upstream.clone(), clock.clone());

        invmgr.peer_negotiated(
            Socket::new(([66, 66, 66, 66], 8333)),
            ServiceFlags::NETWORK,
            true,
            true,
        );
        invmgr.peer_negotiated(
            Socket::new(([77, 77, 77, 77], 8333)),
            ServiceFlags::NETWORK,
            true,
            true,
        );
        invmgr.peer_negotiated(
            Socket::new(([88, 88, 88, 88], 8333)),
            ServiceFlags::NETWORK,
            true,
            true,
        );
        invmgr.peer_negotiated(
            Socket::new(([99, 99, 99, 99], 8333)),
            ServiceFlags::NETWORK,
            true,
            true,
        );

        invmgr.get_block(hash);

        let mut requested = HashSet::with_hasher(rng.clone().into());
        let mut last_request = LocalTime::default();

        loop {
            clock.elapse(LocalDuration::from_secs(rng.u64(10..30)));
            invmgr.received_wake(&tree);
            assert!(!invmgr.remaining.is_empty());

            if let Some((addr, _)) = output::test::messages(&mut upstream)
                .find(|(_, m)| matches!(m, NetworkMessage::GetData(i) if i == &inv))
            {
                assert!(
                    clock.local_time() - last_request >= REQUEST_TIMEOUT,
                    "Requests are never made within the request timeout"
                );
                last_request = clock.local_time();

                requested.insert(addr);
                if requested.len() < invmgr.peers.len() {
                    // We're not done until we've requested all peers.
                    continue;
                }
                invmgr.received_block(&addr, block.clone(), &tree);

                assert!(invmgr.remaining.is_empty(), "No more blocks to remaining");
                events(upstream.drain())
                    .find(|e| matches!(e, Event::BlockReceived { .. }))
                    .expect("An event is emitted when a block is received");

                break;
            }
        }
        clock.elapse(REQUEST_TIMEOUT);
        invmgr.received_wake(&tree);
        assert_eq!(
            upstream
                .drain()
                .filter(|o| matches!(o, Io::Write(_, _)))
                .count(),
            0,
            "No more requests are sent"
        );
    }

    #[test]
    fn test_rebroadcast_timeout() {
        let network = Network::Mainnet;
        let mut upstream = Outbox::new(network, PROTOCOL_VERSION);
        let tree = model::Cache::from(NonEmpty::new(network.genesis()));
        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let mut rng = fastrand::Rng::with_seed(1);

        let clock = RefClock::from(LocalTime::now());
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, upstream.clone(), clock.clone());

        invmgr.peer_negotiated(remote.into(), ServiceFlags::NETWORK, true, false);
        invmgr.announce(tx);
        invmgr.received_wake(&tree);

        assert_eq!(
            output::test::messages_from(&mut upstream, &remote)
                .filter(|m| matches!(m, NetworkMessage::Inv(_)))
                .count(),
            1
        );
        upstream.drain().for_each(drop);

        invmgr.received_wake(&tree);
        assert_eq!(upstream.drain().count(), 0, "Timeout hasn't lapsed");

        clock.elapse(REBROADCAST_TIMEOUT);

        invmgr.received_wake(&tree);
        assert_eq!(
            output::test::messages_from(&mut upstream, &remote)
                .filter(|m| matches!(m, NetworkMessage::Inv(_)))
                .count(),
            1,
            "Timeout has lapsed",
        );
    }

    #[test]
    fn test_max_attemps() {
        let network = Network::Mainnet;
        let mut upstream = Outbox::new(network, PROTOCOL_VERSION);
        let tree = model::Cache::from(NonEmpty::new(network.genesis()));

        let mut rng = fastrand::Rng::with_seed(1);
        let clock = RefClock::from(LocalTime::now());

        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, upstream.clone(), clock.clone());

        invmgr.peer_negotiated(remote.into(), ServiceFlags::NETWORK, true, false);
        invmgr.announce(tx.clone());

        // We attempt to broadcast up to `MAX_ATTEMPTS` times.
        for _ in 0..MAX_ATTEMPTS {
            invmgr.received_wake(&tree);
            output::test::messages_from(&mut upstream, &remote)
                .find(|m| matches!(m, NetworkMessage::Inv(_),))
                .expect("Inventory is announced");

            clock.elapse(REBROADCAST_TIMEOUT);
        }

        // The next time we time out, we disconnect the peer.
        invmgr.received_wake(&tree);
        events(upstream.drain())
            .find(|e| matches!(e, Event::TimedOut { peer } if peer == &remote))
            .expect("Peer times out");

        assert!(invmgr.contains(&tx.wtxid()));
        assert!(invmgr.peers.is_empty());
    }

    #[test]
    fn test_block_reverted() {
        let network = Network::Regtest;
        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
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

        let mut upstream = Outbox::new(network, PROTOCOL_VERSION);
        let time = LocalTime::now();

        let mut tree = model::Cache::from(headers);
        let mut invmgr = InventoryManager::new(rng, upstream.clone(), time);

        invmgr.peer_negotiated(remote.into(), ServiceFlags::NETWORK, true, false);
        invmgr.announce(tx.clone());
        invmgr.get_block(main_block1.block_hash());
        invmgr.received_block(&remote, main_block1, &tree);

        assert!(!invmgr.contains(&tx.wtxid()));

        let mut events = events(upstream.drain());

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
        assert!(invmgr.contains(&tx.wtxid()));

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

    #[test]
    fn test_wtx_inv() {
        let network = Network::Mainnet;
        let mut upstream = Outbox::new(network, PROTOCOL_VERSION);
        let tree = model::Cache::from(NonEmpty::new(network.genesis()));

        let mut rng = fastrand::Rng::with_seed(1);
        let time = LocalTime::now();

        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let remote2: net::SocketAddr = ([88, 88, 88, 89], 8333).into();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, upstream.clone(), time);

        invmgr.peer_negotiated(remote.into(), ServiceFlags::NETWORK, true, true);
        invmgr.announce(tx);

        invmgr.received_wake(&tree);
        let invs = output::test::messages_from(&mut upstream, &remote)
            .filter_map(|m| {
                if let NetworkMessage::Inv(invs) = m {
                    Some(invs)
                } else {
                    None
                }
            })
            .next()
            .unwrap();
        assert_matches!(invs.first(), Some(Inventory::WTx(_)));

        invmgr.peer_negotiated(remote2.into(), ServiceFlags::NETWORK, true, false);
        invmgr.received_wake(&tree);
        let invs = output::test::messages_from(&mut upstream, &remote2)
            .filter_map(|m| match m {
                NetworkMessage::Inv(invs) => Some(invs),
                _ => None,
            })
            .next()
            .unwrap();
        assert_matches!(invs.first(), Some(Inventory::Transaction(_)));
    }

    #[test]
    fn test_wtx_getdata() {
        let network = Network::Mainnet;
        let mut upstream = Outbox::new(network, PROTOCOL_VERSION);

        let mut rng = fastrand::Rng::with_seed(1);

        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, upstream.clone(), LocalTime::now());

        invmgr.peer_negotiated(remote.into(), ServiceFlags::NETWORK, true, true);
        invmgr.announce(tx.clone());

        invmgr.received_getdata(remote, &[Inventory::Transaction(tx.txid())]);
        let tr = output::test::messages_from(&mut upstream, &remote)
            .filter_map(|m| {
                if let NetworkMessage::Tx(tr) = m {
                    Some(tr)
                } else {
                    None
                }
            })
            .next()
            .unwrap();
        assert_eq!(tr.txid(), tx.txid());

        invmgr.received_getdata(remote, &[Inventory::WTx(tx.wtxid())]);
        let tr = output::test::messages_from(&mut upstream, &remote)
            .filter_map(|m| {
                if let NetworkMessage::Tx(tr) = m {
                    Some(tr)
                } else {
                    None
                }
            })
            .next()
            .unwrap();
        assert_eq!(tr.wtxid(), tx.wtxid());
    }
}
