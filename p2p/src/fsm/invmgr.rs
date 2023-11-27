//! Inventory manager.
//! Takes care of sending and fetching inventories.
//!
//! ## Handling of reverted blocks
//!
//! When a block is reverted, the inventory manager is notified, via the
//! [`InventoryManager::block_reverted`] function. Since confirmed transactions are held
//! for some time in memory, the transactions that were confirmed in the reverted block
//! can be matched and the user can be notified via a [`Event::TxStatusChanged`] event.
//! These transactions are then placed back into the local mempool, to ensure that they get
//! re-broadcast and eventually included in a new block.
//!
//! To ensure that any new and/or conflicting block that may contain the transaction is matched,
//! the filter manager is told to re-watch all reverted transactions. Thus, the inventory manager
//! can expect to receive the new block that contains the transaction that was reverted, via
//! the [`InventoryManager::received_block`] event.
//!
//! To keep only the smallest set of confirmed transactions in memory, we prune the set every time
//! the [`InventoryManager::timer_expired`] function is called. Confirmed transactions are removed
//! after they are burried at a certain depth.
//!
use std::collections::BTreeMap;

use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::bitcoin::network::{constants::ServiceFlags, message_blockdata::Inventory};
use nakamoto_common::bitcoin::{Block, BlockHash, Transaction, Txid, Wtxid};

// TODO: Timeout should be configurable
// TODO: Add exponential back-off

use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::BlockReader;
use nakamoto_common::collections::{AddressBook, HashMap};

use super::fees::FeeEstimator;
use super::output::{Io, Outbox};
use super::{event::TxStatus, Event, Height, PeerId};

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
pub struct InventoryManager<C> {
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
    outbox: Outbox,
    clock: C,
}

impl<C> Iterator for InventoryManager<C> {
    type Item = Io;

    fn next(&mut self) -> Option<Self::Item> {
        self.outbox.next()
    }
}

impl<C: Clock> InventoryManager<C> {
    /// Create a new inventory manager.
    pub fn new(rng: fastrand::Rng, clock: C) -> Self {
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
            outbox: Outbox::default(),
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

    /// Event received.
    pub fn received_event<T: BlockReader>(&mut self, event: Event, tree: &T) {
        match event {
            Event::PeerNegotiated {
                addr,
                services,
                relay,
                wtxid_relay,
                ..
            } => {
                self.peer_negotiated(addr, services, relay, wtxid_relay);
            }
            Event::PeerDisconnected { addr, .. } => {
                self.peers.remove(&addr);
            }
            Event::BlockHeadersImported { reverted, .. } => {
                for (height, _) in reverted {
                    self.block_reverted(height);
                }
            }
            Event::MessageReceived { from, message } => match message.as_ref() {
                NetworkMessage::Block(block) => {
                    self.received_block(&from, block.clone(), tree);
                }
                NetworkMessage::GetData(invs) => {
                    self.received_getdata(from, invs);
                    // TODO: (*self.hooks.on_getdata)(addr, invs, &self.outbox);
                }
                _ => {}
            },
            _ => {}
        }
    }

    /// Called when a peer is negotiated.
    fn peer_negotiated(
        &mut self,
        addr: PeerId,
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
            addr,
            Peer {
                services,
                attempts: 0,
                relay,
                wtxidrelay,
                outbox,
                last_attempt: None,
                requests: HashMap::with_hasher(self.rng.clone().into()),
            },
        );
    }

    /// Called when a block is reverted.
    pub fn block_reverted(&mut self, height: Height) {
        self.estimator.rollback(height - 1);

        if let Some(transactions) = self.confirmed.remove(&height) {
            for transaction in transactions {
                self.announce(transaction.clone());
                self.outbox.event(Event::TxStatusChanged {
                    txid: transaction.txid(),
                    status: TxStatus::Reverted { transaction },
                });
            }
        }
    }

    /// Lookup a submitted transaction in the local mempool.
    pub fn get_submitted_tx(&mut self, txid: &Txid) -> Option<Transaction> {
        self.mempool.values().find(|tx| tx.txid() == *txid).cloned()
    }

    /// Called when we receive a tick.
    pub fn timer_expired<T: BlockReader>(&mut self, tree: &T) {
        let now = self.clock.local_time();
        if now - self.last_tick.unwrap_or_default() >= IDLE_TIMEOUT {
            self.last_tick = Some(now);
            self.outbox.set_timer(IDLE_TIMEOUT);
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
                self.outbox.inv(*addr, invs);
                self.outbox.set_timer(self.timeout);
            }
        }

        for addr in disconnect {
            self.peers.remove(&addr);
            self.outbox.event(Event::PeerTimedOut { addr });
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
                log::debug!(target: "p2p", "Requesting block {} from {}", block_hash, addr);

                self.outbox
                    .get_data(*addr, vec![Inventory::Block(*block_hash)]);
                self.outbox.set_timer(REQUEST_TIMEOUT);

                *last_request = Some(now);
            } else {
                log::debug!(
                    target: "p2p",
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
                        self.outbox.tx(addr, tx.clone());

                        // Since we received a `getdata` from the peer, it means it received our
                        // inventory broadcast and we no longer need to send it.
                        if let Some(peer) = self.peers.get_mut(&addr) {
                            if peer.outbox.remove(&wtxid).is_some() {
                                // Reset retry state.
                                peer.reset();

                                if peer.outbox.is_empty() {
                                    log::debug!(target: "p2p", "Peer {} transaction outbox is empty", &addr);
                                }
                                self.outbox.event(Event::TxStatusChanged {
                                    txid: *txid,
                                    status: TxStatus::Acknowledged { peer: addr },
                                });
                            }
                        }
                    }
                }
                Inventory::WTx(wtxid) => {
                    if let Some(tx) = self.mempool.get(wtxid) {
                        self.outbox.tx(addr, tx.clone());
                    }

                    // Since we received a `getdata` from the peer, it means it received our
                    // inventory broadcast and we no longer need to send it.
                    if let Some(peer) = self.peers.get_mut(&addr) {
                        if let Some(txid) = peer.outbox.remove(wtxid) {
                            // Reset retry state.
                            peer.reset();

                            if peer.outbox.is_empty() {
                                log::debug!(target: "p2p", "Peer {} transaction outbox is empty", &addr);
                            }
                            self.outbox.event(Event::TxStatusChanged {
                                txid,
                                status: TxStatus::Acknowledged { peer: addr },
                            });
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
        _from: &PeerId,
        block: Block,
        tree: &T,
    ) -> Vec<Txid> {
        let hash = block.block_hash();

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

                    self.outbox.event(Event::TxStatusChanged {
                        txid: transaction.txid(),
                        status: TxStatus::Confirmed {
                            block: hash,
                            height,
                        },
                    });
                }
            }
            // Process block through fee estimator.
            let fees = self.estimator.process(block.clone(), height);

            self.outbox.event(Event::BlockProcessed {
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
        log::debug!(target: "p2p", "Queueing block {hash} to be requested");

        self.remaining.entry(hash).or_insert(None);
        self.schedule_tick();
    }

    ////////////////////////////////////////////////////////////////////////////

    fn schedule_tick(&mut self) {
        self.last_tick = None; // Disable rate-limiting for the next tick.
        self.outbox.set_timer(LocalDuration::from_secs(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net;

    use crate::fsm::network::Network;
    use crate::fsm::output;

    use nakamoto_common::bitcoin::network::message::NetworkMessage;
    use nakamoto_common::block::time::RefClock;
    use nakamoto_common::block::tree::BlockTree as _;
    use nakamoto_common::collections::HashSet;
    use nakamoto_common::nonempty::NonEmpty;
    use nakamoto_test::block::cache::model;
    use nakamoto_test::block::gen;
    use nakamoto_test::{assert_matches, logger};

    fn events(outputs: impl Iterator<Item = output::Io>) -> impl Iterator<Item = Event> {
        outputs.filter_map(|o| match o {
            output::Io::Event(e) => Some(e),
            _ => None,
        })
    }

    #[test]
    fn test_get_block() {
        logger::init(log::Level::Debug);

        let network = Network::Regtest;

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

        let mut invmgr = InventoryManager::new(rng.clone(), clock.clone());

        invmgr.peer_negotiated(
            ([66, 66, 66, 66], 8333).into(),
            ServiceFlags::NETWORK,
            true,
            true,
        );
        invmgr.peer_negotiated(
            ([77, 77, 77, 77], 8333).into(),
            ServiceFlags::NETWORK,
            true,
            true,
        );
        invmgr.peer_negotiated(
            ([88, 88, 88, 88], 8333).into(),
            ServiceFlags::NETWORK,
            true,
            true,
        );
        invmgr.peer_negotiated(
            ([99, 99, 99, 99], 8333).into(),
            ServiceFlags::NETWORK,
            true,
            true,
        );

        invmgr.get_block(hash);

        let mut requested = HashSet::with_hasher(rng.clone().into());
        let mut last_request = LocalTime::default();

        loop {
            clock.elapse(LocalDuration::from_secs(rng.u64(10..30)));
            invmgr.timer_expired(&tree);
            assert!(!invmgr.remaining.is_empty());

            let Some((addr, _)) = output::test::messages(&mut invmgr)
                .find(|(_, m)| matches!(m, NetworkMessage::GetData(i) if i == &inv)) else {
                    continue;
                };

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

            break;
        }
        assert!(invmgr.remaining.is_empty(), "No more blocks remaining");
        assert!(invmgr.received.is_empty());
        invmgr
            .find(|io| {
                matches!(io,
                    Io::Event(Event::BlockProcessed { block: b, .. })
                    if b.block_hash() == block.block_hash()
                )
            })
            .unwrap();

        clock.elapse(REQUEST_TIMEOUT);
        invmgr.timer_expired(&tree);
        assert_eq!(
            invmgr
                .outbox
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
        let tree = model::Cache::from(NonEmpty::new(network.genesis()));
        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let mut rng = fastrand::Rng::with_seed(1);

        let clock = RefClock::from(LocalTime::now());
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, clock.clone());

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true, false);
        invmgr.announce(tx);
        invmgr.timer_expired(&tree);

        assert_eq!(
            output::test::messages_from(&mut invmgr, &remote)
                .filter(|m| matches!(m, NetworkMessage::Inv(_)))
                .count(),
            1
        );
        invmgr.outbox.drain().for_each(drop);

        invmgr.timer_expired(&tree);
        assert_eq!(invmgr.outbox.drain().count(), 0, "Timeout hasn't lapsed");

        clock.elapse(REBROADCAST_TIMEOUT);

        invmgr.timer_expired(&tree);
        assert_eq!(
            output::test::messages_from(&mut invmgr.outbox, &remote)
                .filter(|m| matches!(m, NetworkMessage::Inv(_)))
                .count(),
            1,
            "Timeout has lapsed",
        );
    }

    #[test]
    fn test_max_attemps() {
        let network = Network::Mainnet;
        let tree = model::Cache::from(NonEmpty::new(network.genesis()));

        let mut rng = fastrand::Rng::with_seed(1);
        let clock = RefClock::from(LocalTime::now());

        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, clock.clone());

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true, false);
        invmgr.announce(tx.clone());

        // We attempt to broadcast up to `MAX_ATTEMPTS` times.
        for _ in 0..MAX_ATTEMPTS {
            invmgr.timer_expired(&tree);
            output::test::messages_from(&mut invmgr.outbox, &remote)
                .find(|m| matches!(m, NetworkMessage::Inv(_),))
                .expect("Inventory is announced");

            clock.elapse(REBROADCAST_TIMEOUT);
        }

        // The next time we time out, we disconnect the peer.
        invmgr.timer_expired(&tree);
        events(invmgr.outbox.drain())
            .find(|e| matches!(e, Event::PeerTimedOut { addr } if addr == &remote))
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

        let time = LocalTime::now();

        let mut tree = model::Cache::from(headers);
        let mut invmgr = InventoryManager::new(rng, time);

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true, false);
        invmgr.announce(tx.clone());
        invmgr.get_block(main_block1.block_hash());
        invmgr.received_block(&remote, main_block1, &tree);

        assert!(!invmgr.contains(&tx.wtxid()));

        events(invmgr.outbox.drain())
            .find(|e| {
                matches! {
                    e, Event::TxStatusChanged { txid, status: TxStatus::Confirmed { .. } }
                    if *txid == tx.txid()
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

        events(invmgr.outbox.drain())
            .find(|e| {
                matches! {
                    e, Event::TxStatusChanged { txid, status: TxStatus::Reverted { .. } }
                    if *txid == tx.txid()
                }
            })
            .unwrap();

        invmgr.get_block(fork_block1.block_hash());
        invmgr.received_block(&remote, fork_block1.clone(), &tree);

        events(invmgr.outbox.drain())
            .find(|e| {
                matches! {
                    e, Event::TxStatusChanged { txid, status: TxStatus::Confirmed { block, .. } }
                    if *txid == tx.txid() && block == &fork_block1.block_hash()
                }
            })
            .unwrap();
    }

    #[test]
    fn test_wtx_inv() {
        let network = Network::Mainnet;
        let tree = model::Cache::from(NonEmpty::new(network.genesis()));

        let mut rng = fastrand::Rng::with_seed(1);
        let time = LocalTime::now();

        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let remote2: net::SocketAddr = ([88, 88, 88, 89], 8333).into();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, time);

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true, true);
        invmgr.announce(tx);

        invmgr.timer_expired(&tree);
        let invs = output::test::messages_from(&mut invmgr.outbox, &remote)
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

        invmgr.peer_negotiated(remote2, ServiceFlags::NETWORK, true, false);
        invmgr.timer_expired(&tree);
        let invs = output::test::messages_from(&mut invmgr.outbox, &remote2)
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
        let mut rng = fastrand::Rng::with_seed(1);
        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let tx = gen::transaction(&mut rng);

        let mut invmgr = InventoryManager::new(rng, LocalTime::now());

        invmgr.peer_negotiated(remote, ServiceFlags::NETWORK, true, true);
        invmgr.announce(tx.clone());

        invmgr.received_getdata(remote, &[Inventory::Transaction(tx.txid())]);
        let tr = output::test::messages_from(&mut invmgr.outbox, &remote)
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
        let tr = output::test::messages_from(&mut invmgr.outbox, &remote)
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
