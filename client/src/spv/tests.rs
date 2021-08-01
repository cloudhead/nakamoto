//! Properties of the [`super::Client`] we'd like to test.
//!
//! 1. The final output is invariant to the order in which `block` and `cfilter` messages are
//!    received.
//!
//!    Rationale: Blocks and compact filters are often fetched from multiple peers in parallel.
//!    Hence, it's important that the system be able to handle out-of-order receipt of this data,
//!    and that it not affect the final outcome, eg. the balance of the UTXOs.
//!
//! 2. The final output is invariant to the granularity of the the filter header chain updates.
//!
//!    Rationale: Filter header updates are received via the `cfheaders` message. These messages
//!    can carry anywhere between 1 and [`nakamoto_p2p::protocol::spvmgr::MAX_MESSAGE_CFHEADERS`]
//!    headers. The system should handle many small messages the same way as it handles a few
//!    large ones.
//!
//! 3. The final output is invariant to chain re-orgs.
//!
//!    Rationale: Chain re-organizations happen, and filters can be invalidated. The final output
//!    of the system should always match the main chain at any given point in time.
//!
//! 4. The final output is always a function of the input.
//!
//!    Rationale: Irrespective to how the system converges towards its final state, the final output
//!    should always match the given input.
//!
//! 5. The commands `watch_address`, `unwatch_address`, `watch_scripts`, `unwatch_scripts`,
//!    `submit` are idempotent.
//!
//! 6. The `rescan` command is always a no-op if the start of the range is equal or greater
//!    than the current synced height plus one.
//!
//!    Rationale: Any re-scans for future blocks are equivalent to the default behavior of
//!    scanning incoming blocks as they come.
//!
//! 7. The system is *injective*, in the sense that for every input there is a unique, distinct
//!    output.
//!
use std::net;
use std::thread;

use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

use nakamoto_common::network::Network;
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_test::block::gen;
use nakamoto_test::logger;

use super::handle::Handle as _;
use super::p2p::protocol::spvmgr;
use super::*;

use crate::handle::Handle as _;
use crate::tests::mock;

struct TestNode {
    height: Height,
    peer: Height,
    client: mock::Client,
    chain: NonEmpty<Block>,
    address: net::SocketAddr,
    rng: fastrand::Rng,
}

impl TestNode {
    fn new(client: mock::Client, height: Height, mut rng: fastrand::Rng) -> Self {
        let genesis = client.network.genesis_block();
        let chain = gen::blockchain(genesis, height, &mut rng);
        let address = ([99, 99, 99, 99], 8333).into();
        let peer = 0;

        Self {
            height,
            peer,
            client,
            chain,
            address,
            rng,
        }
    }

    fn run(mut self) {
        let mut requested_blocks: Vec<BlockHash> = Vec::new();
        let mut requested_filters: Vec<Height> = Vec::new();

        loop {
            chan::select! {
                recv(self.client.commands) -> cmd => {
                    match cmd.unwrap() {
                        client::Command::GetFilters(range, reply) => {
                            requested_filters.extend(range);
                            self.rng.shuffle(&mut requested_filters);
                            reply.send(Ok(())).unwrap();
                        }
                        client::Command::GetBlock(hash, reply) => {
                            requested_blocks.push(hash);
                            self.rng.shuffle(&mut requested_blocks);
                            reply.send(Ok(self.address)).unwrap();
                        }
                        client::Command::Shutdown => {
                            break;
                        }
                        other => {
                            panic!("TestNode::run: unexpected command: {:?}", other);
                        }
                    }
                }
                default => {
                    if self.peer < self.height {
                        log::info!(target: "test", "Sending filter headers to client..");

                        let height = self.rng.u64(self.peer + 1..=self.height);
                        let event = client::Event::SpvManager(spvmgr::Event::FilterHeadersImported {
                            height,
                            block_hash: self.chain[height as usize].block_hash(),
                        });
                        self.client.events.send(event).unwrap();
                        self.peer = height;
                    }

                    if !requested_filters.is_empty() {
                        log::info!(target: "test", "Sending requested filters to client..");

                        // Construct list of filters to send to client, as well as set of matching
                        // block hashes, including false-positives.
                        let count = self.rng.usize(1..=requested_filters.len());
                        for _ in 0..count {
                            let h = requested_filters.pop().unwrap();
                            let filter = gen::cfilter(&self.chain[h as usize]);
                            let block_hash = self.chain[h as usize].block_hash();

                            log::info!(target: "test", "Sending filter #{} to client", h);
                            self.client.filters.send((filter, block_hash, h as Height)).unwrap();
                        }
                    }
                    if !requested_blocks.is_empty() {
                        log::info!(target: "test", "Sending requested blocks to client..");

                        let count = self.rng.usize(1..=requested_blocks.len());
                        for _ in 0..count {
                            let hash = requested_blocks.pop().unwrap();
                            let (height, blk) = self.chain
                                .iter()
                                .enumerate()
                                .find(|(_, blk)| blk.block_hash() == hash)
                                .unwrap();

                            log::info!(target: "test", "Sending block #{} to client", height);
                            self.client.blocks.send((blk.clone(), height as Height)).unwrap();
                        }
                    }
                }
            }
        }
    }
}

mod utils {
    use super::*;

    pub fn populate_watchlist(
        watchlist: &mut Watchlist,
        birth: Height,
        chain: &NonEmpty<Block>,
        rng: &mut fastrand::Rng,
    ) -> u64 {
        let mut balance = 0;
        for (h, blk) in chain.iter().enumerate().skip(birth as usize) {
            // Randomly pick certain blocks.
            if rng.bool() {
                // Randomly pick a transaction and add its output to the watchlist.
                let tx = &blk.txdata[rng.usize(0..blk.txdata.len())];
                watchlist.insert_script(tx.output[0].script_pubkey.clone());
                balance += tx.output[0].value;

                log::debug!(
                    target: "test",
                    "Marking txid {} block #{} ({})",
                    tx.txid(),
                    h,
                    blk.block_hash()
                );
            }
        }
        balance
    }

    pub fn is_sorted<T>(data: &[T]) -> bool
    where
        T: Ord,
    {
        data.windows(2).all(|w| w[0] <= w[1])
    }
}

#[quickcheck]
fn prop_client_side_filtering(birth: Height, height: Height, seed: u64) -> TestResult {
    if height < 1 || height > 24 || birth >= height {
        return TestResult::discard();
    }
    logger::init(log::Level::Debug);

    log::info!(
        target: "test",
        "--- Test case with chain height of {} and birth height of {} ---",
        height,
        birth,
    );

    let mc = mock::Client::new(Network::Regtest);
    let client = mc.handle();
    let mut rng = fastrand::Rng::with_seed(seed);
    let node = TestNode::new(mc, height, rng.clone());

    // Build watchlist.
    let mut watchlist = Watchlist::new();
    let balance = utils::populate_watchlist(&mut watchlist, birth, &node.chain, &mut rng);
    log::info!(target: "test", "Transaction balance is {}", balance);

    let config = Config { genesis: birth };
    let spv = super::Client::new(node.client.handle(), watchlist, config);
    let mut handle = spv.handle();
    let events = handle.events();

    let t = thread::spawn(|| spv.run().unwrap());
    let n = thread::spawn(|| node.run());

    loop {
        if let Event::Synced { height: synced } = events.recv().unwrap() {
            log::info!(target: "test", "Synced {}/{}", synced, height);

            if synced == height {
                break;
            }
        }
    }
    log::info!(target: "test", "Finished syncing");
    assert_eq!(balance, handle.utxos.lock().unwrap().balance());

    // Shutdown.
    handle.shutdown().unwrap();
    client.shutdown().unwrap();

    t.join().unwrap();
    n.join().unwrap();

    TestResult::passed()
}

#[ignore]
#[quickcheck]
fn prop_event_ordering(birth: Height, height: Height, seed: u64) -> TestResult {
    if !(24..48).contains(&height) || birth >= height || birth == 0 {
        return TestResult::discard();
    }

    let mc = mock::Client::new(Network::Regtest);
    let client = mc.handle();
    let mut rng = fastrand::Rng::with_seed(seed);
    let node = TestNode::new(mc, height, rng.clone());
    let mut watchlist = Watchlist::new();

    let balance = utils::populate_watchlist(&mut watchlist, birth, &node.chain, &mut rng);

    let config = Config { genesis: birth };
    let txmgr = TxManager::new(node.client.handle(), watchlist, config);
    let mut handle = txmgr.handle();
    let events = handle.events();

    let t = thread::spawn(|| txmgr.run().unwrap());
    let n = thread::spawn(|| node.run());

    let mut received = vec![];
    let mut done = false;

    while !done {
        let event = events.recv().unwrap();
        if let Event::Synced { height: synced } = event {
            if synced == height {
                done = true;
            }
        }
        received.push(event.clone());
    }
    log::info!(target: "test", "Finished syncing");

    let synced = received
        .iter()
        .filter_map(|e| {
            if let Event::Synced { height } = e {
                Some(height)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let filter_processed = received
        .iter()
        .filter_map(|e| {
            if let Event::FilterProcessed { height, .. } = e {
                Some(height)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let block_processed = received
        .iter()
        .filter_map(|e| {
            if let Event::BlockProcessed { height, .. } = e {
                Some(height)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    assert!(balance == 0 || !block_processed.is_empty());
    assert!(!filter_processed.is_empty());
    assert!(!synced.is_empty());

    assert!(utils::is_sorted(&synced));
    assert!(utils::is_sorted(&filter_processed));
    assert!(utils::is_sorted(&block_processed));

    // Shutdown.
    handle.shutdown().unwrap();
    client.shutdown().unwrap();

    t.join().unwrap();
    n.join().unwrap();

    TestResult::passed()
}

#[test]
fn test_tx_status_ordering() {
    assert!(
        TxStatus::Unconfirmed
            < TxStatus::Acknowledged {
                peer: ([0, 0, 0, 0], 0).into()
            }
    );
    assert!(
        TxStatus::Acknowledged {
            peer: ([0, 0, 0, 0], 0).into()
        } < TxStatus::Confirmed {
            height: 0,
            block: BlockHash::default(),
        }
    );
    assert!(
        TxStatus::Confirmed {
            height: 0,
            block: BlockHash::default(),
        } < TxStatus::Reverted
    );
    assert!(
        TxStatus::Reverted
            < TxStatus::Stale {
                replaced_by: Default::default(),
                block: BlockHash::default()
            }
    );
}
