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
use std::collections::HashSet;
use std::thread;

use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

use nakamoto_common::network::Network;
use nakamoto_test::block::gen;
use nakamoto_test::logger;

use super::handle::Handle as _;
use super::p2p::protocol::spvmgr;
use super::*;

use crate::tests::mock;

#[quickcheck]
fn prop_client_side_filtering(birth: Height, count: usize, seed: u64) -> TestResult {
    if count < 1 || count > 16 {
        return TestResult::discard();
    }
    if birth >= count as Height {
        return TestResult::discard();
    }
    logger::init(log::Level::Debug);

    let mut rng = fastrand::Rng::with_seed(seed);
    let network = Network::Regtest;
    let genesis = network.genesis_block();
    let chain = gen::blockchain(genesis, count as Height + 1, &mut rng);
    let remote = ([99, 99, 99, 99], 8333).into();
    let delta = chain.tail.len() - birth as usize;

    log::info!(
        target: "test",
        "--- Test case with chain length of {}, height of {} and birth height of {} (delta={}) ---",
        chain.len(),
        chain.len() - 1,
        birth,
        delta,
    );

    // Build watchlist.
    let mut watchlist = Watchlist::new();
    let mut balance = 0;
    for (h, blk) in chain.iter().enumerate().skip(birth as usize) {
        // Randomly pick certain blocks.
        if rng.bool() {
            // Randomly pick a transaction and add its output to the watchlist.
            let tx = &blk.txdata[rng.usize(0..blk.txdata.len())];
            watchlist.insert_script(tx.output[0].script_pubkey.clone());
            balance += tx.output[0].value;

            log::info!(target: "test",
                "Marking txid {} block #{} ({})", tx.txid(), h, blk.block_hash());
        }
    }

    // Construct list of filters to send to client, as well as set of matching block hashes,
    // including false-positives.
    let mut filters = Vec::with_capacity(count);
    let mut matching = HashSet::new();
    for h in birth as usize..chain.len() {
        let filter = gen::cfilter(&chain[h]);
        let block_hash = chain[h].block_hash();

        if watchlist.match_filter(&filter, &block_hash).unwrap() {
            matching.insert(block_hash);
        }
        filters.push((filter, block_hash, h as Height));
    }
    rng.shuffle(&mut filters); // Filters are received out of order

    log::info!(target: "test", "Marked {} block(s) for matching", matching.len());
    log::info!(target: "test", "Transaction balance is {}", balance);

    let config = Config { genesis: birth };
    let client = mock::Client::new(network);
    let spv = super::Client::new(client.handle(), watchlist, config);
    let handle = spv.handle();

    let t = thread::spawn(|| spv.run().unwrap());

    // Split the filter headers in random chunks to be received by the client.
    let mut chunks = Vec::new();
    {
        let mut remaining = delta;

        while remaining > 0 {
            let count = rng.usize(1..=remaining);
            chunks.push(count);
            remaining -= count;
        }
    }

    log::info!(
        "Splitting filter headers into {} chunk(s): {:?}",
        chunks.len(),
        chunks
    );

    // Send the filter headers to the client in chunks.
    {
        let mut height = birth;

        for chunk in chunks {
            let tip = height + chunk as Height;

            // The filter header chain has advanced by `chunk`.
            let event = client::Event::SpvManager(spvmgr::Event::FilterHeadersImported {
                height: tip,
                block_hash: chain[tip as usize].block_hash(),
            });
            client.events.send(event).unwrap();

            // We expect the client to fetch the corresponding filters from the network,
            // including the birth height.
            match client.commands.recv().unwrap() {
                client::Command::GetFilters(range, reply) => {
                    assert_eq!(*range.end(), tip);
                    reply.send(Ok(())).unwrap();
                }
                _ => panic!("expected `GetFilters` command"),
            }
            height = tip;
        }
    }

    log::info!(target: "test", "Sending requested filters to client..");
    for filter in filters {
        client.filters.send(filter).unwrap();
    }

    log::info!(target: "test", "Waiting for client to fetch matching blocks..");

    let mut requested = Vec::new();
    while !matching.is_empty() {
        log::info!(target: "test", "Blocks remaining to fetch: {}", matching.len());

        match client.commands.recv().unwrap() {
            client::Command::GetBlock(hash, reply) => {
                if !matching.remove(&hash) {
                    log::info!("Client matched false-positive {}", hash);
                }
                reply.send(Ok(remote)).unwrap();
                requested.push(hash);
            }
            _ => panic!("expected `GetBlock` command"),
        }
    }
    log::info!(target: "test", "All matching blocks have been fetched");

    rng.shuffle(&mut requested);
    for hash in requested {
        let (height, blk) = chain
            .iter()
            .enumerate()
            .find(|(_, blk)| blk.block_hash() == hash)
            .unwrap();

        log::info!(target: "test", "Sending block #{} to client", height);
        client.blocks.send((blk.clone(), height as Height)).unwrap();
    }
    log::info!(target: "test", "Checking matches..");

    // Shutdown.
    handle.shutdown().ok();
    t.join().unwrap();

    assert_eq!(balance, handle.utxos.lock().unwrap().balance());
    log::info!(target: "test", "--- Balance of {} matched ---", balance);

    TestResult::passed()
}
