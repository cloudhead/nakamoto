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
//!    can carry anywhere between 1 and [`nakamoto_p2p::protocol::cbfmgr::MAX_MESSAGE_CFHEADERS`]
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
#![allow(unused_imports)]
use std::{iter, net, thread};

use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

use nakamoto_common::bitcoin::OutPoint;

use nakamoto_common::network::Network;
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_test::block::gen;
use nakamoto_test::logger;

use p2p::protocol::syncmgr;

use super::p2p::protocol::{cbfmgr, invmgr};
use super::utxos::Utxos;
use super::*;

use crate::handle::Handle as _;
use crate::tests::mock;

#[quickcheck]
fn prop_client_side_filtering(birth: Height, height: Height, seed: u64) -> TestResult {
    if height < 1 || height > 24 || birth >= height {
        return TestResult::discard();
    }

    let mut rng = fastrand::Rng::with_seed(seed);
    let network = Network::Regtest;
    let genesis = network.genesis_block();
    let chain = gen::blockchain(genesis, height, &mut rng);
    let mock = mock::Client::new(network);
    let mut client = mock.handle();

    client.tip = (height, chain[height as usize].header);

    // Build watchlist.
    // let mut utxos = Utxos::new();
    let mut spent = 0;
    let (watch, heights, balance) = gen::watchlist(birth, chain.iter(), &mut rng);

    let mut spv = super::Mapper::new();

    log::debug!(
        "-- Test case with birth = {} and height = {}",
        birth,
        height
    );

    let (mut publish, subscribe) = p2p::event::broadcast(move |e, p| spv.process(e, p));
    let subscriber = subscribe.subscribe();

    publish.broadcast(protocol::Event::SyncManager(syncmgr::Event::Synced(
        chain.last().block_hash(),
        height,
    )));

    for h in birth..=height {
        let matched = heights.contains(&h);
        let block = chain[h as usize].clone();

        publish.broadcast(protocol::Event::FilterManager(
            cbfmgr::Event::FilterProcessed {
                block: block.block_hash(),
                height: h,
                matched,
            },
        ));

        if matched {
            publish.broadcast(protocol::Event::InventoryManager(
                invmgr::Event::BlockProcessed { block, height: h },
            ));
        }
    }

    for event in subscriber.try_iter() {
        match event {
            Event::BlockMatched { transactions, .. } => {
                for t in &transactions {
                    for output in &t.output {
                        if watch.contains(&output.script_pubkey) {
                            spent += output.value;
                        }
                    }
                }
            }
            Event::Synced {
                height: sync_height,
                tip,
            } => {
                assert_eq!(height, tip);

                if sync_height == tip {
                    break;
                }
            }
            _ => {}
        }
    }
    assert_eq!(balance, spent);
    client.shutdown().unwrap();

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
