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
use std::{io, iter, net, thread};

use quickcheck::TestResult;
use quickcheck_macros::quickcheck;

use nakamoto_common::bitcoin::OutPoint;

use nakamoto_common::block::time::LocalTime;
use nakamoto_common::network::Network;
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_test::assert_matches;
use nakamoto_test::block::gen;
use nakamoto_test::logger;

use p2p::protocol::{Command, DisconnectReason};
use p2p::traits::Protocol as _;

use super::p2p::protocol::Link;
use super::utxos::Utxos;
use super::Event;
use super::*;

use crate::handle::Handle as _;
use crate::tests::mock;

#[test]
fn test_peer_connected_disconnected() {
    let network = Network::Regtest;
    let mut client = mock::Client::new(network);
    let handle = client.handle();
    let remote = ([44, 44, 44, 44], 8333).into();
    let local_addr = ([0, 0, 0, 0], 16333).into();
    let events = handle.subscribe();

    client
        .protocol
        .connected(remote, &local_addr, Link::Inbound);
    client.step();

    assert_matches!(
        events.try_recv(),
        Ok(Event::PeerConnected { addr, link, .. })
        if addr == remote && link == Link::Inbound
    );

    client.protocol.disconnected(
        &remote,
        DisconnectReason::ConnectionError(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
    );
    client.step();

    assert_matches!(
        events.try_recv(),
        Ok(Event::PeerDisconnected { addr, reason: DisconnectReason::ConnectionError(_) })
        if addr == remote
    );
}

#[test]
fn test_peer_connection_failed() {
    let network = Network::Regtest;
    let mut client = mock::Client::new(network);
    let handle = client.handle();
    let remote = ([44, 44, 44, 44], 8333).into();
    let events = handle.subscribe();

    client.protocol.command(Command::Connect(remote));
    client.protocol.attempted(&remote);
    client.step();

    assert_matches!(events.try_recv(), Err(_));

    client.protocol.disconnected(
        &remote,
        DisconnectReason::ConnectionError(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
    );
    client.step();

    assert_matches!(
        events.try_recv(),
        Ok(Event::PeerConnectionFailed { addr, error })
        if addr == remote && error.kind() == io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn test_peer_negotiated() {
    use nakamoto_common::bitcoin::network::address::Address;
    use nakamoto_common::bitcoin::network::constants::ServiceFlags;
    use nakamoto_common::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
    use nakamoto_common::bitcoin::network::message_network::VersionMessage;

    let network = Network::default();
    let mut client = mock::Client::new(network);
    let handle = client.handle();
    let remote = ([44, 44, 44, 44], 8333).into();
    let local_time = LocalTime::now();
    let local_addr = ([0, 0, 0, 0], 16333).into();
    let events = handle.subscribe();

    client
        .protocol
        .connected(remote, &local_addr, Link::Inbound);
    client.step();

    let version = NetworkMessage::Version(VersionMessage {
        version: protocol::MIN_PROTOCOL_VERSION,
        services: ServiceFlags::NETWORK,
        timestamp: local_time.block_time() as i64,
        receiver: Address::new(&remote, ServiceFlags::NONE),
        sender: Address::new(&local_addr, ServiceFlags::NONE),
        nonce: 42,
        user_agent: "?".to_owned(),
        start_height: 42,
        relay: false,
    });

    client.received(&remote, version);
    client.received(&remote, NetworkMessage::Verack);
    client.step();

    assert_matches!(events.try_recv(), Ok(Event::PeerConnected { .. }));
    assert_matches!(
        events.try_recv(),
        Ok(Event::PeerNegotiated { addr, height, user_agent, .. })
        if addr == remote && height == 42 && user_agent == "?"
    );
}

#[quickcheck]
fn prop_client_side_filtering(birth: Height, height: Height, seed: u64) -> TestResult {
    if height < 1 || height > 24 || birth >= height {
        return TestResult::discard();
    }

    let mut rng = fastrand::Rng::with_seed(seed);
    let network = Network::Regtest;
    let genesis = network.genesis_block();
    let chain = gen::blockchain(genesis, height, &mut rng);
    let mut mock = mock::Client::new(network);
    let mut client = mock.handle();

    client.tip = (height, chain[height as usize].header);

    let mut spent = 0;
    let (watch, heights, balance) = gen::watchlist_rng(birth, chain.iter(), &mut rng);

    log::debug!(
        "-- Test case with birth = {} and height = {}",
        birth,
        height
    );
    let subscriber = client.subscribe();

    mock.subscriber
        .broadcast(protocol::Event::Chain(protocol::ChainEvent::Synced(
            chain.last().block_hash(),
            height,
        )));

    for h in birth..=height {
        let matched = heights.contains(&h);
        let block = chain[h as usize].clone();

        mock.subscriber.broadcast(protocol::Event::Filter(
            protocol::FilterEvent::FilterProcessed {
                block: block.block_hash(),
                height: h,
                matched,
                cached: false,
                valid: true,
            },
        ));

        if matched {
            mock.subscriber.broadcast(protocol::Event::Inventory(
                protocol::InventoryEvent::BlockProcessed {
                    block,
                    height: h,
                    fees: None,
                },
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
