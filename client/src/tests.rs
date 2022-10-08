#[cfg(test)]
pub mod mock;

use std::collections::HashMap;
use std::net;
use std::thread;
use std::time;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_chain::filter::cache::FilterCache;
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::block::time::AdjustedTime;
use nakamoto_common::block::Height;
use nakamoto_common::network::Services;
use nakamoto_net::event;
use nakamoto_p2p::protocol;
use nakamoto_p2p::protocol::Protocol;
use nakamoto_test::{logger, BITCOIN_HEADERS};

use crate::client::{self, Client, Config};
use crate::error;
use crate::handle::Handle as _;

type Reactor = nakamoto_net_poll::Reactor<net::TcpStream, net::SocketAddr>;

fn network(
    cfgs: &[Config],
) -> Result<
    Vec<(
        client::Handle<nakamoto_net_poll::Waker>,
        net::SocketAddr,
        thread::JoinHandle<()>,
    )>,
    error::Error,
> {
    let mut handles = Vec::new();

    for cfg in cfgs.iter().cloned() {
        let checkpoints = cfg.protocol.network.checkpoints().collect::<Vec<_>>();
        let genesis = cfg.protocol.network.genesis();
        let params = cfg.protocol.network.params();

        let node = Client::<Reactor>::new()?;
        let mut handle = node.handle();
        handle.set_timeout(time::Duration::from_secs(5));

        let t = thread::spawn({
            let params = params.clone();
            let checkpoints = checkpoints.clone();

            move || {
                let store = store::Memory::new((genesis, vec![]).into());
                let cache = BlockCache::from(store, params, &checkpoints).unwrap();
                let filters = FilterCache::from(store::Memory::default()).unwrap();
                let peers = HashMap::new();
                let local_time = time::SystemTime::now().into();
                let clock = AdjustedTime::<net::SocketAddr>::new(local_time);
                let rng = fastrand::Rng::new();

                node.run_with(
                    vec![([0, 0, 0, 0], 0).into()],
                    Protocol::new(cache, filters, peers, clock, rng, cfg.protocol),
                )
                .unwrap();
            }
        });
        let addr = handle.listening().unwrap();

        handles.push((handle, addr, t));
    }

    for (i, (handle, _, _)) in handles.iter().enumerate() {
        for (_, peer, _) in handles.iter().skip(i + 1) {
            handle.connect(*peer).unwrap();
        }
    }

    Ok(handles)
}

#[test]
fn test_full_sync() {
    logger::init(log::Level::Debug);

    fn config(name: &'static str) -> Config {
        Config {
            name,
            protocol: protocol::Config {
                services: ServiceFlags::NETWORK,
                ..protocol::Config::default()
            },
            ..Config::default()
        }
    }

    let nodes = network(&[config("olive"), config("alice"), config("misha")]).unwrap();
    let (handle, _, _) = nodes.last().unwrap();
    let headers = BITCOIN_HEADERS.tail.clone();
    let height = headers.len() as Height;
    let hash = headers.last().unwrap().block_hash();

    // Ensure all peers are connected to misha,
    // so that misha can effectively send blocks to
    // all peers on time.
    handle.wait_for_peers(2, Services::Chain).unwrap();

    handle
        .import_headers(headers)
        .expect("command is successful")
        .expect("chain is valid");

    for (mut node, _, thread) in nodes.into_iter() {
        node.set_timeout(std::time::Duration::from_secs(5));
        assert_eq!(node.wait_for_height(height).unwrap(), hash);

        node.shutdown().unwrap();
        thread.join().unwrap();
    }
}

#[test]
fn test_wait_for_peers() {
    logger::init(log::Level::Debug);

    let cfgs = vec![
        Config {
            protocol: protocol::Config {
                services: ServiceFlags::NETWORK,
                ..protocol::Config::default()
            },
            ..Default::default()
        };
        5
    ];

    let nodes = network(&cfgs).unwrap();
    let (handle, _, _) = nodes.first().unwrap();

    let peers = handle
        .wait_for_peers(nodes.len() - 1, Services::Chain)
        .unwrap();

    assert_eq!(peers.len(), nodes.len() - 1);
}

#[test]
fn test_send_handle() {
    let client: Client<Reactor> = Client::new().unwrap();
    let handle = client.handle();

    thread::spawn(move || {
        handle.wait_for_height(1).unwrap();
    });
}

#[test]
fn test_multiple_handle_events() {
    use std::time;

    let cfg = protocol::Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();
    let client: Client<Reactor> = Client::new().unwrap();
    let store = store::Memory::new((genesis, vec![]).into());
    let cache = BlockCache::from(store, params, &[]).unwrap();
    let filters = FilterCache::from(store::Memory::default()).unwrap();
    let peers = HashMap::new();

    let alice = client.handle();
    let bob = alice.clone();
    let alice_events = alice.events();
    let bob_events = bob.events();

    thread::spawn(|| {
        let local_time = time::SystemTime::now().into();
        let clock = AdjustedTime::<net::SocketAddr>::new(local_time);
        let rng = fastrand::Rng::new();

        client
            .run_with(
                vec![([0, 0, 0, 0], 0).into()],
                Protocol::new(cache, filters, peers, clock, rng, cfg),
            )
            .unwrap();
    });

    event::wait(
        &alice_events,
        |e| match e {
            protocol::Event::Ready { .. } => Some(()),
            _ => None,
        },
        time::Duration::from_secs(2),
    )
    .unwrap();

    event::wait(
        &bob_events,
        |e| match e {
            protocol::Event::Ready { .. } => Some(()),
            _ => None,
        },
        time::Duration::from_secs(2),
    )
    .unwrap();
}

#[test]
fn test_handle_shutdown() {
    let cfg = protocol::Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();
    let client: Client<Reactor> = Client::new().unwrap();
    let handle = client.handle();
    let store = store::Memory::new((genesis, vec![]).into());
    let cache = BlockCache::from(store, params, &[]).unwrap();
    let filters = FilterCache::from(store::Memory::default()).unwrap();
    let peers = HashMap::new();

    let th = thread::spawn(|| {
        let local_time = time::SystemTime::now().into();
        let clock = AdjustedTime::<net::SocketAddr>::new(local_time);
        let rng = fastrand::Rng::new();

        client.run_with(
            vec![],
            Protocol::new(cache, filters, peers, clock, rng, cfg),
        )
    });

    handle.shutdown().unwrap();
    th.join().unwrap().unwrap();
}

#[test]
fn test_client_dropped() {
    let client: Client<Reactor> = Client::new().unwrap();
    let handle = client.handle();

    drop(client);

    assert!(matches!(
        handle.get_tip(),
        Err(client::handle::Error::Disconnected)
    ));
}

#[test]
fn test_query_headers() {
    let cfg = protocol::Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();
    let client: Client<Reactor> = Client::new().unwrap();
    let handle = client.handle();
    let store = store::Memory::new((genesis, BITCOIN_HEADERS.tail.clone()).into());
    let cache = BlockCache::from(store, params, &[]).unwrap();
    let filters = FilterCache::from(store::Memory::default()).unwrap();

    thread::spawn(|| {
        let local_time = time::SystemTime::now().into();
        let clock = AdjustedTime::<net::SocketAddr>::new(local_time);
        let rng = fastrand::Rng::new();

        client.run_with(
            vec![],
            Protocol::new(cache, filters, HashMap::new(), clock, rng, cfg),
        )
    });

    let height = 1;
    let (tx, rx) = crate::chan::bounded(1);

    handle
        .query_tree(move |r| {
            let blk = r.get_block_by_height(height).cloned();
            tx.send((blk, blk.is_some())).ok();
        })
        .unwrap();

    let (header, found) = rx.recv().unwrap();

    assert_eq!(header, BITCOIN_HEADERS.tail.first().cloned());
    assert!(found);
}
