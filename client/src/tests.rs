#[cfg(test)]
pub mod mock;

use std::collections::HashMap;
use std::net;
use std::thread;
use std::time;

use bitcoin::network::constants::ServiceFlags;
use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_chain::filter::cache::FilterCache;
use nakamoto_common::block::Height;
use nakamoto_common::network::Services;
use nakamoto_p2p::protocol::syncmgr;
use nakamoto_test::{logger, BITCOIN_HEADERS};

use crate::client::{self, event, Client, Config, Event};
use crate::error;
use crate::handle::Handle as _;

type Reactor = nakamoto_net_poll::Reactor<net::TcpStream, client::Publisher>;

fn network(
    cfgs: &[Config],
) -> Result<
    Vec<(
        client::Handle<Reactor>,
        net::SocketAddr,
        thread::JoinHandle<()>,
    )>,
    error::Error,
> {
    let mut handles = Vec::new();

    for cfg in cfgs.iter().cloned() {
        let checkpoints = cfg.network.checkpoints().collect::<Vec<_>>();
        let genesis = cfg.network.genesis();
        let params = cfg.network.params();

        let node = Client::new(cfg)?;
        let handle = node.handle();
        let events = handle.events();

        let t = thread::spawn({
            let params = params.clone();
            let checkpoints = checkpoints.clone();

            move || {
                let store = store::Memory::new((genesis, vec![]).into());
                let cache = BlockCache::from(store, params, &checkpoints).unwrap();
                let filters = FilterCache::from(store::Memory::default()).unwrap();
                let peers = HashMap::new();

                node.run_with(cache, filters, peers).unwrap();
            }
        });

        let addr = event::wait(
            &events,
            |e| match e {
                Event::Listening(addr) => Some(addr),
                _ => None,
            },
            time::Duration::from_secs(5),
        )
        .unwrap();

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
            services: syncmgr::REQUIRED_SERVICES,
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
            services: ServiceFlags::NETWORK,
            ..Default::default()
        };
        5
    ];

    let nodes = network(&cfgs).unwrap();
    let (handle, _, _) = nodes.first().unwrap();

    handle
        .wait_for_peers(nodes.len() - 1, Services::Chain)
        .unwrap();
}

#[test]
fn test_send_handle() {
    let cfg = Config::default();

    let client: Client<Reactor> = Client::new(cfg).unwrap();
    let handle = client.handle();

    thread::spawn(move || {
        handle.wait_for_ready().unwrap();
    });
}

#[test]
fn test_multiple_handle_events() {
    use std::time;

    let cfg = Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();
    let client: Client<Reactor> = Client::new(cfg).unwrap();
    let store = store::Memory::new((genesis, vec![]).into());
    let cache = BlockCache::from(store, params, &[]).unwrap();
    let filters = FilterCache::from(store::Memory::default()).unwrap();
    let peers = HashMap::new();

    let alice = client.handle();
    let bob = alice.clone();
    let alice_events = alice.events();
    let bob_events = bob.events();

    thread::spawn(|| {
        client.run_with(cache, filters, peers).unwrap();
    });

    event::wait(
        &alice_events,
        |e| match e {
            Event::Listening(_) => Some(()),
            _ => None,
        },
        time::Duration::from_secs(2),
    )
    .unwrap();

    event::wait(
        &bob_events,
        |e| match e {
            Event::Listening(_) => Some(()),
            _ => None,
        },
        time::Duration::from_secs(2),
    )
    .unwrap();
}

#[test]
fn test_handle_shutdown() {
    let cfg = Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();
    let client: Client<Reactor> = Client::new(cfg).unwrap();
    let handle = client.handle();
    let store = store::Memory::new((genesis, vec![]).into());
    let cache = BlockCache::from(store, params, &[]).unwrap();
    let filters = FilterCache::from(store::Memory::default()).unwrap();
    let peers = HashMap::new();

    let th = thread::spawn(|| client.run_with(cache, filters, peers));

    handle.shutdown().unwrap();
    th.join().unwrap().unwrap();
}

#[test]
fn test_client_dropped() {
    let cfg = Config::default();
    let client: Client<Reactor> = Client::new(cfg).unwrap();
    let handle = client.handle();

    drop(client);

    assert!(matches!(
        handle.get_tip(),
        Err(client::handle::Error::Disconnected)
    ));
}
