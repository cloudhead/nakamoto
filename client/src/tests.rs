use std::net;
use std::thread;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_chain::filter::cache::FilterCache;
use nakamoto_common::block::Height;
use nakamoto_test::{logger, BITCOIN_HEADERS};

use crate::client::{Client, ClientConfig, ClientHandle, Event};
use crate::error;
use crate::handle::Handle;

type Reactor = nakamoto_net_poll::Reactor<net::TcpStream>;

fn network(
    cfgs: &[ClientConfig],
) -> Result<
    Vec<(
        ClientHandle<Reactor>,
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

        let t = thread::spawn({
            let params = params.clone();
            let checkpoints = checkpoints.clone();

            move || {
                let store = store::Memory::new((genesis, vec![]).into());
                let cache = BlockCache::from(store, params, &checkpoints).unwrap();
                let filters = FilterCache::from(store::Memory::default()).unwrap();

                node.run_with(cache, filters).unwrap();
            }
        });

        let addr = handle
            .wait(|e| match e {
                Event::Listening(addr) => Some(addr),
                _ => None,
            })
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

    let nodes = network(&[
        ClientConfig::named("olive"),
        ClientConfig::named("alice"),
        ClientConfig::named("misha"),
    ])
    .unwrap();

    let (handle, _, _) = nodes.last().unwrap();
    let headers = BITCOIN_HEADERS.tail.clone();
    let height = headers.len() as Height;
    let hash = headers.last().unwrap().block_hash();

    handle
        .import_headers(headers)
        .expect("command is successful")
        .expect("chain is valid");

    for (node, _, thread) in nodes.into_iter() {
        assert_eq!(node.wait_for_height(height).unwrap(), hash);
        node.shutdown().unwrap();
        thread.join().unwrap();
    }
}
