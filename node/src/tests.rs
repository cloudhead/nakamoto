use std::net;
use std::thread;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_common::block::Height;
use nakamoto_p2p::bitcoin::util::hash::BitcoinHash;
use nakamoto_test::{logger, TREE};

use crate::error;
use crate::handle::Handle;
use crate::node::{Event, Node, NodeConfig, NodeHandle};

fn network(
    cfgs: &[NodeConfig],
) -> Result<Vec<(NodeHandle, net::SocketAddr, thread::JoinHandle<()>)>, error::Error> {
    let mut handles = Vec::new();

    for cfg in cfgs.iter().cloned() {
        let checkpoints = cfg.network.checkpoints().collect::<Vec<_>>();
        let genesis = cfg.network.genesis();
        let params = cfg.network.params();

        let mut node = Node::new(cfg)?;
        let handle = node.handle();

        let t = thread::spawn({
            let params = params.clone();
            let checkpoints = checkpoints.clone();

            move || {
                let store = store::Memory::new((genesis, vec![]).into());
                let cache = BlockCache::from(store, params, &checkpoints).unwrap();

                node.run_with(cache).unwrap();
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
        NodeConfig::named("olive"),
        NodeConfig::named("alice"),
        NodeConfig::named("misha"),
    ])
    .unwrap();

    let (handle, _, _) = nodes.last().unwrap();
    let headers = TREE.chain.tail.clone();
    let height = headers.len() as Height;
    let hash = headers.last().unwrap().bitcoin_hash();

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
