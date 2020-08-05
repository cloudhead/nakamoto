use std::net;
use std::thread;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_chain::block::Height;
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

    for (i, (handle, addr, _)) in handles.iter().enumerate() {
        for (_, peer, _) in handles.iter().skip(i) {
            if peer != addr {
                handle.connect(*peer).unwrap();
            }
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

    let (handle, _, _t) = nodes.last().unwrap();

    handle
        .import_headers(TREE.chain.tail.clone())
        .expect("command is successful")
        .expect("chain is valid");

    // TODO: When done, start disconnecting.

    for (_, _, t) in nodes.into_iter() {
        t.join().unwrap();
    }
}
