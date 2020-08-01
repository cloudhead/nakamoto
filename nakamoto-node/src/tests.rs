use std::thread;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;

use crate::error;
use crate::node::{Node, NodeConfig};

#[allow(dead_code)]
fn setup() -> Result<(), error::Error> {
    let cfg = NodeConfig::default();

    let mut olive = Node::new(cfg.clone())?;
    let mut joey = Node::new(cfg.clone())?;
    let mut alice = Node::new(cfg.clone())?;

    olive.seed(joey.config.listen.clone())?;
    olive.seed(alice.config.listen.clone())?;

    joey.seed(olive.config.listen.clone())?;
    joey.seed(alice.config.listen.clone())?;

    alice.seed(olive.config.listen.clone())?;
    alice.seed(joey.config.listen.clone())?;

    let _olive_handle = olive.handle();
    let _joey_handle = joey.handle();
    let _alice_handle = alice.handle();

    let checkpoints = cfg.network.checkpoints().collect::<Vec<_>>();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();

    thread::spawn({
        let params = params.clone();
        let checkpoints = checkpoints.clone();

        move || {
            let store = store::Memory::new((genesis, vec![]).into());
            let cache = BlockCache::from(store, params.clone(), &checkpoints).unwrap();

            olive.run_with(cache).unwrap();
        }
    });
    thread::spawn({
        let params = params.clone();
        let checkpoints = checkpoints.clone();

        move || {
            let store = store::Memory::new((genesis, vec![]).into());
            let cache = BlockCache::from(store, params.clone(), &checkpoints).unwrap();

            joey.run_with(cache).unwrap();
        }
    });
    thread::spawn({
        let params = params.clone();
        let checkpoints = checkpoints.clone();

        move || {
            let store = store::Memory::new((genesis, vec![]).into());
            let cache = BlockCache::from(store, params.clone(), &checkpoints).unwrap();

            alice.run_with(cache).unwrap();
        }
    });

    Ok(())
}
