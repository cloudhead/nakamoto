pub mod error;
pub mod peer;

use nakamoto_chain::blocktree::BlockCache;

use std::sync::Arc;
use std::sync::RwLock;

use log::*;

pub struct Network {
    peer_config: peer::Config,
    block_cache: Arc<RwLock<BlockCache>>,
}

impl Network {
    pub fn new(peer_config: peer::Config, block_cache: Arc<RwLock<BlockCache>>) -> Self {
        Self {
            peer_config,
            block_cache,
        }
    }

    pub fn connect(&mut self, host: &str) -> Result<(), error::Error> {
        let addr = format!("{}:{}", host, self.peer_config.port());
        let mut p = peer::Peer::connect(&addr, &self.peer_config)?;

        debug!("Connected to {}", p.address);
        trace!("{:#?}", p);

        p.handshake(0)?;
        p.sync(0..1, self.block_cache.clone())?;

        Ok(())
    }
}
