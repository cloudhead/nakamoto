use std::collections::HashSet;

use bitcoin::util::bip158;

use nakamoto_chain::filter::BlockFilter;
use nakamoto_common::block::{BlockHash, Height};

use crate::client;
use crate::spv::Watchlist;

#[allow(dead_code)]
pub struct FilterManager<H> {
    remaining: HashSet<BlockHash>,
    height: Height,
    client: H,
}

impl<H: client::handle::Handle> FilterManager<H> {
    pub fn new(height: Height, client: H) -> Self {
        Self {
            remaining: HashSet::new(),
            height,
            client,
        }
    }

    // TODO: For BIP32 wallets, add one more address to check, if the
    // matching one was the highest-index one.
    pub fn process(
        &mut self,
        filter: BlockFilter,
        block_hash: BlockHash,
        height: Height,
        watchlist: &Watchlist,
    ) -> Result<bool, bip158::Error> {
        self.remaining.remove(&block_hash);

        // Process filters in-order.
        if height == self.height {
            self.height = height + 1;
            self.remaining.remove(&block_hash);

            return filter.match_any(&block_hash, &mut watchlist.iter());
        } else {
            // TODO: If this condition triggers, we should just queue the filters
            // for later processing.
            panic!(
                "Filter received is too far ahead: expected height={}, got height={}",
                self.height, height
            );
        }
    }
}
