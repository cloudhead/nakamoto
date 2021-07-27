use std::collections::{HashMap, HashSet};

use bitcoin::util::bip158;

use nakamoto_chain::filter::BlockFilter;
use nakamoto_common::block::{BlockHash, Height};

use crate::client;
use crate::spv::{Error, Watchlist};

#[allow(dead_code)]
pub struct FilterManager<H> {
    /// Filters remaining to download.
    remaining: HashSet<Height>,
    /// Filters waiting to be processed.
    pending: HashMap<Height, (BlockFilter, BlockHash)>,
    /// Current height of the filter header chain.
    header_height: Height,
    /// Height of the last processed compact filter.
    filter_height: Height,
    /// Client handle.
    client: H,
}

impl<H: client::handle::Handle> FilterManager<H> {
    pub fn new(height: Height, client: H) -> Self {
        Self {
            remaining: HashSet::new(),
            header_height: height,
            filter_height: height,
            pending: HashMap::new(),
            client,
        }
    }

    pub fn filter_received(&mut self, filter: BlockFilter, block_hash: BlockHash, height: Height) {
        self.remaining.remove(&height);
        self.pending.insert(height, (filter, block_hash));
    }

    /// Process the next filter in the queue that can be processed.
    ///
    /// Checks whether any of the queued filters is next in line (by height) and if so,
    /// processes it and returns the result of trying to match it with the watchlist.
    ///
    /// Returns nothing if there was no match or filter to process.
    pub fn process(
        &mut self,
        watchlist: &Watchlist,
    ) -> Result<Option<(BlockHash, Height)>, bip158::Error> {
        // TODO: For BIP32 wallets, add one more address to check, if the
        // matching one was the highest-index one.
        let height = self.filter_height + 1; // Next height to process.

        if let Some((filter, block_hash)) = self.pending.remove(&height) {
            self.filter_height = height;

            if filter.match_any(&block_hash, &mut watchlist.iter())? {
                return Ok(Some((block_hash, height)));
            }
        }
        Ok(None)
    }

    /// Called when filter headers were successfully imported.
    ///
    /// The height is the new filter header chain height, and the hash is the
    /// hash of the block corresponding to the last filter header.
    ///
    /// When new headers are imported, we want to download the corresponding compact filters
    /// to check them for matches.
    pub fn headers_imported(
        &mut self,
        height: Height,
        _block_hash: BlockHash,
    ) -> Result<(), Error> {
        debug_assert!(height > self.header_height);

        // Nb. Range end is non-exclusive.
        let range = self.header_height..height + 1;

        self.client.get_filters(range.clone())?;
        self.remaining.extend(range);
        self.header_height = height;

        Ok(())
    }
}
