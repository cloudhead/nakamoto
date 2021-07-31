use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bitcoin::util::bip158;

use nakamoto_chain::filter::BlockFilter;
use nakamoto_chain::store::Genesis as _;
use nakamoto_common::block::{BlockHash, Height};

use crate::client;
use crate::spv::{Error, Watchlist};

#[allow(dead_code)]
pub struct FilterManager<H> {
    /// Filters remaining to download.
    remaining: HashSet<Height>,
    /// Filters waiting to be processed.
    pending: HashMap<Height, (BlockFilter, BlockHash)>,
    /// Range of blocks we are currently syncing.
    ///
    /// The start of the range is the oldest block we are syncing, while the end of the range
    /// is the youngest block we know of.
    ///
    /// The range start is updated when filters are processed, while the range end is updated
    /// when filter *headers* are processed.
    ///
    /// When the range is empty, it means we are all caught up, and there are no *known* blocks,
    /// from the perspective of the filter manager that have not had their filters processed.
    sync: Range<Height>,
    /// Client handle.
    client: H,
    watchlist: Arc<Mutex<Watchlist>>,
}

impl<H: client::handle::Handle> FilterManager<H> {
    /// Create a new filter manager, given a block height, client handle and watchlist.
    /// The height should be the starting height from which filters should be synced and
    /// processed.
    pub fn new(start: Height, client: H, watchlist: Arc<Mutex<Watchlist>>) -> Self {
        // If we're starting from genesis, preload the `pending` set with the filter for the
        // genesis block. The next time `process` is called, the filter will be matched.
        let (sync, pending) = if start == 0 {
            let network = client.network();
            let filter = BlockFilter::genesis(network);
            let pending = HashMap::from_iter(vec![(0, (filter, network.genesis_hash()))]);

            // Since the genesis is "known", we consider its header synced and thus set the
            // end of the range to be the following block.
            (start..start + 1, pending)
        } else {
            (start..start, HashMap::new())
        };

        Self {
            remaining: HashSet::new(),
            sync,
            pending,
            client,
            watchlist,
        }
    }

    pub fn is_synced(&self) -> bool {
        self.sync.is_empty()
    }

    pub fn height(&self) -> Option<Height> {
        // The sync start height is not the "synced" height, but rather the height currently being
        // synced. Thus, we subtract one from it.
        self.sync.start.checked_sub(1)
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
    pub fn process(&mut self) -> Result<Vec<(BlockHash, Height)>, bip158::Error> {
        assert!(self.sync.end >= self.sync.start);
        // TODO: For BIP32 wallets, add one more address to check, if the
        // matching one was the highest-index one.
        let mut matches = Vec::new();

        while let Some((filter, block_hash)) = self.pending.remove(&self.sync.start) {
            let watchlist = self.watchlist.lock().unwrap();
            if watchlist.match_filter(&filter, &block_hash)? {
                matches.push((block_hash, self.sync.start));
            }
            self.sync.start += 1;
        }
        Ok(matches)
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
        assert!(height >= self.sync.end);
        assert!(self.sync.end >= self.sync.start);

        let range = self.sync.end..=height;

        self.client.get_filters(range.clone())?;
        self.remaining.extend(range);
        self.sync.end = height + 1;

        Ok(())
    }

    pub fn get_filters() {}
}

/// Invariants.
///
/// 1. The `remaining` and `pending` sets, which represent filters waiting to be received or
///    processed should always only contain heights within the `sync` range.
///
/// 2. When `pending` and `remaining` are empty, so should the `sync` range, and vice-versa.
///
#[cfg(test)]
mod tests {}
