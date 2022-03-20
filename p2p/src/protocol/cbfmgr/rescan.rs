//! Blockchain (re-)scanning for matching scripts.
use std::collections::BTreeSet;
use std::ops::RangeInclusive;
use std::rc::Rc;

use nakamoto_common::bitcoin::util::bip158;
use nakamoto_common::bitcoin::{Script, Txid};
use nakamoto_common::block::filter::BlockFilter;
use nakamoto_common::block::tree::BlockReader;
use nakamoto_common::block::{BlockHash, Height};
use nakamoto_common::collections::{HashMap, HashSet};

use super::{Event, Events, FilterCache, HeightIterator, MAX_MESSAGE_CFILTERS};

/// Filter (re)scan state.
#[derive(Debug, Default)]
pub struct Rescan {
    /// Whether a rescan is currently in progress.
    pub active: bool,
    /// Current height from which we're synced filters.
    /// Must be between `start` and `end`.
    pub current: Height,
    /// Start height of the filter rescan.
    pub start: Height,
    /// End height of the filter rescan. If `None`, keeps scanning new blocks until stopped.
    pub end: Option<Height>,
    /// Filter cache.
    pub cache: FilterCache<Rc<BlockFilter>>,
    /// Addresses and outpoints to watch for.
    pub watch: HashSet<Script>,
    /// Transactions to watch for.
    pub transactions: HashMap<Txid, HashSet<Script>>,

    /// Filters requested and remaining to download.
    requested: BTreeSet<Height>,
    /// Received filters waiting to be matched.
    received: HashMap<Height, (Rc<BlockFilter>, BlockHash, bool)>,
}

impl Rescan {
    /// Create a new rescan state.
    pub fn new(cache: usize) -> Self {
        let cache = FilterCache::new(cache);

        Self {
            cache,
            ..Self::default()
        }
    }

    /// Return info string on rescan state.
    #[cfg(not(test))]
    pub fn info(&self) -> String {
        format!(
            "rescan current = {}, watch = {}, txs = {}, filter queue = {}, requested = {}",
            self.current,
            self.watch.len(),
            self.transactions.len(),
            self.received.len(),
            self.requested.len()
        )
    }

    /// Rollback state to height.
    pub fn rollback(&mut self, to: Height) {
        self.cache.rollback(to)
    }

    /// A filter was received.
    pub fn received(&mut self, height: Height, filter: BlockFilter, block_hash: BlockHash) -> bool {
        let requested = self.requested.remove(&height);
        if requested {
            // We use a reference counted pointer here because it's possible for a filter to be
            // both in the processing queue and in the cache, or only in one or the other.
            let filter = Rc::new(filter);

            self.cache.push(height, filter.clone());
            self.received.insert(height, (filter, block_hash, false));
        }
        requested
    }

    /// Process the next filters in the queue that can be processed.
    ///
    /// Checks whether any of the queued filters is next in line (by height) and if so,
    /// processes it and returns the result of trying to match it with the watch list.
    pub fn process(
        &mut self,
        events: &impl Events,
    ) -> Result<Vec<(Height, BlockHash)>, bip158::Error> {
        let mut matches = Vec::new();
        let mut current = self.current;

        while let Some((filter, block_hash, cached)) = self.received.remove(&current) {
            let matched = self.match_filter(&filter, &block_hash)?;
            if matched {
                matches.push((current, block_hash));
            }
            events.event(Event::FilterProcessed {
                block: block_hash,
                height: current,
                matched,
                cached,
            });
            current += 1;
        }
        self.current = current;

        if let Some(stop) = self.end {
            if self.current == stop {
                self.active = false;
                events.event(Event::RescanCompleted { height: stop });
            }
        }

        Ok(matches)
    }

    /// Check whether a filter matches one of our scripts.
    pub fn match_filter(
        &self,
        filter: &BlockFilter,
        block_hash: &BlockHash,
    ) -> Result<bool, bip158::Error> {
        let mut matched = false;

        // Match scripts first, then match transactions. All outputs of a transaction must
        // match to consider the transaction matched.
        if !self.watch.is_empty() {
            matched = filter.match_any(block_hash, &mut self.watch.iter().map(|k| k.as_bytes()))?;
        }
        if !matched && !self.transactions.is_empty() {
            matched = self.transactions.values().any(|outs| {
                let mut outs = outs.iter().map(|k| k.as_bytes());
                filter.match_all(block_hash, &mut outs).unwrap_or(false)
            })
        }
        Ok(matched)
    }

    /// Given a range of filter heights, return the ranges that are missing.
    /// This is useful to figure out which ranges to fetch while ensuring we don't request
    /// the same heights more than once.
    pub fn requests<T: BlockReader>(
        &mut self,
        range: RangeInclusive<Height>,
        tree: &T,
    ) -> Vec<RangeInclusive<Height>> {
        if range.is_empty() {
            return vec![];
        }

        for height in range.clone() {
            if let Some(filter) = self.cache.get(&height) {
                if let Some(header) = tree.get_block_by_height(height) {
                    let block_hash = header.block_hash();
                    // Insert the cached filters into the processing queue.
                    self.received
                        .insert(height, (filter.clone(), block_hash, true));
                }
            }
        }

        // Heights to skip.
        // Note that cached heights will have been added to the `received` list.
        let mut skip: BTreeSet<Height> = BTreeSet::new();
        // Heights we've received but not processed.
        skip.extend(self.received.keys().cloned());
        // Heights we've already requested.
        skip.extend(&self.requested);

        // Iterate over requested ranges, taking care that heights are only requested once.
        // If there are gaps in the requested range after the difference is taken, split
        // the requests in groups of consecutive heights.
        let mut ranges: Vec<RangeInclusive<Height>> = Vec::new();
        for height in range.collect::<BTreeSet<_>>().difference(&skip) {
            if let Some(r) = ranges.last_mut() {
                if *height == r.end() + 1 {
                    *r = *r.start()..=r.end() + 1;
                    continue;
                }
            }
            // Either this is the first range request, or there is a gap between the previous
            // range and this height. Start a new range.
            let range = *height..=*height;

            ranges.push(range);
        }

        // Limit the requested ranges to `MAX_MESSAGE_CFILTERS`.
        let ranges: Vec<RangeInclusive<Height>> = ranges
            .into_iter()
            .flat_map(|r| HeightIterator {
                start: *r.start(),
                stop: *r.end(),
                step: MAX_MESSAGE_CFILTERS as Height,
            })
            .collect();

        for range in &ranges {
            self.requested.extend(range.clone());
        }
        ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nakamoto_common::network::Network;
    use nakamoto_test::block::cache::model;

    #[test]
    fn test_rescan_requests() {
        let mut rescan = Rescan::default();
        let t = model::Cache::new(Network::Mainnet.genesis());

        // Add a range that has already been requested.
        rescan.requested.extend(4..=5);
        // Now try to request an overlapping range.
        assert_eq!(rescan.requests(2..=10, &t), vec![2..=3, 6..=10]);

        rescan.requested.extend(7..=9);
        rescan.requested.extend(13..=20);
        assert_eq!(rescan.requests(8..=19, &t), vec![11..=12]);

        rescan.requested.clear();
        rescan.requested.extend(4..=6);
        rescan.requested.extend(9..=9);
        rescan.requested.extend(12..=14);

        assert_eq!(
            rescan.requests(0..=16, &t),
            vec![0..=3, 7..=8, 10..=11, 15..=16]
        );
    }
}
