//! Block tree implementation with generic storage backend.
//!
//! *Handles block import, chain selection, difficulty calculation and block storage.*
//!
#![warn(missing_docs)]

#[cfg(test)]
pub mod test;

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;
use bitcoin::network::constants::Network;
use bitcoin::util::BitArray;

use bitcoin::util::uint::Uint256;
use nakamoto_common::block::tree::{self, BlockTree, Branch, Error, ImportResult};
use nakamoto_common::block::{
    self,
    iter::Iter,
    store::Store,
    time::{self, Clock},
    Bits, BlockTime, Height, Work,
};
use nakamoto_common::nonempty::NonEmpty;

/// A block that is being stored by the block cache.
#[derive(Debug, Clone, Copy)]
struct CachedBlock {
    pub height: Height,
    pub hash: BlockHash,
    pub header: BlockHeader,
}

impl std::ops::Deref for CachedBlock {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl tree::Header for CachedBlock {
    fn work(&self) -> Work {
        self.header.work()
    }
}

/// A chain candidate, forking off the active chain.
#[derive(Debug)]
struct Candidate {
    tip: BlockHash,
    headers: Vec<BlockHeader>,
    fork_height: Height,
    fork_hash: BlockHash,
}

/// An implementation of [`BlockTree`] using a generic storage backend.
/// Most of the functionality is accessible via the trait.
///
/// [`BlockTree`]: ../../../nakamoto_common/block/tree/trait.BlockTree.html
///
#[derive(Debug, Clone)]
pub struct BlockCache<S: Store> {
    chain: NonEmpty<CachedBlock>,
    headers: HashMap<BlockHash, Height>,
    orphans: HashMap<BlockHash, BlockHeader>,
    checkpoints: BTreeMap<Height, BlockHash>,
    params: Params,
    store: S,
}

impl<S: Store<Header = BlockHeader>> BlockCache<S> {
    /// Create a new `BlockCache` from a `Store`, consensus parameters, and checkpoints.
    pub fn from(
        store: S,
        params: Params,
        checkpoints: &[(Height, BlockHash)],
    ) -> Result<Self, Error> {
        let genesis = store.genesis();
        let length = store.len()?;
        let orphans = HashMap::new();
        let checkpoints = checkpoints.iter().cloned().collect();

        let chain = NonEmpty::from((
            CachedBlock {
                height: 0,
                hash: genesis.block_hash(),
                header: genesis,
            },
            Vec::with_capacity(length - 1),
        ));
        let mut headers = HashMap::with_capacity(length);
        // Insert genesis in the headers map, but skip it during iteration.
        headers.insert(chain.head.hash, 0);

        let mut cache = Self {
            chain,
            headers,
            orphans,
            params,
            checkpoints,
            store,
        };

        for result in cache.store.iter().skip(1) {
            let (height, header) = result?;
            let hash = header.block_hash();

            cache.extend_chain(height, hash, header);
        }

        assert_eq!(length, cache.chain.len());
        assert_eq!(length, cache.headers.len());

        Ok(cache)
    }

    /// Iterate over a range of blocks.
    ///
    /// # Errors
    ///
    /// Panics if the range is negative.
    ///
    fn range<'a>(
        &'a self,
        range: std::ops::Range<Height>,
    ) -> impl Iterator<Item = &CachedBlock> + 'a {
        assert!(
            range.start <= range.end,
            "BlockCache::range: range start must not be greater than range end"
        );

        self.chain
            .iter()
            .skip(range.start as usize)
            .take((range.end - range.start) as usize)
    }

    /// Get the median time past for the blocks leading up to the given height.
    ///
    /// # Errors
    ///
    /// Panics if height is `0`.
    ///
    pub fn median_time_past(&self, height: Height) -> BlockTime {
        assert!(height != 0, "height must be > 0");

        let mut times = [0; time::MEDIAN_TIME_SPAN as usize];

        let start = height.saturating_sub(time::MEDIAN_TIME_SPAN);
        let end = height;

        for (i, blk) in self.range(start..end).enumerate() {
            times[i] = blk.time;
        }

        // Gracefully handle the case where `height` < `MEDIUM_TIME_SPAN`.
        let available = &mut times[0..(end - start) as usize];

        available.sort_unstable();
        available[available.len() / 2]
    }

    /// Import a block into the tree. Performs header validation. This function may trigger
    /// a chain re-org.
    fn import_block(
        &mut self,
        header: BlockHeader,
        clock: &impl Clock,
    ) -> Result<ImportResult, Error> {
        let hash = header.block_hash();
        let tip = self.chain.last();
        let best = tip.hash;

        // Block extends the active chain.
        if header.prev_blockhash == best {
            let height = tip.height + 1;

            self.validate(&tip, &header, clock)?;
            self.extend_chain(height, hash, header);
            self.store.put(std::iter::once(header))?;
        } else if self.headers.contains_key(&hash) || self.orphans.contains_key(&hash) {
            // FIXME: This shouldn't be an error.
            return Err(Error::DuplicateBlock(hash));
        } else {
            if let Some(height) = self.headers.get(&header.prev_blockhash) {
                // Don't accept any forks from the main chain, prior to the last checkpoint.
                if *height < self.last_checkpoint() {
                    return Err(Error::InvalidBlockHeight(*height + 1));
                }
            }

            // Validate that the block's PoW (1) is valid against its difficulty target, and (2)
            // is greater than the minimum allowed for this network.
            //
            // We do this because it's cheap to verify and prevents flooding attacks.
            let target = header.target();
            match header.validate_pow(&target) {
                Ok(_) => {
                    let limit = self.params.pow_limit;
                    if target > limit {
                        return Err(Error::InvalidBlockTarget(target, limit));
                    }
                }
                Err(bitcoin::util::Error::BlockBadProofOfWork) => {
                    return Err(Error::InvalidBlockPoW);
                }
                Err(bitcoin::util::Error::BlockBadTarget) => {
                    // The only way to get a 'bad target' error is to pass a different target
                    // than the one specified in the header.
                    unreachable!();
                }
                Err(_) => {
                    // We've handled all possible errors above.
                    unreachable!();
                }
            }
            self.orphans.insert(hash, header);
        }

        // Activate the chain with the most work.

        let candidates = self.chain_candidates(clock);

        // TODO: What are we trying to do here? We're saying that if there are no
        // forks, and this header has no parent, we return an error. But:
        //
        // If there are forks, it doesn't mean this header is part of one. It could
        // be a fork that already existed before this header was received.
        //
        // What we should do is simply: if the block has no parent (is orphan), we
        // know it's a no-op, ie. we won't discover a better branch. So we always
        // return the error without even checking for candidates. Otherwise, if
        // it *does* have a parent, we check for candidates.
        if candidates.is_empty()
            && !self.headers.contains_key(&header.prev_blockhash)
            && !self.orphans.contains_key(&header.prev_blockhash)
        {
            // FIXME: This shouldn't be an error.
            return Err(Error::BlockMissing(header.prev_blockhash));
        }

        // Find the best fork.
        //
        // Note that we can't compare candidates with each other directly, as they may have
        // different fork heights. So a candidate with less work than another may infact result
        // in a longer chain if selected, due to replacing less blocks on the active chain.
        let mut best_branch = None;
        let mut best_hash = self.chain.last().hash;
        let mut best_work = Uint256::zero();

        for branch in candidates.iter() {
            // Total work included in this branch.
            let candidate_work = Branch(&branch.headers).work();
            // Work included on the active chain that would be lost if we switched to the candidate
            // branch.
            let lost_work = Branch(self.chain_suffix(branch.fork_height)).work();
            // Not interested in candidates that result in a shorter chain.
            if candidate_work < lost_work {
                continue;
            }
            // Work added onto the main chain if this candidate were selected.
            let added = candidate_work - lost_work;

            // TODO: Validate branch before switching to it.
            if added > best_work {
                best_branch = Some(branch);
                best_work = added;
                best_hash = branch.tip;
            } else if self.params.network != Network::Bitcoin {
                if added == best_work {
                    // Nb. We intend here to compare the hashes as integers, and pick the lowest
                    // hash as the winner. However, the `PartialEq` on `BlockHash` is implemented on
                    // the underlying `[u8]` array, and does something different (lexographical
                    // comparison). Since this code isn't run on Mainnet, it's okay, as it serves
                    // its purpose of being determinstic when choosing the active chain.
                    if branch.tip < best_hash {
                        best_branch = Some(branch);
                        best_hash = branch.tip;
                    }
                }
            }
        }
        // TODO: Prune candidates that ended up as a prefix of the main chain.

        // Stale blocks after potential re-org.
        let mut stale = Vec::new();

        if let Some(branch) = best_branch {
            stale = self.switch_to_fork(branch)?;
        }

        let (hash, _) = self.tip();
        if hash != best {
            // TODO: Test the reverted blocks.
            Ok(ImportResult::TipChanged(
                header,
                hash,
                self.height(),
                stale
                    .into_iter()
                    .map(|(height, header)| (height, header.block_hash()))
                    .collect(),
            ))
        } else {
            Ok(ImportResult::TipUnchanged)
        }
    }

    /// Find all the potential forks off the main chain.
    fn chain_candidates(&self, clock: &impl Clock) -> Vec<Candidate> {
        let mut branches = Vec::new();

        for tip in self.orphans.keys() {
            if let Some(branch) = self.fork(tip) {
                if self.validate_branch(&branch, clock).is_ok() {
                    branches.push(branch);
                }
            }
        }
        branches
    }

    /// Find a potential branch starting from the active chain and ending at the given tip.
    /// The tip must be not be an active block. Returns `None` if no branch was found.
    ///
    /// # Errors
    ///
    /// Panics if the provided tip is on the active chain.
    ///
    fn fork(&self, tip: &BlockHash) -> Option<Candidate> {
        let tip = *tip;

        let mut headers = VecDeque::new();
        let mut cursor = tip;

        assert!(
            !self.headers.contains_key(&tip),
            "BlockCache::fork: the provided tip must not be on the active chain"
        );

        while let Some(header) = self.orphans.get(&cursor) {
            cursor = header.prev_blockhash;
            headers.push_front(*header);
        }

        if let Some(height) = self.headers.get(&cursor) {
            return Some(Candidate {
                tip,
                fork_height: *height,
                fork_hash: cursor,
                headers: headers.into(),
            });
        }
        None
    }

    /// Validate a candidate branch. This function is useful for chain selection.
    fn validate_branch(&self, candidate: &Candidate, clock: &impl Clock) -> Result<(), Error> {
        let fork_header = self
            .get_block_by_height(candidate.fork_height)
            .expect("the given candidate must fork from a known block");
        let mut tip = CachedBlock {
            height: candidate.fork_height,
            hash: candidate.fork_hash,
            header: *fork_header,
        };

        for header in candidate.headers.iter() {
            self.validate(&tip, header, clock)?;

            tip = CachedBlock {
                height: tip.height + 1,
                hash: header.block_hash(),
                header: *header,
            };
        }
        Ok(())
    }

    /// Validate a block header as a potential new tip. This performs full header validation.
    fn validate(
        &self,
        tip: &CachedBlock,
        header: &BlockHeader,
        clock: &impl Clock,
    ) -> Result<(), Error> {
        assert_eq!(tip.hash, header.prev_blockhash);

        let compact_target = if self.params.allow_min_difficulty_blocks
            && (tip.height + 1) % self.params.difficulty_adjustment_interval() != 0
        {
            if header.time > tip.time + self.params.pow_target_spacing as BlockTime * 2 {
                block::pow_limit_bits(&self.params.network)
            } else {
                self.next_min_difficulty_target(&self.params)
            }
        } else {
            self.next_difficulty_target(tip.height, tip.time, tip.target(), &self.params)
        };

        let target = BlockHeader::u256_from_compact_target(compact_target);

        match header.validate_pow(&target) {
            Err(bitcoin::util::Error::BlockBadProofOfWork) => {
                return Err(Error::InvalidBlockPoW);
            }
            Err(bitcoin::util::Error::BlockBadTarget) => {
                return Err(Error::InvalidBlockTarget(header.target(), target));
            }
            Err(_) => unreachable!(),
            Ok(_) => {}
        }

        // Validate against block checkpoints.
        let height = tip.height + 1;

        if let Some(checkpoint) = self.checkpoints.get(&height) {
            let hash = header.block_hash();

            if &hash != checkpoint {
                return Err(Error::InvalidBlockHash(hash, height));
            }
        }

        // A timestamp is accepted as valid if it is greater than the median timestamp of
        // the previous MEDIAN_TIME_SPAN blocks, and less than the network-adjusted
        // time + MAX_FUTURE_BLOCK_TIME.
        if header.time <= self.median_time_past(height) {
            return Err(Error::InvalidBlockTime(header.time, Ordering::Less));
        }
        if header.time > clock.block_time() + time::MAX_FUTURE_BLOCK_TIME {
            return Err(Error::InvalidBlockTime(header.time, Ordering::Greater));
        }

        Ok(())
    }

    /// Get the height of the last checkpoint block.
    fn last_checkpoint(&self) -> Height {
        let height = self.height();

        self.checkpoints
            .iter()
            .rev()
            .map(|(h, _)| *h)
            .find(|h| *h <= height)
            .unwrap_or(0)
    }

    /// Get the next minimum-difficulty target. Only valid in testnet and regtest networks.
    fn next_min_difficulty_target(&self, params: &Params) -> Bits {
        assert!(params.allow_min_difficulty_blocks);

        let pow_limit_bits = block::pow_limit_bits(&params.network);

        for (height, header) in self.iter().rev() {
            if header.bits != pow_limit_bits
                || height % self.params.difficulty_adjustment_interval() == 0
            {
                return header.bits;
            }
        }
        pow_limit_bits
    }

    /// Rollback active chain to the given height. Returns the list of rolled-back headers.
    fn rollback(&mut self, height: Height) -> Result<Vec<(Height, BlockHeader)>, Error> {
        let mut stale = Vec::new();

        for (block, height) in self.chain.tail.drain(height as usize..).zip(height + 1..) {
            stale.push((height, block.header));

            self.headers.remove(&block.hash);
            self.orphans.insert(block.hash, block.header);
        }
        self.store.rollback(height)?;

        Ok(stale)
    }

    /// Activate a fork candidate. Returns the list of rolled-back (stale) headers.
    fn switch_to_fork(&mut self, branch: &Candidate) -> Result<Vec<(Height, BlockHeader)>, Error> {
        let stale = self.rollback(branch.fork_height)?;

        for (i, header) in branch.headers.iter().enumerate() {
            self.extend_chain(
                branch.fork_height + i as Height + 1,
                header.block_hash(),
                *header,
            );
        }
        self.store.put(branch.headers.iter().cloned())?;

        Ok(stale)
    }

    /// Extend the active chain with a block.
    fn extend_chain(&mut self, height: Height, hash: BlockHash, header: BlockHeader) {
        assert_eq!(header.prev_blockhash, self.chain.last().hash);

        self.headers.insert(hash, height);
        self.orphans.remove(&hash);
        self.chain.push(CachedBlock {
            height,
            hash,
            header,
        });
    }

    /// Get the blocks starting from the given height.
    fn chain_suffix(&self, height: Height) -> &[CachedBlock] {
        &self.chain.tail[height as usize..]
    }
}

impl<S: Store<Header = BlockHeader>> BlockTree for BlockCache<S> {
    /// Import blocks into the block tree. Blocks imported this way don't have to form a chain.
    fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        chain: I,
        context: &C,
    ) -> Result<ImportResult, Error> {
        let mut result = None;
        let mut reverted = BTreeSet::new();

        // FIXME: Remove this function!

        for (i, header) in chain.enumerate() {
            match self.import_block(header, context) {
                Ok(ImportResult::TipChanged(header, hash, height, r)) => {
                    reverted.extend(r);
                    reverted.retain(|(_, h)| !self.contains(h));
                    result = Some(ImportResult::TipChanged(
                        header,
                        hash,
                        height,
                        reverted.iter().cloned().collect(),
                    ));
                }
                Ok(r @ ImportResult::TipUnchanged) => {
                    result = Some(r);
                }
                Err(Error::DuplicateBlock(hash)) => log::trace!("Duplicate block {}", hash),
                Err(Error::BlockMissing(hash)) => log::trace!("Missing block {}", hash),
                Err(err) => return Err(Error::BlockImportAborted(err.into(), i, self.height())),
            }
        }
        Ok(result.unwrap_or(ImportResult::TipUnchanged))
    }

    /// Extend the active chain.
    fn extend_tip<C: Clock>(
        &mut self,
        header: BlockHeader,
        clock: &C,
    ) -> Result<ImportResult, Error> {
        let tip = self.chain.last();
        let hash = header.block_hash();

        if header.prev_blockhash == tip.hash {
            let height = tip.height + 1;

            self.validate(&tip, &header, clock)?;
            self.extend_chain(height, hash, header);
            self.store.put(std::iter::once(header))?;

            Ok(ImportResult::TipChanged(header, hash, height, vec![]))
        } else {
            Ok(ImportResult::TipUnchanged)
        }
    }

    /// Get a block by hash. Only searches the active chain.
    fn get_block(&self, hash: &BlockHash) -> Option<(Height, &BlockHeader)> {
        self.headers
            .get(hash)
            .and_then(|height| self.chain.get(*height as usize))
            .map(|blk| (blk.height, &blk.header))
    }

    /// Get a block by height.
    fn get_block_by_height(&self, height: Height) -> Option<&BlockHeader> {
        self.chain.get(height as usize).map(|b| &b.header)
    }

    /// Get the best block hash and header.
    fn tip(&self) -> (BlockHash, BlockHeader) {
        (self.chain.last().hash, self.chain.last().header)
    }

    /// Get the genesis block header.
    fn genesis(&self) -> &BlockHeader {
        &self.chain.first().header
    }

    /// Iterate over the longest chain, starting from genesis.
    fn iter<'a>(&'a self) -> Box<dyn DoubleEndedIterator<Item = (Height, BlockHeader)> + 'a> {
        Box::new(Iter::new(&self.chain).map(|(i, h)| (i, h.header)))
    }

    /// Iterate over a range of blocks.
    fn range<'a>(
        &'a self,
        range: std::ops::Range<Height>,
    ) -> Box<dyn Iterator<Item = (Height, BlockHash)> + 'a> {
        Box::new(
            self.chain
                .iter()
                .map(|block| (block.height, block.hash))
                .skip(range.start as usize)
                .take((range.end - range.start) as usize),
        )
    }

    /// Return the height of the longest chain.
    fn height(&self) -> Height {
        self.chain.tail.len() as Height
    }

    /// Check whether this block hash is known.
    fn is_known(&self, hash: &BlockHash) -> bool {
        self.headers.contains_key(hash) || self.orphans.contains_key(hash)
    }

    /// Check whether this block hash is part of the active chain.
    fn contains(&self, hash: &BlockHash) -> bool {
        self.headers.contains_key(hash)
    }

    /// Return headers after the first known hash in the locators list, and until the stop hash
    /// is reached.
    ///
    /// This function will never return more than `max_headers`.
    ///
    /// * When no locators are provided, the stop hash is treated as a request for that header
    ///   alone.
    /// * When locators *are* provided, but none of them are known, it is equivalent to having
    ///   the genesis hash as locator.
    ///
    fn locate_headers(
        &self,
        locators: &[BlockHash],
        stop_hash: BlockHash,
        max_headers: usize,
    ) -> Vec<BlockHeader> {
        if locators.is_empty() {
            if let Some((_, header)) = self.get_block(&stop_hash) {
                return vec![*header];
            }
            return vec![];
        }

        // Start from the highest locator hash that is on our active chain.
        // We don't respond with anything if none of the locators were found.
        let start = if let Some(hash) = locators.iter().find(|h| self.contains(h)) {
            let (height, _) = self.get_block(hash).unwrap();
            height
        } else {
            0
        };

        let start = start + 1;
        let stop = self
            .get_block(&stop_hash)
            .map(|(h, _)| h)
            .unwrap_or_else(|| self.height());
        let stop = Height::min(start + max_headers as Height, stop + 1);

        if start > stop {
            return vec![];
        }

        self.range(start..stop).map(|h| h.header).collect()
    }

    /// Get the locator hashes for the active chain, starting at the given height.
    ///
    /// *Panics* if the given starting height is out of bounds.
    ///
    fn locator_hashes(&self, from: Height) -> Vec<BlockHash> {
        let mut hashes = Vec::new();

        assert!(from <= self.height());

        let last_checkpoint = self.last_checkpoint();

        for height in block::locators_indexes(from).into_iter() {
            if height < last_checkpoint {
                // Don't go past the latest checkpoint. We never want to accept a fork
                // older than our last checkpoint.
                break;
            }
            if let Some(blk) = self.chain.get(height as usize) {
                hashes.push(blk.hash);
            }
        }
        hashes
    }
}
