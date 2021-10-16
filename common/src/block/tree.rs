//! Types and functions relating to block trees.
#![warn(missing_docs)]
use std::collections::BTreeMap;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;

use thiserror::Error;

use crate::block::store;
use crate::block::time::Clock;
use crate::block::{Bits, BlockTime, Height, Target, Work};
use crate::nonempty::NonEmpty;

/// An error related to the block tree.
#[derive(Debug, Error)]
pub enum Error {
    /// The block's proof-of-work is invalid.
    #[error("invalid block proof-of-work")]
    InvalidBlockPoW,

    /// The block's difficulty target is invalid.
    #[error("invalid block difficulty target: {0}, expected {1}")]
    InvalidBlockTarget(Target, Target),

    /// The block's hash doesn't match the checkpoint.
    #[error("invalid checkpoint block hash {0} at height {1}")]
    InvalidBlockHash(BlockHash, Height),

    /// The block forks off the main chain prior to the last checkpoint.
    #[error("block height {0} is prior to last checkpoint")]
    InvalidBlockHeight(Height),

    /// The block timestamp is invalid.
    #[error("block timestamp {0} is invalid")]
    InvalidBlockTime(BlockTime, std::cmp::Ordering),

    /// The block is already known.
    #[error("duplicate block {0}")]
    DuplicateBlock(BlockHash),

    /// The block is orphan.
    #[error("block missing: {0}")]
    BlockMissing(BlockHash),

    /// A block import was aborted. FIXME: Move this error out of here.
    #[error("block import aborted at height {2}: {0} ({1} block(s) imported)")]
    BlockImportAborted(Box<Self>, usize, Height),

    /// A storage error occured.
    #[error("storage error: {0}")]
    Store(#[from] store::Error),
}

/// A generic block header.
pub trait Header {
    /// Return the proof-of-work of this header.
    fn work(&self) -> Work;
}

impl Header for BlockHeader {
    fn work(&self) -> Work {
        self.work()
    }
}

/// The outcome of a successful block header import.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportResult {
    /// A new tip was found. This can happen in either of two scenarios:
    ///
    /// 1. The imported block(s) extended the active chain, or
    /// 2. The imported block(s) caused a chain re-org. In that case, the last field is
    ///    populated with the now stale blocks.
    ///
    TipChanged(
        BlockHeader,
        BlockHash,
        Height,
        Vec<(Height, BlockHash)>,
        NonEmpty<(Height, BlockHeader)>,
    ),
    /// The block headers were imported successfully, but our best block hasn't changed.
    /// This will happen if we imported a duplicate, orphan or stale block.
    TipUnchanged, // TODO: We could add a parameter eg. BlockMissing or DuplicateBlock.
}

/// A chain of block headers that may or may not lead back to genesis.
#[derive(Debug, Clone)]
pub struct Branch<'a, H: Header>(pub &'a [H]);

impl<'a, H: Header> Branch<'a, H> {
    /// Compute the total proof-of-work carried by this branch.
    pub fn work(&self) -> Work {
        let mut work = Work::default();
        for header in self.0.iter() {
            work = work + header.work();
        }
        work
    }
}

/// A representation of all known blocks that keeps track of the longest chain.
pub trait BlockTree: BlockReader {
    /// Import a chain of block headers into the block tree.
    fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        chain: I,
        context: &C,
    ) -> Result<ImportResult, Error>;
    /// Attempts to extend the active chain. Returns `Ok` with `ImportResult::TipUnchanged` if
    /// the block didn't connect, and `Err` if the block was invalid.
    fn extend_tip<C: Clock>(
        &mut self,
        header: BlockHeader,
        context: &C,
    ) -> Result<ImportResult, Error>;
}

/// Read block header state.
pub trait BlockReader {
    /// Get a block by hash.
    fn get_block(&self, hash: &BlockHash) -> Option<(Height, &BlockHeader)>;
    /// Get a block by height.
    fn get_block_by_height(&self, height: Height) -> Option<&BlockHeader>;
    /// Find a path from the active chain to the provided (stale) block hash.
    ///
    /// If a path is found, the height of the start/fork block is returned, along with the
    /// headers up to and including the tip, forming a branch.
    ///
    /// If the given block is on the active chain, its height and header is returned.
    fn find_branch(&self, to: &BlockHash) -> Option<(Height, NonEmpty<BlockHeader>)>;
    /// Iterate over the longest chain, starting from genesis.
    fn chain<'a>(&'a self) -> Box<dyn Iterator<Item = BlockHeader> + 'a> {
        Box::new(self.iter().map(|(_, h)| h))
    }
    /// Iterate over the longest chain, starting from genesis, including heights.
    fn iter<'a>(&'a self) -> Box<dyn DoubleEndedIterator<Item = (Height, BlockHeader)> + 'a>;
    /// Iterate over a range of blocks.
    fn range<'a>(
        &'a self,
        range: std::ops::Range<Height>,
    ) -> Box<dyn Iterator<Item = (Height, BlockHash)> + 'a> {
        Box::new(
            self.iter()
                .map(|(height, header)| (height, header.block_hash()))
                .skip(range.start as usize)
                .take((range.end - range.start) as usize),
        )
    }
    /// Return the height of the longest chain.
    fn height(&self) -> Height;
    /// Get the tip of the longest chain.
    fn tip(&self) -> (BlockHash, BlockHeader);
    /// Get the last block of the longest chain.
    fn best_block(&self) -> (Height, &BlockHeader) {
        let height = self.height();
        (
            height,
            self.get_block_by_height(height)
                .expect("the best block is always present"),
        )
    }
    /// Get the height of the last checkpoint block.
    fn last_checkpoint(&self) -> Height;
    /// Known checkpoints.
    fn checkpoints(&self) -> BTreeMap<Height, BlockHash>;
    /// Return the genesis block header.
    fn genesis(&self) -> &BlockHeader {
        self.get_block_by_height(0)
            .expect("the genesis block is always present")
    }
    /// Check whether a block hash is known.
    fn is_known(&self, hash: &BlockHash) -> bool;
    /// Check whether a block hash is part of the active chain.
    fn contains(&self, hash: &BlockHash) -> bool;
    /// Return the headers corresponding to the given locators, up to a maximum.
    fn locate_headers(
        &self,
        locators: &[BlockHash],
        stop_hash: BlockHash,
        max_headers: usize,
    ) -> Vec<BlockHeader>;
    /// Get the locator hashes starting from the given height and going backwards.
    fn locator_hashes(&self, from: Height) -> Vec<BlockHash>;
    /// Get the next difficulty given a block height, time and bits.
    fn next_difficulty_target(
        &self,
        last_height: Height,
        last_time: BlockTime,
        last_target: Target,
        params: &Params,
    ) -> Bits {
        // Only adjust on set intervals. Otherwise return current target.
        // Since the height is 0-indexed, we add `1` to check it against the interval.
        if (last_height + 1) % params.difficulty_adjustment_interval() != 0 {
            return BlockHeader::compact_target_from_u256(&last_target);
        }

        let last_adjustment_height =
            last_height.saturating_sub(params.difficulty_adjustment_interval() - 1);
        let last_adjustment_block = self
            .get_block_by_height(last_adjustment_height)
            .unwrap_or_else(|| self.genesis());
        let last_adjustment_time = last_adjustment_block.time;

        if params.no_pow_retargeting {
            return last_adjustment_block.bits;
        }

        let actual_timespan = last_time - last_adjustment_time;
        let mut adjusted_timespan = actual_timespan;

        if actual_timespan < params.pow_target_timespan as BlockTime / 4 {
            adjusted_timespan = params.pow_target_timespan as BlockTime / 4;
        } else if actual_timespan > params.pow_target_timespan as BlockTime * 4 {
            adjusted_timespan = params.pow_target_timespan as BlockTime * 4;
        }

        let mut target = last_target;

        target = target.mul_u32(adjusted_timespan);
        target = target / Target::from_u64(params.pow_target_timespan).unwrap();

        // Ensure a difficulty floor.
        if target > params.pow_limit {
            target = params.pow_limit;
        }

        BlockHeader::compact_target_from_u256(&target)
    }
}
