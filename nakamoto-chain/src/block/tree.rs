use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;

use thiserror::Error;

use crate::block::store;
use crate::block::time::Clock;
use crate::block::{self, Bits, Height, Target, Time, Work};

/// An error related to the block tree.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid block proof-of-work")]
    InvalidBlockPoW,
    #[error("invalid block difficulty target: {0}, expected {1}")]
    InvalidBlockTarget(Target, Target),
    #[error("invalid checkpoint block hash {0} at height {1}")]
    InvalidBlockHash(BlockHash, Height),
    #[error("block height {0} is prior to last checkpoint")]
    InvalidBlockHeight(Height),
    #[error("block timestamp {0} is invalid")]
    InvalidTimestamp(Time, std::cmp::Ordering),
    #[error("duplicate block {0}")]
    DuplicateBlock(BlockHash),
    #[error("invalid chain")]
    InvalidChain,
    #[error("empty chain")]
    EmptyChain,
    #[error("block missing: {0}")]
    BlockMissing(BlockHash),
    #[error("block import aborted at height {2}: {0} ({1} block(s) imported)")]
    BlockImportAborted(Box<Self>, usize, Height),
    #[error("storage error: {0}")]
    Store(#[from] store::Error),
}

/// A generic block header.
pub trait Header {
    fn work(&self) -> Work;
}

impl Header for BlockHeader {
    fn work(&self) -> Work {
        self.work()
    }
}

/// The outcome of a successful block header import.
#[derive(Debug)]
pub enum ImportResult {
    /// A new tip was found. This can happen in either of two scenarios:
    ///
    /// 1. The imported block(s) extended the active chain, or
    /// 2. The imported block(s) caused a chain re-org. In that case, the last field is
    ///    populated with the now stale blocks.
    ///
    TipChanged(BlockHash, Height, Vec<BlockHash>),
    /// The block headers were imported successfully, but our best block hasn't changed.
    /// This will happen if we imported a duplicate, orphan or stale block.
    TipUnchanged,
}

#[derive(Debug, Clone)]
pub struct Branch<'a, H: Header>(pub &'a [H]);

impl<'a, H: Header> Branch<'a, H> {
    pub fn work(&self) -> Work {
        let mut work = Work::default();
        for header in self.0.iter() {
            work = work + header.work();
        }
        work
    }
}

/// A representation of all known blocks that keeps track of the longest chain.
pub trait BlockTree {
    /// Import a chain of block headers into the block tree.
    fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        chain: I,
        context: &C,
    ) -> Result<ImportResult, Error>;
    /// Get a block by hash.
    fn get_block(&self, hash: &BlockHash) -> Option<(Height, &BlockHeader)>;
    /// Get a block by height.
    fn get_block_by_height(&self, height: Height) -> Option<&BlockHeader>;
    /// Iterate over the longest chain, starting from genesis.
    fn chain(&self) -> Box<dyn Iterator<Item = BlockHeader>> {
        Box::new(self.iter().map(|(_, h)| h))
    }
    /// Iterate over the longest chain, starting from genesis, including heights.
    fn iter(&self) -> Box<dyn Iterator<Item = (Height, BlockHeader)>>;
    /// Iterate over a range of blocks.
    fn range(&self, range: std::ops::Range<Height>) -> Box<dyn Iterator<Item = BlockHeader>> {
        // TODO: Don't box twice?
        Box::new(
            self.iter()
                .map(|(_, h)| h)
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
    /// Return the genesis block header.
    fn genesis(&self) -> &BlockHeader {
        self.get_block_by_height(0)
            .expect("the genesis block is always present")
    }
    /// Check whether a block hash is known.
    fn is_known(&self, hash: &BlockHash) -> bool;
    /// Check whether a block hash is part of the active chain.
    fn contains(&self, hash: &BlockHash) -> bool;
    /// Get the locator hashes starting from the given height and going backwards.
    fn locators_hashes(&self, from: Height) -> Vec<BlockHash>;
    /// Get the next difficulty given a block height, time and bits.
    fn next_difficulty_target(
        &self,
        last_height: Height,
        last_time: Time,
        last_bits: Bits,
        params: &Params,
    ) -> Bits {
        // Only adjust on set intervals. Otherwise return current target.
        // Since the height is 0-indexed, we add `1` to check it against the interval.
        if (last_height + 1) % params.difficulty_adjustment_interval() != 0 {
            return last_bits;
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

        if actual_timespan < params.pow_target_timespan as Time / 4 {
            adjusted_timespan = params.pow_target_timespan as Time / 4;
        } else if actual_timespan > params.pow_target_timespan as Time * 4 {
            adjusted_timespan = params.pow_target_timespan as Time * 4;
        }

        let mut target = block::target_from_bits(last_bits);

        target = target.mul_u32(adjusted_timespan);
        target = target / Target::from_u64(params.pow_target_timespan).unwrap();

        // Ensure a difficulty floor.
        if target > params.pow_limit {
            target = params.pow_limit;
        }

        BlockHeader::compact_target_from_u256(&target)
    }
}
