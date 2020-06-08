use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;

use nonempty::NonEmpty;
use thiserror::Error;

use crate::block::store;
use crate::block::{self, Bits, CachedBlock, Height, Target, Time};

/// An error related to the block tree.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid block proof-of-work")]
    InvalidBlockPoW,
    #[error("invalid block difficulty target: {0}, expected {1}")]
    InvalidBlockTarget(Target, Target),
    #[error("invalid checkpoint block hash {0} at height {1}")]
    InvalidBlockHash(BlockHash, Height),
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

/// A representation of all known blocks that keeps track of the longest chain.
pub trait BlockTree {
    /// Import a chain of block headers into the block tree.
    fn import_blocks<I: Iterator<Item = BlockHeader>>(
        &mut self,
        chain: I,
    ) -> Result<(BlockHash, Height), Error>;
    /// Get a block by height.
    fn get_block_by_height(&self, height: Height) -> Option<&CachedBlock>;
    /// Iterate over the longest chain, starting from the tip.
    fn chain(&self) -> &NonEmpty<CachedBlock>;
    /// Return the height of the longest chain.
    fn height(&self) -> Height;
    /// Get the cached tip of the longest chain.
    fn get_tip(&self) -> &CachedBlock;
    /// Return the tip of the longest chain.
    fn tip(&self) -> &BlockHash {
        &self.get_tip().hash
    }
    /// Return the genesis block header.
    fn genesis(&self) -> &CachedBlock {
        self.get_block_by_height(0)
            .expect("the genesis block is always present")
    }
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
            .unwrap_or(self.genesis());
        let last_adjustment_time = last_adjustment_block.header.time;

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
