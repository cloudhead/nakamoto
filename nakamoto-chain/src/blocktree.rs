use std::ops::Deref;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;
use bitcoin::util::hash::BitcoinHash;

use nonempty::NonEmpty;
use thiserror::Error;

/// Difficulty target of a block.
type Target = bitcoin::util::uint::Uint256;

/// Height of a block.
type Height = u64;

/// Block timestamp.
type Time = u32;

/// An error related to the block tree.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid block")]
    InvalidBlock,
    #[error("invalid chain")]
    InvalidChain,
    #[error("empty chain")]
    EmptyChain,
    #[error("bitcoin error")]
    Bitcoin(#[from] bitcoin::util::Error),
}

/// A representation of all known blocks that keeps track of the longest chain.
pub trait BlockTree {
    /// Import a chain of block headers into the block tree.
    fn import_blocks(&mut self, chain: Vec<BlockHeader>) -> Result<(BlockHash, Height), Error>;
    /// Get a block by height.
    fn get_block_by_height(&self, height: Height) -> Option<&CachedBlock>;
    /// Iterate over the longest chain, starting from the tip.
    fn chain(&self) -> &NonEmpty<CachedBlock>;
    /// Return the height of the longest chain.
    fn height(&self) -> Height;
    /// Return the tip of the longest chain.
    fn tip(&self) -> &BlockHash;
    /// Get the cached tip of the longest chain.
    fn get_tip(&self) -> &CachedBlock;
    /// Get the next target difficulty.
    fn next_difficulty_target(&self, params: &Params) -> Target {
        self.difficulty_target(self.height() + 1, params).unwrap()
    }
    /// Get the target difficulty at a certain block height.
    fn difficulty_target(&self, height: Height, params: &Params) -> Option<Target> {
        let block = self.get_block_by_height(height)?;
        let time = block.time;

        // Only adjust on set intervals. Otherwise return current target.
        // Since the height is 0-indexed, we add `1` to check it against the interval.
        if (height + 1) % params.difficulty_adjustment_interval() != 0 {
            return Some(self.get_tip().target());
        }

        let last_adjustment_height =
            height.saturating_sub(params.difficulty_adjustment_interval() - 1);
        let last_adjustment_block = self.get_block_by_height(last_adjustment_height)?;
        let last_adjustment_time = last_adjustment_block.header.time;

        let mut actual_timespan = time - last_adjustment_time;

        if actual_timespan < params.pow_target_timespan as Time / 4 {
            actual_timespan = params.pow_target_timespan as Time / 4;
        }
        if actual_timespan > params.pow_target_timespan as Time * 4 {
            actual_timespan = params.pow_target_timespan as Time * 4;
        }

        let mut target = block.target();
        target = target.mul_u32(actual_timespan);
        target = target / Target::from_u64(params.pow_target_timespan).unwrap();

        // Ensure a difficulty floor.
        if target > params.pow_limit {
            target = params.pow_limit;
        }

        Some(target)
    }
}

#[derive(Debug)]
pub struct CachedBlock {
    hash: BlockHash,
    header: BlockHeader,
}

impl Deref for CachedBlock {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

/// An implementation of `BlockTree`.
#[derive(Debug)]
pub struct BlockCache {
    chain: NonEmpty<CachedBlock>,
    params: Params,
}

impl BlockCache {
    /// Create a new `BlockCache` given the genesis block, and consensus parameters.
    pub fn new(genesis: BlockHeader, params: Params) -> Self {
        Self {
            chain: NonEmpty::new(CachedBlock {
                hash: genesis.bitcoin_hash(),
                header: genesis,
            }),
            params,
        }
    }

    fn insert_block(
        &mut self,
        hash: BlockHash,
        header: BlockHeader,
    ) -> Result<(BlockHash, Height), Error> {
        let tip = self.get_tip();

        if header.prev_blockhash == tip.hash {
            // TODO: Validate timestamp.
            let target = self.next_difficulty_target(&self.params);

            match header.validate_pow(&target) {
                Err(bitcoin::util::Error::BlockBadProofOfWork) => {
                    return Err(Error::InvalidBlock);
                }
                Err(bitcoin::util::Error::BlockBadTarget) => {
                    return Err(Error::InvalidBlock);
                }
                Err(err) => {
                    return Err(Error::Bitcoin(err));
                }
                Ok(_) => {}
            }

            self.chain.push(CachedBlock { hash, header });

            Ok((hash, self.height()))
        } else {
            todo!() // TODO: Should be an error.
        }
    }
}

impl BlockTree for BlockCache {
    fn import_blocks(&mut self, chain: Vec<BlockHeader>) -> Result<(BlockHash, Height), Error> {
        let chain = NonEmpty::from_vec(chain).ok_or(Error::EmptyChain)?;
        let mut result = self.insert_block(chain.head.bitcoin_hash(), chain.head)?;

        for header in chain.tail.into_iter() {
            result = self.insert_block(header.bitcoin_hash(), header)?;
        }
        Ok(result)
    }

    fn get_block_by_height(&self, height: Height) -> Option<&CachedBlock> {
        self.chain.get(height as usize)
    }

    fn get_tip(&self) -> &CachedBlock {
        self.chain.last()
    }

    /// Iterate over the longest chain, starting from the tip.
    fn chain(&self) -> &NonEmpty<CachedBlock> {
        &self.chain
    }

    /// Return the height of the longest chain.
    fn height(&self) -> Height {
        self.chain.tail.len() as Height
    }

    /// Return the tip of the longest chain.
    fn tip(&self) -> &BlockHash {
        &self.chain.last().hash
    }
}
