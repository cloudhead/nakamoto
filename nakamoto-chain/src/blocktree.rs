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
    #[error("invalid block proof-of-work")]
    InvalidBlockPoW,
    #[error("invalid block difficulty target: 0x{0:x}, expected 0x{1:x}")]
    InvalidBlockTarget(u32, u32),
    #[error("invalid chain")]
    InvalidChain,
    #[error("empty chain")]
    EmptyChain,
    #[error("block ignored: {0}")]
    BlockIgnored(BlockHash),
    #[error("block import aborted at height {2}: {0} ({1} block(s) imported)")]
    BlockImportAborted(Box<Self>, usize, Height),
    #[error("bitcoin error")]
    Bitcoin(#[from] bitcoin::util::Error),
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
    /// Return the tip of the longest chain.
    fn tip(&self) -> &BlockHash;
    /// Get the cached tip of the longest chain.
    fn get_tip(&self) -> &CachedBlock;
    /// Get the next target difficulty.
    fn next_difficulty_target(&self, params: &Params) -> Target {
        self.difficulty_target(self.height(), params).unwrap()
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

        if params.no_pow_retargeting {
            return Some(last_adjustment_block.target());
        }

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
                    return Err(Error::InvalidBlockPoW);
                }
                Err(bitcoin::util::Error::BlockBadTarget) => {
                    return Err(Error::InvalidBlockTarget(
                        header.bits,
                        BlockHeader::compact_target_from_u256(&target),
                    ));
                }
                Err(err) => {
                    return Err(Error::Bitcoin(err));
                }
                Ok(_) => {}
            }

            self.chain.push(CachedBlock { hash, header });

            Ok((hash, self.height()))
        } else {
            Err(Error::BlockIgnored(hash))
        }
    }
}

impl BlockTree for BlockCache {
    fn import_blocks<I: Iterator<Item = BlockHeader>>(
        &mut self,
        chain: I,
    ) -> Result<(BlockHash, Height), Error> {
        let mut result = None;

        for (i, header) in chain.enumerate() {
            match self.insert_block(header.bitcoin_hash(), header) {
                Ok(r) => result = Some(r),
                Err(err) => return Err(Error::BlockImportAborted(err.into(), i, self.height())),
            }
        }
        Ok(result.unwrap_or((*self.tip(), self.height())))
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

#[cfg(test)]
mod test {
    use super::{BlockCache, BlockTree, Height, Target, Time};

    use quickcheck::{self, Arbitrary, Gen, TestResult};
    use quickcheck_macros::quickcheck;

    use nonempty::NonEmpty;
    use rand::Rng;

    use bitcoin::blockdata::block::BlockHeader;
    use bitcoin::consensus::params::Params;
    use bitcoin::hash_types::{BlockHash, TxMerkleNode};
    use bitcoin::util::hash::BitcoinHash;
    use bitcoin::util::uint::Uint256;

    // Lowest possible difficulty.
    const TARGET: Uint256 = Uint256([
        0xffffffffffffffffu64,
        0xffffffffffffffffu64,
        0xffffffffffffffffu64,
        0x7fffffffffffffffu64,
    ]);

    #[derive(Clone)]
    struct TestCase(NonEmpty<BlockHeader>);

    impl Arbitrary for TestCase {
        fn arbitrary<G: Gen>(g: &mut G) -> TestCase {
            let height = g.gen_range(0, 256);
            Self(arbitrary_chain(height, g))
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let Self(chain) = self;
            let mut shrunk = Vec::new();

            if let Some((_, rest)) = chain.tail.split_last() {
                shrunk.push(Self(NonEmpty::from((chain.head, rest.to_vec()))));
            }
            Box::new(shrunk.into_iter())
        }
    }

    impl std::fmt::Debug for TestCase {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            write!(fmt, "\n")?;

            for (height, header) in self.0.iter().enumerate() {
                writeln!(
                    fmt,
                    "#{:03} {} time={:05} bits={:x} nonce={}",
                    height,
                    header.bitcoin_hash(),
                    header.time,
                    header.bits,
                    header.nonce
                )?;
            }
            Ok(())
        }
    }

    fn arbitrary_header<G: Gen>(
        prev_blockhash: BlockHash,
        prev_time: Time,
        target: &Target,
        g: &mut G,
    ) -> BlockHeader {
        let delta = u32::arbitrary(g);

        let time = if delta == 0 {
            prev_time
        } else if delta < prev_time && g.gen_bool(1. / 100.) {
            // Small probability that this block's timestamp is in the past.
            g.gen_range(prev_time.saturating_sub(delta), prev_time)
        } else {
            g.gen_range(prev_time, prev_time + delta)
        };

        let bits = BlockHeader::compact_target_from_u256(&target);

        let mut header = BlockHeader {
            version: 1,
            time,
            nonce: 0,
            bits,
            merkle_root: TxMerkleNode::default(),
            prev_blockhash,
        };

        let target = header.target();
        while header.validate_pow(&target).is_err() {
            header.nonce += 1;
        }

        header
    }

    fn arbitrary_chain<G: Gen>(height: Height, g: &mut G) -> NonEmpty<BlockHeader> {
        let mut prev_time = 0; // Epoch.
        let mut prev_hash = BlockHash::default();

        let genesis = arbitrary_header(prev_hash, prev_time, &TARGET, g);
        let mut chain = NonEmpty::new(genesis);

        prev_hash = genesis.bitcoin_hash();
        prev_time = genesis.time;

        for _ in 0..height {
            let header = arbitrary_header(prev_hash, prev_time, &TARGET, g);
            prev_time = header.time;
            prev_hash = header.bitcoin_hash();

            chain.push(header);
        }
        chain
    }

    #[quickcheck]
    fn test_block_import(TestCase(chain): TestCase) -> TestResult {
        let params = Params {
            pow_limit: TARGET,
            pow_target_timespan: 12 * 60 * 60, // 12 hours.
            ..Params::new(bitcoin::Network::Bitcoin)
        };
        let mut cache = BlockCache::new(chain.head, params);

        if chain.tail.is_empty() {
            return TestResult::discard();
        }

        match cache.import_blocks(chain.tail.into_iter()) {
            Ok(_) => TestResult::passed(),
            Err(err) => TestResult::error(err.to_string()),
        }
    }
}
