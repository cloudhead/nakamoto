use std::collections::HashMap;
use std::ops::Deref;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;
use bitcoin::util::hash::BitcoinHash;

use nonempty::NonEmpty;
use thiserror::Error;

use crate::block::store::{self, Store};
use crate::checkpoints;

/// Difficulty target of a block.
pub type Target = bitcoin::util::uint::Uint256;

/// Compact difficulty bits (target) of a block.
pub type Bits = u32;

/// Height of a block.
pub type Height = u64;

/// Block timestamp.
pub type Time = u32;

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

        let mut target = target_from_bits(last_bits);

        target = target.mul_u32(adjusted_timespan);
        target = target / Target::from_u64(params.pow_target_timespan).unwrap();

        // Ensure a difficulty floor.
        if target > params.pow_limit {
            target = params.pow_limit;
        }

        BlockHeader::compact_target_from_u256(&target)
    }
}

#[derive(Debug, Clone)]
pub struct CachedBlock {
    height: Height,
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
#[derive(Debug, Clone)]
pub struct BlockCache<S: Store> {
    chain: NonEmpty<CachedBlock>,
    headers: HashMap<BlockHash, Height>,
    orphans: HashMap<BlockHash, BlockHeader>,
    checkpoints: HashMap<Height, BlockHash>,
    params: Params,
    store: S,
}

impl<S: Store> BlockCache<S> {
    /// Create a new `BlockCache` from a `Store` and consensus parameters.
    pub fn from(store: S, params: Params) -> Result<Self, Error> {
        // TODO: This should come from somewhere else.
        let checkpoints = checkpoints::checkpoints();
        let genesis = store.genesis()?;
        let length = store.len()?;
        let orphans = HashMap::new();

        let mut chain = NonEmpty::from((
            CachedBlock {
                height: 0,
                hash: genesis.bitcoin_hash(),
                header: genesis,
            },
            Vec::with_capacity(length - 1),
        ));
        let mut headers = HashMap::with_capacity(length);

        for result in store.iter() {
            let (height, header) = result?;
            let hash = header.bitcoin_hash();

            chain.push(CachedBlock {
                height,
                hash,
                header,
            });
            headers.insert(hash, height);
        }

        Ok(Self {
            chain,
            headers,
            orphans,
            params,
            checkpoints,
            store,
        })
    }

    /// Import a block into the tree. Performs header validation.
    fn import_block(&mut self, header: BlockHeader) -> Result<(BlockHash, Height), Error> {
        let hash = header.bitcoin_hash();
        let tip = self.get_tip();

        if header.prev_blockhash == tip.hash {
            let height = tip.height + 1;
            let bits = if self.params.allow_min_difficulty_blocks {
                if header.time > tip.time + self.params.pow_target_spacing as Time * 2 {
                    BlockHeader::compact_target_from_u256(&self.params.pow_limit)
                } else {
                    let pow_limit = BlockHeader::compact_target_from_u256(&self.params.pow_limit);
                    let mut bits = self.get_tip().bits;

                    for (height, header) in self.chain.tail.iter().enumerate().rev() {
                        if height as Height % self.params.difficulty_adjustment_interval() == 0
                            || header.bits != pow_limit
                        {
                            bits = header.bits;
                            break;
                        }
                    }
                    bits
                }
            } else {
                self.next_difficulty_target(tip.height, tip.time, tip.bits, &self.params)
            };

            // TODO: Validate timestamp.
            let target = target_from_bits(bits);
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
            if let Some(checkpoint) = self.checkpoints.get(&height) {
                if &hash != checkpoint {
                    return Err(Error::InvalidBlockHash(hash, height));
                }
            }

            self.chain.push(CachedBlock {
                height,
                hash,
                header,
            });
            self.headers.insert(hash, height);

            Ok((hash, self.height()))
        } else if self.headers.contains_key(&hash) {
            Err(Error::DuplicateBlock(hash))
        } else {
            if self.orphans.insert(hash, header).is_some() {
                return Err(Error::DuplicateBlock(hash));
            }
            Err(Error::BlockMissing(header.prev_blockhash))
        }
    }
}

impl<S: Store> BlockTree for BlockCache<S> {
    fn import_blocks<I: Iterator<Item = BlockHeader>>(
        &mut self,
        chain: I,
    ) -> Result<(BlockHash, Height), Error> {
        let mut result = None;

        for (i, header) in chain.enumerate() {
            match self.import_block(header) {
                Ok(r) => result = Some(r),
                Err(Error::DuplicateBlock(hash)) => log::debug!("Duplicate block {}", hash),
                Err(Error::BlockMissing(hash)) => log::debug!("Missing block {}", hash),
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

    fn genesis(&self) -> &CachedBlock {
        self.chain.first()
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

// Convert a compact difficulty representation to 256-bits.
// Taken from `BlockHeader::target` from the `bitcoin` library.
fn target_from_bits(bits: u32) -> Target {
    let (mant, expt) = {
        let unshifted_expt = bits >> 24;
        if unshifted_expt <= 3 {
            ((bits & 0xFFFFFF) >> (8 * (3 - unshifted_expt as usize)), 0)
        } else {
            (bits & 0xFFFFFF, 8 * ((bits >> 24) - 3))
        }
    };

    // The mantissa is signed but may not be negative
    if mant > 0x7FFFFF {
        Default::default()
    } else {
        Target::from_u64(mant as u64).unwrap() << (expt as usize)
    }
}

#[cfg(test)]
mod test {
    use super::{BlockCache, BlockTree, CachedBlock, Error, Height, Target, Time};

    use crate::block::store::{self, Store};

    use std::collections::BTreeMap;
    use std::iter;
    use std::path::Path;

    use nonempty::NonEmpty;
    use quickcheck::{self, Arbitrary, Gen};
    use quickcheck_macros::quickcheck;
    use rand::Rng;

    use bitcoin::blockdata::block::BlockHeader;
    use bitcoin::blockdata::constants;
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
    // Target block time (1 minute).
    const TARGET_SPACING: Time = 60;
    // Target time span (1 hour).
    const TARGET_TIMESPAN: Time = 60 * 60;

    #[derive(Debug)]
    struct TestCache {
        headers: BTreeMap<Height, CachedBlock>,
        height: Height,
    }

    impl TestCache {
        fn new(genesis: BlockHeader) -> Self {
            let mut headers = BTreeMap::new();
            let height = 0;

            headers.insert(
                height,
                CachedBlock {
                    height,
                    hash: genesis.bitcoin_hash(),
                    header: genesis,
                },
            );
            Self { headers, height }
        }

        fn insert(&mut self, height: Height, header: BlockHeader) {
            assert!(height > self.height);
            assert!(!self.headers.contains_key(&height));

            let hash = header.bitcoin_hash();
            self.headers.insert(
                height,
                CachedBlock {
                    hash,
                    height,
                    header,
                },
            );
            self.height = height;
        }
    }

    impl BlockTree for TestCache {
        fn import_blocks<I: Iterator<Item = BlockHeader>>(
            &mut self,
            _chain: I,
        ) -> Result<(BlockHash, Height), super::Error> {
            unimplemented!()
        }

        fn get_block_by_height(&self, height: Height) -> Option<&CachedBlock> {
            self.headers.get(&height)
        }

        fn get_tip(&self) -> &CachedBlock {
            self.headers.get(&self.height).unwrap()
        }

        fn height(&self) -> Height {
            self.height
        }

        fn chain(&self) -> &NonEmpty<CachedBlock> {
            unimplemented!()
        }
    }

    #[derive(Clone)]
    struct TestCase {
        chain: NonEmpty<BlockHeader>,
    }

    impl Arbitrary for TestCase {
        fn arbitrary<G: Gen>(g: &mut G) -> TestCase {
            let height = g.gen_range(1, (TARGET_TIMESPAN / TARGET_SPACING) * 4) as Height;
            Self {
                chain: arbitrary_chain(height, g),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let Self { chain } = self;
            let mut shrunk = Vec::new();

            if let Some((_, rest)) = chain.tail.split_last() {
                shrunk.push(Self {
                    chain: NonEmpty::from((chain.head, rest.to_vec())),
                });
            }
            Box::new(shrunk.into_iter())
        }
    }

    impl std::fmt::Debug for TestCase {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            write!(fmt, "\n")?;

            for (height, header) in self.chain.iter().enumerate() {
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
        let delta = g.gen_range(TARGET_SPACING / 2, TARGET_SPACING * 2);

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

    #[derive(Clone)]
    struct BlockImport(BlockCache<store::Memory>, BlockHeader);

    impl std::fmt::Debug for BlockImport {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            let BlockImport(_, header) = self;
            write!(fmt, "{:#?}", header)
        }
    }

    impl quickcheck::Arbitrary for BlockImport {
        fn arbitrary<G: Gen>(g: &mut G) -> BlockImport {
            let network = bitcoin::Network::Regtest;
            let genesis = constants::genesis_block(network).header;
            let params = Params::new(network);
            let store = store::Memory::new(NonEmpty::new(genesis));
            let cache = BlockCache::from(store, params).unwrap();
            let header =
                arbitrary_header(genesis.bitcoin_hash(), genesis.time, &genesis.target(), g);

            cache
                .clone()
                .import_blocks(iter::once(header))
                .expect("the header is valid");

            Self(cache, header)
        }
    }

    #[quickcheck]
    fn prop_block_missing(import: BlockImport) -> bool {
        let BlockImport(mut cache, header) = import;
        let prev_blockhash = constants::genesis_block(bitcoin::Network::Testnet)
            .header
            .bitcoin_hash();

        let header = BlockHeader {
            prev_blockhash,
            ..header
        };

        matches! {
            cache.import_block(header).err(),
            Some(Error::BlockMissing(hash)) if hash == prev_blockhash
        }
    }

    #[quickcheck]
    fn prop_invalid_block_target(import: BlockImport) -> bool {
        let BlockImport(mut cache, header) = import;
        let genesis = cache.genesis().clone();

        assert!(cache.clone().import_block(header).is_ok());

        let header = BlockHeader {
            bits: genesis.bits - 1,
            ..header
        };

        matches! {
            cache.import_block(header).err(),
            Some(Error::InvalidBlockTarget(actual, expected))
                if actual == super::target_from_bits(genesis.bits - 1)
                    && expected == genesis.target()
        }
    }

    #[quickcheck]
    fn prop_invalid_block_pow(import: BlockImport) -> bool {
        let BlockImport(mut cache, header) = import;
        let mut header = header.clone();

        // Find an *invalid* nonce.
        while header.validate_pow(&header.target()).is_ok() {
            header.nonce += 1;
        }

        matches! {
            cache.import_block(header).err(),
            Some(Error::InvalidBlockPoW)
        }
    }

    // Test our difficulty validation against values from the bitcoin main chain.
    #[test]
    fn test_bitcoin_difficulty() {
        use crate::tests;

        let network = bitcoin::Network::Bitcoin;
        let genesis = constants::genesis_block(network).header;
        let params = Params::new(network);

        let mut cache = TestCache::new(genesis);

        for (height, prev_time, prev_bits, time, bits) in tests::TARGETS.iter().cloned() {
            let target = cache.next_difficulty_target(height - 1, prev_time, prev_bits, &params);

            assert_eq!(height % params.difficulty_adjustment_interval(), 0);
            assert_eq!(target, bits);

            // We store the retargeting blocks, since they are used in the difficulty calculation.
            cache.insert(
                height,
                BlockHeader {
                    version: 1,
                    time,
                    bits,
                    merkle_root: Default::default(),
                    prev_blockhash: Default::default(),
                    nonce: 0,
                },
            );
        }
    }

    // Test that we're correctly loading headers from the header store.
    #[test]
    fn test_from_store() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/tests/data/headers.bin");
        let store = store::File::open(path).unwrap();

        let store_headers = store.iter().collect::<Result<Vec<_>, _>>().unwrap();

        let network = bitcoin::Network::Bitcoin;
        let params = Params::new(network);

        let cache = BlockCache::from(store, params).unwrap();
        let cache_headers = cache
            .chain()
            // TODO: Also test genesis, once we have it in the store.
            .tail
            .iter()
            .enumerate()
            .map(|(i, h)| (i as Height, h.header))
            .collect::<Vec<_>>();

        assert_eq!(
            store_headers, cache_headers,
            "all stored headers figure in the cache"
        );

        // Make sure all cached headers are also in the `headers` map.
        for (height, header) in store_headers.iter() {
            let result = cache.headers.get(&header.bitcoin_hash());
            assert_eq!(result, Some(height));
        }
    }
}
