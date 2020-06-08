use super::{BlockCache, BlockTree, Error};

use crate::block::store::{self, Store};
use crate::block::{self, CachedBlock, Height, Target, Time};

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
        let hash = genesis.bitcoin_hash();

        headers.insert(
            height,
            CachedBlock {
                height,
                hash,
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
        let header = arbitrary_header(genesis.bitcoin_hash(), genesis.time, &genesis.target(), g);

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
            if actual == block::target_from_bits(genesis.bits - 1)
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
        .iter()
        .enumerate()
        .map(|(i, h)| (i as Height, h.header))
        .collect::<Vec<_>>();

    assert_eq!(store_headers.len(), cache_headers.len());
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
