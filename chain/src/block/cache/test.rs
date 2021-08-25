use super::BlockCache;

use nakamoto_common::block::time::{AdjustedTime, Clock, LocalTime};
use nakamoto_common::block::tree::{BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockTime, Height, Target};
use nakamoto_common::nonempty::NonEmpty;

use nakamoto_test::assert_matches;
use nakamoto_test::block;
use nakamoto_test::block::cache::model;

use crate::block::store::{self, Store};

use std::collections::{BTreeMap, VecDeque};
use std::iter;
use std::net;
use std::sync::{Arc, RwLock};

use quickcheck as qc;
use quickcheck::{Arbitrary, Gen, QuickCheck};
use quickcheck_macros::quickcheck;
use rand::Rng;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::blockdata::constants;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::{BlockHash, TxMerkleNode};
use bitcoin_hashes::hex::FromHex;

use bitcoin::util::uint::Uint256;

/// Sun, 12 Jul 2020 15:03:05 +0000.
const LOCAL_TIME: LocalTime = LocalTime::from_secs(1594566185);

/// Lowest possible difficulty.
const TARGET: Uint256 = Uint256([
    0xffffffffffffffffu64,
    0xffffffffffffffffu64,
    0xffffffffffffffffu64,
    0x7fffffffffffffffu64,
]);
/// Target block time (1 minute).
const TARGET_SPACING: BlockTime = 60 * 10;
/// Target time span (1 hour).
const _TARGET_TIMESPAN: BlockTime = 60 * 60;

#[derive(Debug)]
struct HeightCache {
    headers: BTreeMap<Height, BlockHeader>,
    height: Height,
}

impl HeightCache {
    fn new(genesis: BlockHeader) -> Self {
        let mut headers = BTreeMap::new();
        let height = 0;

        headers.insert(height, genesis);

        Self { headers, height }
    }

    fn import(&mut self, height: Height, header: BlockHeader) {
        assert!(height > self.height);
        assert!(!self.headers.contains_key(&height));

        self.headers.insert(height, header);
        self.height = height;
    }
}

impl BlockTree for HeightCache {
    fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        _chain: I,
        _ctx: &C,
    ) -> Result<ImportResult, super::Error> {
        unimplemented!()
    }

    fn extend_tip<C>(&mut self, _header: BlockHeader, _context: &C) -> Result<ImportResult, Error> {
        unimplemented!()
    }

    fn get_block(&self, _hash: &BlockHash) -> Option<(Height, &BlockHeader)> {
        unimplemented!()
    }

    fn get_block_by_height(&self, height: Height) -> Option<&BlockHeader> {
        self.headers.get(&height)
    }

    fn last_checkpoint(&self) -> Height {
        0
    }

    fn checkpoints(&self) -> BTreeMap<Height, BlockHash> {
        BTreeMap::new()
    }

    fn tip(&self) -> (BlockHash, BlockHeader) {
        let header = self.headers.get(&self.height).unwrap();
        (header.block_hash(), *header)
    }

    fn height(&self) -> Height {
        self.height
    }

    fn iter(&self) -> Box<dyn DoubleEndedIterator<Item = (Height, BlockHeader)>> {
        unimplemented!()
    }

    fn is_known(&self, _hash: &BlockHash) -> bool {
        unimplemented!()
    }

    fn contains(&self, _hash: &BlockHash) -> bool {
        unimplemented!()
    }

    fn locate_headers(
        &self,
        _locators: &[BlockHash],
        _stop_hash: BlockHash,
        _max: usize,
    ) -> Vec<BlockHeader> {
        unimplemented!()
    }

    fn locator_hashes(&self, _from: Height) -> Vec<BlockHash> {
        unimplemented!()
    }
}

mod arbitrary {
    use super::*;

    #[derive(Clone)]
    pub struct OrderedHeaders {
        pub headers: NonEmpty<BlockHeader>,
    }

    impl Arbitrary for OrderedHeaders {
        fn arbitrary<G: Gen>(g: &mut G) -> OrderedHeaders {
            let height = g.gen_range(1, g.size() + 1) as Height;
            Self {
                headers: arbitrary_chain(height, g),
            }
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let Self { headers } = self;
            let mut shrunk = Vec::new();

            if let Some((_, rest)) = headers.tail.split_last() {
                shrunk.push(Self {
                    headers: NonEmpty::from((headers.head, rest.to_vec())),
                });
            }
            Box::new(shrunk.into_iter())
        }
    }

    impl std::fmt::Debug for OrderedHeaders {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            writeln!(fmt)?;

            for (height, header) in self.headers.iter().enumerate() {
                writeln!(
                    fmt,
                    "#{:03} {} time={:05} bits={:x} nonce={}",
                    height,
                    header.block_hash(),
                    header.time,
                    header.bits,
                    header.nonce
                )?;
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    pub struct UnorderedHeaders {
        pub headers: Vec<BlockHeader>,
        pub genesis: BlockHeader,
        pub tip: BlockHash,
    }

    impl UnorderedHeaders {
        fn new(ordered: NonEmpty<BlockHeader>) -> Self {
            let genesis = *ordered.first();
            let tip = ordered.last().block_hash();
            let headers = ordered.tail;

            UnorderedHeaders {
                headers,
                genesis,
                tip,
            }
        }

        fn shuffle<G: Gen>(&mut self, g: &mut G) {
            use rand::seq::SliceRandom;
            self.headers.shuffle(g);
        }
    }

    impl Arbitrary for UnorderedHeaders {
        fn arbitrary<G: Gen>(g: &mut G) -> UnorderedHeaders {
            let OrderedHeaders { headers: ordered } = OrderedHeaders::arbitrary(g);
            let mut unordered = UnorderedHeaders::new(ordered);

            unordered.shuffle(g);
            unordered
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let mut shrunk = Vec::new();

            if self.tip != self.genesis.block_hash() {
                let mut unordered = self.clone();
                let ix = unordered
                    .headers
                    .iter()
                    .position(|h| h.block_hash() == self.tip)
                    .unwrap();
                let tip = unordered.headers[ix];

                unordered.tip = tip.prev_blockhash;
                unordered.headers.swap_remove(ix);

                shrunk.push(unordered);
            }

            Box::new(shrunk.into_iter())
        }
    }

    impl std::fmt::Debug for UnorderedHeaders {
        fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
            writeln!(fmt)?;

            for header in self.headers.iter() {
                writeln!(
                    fmt,
                    "{} {} time={:05} bits={:x}",
                    header.block_hash(),
                    header.prev_blockhash,
                    header.time,
                    header.bits,
                )?;
            }
            Ok(())
        }
    }
}

fn arbitrary_header<G: Gen>(
    prev_blockhash: BlockHash,
    prev_time: BlockTime,
    target: &Target,
    g: &mut G,
) -> BlockHeader {
    let delta = g.gen_range(TARGET_SPACING / 2, TARGET_SPACING * 2);

    let time = prev_time + delta;
    let bits = BlockHeader::compact_target_from_u256(target);

    let mut header = BlockHeader {
        version: 1,
        time,
        nonce: 0,
        bits,
        merkle_root: TxMerkleNode::default(),
        prev_blockhash,
    };
    block::solve(&mut header);

    header
}

fn arbitrary_chain<G: Gen>(height: Height, g: &mut G) -> NonEmpty<BlockHeader> {
    let mut prev_time = 0; // Epoch.
    let mut prev_hash = BlockHash::default();

    let genesis = arbitrary_header(prev_hash, prev_time, &TARGET, g);
    let mut chain = NonEmpty::new(genesis);

    prev_hash = genesis.block_hash();
    prev_time = genesis.time;

    for _ in 0..height {
        let header = arbitrary_header(prev_hash, prev_time, &TARGET, g);
        prev_time = header.time;
        prev_hash = header.block_hash();

        chain.push(header);
    }
    chain
}

#[derive(Clone)]
struct BlockImport(BlockCache<store::Memory<BlockHeader>>, BlockHeader);

impl std::fmt::Debug for BlockImport {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let BlockImport(_, header) = self;
        write!(fmt, "{:#?}", header)
    }
}

impl Arbitrary for BlockImport {
    fn arbitrary<G: Gen>(g: &mut G) -> BlockImport {
        let network = bitcoin::Network::Regtest;
        let genesis = constants::genesis_block(network).header;
        let params = Params::new(network);
        let store = store::Memory::new(NonEmpty::new(genesis));
        let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
        let cache = BlockCache::from(store, params, &[]).unwrap();
        let header = arbitrary_header(genesis.block_hash(), genesis.time, &genesis.target(), g);

        cache
            .clone()
            .import_blocks(iter::once(header), &ctx)
            .expect("the header is valid");

        Self(cache, header)
    }
}

#[quickcheck]
fn prop_block_missing(import: BlockImport) -> bool {
    let BlockImport(mut cache, header) = import;
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let prev_blockhash = constants::genesis_block(bitcoin::Network::Testnet)
        .header
        .block_hash();

    let mut header = BlockHeader {
        prev_blockhash,
        ..header
    };
    block::solve(&mut header);

    matches! {
        cache.import_block(header, &ctx).err(),
        Some(Error::BlockMissing(hash)) if hash == prev_blockhash
    }
}

#[quickcheck]
fn prop_invalid_block_target(import: BlockImport) -> bool {
    let BlockImport(mut cache, header) = import;
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let genesis = *cache.genesis();

    assert!(cache.clone().import_block(header, &ctx).is_ok());

    let header = BlockHeader {
        bits: genesis.bits - 1,
        ..header
    };

    matches! {
        cache.import_block(header, &ctx).err(),
        Some(Error::InvalidBlockTarget(actual, expected))
            if actual == BlockHeader::u256_from_compact_target(genesis.bits - 1)
                && expected == genesis.target()
    }
}

#[test]
fn test_invalid_orphan_block_target() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let store = store::Memory::new(NonEmpty::new(genesis));
    let clock = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let params = Params::new(network);

    // A lower difficulty target than expected.
    let invalid_bits: Uint256 = Uint256([
        0xffffffffffffffffu64,
        0xffffffffffffffffu64,
        0xffffffffffffffffu64,
        0x9fffffffffffffffu64,
    ]);

    let mut cache = BlockCache::from(store, params.clone(), &[]).unwrap();

    // Some arbitrary previous block we don't have.
    let prev_blockhash =
        BlockHash::from_hex("0f9188f13cb7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();

    // A valid header.
    let mut header = BlockHeader {
        prev_blockhash,
        bits: genesis.bits,
        time: genesis.time,
        version: genesis.version,
        nonce: 0,
        merkle_root: TxMerkleNode::default(),
    };
    block::solve(&mut header);

    assert!(matches!(
        cache.import_block(header, &clock).err(),
        Some(Error::BlockMissing(h)) if h == prev_blockhash
    ));

    // An invalid header.
    let mut header = BlockHeader {
        bits: BlockHeader::compact_target_from_u256(&invalid_bits),
        ..header
    };
    block::solve(&mut header);

    // Even though the header can't be connected to the main chain, we still get an error.
    match cache.import_block(header, &clock).unwrap_err() {
        Error::InvalidBlockTarget(actual, expected) => {
            assert_eq!(
                BlockHeader::compact_target_from_u256(&actual),
                BlockHeader::compact_target_from_u256(&invalid_bits)
            );
            assert_eq!(
                BlockHeader::compact_target_from_u256(&expected),
                BlockHeader::compact_target_from_u256(&params.pow_limit)
            );
        }
        err => panic!("wrong error returned: {:?}", err),
    }
}

#[test]
fn test_invalid_orphan_block_pow() {
    let network = bitcoin::Network::Bitcoin;
    let genesis = constants::genesis_block(network).header;
    let store = store::Memory::new(NonEmpty::new(genesis));
    let clock = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let params = Params::new(network);

    let mut cache = BlockCache::from(store, params, &[]).unwrap();

    // Some arbitrary previous block we don't have.
    let prev_blockhash =
        BlockHash::from_hex("0f9188f13cb7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();

    // An invalid header.
    let header = BlockHeader {
        prev_blockhash,
        bits: genesis.bits,
        time: genesis.time,
        version: genesis.version,
        nonce: 94173,
        merkle_root: TxMerkleNode::default(),
    };

    assert!(
        matches!(
            cache.import_block(header, &clock).err(),
            Some(Error::InvalidBlockPoW)
        ),
        "the orphan header is not imported"
    );
}

#[quickcheck]
fn prop_invalid_block_pow(import: BlockImport) -> bool {
    let BlockImport(mut cache, header) = import;
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut header = header;

    // Find an *invalid* nonce.
    while header.validate_pow(&header.target()).is_ok() {
        header.nonce += 1;
    }

    matches! {
        cache.import_block(header, &ctx).err(),
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

    let mut cache = HeightCache::new(genesis);

    for (height, prev_time, prev_bits, time, bits) in tests::TARGETS.iter().cloned() {
        let target = cache.next_difficulty_target(
            height - 1,
            prev_time,
            BlockHeader::u256_from_compact_target(prev_bits),
            &params,
        );

        assert_eq!(height % params.difficulty_adjustment_interval(), 0);
        assert_eq!(target, bits);

        // We store the retargeting blocks, since they are used in the difficulty calculation.
        cache.import(
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
    let genesis = constants::genesis_block(bitcoin::Network::Bitcoin).header;
    let store = store::File::open(&*nakamoto_test::headers::PATH, genesis).unwrap();
    let store_headers = store.iter().collect::<Result<Vec<_>, _>>().unwrap();

    let network = bitcoin::Network::Bitcoin;
    let params = Params::new(network);

    let cache = BlockCache::from(store, params, &[]).unwrap();
    let cache_headers = cache.iter().collect::<Vec<_>>();

    assert_eq!(store_headers.len(), cache_headers.len());
    assert_eq!(
        store_headers, cache_headers,
        "all stored headers figure in the cache"
    );

    // Make sure all cached headers are also in the `headers` map.
    for (height, header) in store_headers.iter() {
        let result = cache.headers.get(&header.block_hash());
        assert_eq!(result, Some(height));
    }
}

#[test]
fn test_median_time_past() {
    let network = bitcoin::Network::Bitcoin;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::File::open(&*nakamoto_test::headers::PATH, genesis).unwrap();

    let cache = BlockCache::from(store, params, &[]).unwrap();
    let headers = cache.iter().map(|(_, h)| h).collect::<Vec<_>>();

    assert_eq!(cache.median_time_past(1), genesis.time);
    assert_eq!(cache.median_time_past(2), headers[1].time);
    assert_eq!(cache.median_time_past(3), headers[1].time);
    assert_eq!(cache.median_time_past(4), headers[2].time);
    assert_eq!(cache.median_time_past(11), headers[5].time);
    assert_eq!(cache.median_time_past(13), headers[7].time);
}

#[test]
fn prop_cache_import_ordered() {
    fn prop(input: arbitrary::OrderedHeaders) -> bool {
        let arbitrary::OrderedHeaders { headers } = input;
        let mut cache = model::Cache::new(headers.head);
        let tip = *headers.last();
        let clock = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);

        cache
            .import_blocks(headers.tail.iter().cloned(), &clock)
            .unwrap();

        cache.genesis() == &headers.head
            && cache.tip() == (tip.block_hash(), tip)
            && cache
                .iter()
                .all(|(i, h)| headers.get(i as usize) == Some(&h))
    }
    QuickCheck::with_gen(qc::StdGen::new(rand::thread_rng(), 16))
        .quickcheck(prop as fn(arbitrary::OrderedHeaders) -> bool);
}

#[derive(Clone)]
struct Tree {
    headers: Arc<RwLock<BTreeMap<BlockHash, BlockHeader>>>,
    genesis: BlockHeader,
    hash: BlockHash,
    time: BlockTime,
}

impl Tree {
    fn new(genesis: BlockHeader) -> Self {
        let headers = BTreeMap::new();
        let hash = genesis.block_hash();

        Self {
            headers: Arc::new(RwLock::new(headers)),
            time: genesis.time,
            genesis,
            hash,
        }
    }

    fn next(&self, g: &mut impl Rng) -> Tree {
        let nonce = g.gen::<u32>();
        let mut header = BlockHeader {
            version: 1,
            prev_blockhash: self.hash,
            merkle_root: Default::default(),
            bits: BlockHeader::compact_target_from_u256(&TARGET),
            time: self.time + TARGET_SPACING,
            nonce,
        };
        block::solve(&mut header);

        let hash = header.block_hash();
        self.headers.write().unwrap().insert(hash, header);

        Tree {
            hash,
            headers: self.headers.clone(),
            time: header.time,
            genesis: self.genesis,
        }
    }

    fn next_invalid(&self, g: &mut impl Rng) -> Tree {
        let nonce = g.gen::<u32>();
        let mut header = BlockHeader {
            version: 1,
            prev_blockhash: self.hash,
            merkle_root: Default::default(),
            bits: BlockHeader::compact_target_from_u256(&TARGET),
            time: self.time + TARGET_SPACING,
            nonce,
        };
        let target = header.target();
        while header.validate_pow(&target).is_ok() {
            header.nonce += 1;
        }

        let hash = header.block_hash();
        self.headers.write().unwrap().insert(hash, header);

        Tree {
            hash,
            headers: self.headers.clone(),
            time: header.time,
            genesis: self.genesis,
        }
    }

    fn sample<G: Gen>(&self, g: &mut G) -> Tree {
        let headers = self.headers.read().unwrap();
        let ix = g.gen_range(0, headers.len());
        let header = headers.values().nth(ix).unwrap();

        Tree {
            hash: header.block_hash(),
            headers: self.headers.clone(),
            time: header.time,
            genesis: self.genesis,
        }
    }

    fn headers(&self) -> Vec<BlockHeader> {
        self.headers
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>()
    }

    fn block(&self) -> BlockHeader {
        if self.hash == self.genesis.block_hash() {
            return self.genesis;
        }
        self.headers
            .read()
            .unwrap()
            .get(&self.hash)
            .cloned()
            .unwrap()
    }

    fn branch(&self, range: [&Tree; 2]) -> impl Iterator<Item = BlockHeader> {
        let headers = self.headers.read().unwrap();
        let mut blocks = VecDeque::new();

        let [from, to] = range;
        let mut tip = &to.hash;

        while let Some(h) = headers.get(tip) {
            blocks.push_front(*h);
            if tip == &from.hash {
                break;
            }
            tip = &h.prev_blockhash;
        }

        blocks.into_iter()
    }
}

impl Arbitrary for Tree {
    fn arbitrary<G: Gen>(g: &mut G) -> Tree {
        let network = bitcoin::Network::Regtest;
        let genesis = constants::genesis_block(network).header;
        let height = g.gen_range(1, g.size() / 5 + 1);
        let forks = g.gen_range(0, g.size() / 10);

        let mut tree = Tree::new(genesis);

        // Generate trunk.
        for _ in 0..height {
            tree = tree.next(g);
        }
        // Generate forks.
        for _ in 0..forks {
            let mut fork = tree.sample(g);
            let height = g.gen_range(1, g.size() / 5 + 1);

            for _ in 0..height {
                fork = fork.next(g);
            }
        }

        tree
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let headers = self.headers.read().unwrap().clone();
        let mut shrunk = Vec::new();

        if headers.len() <= 1 {
            return Box::new(std::iter::empty());
        }

        {
            let mut headers = headers.clone();
            if let Some(tip) = headers.remove(&self.hash) {
                if let Some(prev) = headers.get(&tip.prev_blockhash) {
                    shrunk.push(Self {
                        time: prev.time,
                        hash: tip.prev_blockhash,
                        headers: Arc::new(RwLock::new(headers)),
                        genesis: self.genesis,
                    });
                }
            }
        }

        for i in 0..headers.len() {
            if let Some(h) = headers.keys().nth(i) {
                if h != &self.hash {
                    let mut headers = headers.clone();
                    headers.remove(h);

                    shrunk.push(Self {
                        time: self.time,
                        hash: self.hash,
                        headers: Arc::new(RwLock::new(headers)),
                        genesis: self.genesis,
                    });
                }
            }
        }

        Box::new(shrunk.into_iter())
    }
}

impl std::fmt::Debug for Tree {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(fmt)?;
        writeln!(fmt, "hash: {:#?}", &self.hash)?;
        writeln!(fmt, "genesis: {:#?}", &self.genesis)?;
        writeln!(fmt, "time: {:#?}", &self.time)?;
        writeln!(fmt, "headers: {:#?}", &self.headers())?;

        Ok(())
    }
}

#[ignore]
#[quickcheck]
fn prop_cache_import_tree_genesis(tree: Tree) -> bool {
    let headers = tree.headers();

    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));

    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut real = BlockCache::from(store, params, &[]).unwrap();
    let mut model = model::Cache::new(genesis);

    let real_result = real.import_blocks(headers.iter().cloned(), &ctx).unwrap();
    let model_result = model.import_blocks(headers.iter().cloned(), &ctx).unwrap();

    assert_eq!(real_result, model_result);

    real.tip() == model.tip()
}

#[ignore]
#[quickcheck]
fn prop_cache_import_tree_randomized(tree: Tree) {
    let headers = tree.headers();

    use nakamoto_test::block::gen;

    let mut rng = fastrand::Rng::new();
    let initial = gen::headers(
        tree.genesis,
        rng.u64(1..headers.len() as Height + 1),
        &mut rng,
    );

    let network = bitcoin::Network::Regtest;
    let params = Params::new(network);
    let store = store::Memory::new(initial.clone());

    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut real = BlockCache::from(store, params, &[]).unwrap();
    let mut model = model::Cache::from(initial);

    assert_eq!(real.tip(), model.tip());

    for headers in headers.windows(rng.usize(1..=headers.len())) {
        let real_result = real.import_blocks(headers.iter().cloned(), &ctx).unwrap();
        let model_result = model.import_blocks(headers.iter().cloned(), &ctx).unwrap();

        assert_eq!(real_result, model_result);

        match (real_result, model_result) {
            (ImportResult::TipUnchanged, ImportResult::TipUnchanged) => {}
            (
                ImportResult::TipChanged(header, hash, height, reverted, connected),
                ImportResult::TipChanged(_, _, _, _, _),
            ) => {
                assert_eq!(connected.last(), &(height, header));
                assert_eq!(header.block_hash(), hash);

                // None of the reverted items are in the active chain.
                for (_, hash) in reverted {
                    assert!(!real.contains(&hash));
                }
                // All of the connected items are in the active chain.
                for (_, header) in connected {
                    assert!(real.contains(&header.block_hash()));
                }
            }
            (actual, expected) => {
                assert_eq!(actual, expected);
            }
        }
        assert_eq!(real.tip(), model.tip());
    }
}

#[test]
fn test_cache_import_unchanged() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut cache = BlockCache::from(store, params, &[]).unwrap();

    let g = &mut rand::thread_rng();

    let a0 = Tree::new(genesis);
    let a1 = a0.next(g);
    let a2 = a1.next(g);

    cache.import_blocks(a0.branch([&a1, &a2]), &ctx).unwrap();
    assert_eq!(cache.tip().0, a2.hash);

    let r = cache
        .import_blocks(a0.branch([&a0.next(g), &a0.next(g)]), &ctx)
        .unwrap();
    assert_matches!(r, ImportResult::TipUnchanged);

    let a3 = a2.next(g);
    let a4 = a3.next(g);
    let a5 = a4.next(g);

    let e = cache.import_block(a4.block(), &ctx).err();
    assert_matches!(e, Some(Error::BlockMissing(h)) if h == a3.block().block_hash());

    let r = cache.import_block(a5.block(), &ctx).unwrap();
    assert_matches!(r, ImportResult::TipUnchanged);

    let r = cache.import_block(a3.block(), &ctx).unwrap();
    assert_matches!(r, ImportResult::TipChanged { .. });
}

#[test]
fn test_cache_import_height_unchanged() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut cache = BlockCache::from(store, params, &[]).unwrap();

    let g = &mut rand::thread_rng();

    let a0 = Tree::new(genesis);
    let a1 = a0.next(g);
    let a2 = a1.next(g);
    let b2 = loop {
        let b2 = a1.next(g);

        // Find a header that will be selected over `a2`.
        if b2.block().block_hash() < a2.block().block_hash() {
            break b2;
        }
    };

    cache.import_block(a1.block(), &ctx).unwrap();
    cache.import_block(a2.block(), &ctx).unwrap();

    assert_eq!(cache.tip().0, a2.hash);

    let height = cache.height();
    let result = cache.import_block(b2.block(), &ctx).unwrap();

    assert_eq!(cache.tip().0, b2.hash);
    assert_eq!(
        result,
        ImportResult::TipChanged(
            b2.block(),
            b2.hash,
            height,
            vec![(2, a2.hash)],
            NonEmpty::new((2, b2.block()))
        )
    );
}

#[test]
fn test_cache_import_back_and_forth() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut cache = BlockCache::from(store, params, &[]).unwrap();

    let g = &mut rand::thread_rng();

    let a0 = Tree::new(genesis);

    // a0 <- a1 <- a2 *
    let a1 = a0.next(g);
    let a2 = a1.next(g);

    let r = cache.import_blocks(a0.branch([&a1, &a2]), &ctx).unwrap();
    assert_eq!(cache.tip().0, a2.hash);
    assert_matches!(r, ImportResult::TipChanged { .. });

    // a0 <- a1 <- a2
    //           \
    //            <- b2 <- b3 *
    let b2 = a1.next(g);
    let b3 = b2.next(g);

    let r = cache.import_blocks(a0.branch([&b2, &b3]), &ctx).unwrap();
    assert_eq!(cache.tip().0, b3.hash);
    assert_matches!(r, ImportResult::TipChanged { .. });

    // a0 <- a1 <- a2 <- a3 <- a4 *
    //           \
    //            <- b2 <- b3
    let a3 = a2.next(g);
    let a4 = a3.next(g);

    let r = cache.import_blocks(a0.branch([&a3, &a4]), &ctx).unwrap();
    assert_eq!(cache.tip().0, a4.hash);
    assert_matches!(r, ImportResult::TipChanged { .. });

    // a0 <- a1 <- a2 <- a3 <- a4
    //           \
    //            <- b2 <- b3 <- b4 <- b5 *
    let b4 = b3.next(g);
    let b5 = b4.next(g);

    let r = cache.import_blocks(a1.branch([&b4, &b5]), &ctx).unwrap();
    assert_eq!(cache.tip().0, b5.hash);
    assert_matches!(r, ImportResult::TipChanged { .. });
}

#[test]
fn test_cache_import_equal_difficulty_blocks() {
    let mut headers = vec![
        BlockHeader {
            version: 1,
            prev_blockhash: BlockHash::from_hex(
                "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206",
            )
            .unwrap(),
            merkle_root: Default::default(),
            time: 1296688662,
            bits: 545259519,
            nonce: 3705677718,
        },
        BlockHeader {
            version: 1,
            prev_blockhash: BlockHash::from_hex(
                "40e6856aba3aa0bab2ba97b5612dc22a485c3a583dc98a9f1cd1706dd858f623",
            )
            .unwrap(),
            merkle_root: Default::default(),
            time: 1296688722,
            bits: 545259519,
            nonce: 3581550584,
        },
        BlockHeader {
            version: 1,
            prev_blockhash: BlockHash::from_hex(
                "40e6856aba3aa0bab2ba97b5612dc22a485c3a583dc98a9f1cd1706dd858f623",
            )
            .unwrap(),
            merkle_root: Default::default(),
            time: 1296688722,
            bits: 545259519,
            nonce: 3850925874,
        },
    ];
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);

    let mut real = BlockCache::from(store.clone(), params.clone(), &[]).unwrap();
    let mut model = model::Cache::new(genesis);

    model.import_blocks(headers.iter().cloned(), &ctx).unwrap();
    real.import_blocks(headers.iter().cloned(), &ctx).unwrap();

    assert_eq!(real.tip(), model.tip());

    let expected =
        BlockHash::from_hex("79cdea612df7f65b541da8ff45913f472eb0bf9376e1b9e3cd2c6ce78f261954")
            .unwrap();

    assert_eq!(real.tip().0, expected);
    assert_eq!(model.tip().0, expected);

    // Swap the import order. Tip should be stable.

    headers.swap(1, 2);

    let mut real = BlockCache::from(store, params, &[]).unwrap();
    let mut model = model::Cache::new(genesis);

    model.import_blocks(headers.iter().cloned(), &ctx).unwrap();
    real.import_blocks(headers.iter().cloned(), &ctx).unwrap();

    assert_eq!(real.tip().0, expected);
    assert_eq!(model.tip().0, expected);
}

#[test]
fn test_cache_import_longer_chain_with_less_difficulty() {
    // TODO
}

#[test]
fn test_cache_import_with_checkpoints() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let g = &mut rand::thread_rng();

    let tree = Tree::new(genesis);

    // a0 <- a1 <- a2 <- a3 *
    let a1 = tree.next(g);
    let a2 = a1.next(g);
    let a3 = a2.next(g);

    let mut cache = BlockCache::from(store.clone(), params.clone(), &[]).unwrap();
    cache.import_blocks(tree.branch([&a1, &a3]), &ctx).unwrap();

    let mut cache =
        BlockCache::from(store.clone(), params.clone(), &[(32, Default::default())]).unwrap();
    cache
        .import_blocks(tree.branch([&a1, &a3]), &ctx)
        .expect("A checkpoint in the future cannot cause any error");

    let mut cache =
        BlockCache::from(store.clone(), params.clone(), &[(1, Default::default())]).unwrap();
    assert!(
        matches! {
            cache.import_block(a1.block(), &ctx),
            Err(Error::InvalidBlockHash(hash, 1)) if hash == a1.hash
        },
        "An incorrect checkpoint at height 1 causes an error"
    );

    let mut cache = BlockCache::from(store, params, &[(1, a1.hash), (2, a2.hash)]).unwrap();
    cache
        .import_blocks(tree.branch([&a1, &a2]), &ctx)
        .expect("Correct checkpoints cause no error");
}

#[test]
fn test_cache_import_invalid_fork() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut cache = BlockCache::from(store, params, &[]).unwrap();
    let g = &mut rand::thread_rng();

    let a0 = Tree::new(genesis);

    // a0 <- a1 <- a2 <- a3 *
    let a1 = a0.next(g);
    let a2 = a1.next(g);
    let a3 = a2.next(g);

    cache.import_blocks(a0.branch([&a1, &a3]), &ctx).unwrap();
    assert_eq!(cache.tip().0, a3.hash);

    //            <- c2 <- c3 <- c4 <- (c5)
    //           /
    // a0 <- a1 <- a2 <- a3 *
    //
    let c2 = a1.next(g);
    let c3 = c2.next(g);
    let c4 = c3.next(g);
    let c5 = c4.next_invalid(g);

    cache
        .import_blocks(a0.branch([&c3, &c5]), &ctx)
        .unwrap_err();
    assert_eq!(cache.tip().0, a3.hash);

    cache.import_block(c2.block(), &ctx).unwrap();
    assert_eq!(
        cache.tip().0,
        c4.hash,
        "Switch to sub-fork, since the last block is invalid"
    );

    //            <- c2 <- c3 <- c4 *
    //           /
    // a0 <- a1 <- a2 <- a3
    //                \
    //                 <- (b3) <- b4 <- b5 <- b6
    let b3 = a2.next_invalid(g);
    let b4 = b3.next(g);
    let b5 = b4.next(g);
    let b6 = b5.next(g);

    cache.import_blocks(a0.branch([&b4, &b6]), &ctx).unwrap();
    cache.import_block(b3.block(), &ctx).unwrap_err();
    assert_eq!(cache.tip().0, c4.hash, "Don't switch to invalid fork");
}

#[test]
fn test_cache_import_fork_with_checkpoints() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let g = &mut rand::thread_rng();

    let a0 = Tree::new(genesis);

    // a0 <- a1 <- [a2] <- a3 *
    let a1 = a0.next(g);
    let a2 = a1.next(g);
    let a3 = a2.next(g);

    // Prevent block `a2` from being reverted.
    let checkpoints = &[(0, genesis.block_hash()), (2, a2.hash)];
    let mut cache = BlockCache::from(store, params, checkpoints).unwrap();

    cache.import_blocks(a0.branch([&a1, &a3]), &ctx).unwrap();
    assert_eq!(cache.tip().0, a3.hash, "{:#?}", cache);

    //            <- c2
    //           /
    // a0 <- a1 <- [a2] <- a3 *
    //    \
    //     <- b1
    let b1 = a0.next(g);
    let c2 = a1.next(g);

    assert!(
        matches! {
            cache.import_block(b1.block(), &ctx),
            Err(Error::InvalidBlockHeight(1))
        },
        "Can't fork passed the last checkpoint"
    );
    assert!(
        matches! {
            cache.import_block(c2.block(), &ctx),
            Err(Error::InvalidBlockHeight(2))
        },
        "Can't fork passed the last checkpoint"
    );

    //            <- c2 <- c3 <- c4
    //           /
    // a0 <- a1 <- [a2] <- a3 *
    let c3 = c2.next(g);
    let c4 = c3.next(g);

    // Import more blocks starting from before the last checkpoint.
    cache.import_blocks(a0.branch([&c3, &c4]), &ctx).unwrap();
    assert_eq!(
        cache.tip().0,
        a3.hash,
        "We can import a longer chain, but it won't overtake the main chain"
    );

    // a0 <- a1 <- [a2] <- a3
    //                  \
    //                   <- b3 <- b4 *
    let b3 = a2.next(g);
    let b4 = b3.next(g);

    cache.import_blocks(a0.branch([&b3, &b4]), &ctx).unwrap();
    assert_eq!(
        cache.tip().0,
        b4.hash,
        "Forking after the last checkpoint is fine"
    );
}

#[test]
fn test_cache_import_fork_with_future_checkpoint() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let g = &mut rand::thread_rng();

    let a0 = Tree::new(genesis);

    // a0 <- a1 <- a2 * ... [c4]
    let a1 = a0.next(g);
    let a2 = a1.next(g);

    // Create a checkpoint block [c4] in the future, at height 4.
    let checkpoints = &[(0, genesis.block_hash()), (4, BlockHash::default())];
    let mut cache = BlockCache::from(store, params, checkpoints).unwrap();

    cache.import_blocks(a0.branch([&a1, &a2]), &ctx).unwrap();
    assert_eq!(cache.tip().0, a2.hash, "{:#?}", cache);

    //            <- b2
    //           /          ... [c4]
    // a0 <- a1 <- a2 *
    //
    let b2 = a1.next(g);

    // We can do this because we don't know yet whether a2 or b2 is the correct branch.
    cache
        .import_block(b2.block(), &ctx)
        .expect("we can import a fork before the next checkpoint");
}

#[test]
#[allow(unused_variables)]
fn test_cache_import_duplicate() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut cache = BlockCache::from(store, params, &[]).unwrap();
    let g = &mut rand::thread_rng();

    let tree = Tree::new(genesis);
    let a0 = tree;

    // a0 <- a1 <- a2 <- a3 *
    let a1 = a0.next(g);
    let a2 = a1.next(g);
    let a3 = a2.next(g);

    assert!(matches! {
        cache.import_block(a1.block(), &ctx), Ok(_)
    });
    assert!(matches! {
        cache.import_block(a1.block(), &ctx),
        Err(Error::DuplicateBlock(h)) if h == a1.hash
    });

    assert!(matches! {
        cache.import_block(a2.block(), &ctx), Ok(_)
    });
    assert!(matches! {
        cache.import_block(a2.block(), &ctx), Err(Error::DuplicateBlock(_))
    });

    // a0 <- a1 <- a2 <- a3 *
    //           \
    //            <- b3
    let b3 = a1.next(g);

    assert!(matches! {
        cache.import_block(b3.block(), &ctx), Ok(_)
    });
    assert!(matches! {
        cache.import_block(b3.block(), &ctx),
        Err(Error::DuplicateBlock(h)) if h == b3.hash
    });
    assert!(matches! {
        cache.import_block(a0.block(), &ctx),
        Err(Error::DuplicateBlock(h)) if h == a0.hash
    });
}

#[test]
#[allow(unused_variables)]
fn test_cache_import_unordered() {
    let network = bitcoin::Network::Regtest;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut cache = BlockCache::from(store, params, &[]).unwrap();
    let mut model = model::Cache::new(genesis);

    let g = &mut rand::thread_rng();

    let a0 = Tree::new(genesis);

    // a0 <- a1 <- a2 <- a3 *
    let a1 = a0.next(g);
    let a2 = a1.next(g);
    let a3 = a2.next(g);

    let r = cache.import_blocks(a0.branch([&a1, &a3]), &ctx).unwrap();
    assert_eq!(cache.tip().0, a3.hash, "{:#?}", cache);
    assert_matches!(r, ImportResult::TipChanged { .. });

    // a0 <- a1 <- a2 <- a3
    //                 \
    //                  <- b3 <- b4 *
    let b3 = a2.next(g);
    let b4 = b3.next(g);

    let r = cache.import_blocks(a0.branch([&b3, &b4]), &ctx).unwrap();
    assert_eq!(cache.tip().0, b4.hash, "{:#?}", cache);
    assert_matches!(r, ImportResult::TipChanged { .. });

    //            <- c2 <- c3 <- c4 <- c5 *
    //           /
    // a0 <- a1 <- a2 <- a3
    //                 \
    //                  <- b3 <- b4
    let c2 = a1.next(g);
    let c3 = c2.next(g);
    let c4 = c3.next(g);
    let c5 = c4.next(g);

    let r = cache.import_blocks(a0.branch([&c2, &c5]), &ctx).unwrap();
    assert_eq!(cache.tip().0, c5.hash, "{:#?}", cache);
    assert_matches!(r, ImportResult::TipChanged { .. });

    //                                <- d5 <- d6 *
    //                               /
    //            <- c2 <- c3 <- c4 <- c5
    //           /
    // a0 <- a1 <- a2 <- a3
    //                 \
    //                  <- b3 <- b4
    let d5 = c4.next(g);
    let d6 = d5.next(g);

    let mut headers = a0.headers();
    let mut rng = rand::thread_rng();
    let len = headers.len();

    // Expected longest chain.
    let expected = vec![
        a0.hash, a1.hash, c2.hash, c3.hash, c4.hash, d5.hash, d6.hash,
    ];

    for _ in 0..len * len {
        use rand::seq::SliceRandom;

        headers.shuffle(&mut rng);

        cache.import_blocks(headers.iter().cloned(), &ctx).unwrap();
        assert_eq!(cache.tip().0, d6.hash);

        model.import_blocks(headers.iter().cloned(), &ctx).unwrap();
        assert_eq!(model.tip().0, d6.hash);

        let actual = cache.chain().map(|h| h.block_hash()).collect::<Vec<_>>();
        assert_eq!(actual, expected);

        let actual = model.chain().map(|h| h.block_hash()).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}

#[test]
#[allow(clippy::identity_op)]
fn test_cache_locate_headers() {
    let network = bitcoin::Network::Bitcoin;
    let genesis = constants::genesis_block(network).header;
    let params = Params::new(network);
    let store = store::Memory::new(NonEmpty::new(genesis));
    let ctx = AdjustedTime::<net::SocketAddr>::new(LOCAL_TIME);
    let mut cache = BlockCache::from(store, params, &[]).unwrap();
    let chain = &nakamoto_test::BITCOIN_HEADERS;
    let height = chain.len() - 1;

    assert!(chain.len() > 99);

    cache.import_blocks(chain.iter().cloned(), &ctx).unwrap();

    assert!(cache
        .locate_headers(&[], BlockHash::default(), 1)
        .is_empty());

    assert_eq!(
        cache.locate_headers(&[], chain.last().block_hash(), 1),
        vec![*chain.last()],
        "Passing just a stop hash requests the block"
    );

    let unknown =
        BlockHash::from_hex("0f9188f13cb7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();

    assert_eq!(
        cache.locate_headers(&[unknown, unknown], BlockHash::default(), 4),
        chain.iter().skip(1).take(4).cloned().collect::<Vec<_>>(),
        "When the locators are unknown, starts from genesis"
    );

    let known1 = chain.get(12).unwrap();
    let known2 = chain.get(45).unwrap();

    assert_eq!(
        cache.locate_headers(
            &[unknown, known1.block_hash(), known2.block_hash()],
            BlockHash::default(),
            9
        ),
        chain
            .iter()
            .skip(12 + 1)
            .take(9)
            .cloned()
            .collect::<Vec<_>>(),
        "The headers start after the first known hash"
    );

    assert_eq!(
        cache.locate_headers(&[chain.last().block_hash()], BlockHash::default(), 9),
        vec![],
        "Nothing is returned if we're starting from the tip"
    );

    assert_eq!(
        cache.locate_headers(
            &[chain.get(height - 3).unwrap().block_hash()],
            BlockHash::default(),
            9
        ),
        vec![
            *chain.get(height - 2).unwrap(),
            *chain.get(height - 1).unwrap(),
            *chain.get(height - 0).unwrap()
        ],
        "If the max amount is more than we have, return up to the tip"
    );

    assert_eq!(
        cache.locate_headers(
            &[chain.get(5).unwrap().block_hash()],
            chain.get(8).unwrap().block_hash(),
            9
        ),
        vec![
            *chain.get(6).unwrap(),
            *chain.get(7).unwrap(),
            *chain.get(8).unwrap()
        ],
        "Stops at the stop hash"
    );

    assert_eq!(
        cache.locate_headers(
            &[chain.get(5).unwrap().block_hash()],
            chain.get(8).unwrap().block_hash(),
            2
        ),
        vec![*chain.get(6).unwrap(), *chain.get(7).unwrap(),],
        "Stops at the max"
    );

    assert_eq!(
        cache.locate_headers(&[unknown], chain.get(3).unwrap().block_hash(), 9),
        vec![
            *chain.get(1).unwrap(),
            *chain.get(2).unwrap(),
            *chain.get(3).unwrap()
        ],
        "If the stop hash is known, but the locators unknown, start from genesis"
    );

    assert_eq!(
        cache.locate_headers(
            &[chain.get(43).unwrap().block_hash()],
            chain.get(3).unwrap().block_hash(),
            9
        ),
        vec![],
        "If the stop height is earlier than the start height, we don't expect anything"
    );

    assert_eq!(
        cache.locate_headers(
            &[chain.get(4).unwrap().block_hash()],
            chain.get(4).unwrap().block_hash(),
            9
        ),
        vec![],
        "If the stop height is equal to the start height, we don't expect anything"
    );
}
