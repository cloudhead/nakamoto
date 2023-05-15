//! Block cache *model*.
//! Not for production use.

use std::ops::RangeInclusive;

use nakamoto_common::bitcoin_hashes::Hash;
use nakamoto_common::block::filter::{self, BlockFilter, FilterHash, FilterHeader, Filters};
use nakamoto_common::block::iter::Iter;
use nakamoto_common::block::tree::{BlockReader, BlockTree, Branch, Error, ImportResult};
use nakamoto_common::block::{Height, Work};
use nakamoto_common::nonempty::NonEmpty;

use std::collections::{BTreeMap, HashMap, VecDeque};

use nakamoto_common::bitcoin::blockdata::block::Header as BlockHeader;
use nakamoto_common::bitcoin::hash_types::BlockHash;

#[derive(Debug, Clone)]
pub struct Cache {
    pub headers: HashMap<BlockHash, BlockHeader>,
    pub chain: NonEmpty<BlockHeader>,
    pub tip: BlockHash,
    pub genesis: BlockHash,
}

impl Cache {
    pub fn new(genesis: BlockHeader) -> Self {
        let mut headers = HashMap::new();
        let hash = genesis.block_hash();
        let chain = NonEmpty::new(genesis);

        headers.insert(hash, genesis);

        Self {
            headers,
            chain,
            tip: hash,
            genesis: hash,
        }
    }

    pub fn from(chain: NonEmpty<BlockHeader>) -> Self {
        let genesis = chain.head.block_hash();
        let tip = chain.last().block_hash();

        let mut headers = HashMap::new();
        for h in chain.iter() {
            headers.insert(h.block_hash(), *h);
        }

        Self {
            headers,
            chain,
            tip,
            genesis,
        }
    }

    pub fn rollback(&mut self, height: Height) -> Result<(), Error> {
        for block in self.chain.tail.drain(height as usize..) {
            self.headers.remove(&block.block_hash());
        }
        Ok(())
    }

    fn branch(&self, tip: &BlockHash) -> Option<NonEmpty<BlockHeader>> {
        let mut headers = VecDeque::new();
        let mut tip = *tip;

        while let Some(header) = self.headers.get(&tip) {
            tip = header.prev_blockhash;
            headers.push_front(*header);
        }

        match headers.pop_front() {
            Some(root) if root.block_hash() == self.genesis => {
                Some(NonEmpty::from((root, headers.into())))
            }
            _ => None,
        }
    }

    fn longest_chain(&self) -> NonEmpty<BlockHeader> {
        let mut branches = Vec::new();

        for tip in self.headers.keys() {
            if let Some(branch) = self.branch(tip) {
                branches.push(branch);
            }
        }

        branches
            .into_iter()
            .max_by(|a, b| {
                let a_work = Branch(&a.tail).work();
                let b_work = Branch(&b.tail).work();

                if a_work == b_work {
                    let a_hash = a.last().block_hash();
                    let b_hash = b.last().block_hash();

                    b_hash.cmp(&a_hash)
                } else {
                    a_work.cmp(&b_work)
                }
            })
            .unwrap()
    }
}

impl BlockTree for Cache {
    fn import_blocks<I: Iterator<Item = BlockHeader>, C>(
        &mut self,
        chain: I,
        _context: &C,
    ) -> Result<ImportResult, Error> {
        let old = self.chain.clone();
        let mut disconnected = Vec::new();
        let mut connected = Vec::new();

        for header in chain {
            self.headers.insert(header.block_hash(), header);
        }
        let tip = self.tip;

        self.chain = self.longest_chain();
        self.tip = self.chain.last().block_hash();

        if tip != self.tip {
            for (height, header) in old.iter().enumerate() {
                if !self.chain.contains(header) {
                    disconnected.push((height as Height, *header));
                }
            }
            for (height, header) in self.chain.iter().enumerate() {
                if !old.contains(header) {
                    connected.push((height as Height, *header));
                }
            }
            let connected = NonEmpty::from_vec(connected).unwrap();

            Ok(ImportResult::TipChanged(
                self.chain.last().to_owned(),
                self.tip,
                self.height(),
                disconnected,
                connected,
            ))
        } else {
            Ok(ImportResult::TipUnchanged)
        }
    }

    fn extend_tip<C>(&mut self, header: BlockHeader, _context: &C) -> Result<ImportResult, Error> {
        if header.prev_blockhash == self.tip {
            let hash = header.block_hash();

            self.headers.insert(hash, header);
            self.chain.push(header);
            self.tip = hash;

            Ok(ImportResult::TipChanged(
                header,
                self.tip,
                self.height(),
                vec![],
                NonEmpty::new((self.height(), header)),
            ))
        } else {
            Ok(ImportResult::TipUnchanged)
        }
    }
}

impl BlockReader for Cache {
    fn get_block(&self, hash: &BlockHash) -> Option<(Height, &BlockHeader)> {
        for (height, header) in self.chain.iter().enumerate() {
            if hash == &header.block_hash() {
                return Some((height as Height, header));
            }
        }
        None
    }

    fn chain_work(&self) -> Work {
        let mut work = Work::REGTEST_MIN;

        for block in self.chain.iter() {
            work = work + block.work();
        }
        work
    }

    fn find_branch(&self, _to: &BlockHash) -> Option<(Height, NonEmpty<BlockHeader>)> {
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

    fn last_checkpoint(&self) -> Height {
        0
    }

    fn checkpoints(&self) -> BTreeMap<Height, BlockHash> {
        BTreeMap::new()
    }

    fn locator_hashes(&self, _from: Height) -> Vec<BlockHash> {
        vec![self.chain.last().block_hash()]
    }

    fn get_block_by_height(&self, height: Height) -> Option<&BlockHeader> {
        self.chain.get(height as usize)
    }

    fn tip(&self) -> (BlockHash, BlockHeader) {
        let tip = self.chain.last();
        (tip.block_hash(), *tip)
    }

    fn height(&self) -> Height {
        self.chain.len() as Height - 1
    }

    fn iter<'a>(&'a self) -> Box<dyn DoubleEndedIterator<Item = (Height, BlockHeader)> + 'a> {
        Box::new(Iter::new(&self.chain).map(|(i, h)| (i, *h)))
    }

    fn contains(&self, hash: &BlockHash) -> bool {
        self.headers.contains_key(hash) && self.chain.iter().any(|b| b.block_hash() == *hash)
    }

    fn is_known(&self, hash: &BlockHash) -> bool {
        self.headers.contains_key(hash)
    }
}

#[derive(Debug, Clone)]
pub struct FilterCache {
    headers: NonEmpty<(FilterHash, FilterHeader)>,
    filters: BTreeMap<Height, BlockFilter>,
}

impl FilterCache {
    pub fn new(genesis: FilterHeader) -> Self {
        Self {
            headers: NonEmpty::new((FilterHash::all_zeros(), genesis)),
            filters: BTreeMap::new(),
        }
    }

    pub fn from(headers: NonEmpty<(FilterHash, FilterHeader)>) -> Self {
        Self {
            headers,
            filters: BTreeMap::new(),
        }
    }
}

impl Filters for FilterCache {
    fn get_header(&self, height: Height) -> Option<(FilterHash, FilterHeader)> {
        self.headers.get(height as usize).copied()
    }

    fn get_headers(&self, range: RangeInclusive<Height>) -> Vec<(FilterHash, FilterHeader)> {
        let (start, end) = (*range.start(), *range.end());

        assert!(start <= end);

        self.headers
            .iter()
            .cloned()
            .skip(start as usize)
            .take(end as usize - start as usize + 1)
            .collect()
    }

    fn import_headers(
        &mut self,
        headers: Vec<(FilterHash, FilterHeader)>,
    ) -> Result<Height, filter::Error> {
        self.headers.tail.extend(headers);

        Ok(self.height())
    }

    fn tip(&self) -> (&FilterHash, &FilterHeader) {
        let (hash, header) = self.headers.last();
        (hash, header)
    }

    fn height(&self) -> Height {
        self.headers.tail.len() as Height
    }

    fn rollback(&mut self, height: Height) -> Result<(), filter::Error> {
        self.headers.tail.truncate(height as usize);

        let heights = self
            .filters
            .range(height + 1..)
            .map(|(h, _)| *h)
            .collect::<Vec<_>>();

        for h in heights {
            self.filters.remove(&h);
        }
        Ok(())
    }

    fn clear(&mut self) -> Result<(), filter::Error> {
        self.headers.tail.clear();
        self.filters.clear();

        Ok(())
    }
}
