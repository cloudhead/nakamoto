//! Block cache *model*.
//! Not for production use.

use std::ops::Range;

use nakamoto_common::block::filter::{self, BlockFilter, FilterHash, FilterHeader, Filters};
use nakamoto_common::block::iter::Iter;
use nakamoto_common::block::tree::{BlockTree, Branch, Error, ImportResult};
use nakamoto_common::block::Height;

use std::collections::{BTreeMap, HashMap, VecDeque};

use nonempty::NonEmpty;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::hash_types::BlockHash;

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
        for header in chain {
            self.headers.insert(header.block_hash(), header);
        }
        let tip = self.tip;

        self.chain = self.longest_chain();
        self.tip = self.chain.last().block_hash();

        if tip != self.tip {
            Ok(ImportResult::TipChanged(self.tip, self.height(), vec![]))
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

            Ok(ImportResult::TipChanged(self.tip, self.height(), vec![]))
        } else {
            Ok(ImportResult::TipUnchanged)
        }
    }

    fn get_block(&self, hash: &BlockHash) -> Option<(Height, &BlockHeader)> {
        for (height, header) in self.chain.iter().enumerate() {
            if hash == &header.block_hash() {
                return Some((height as Height, header));
            }
        }
        None
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

#[derive(Clone)]
pub struct FilterCache {
    headers: NonEmpty<(FilterHash, FilterHeader)>,
    filters: BTreeMap<Height, BlockFilter>,
}

impl FilterCache {
    pub fn new(genesis: FilterHeader) -> Self {
        Self {
            headers: NonEmpty::new((FilterHash::default(), genesis)),
            filters: BTreeMap::new(),
        }
    }
}

impl Filters for FilterCache {
    fn get_filters(&self, range: Range<Height>) -> Result<Vec<BlockFilter>, filter::Error> {
        Ok(self
            .filters
            .range(range)
            .map(|(_, f)| BlockFilter {
                // Nb. `BlockFilter` currently isn't `Clone`.
                content: f.content.clone(),
            })
            .collect())
    }

    fn import_filter(&mut self, height: Height, filter: BlockFilter) -> Result<(), filter::Error> {
        self.filters.insert(height, filter);

        Ok(())
    }

    fn get_header(&self, height: Height) -> Result<(FilterHash, FilterHeader), filter::Error> {
        match self.headers.get(height as usize) {
            Some(result) => Ok(result.clone()),
            None => Err(filter::Error::NotFound(height)),
        }
    }

    fn get_headers(
        &self,
        range: Range<Height>,
    ) -> Result<Vec<(FilterHash, FilterHeader)>, filter::Error> {
        assert!(range.start < range.end);

        Ok(self
            .headers
            .iter()
            .cloned()
            .skip(range.start as usize)
            .take(range.end as usize - range.start as usize)
            .collect())
    }

    fn import_headers(
        &mut self,
        headers: Vec<(FilterHash, FilterHeader)>,
    ) -> Result<(), filter::Error> {
        self.headers.tail.extend(headers);

        Ok(())
    }

    fn tip(&self) -> &(FilterHash, FilterHeader) {
        self.headers.last()
    }

    fn height(&self) -> Height {
        self.headers.tail.len() as Height
    }

    fn rollback(&mut self, n: usize) -> Result<(), filter::Error> {
        // Height to rollback to.
        let height = self.height() - n as Height;

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
}
