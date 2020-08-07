//! Block cache model.
//! Not for production use.
use nakamoto_common::block::tree::{BlockTree, Branch, Error, ImportResult};
use nakamoto_common::block::Height;

use std::collections::{HashMap, VecDeque};

use nonempty::NonEmpty;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::hash_types::BlockHash;

use bitcoin::util::hash::BitcoinHash;

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
        let hash = genesis.bitcoin_hash();
        let chain = NonEmpty::new(genesis);

        headers.insert(hash, genesis);

        Self {
            headers,
            chain,
            tip: hash,
            genesis: hash,
        }
    }

    pub fn from(chain: Vec<BlockHeader>) -> Self {
        let chain = NonEmpty::from_vec(chain).unwrap();
        let genesis = chain.head.bitcoin_hash();
        let tip = chain.last().bitcoin_hash();

        let mut headers = HashMap::new();
        for h in chain.iter() {
            headers.insert(h.bitcoin_hash(), *h);
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
            self.headers.remove(&block.bitcoin_hash());
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
            Some(root) if root.bitcoin_hash() == self.genesis => {
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
                    let a_hash = a.last().bitcoin_hash();
                    let b_hash = b.last().bitcoin_hash();

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
            self.headers.insert(header.bitcoin_hash(), header);
        }
        let tip = self.tip;

        self.chain = self.longest_chain();
        self.tip = self.chain.last().bitcoin_hash();

        if tip != self.tip {
            Ok(ImportResult::TipChanged(self.tip, self.height(), vec![]))
        } else {
            Ok(ImportResult::TipUnchanged)
        }
    }

    fn get_block(&self, hash: &BlockHash) -> Option<(Height, &BlockHeader)> {
        for (height, header) in self.chain.iter().enumerate() {
            if hash == &header.bitcoin_hash() {
                return Some((height as Height, header));
            }
        }
        None
    }

    fn locators_hashes(&self, _from: Height) -> Vec<BlockHash> {
        todo!()
    }

    fn get_block_by_height(&self, height: Height) -> Option<&BlockHeader> {
        self.chain.get(height as usize)
    }

    fn tip(&self) -> (BlockHash, BlockHeader) {
        let tip = self.chain.last();
        (tip.bitcoin_hash(), *tip)
    }

    fn height(&self) -> Height {
        self.chain.len() as Height - 1
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (Height, BlockHeader)>> {
        let iter = self
            .chain
            .clone()
            .into_iter()
            .enumerate()
            .map(|(i, h)| (i as Height, h));

        Box::new(iter)
    }

    fn contains(&self, hash: &BlockHash) -> bool {
        self.headers.contains_key(hash) && self.chain.iter().any(|b| b.bitcoin_hash() == *hash)
    }

    fn is_known(&self, hash: &BlockHash) -> bool {
        self.headers.contains_key(hash)
    }
}
