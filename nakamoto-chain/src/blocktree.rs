use std::collections::HashMap;
use std::ops::Deref;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::hash_types::BlockHash;
use bitcoin::util::hash::BitcoinHash;

use thiserror::Error;

/// An error related to the block tree.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid chain")]
    InvalidChain,
}

/// A valid hash-linked sequence of blocks.
#[derive(Debug)]
pub struct Chain(Vec<(BlockHash, BlockHeader)>);

impl Chain {
    pub fn try_from(blks: Vec<BlockHeader>) -> Result<Self, Error> {
        // TODO: Validate chain.
        let chain = blks.into_iter().map(|h| (h.bitcoin_hash(), h)).collect();
        Ok(Self(chain))
    }
}

impl From<Chain> for Vec<(BlockHash, BlockHeader)> {
    fn from(Chain(blks): Chain) -> Self {
        blks
    }
}

impl Deref for Chain {
    type Target = Vec<(BlockHash, BlockHeader)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A representation of all known blocks that keeps track of the longest chain.
pub trait BlockTree {
    fn insert_block(&mut self, hash: BlockHash, block: BlockHeader) -> Option<BlockHeader>;
    fn insert_chain(&mut self, chain: Chain);

    fn get_block(&self, hash: &BlockHash) -> Option<&BlockHeader>;

    fn chain(&self) -> ChainIter;
    fn height(&self) -> u64;
    fn tip(&self) -> &BlockHash;
}

/// An implementation of `BlockTree`.
#[derive(Debug)]
pub struct BlockCache {
    headers: HashMap<BlockHash, BlockHeader>,
    tip: BlockHash,
    height: u64,
}

impl BlockCache {
    /// Create a new `BlockCache` given the hash of the genesis block.
    pub fn new(genesis: BlockHash) -> Self {
        Self {
            headers: HashMap::new(),
            tip: genesis,
            height: 0,
        }
    }
}

impl BlockTree for BlockCache {
    fn insert_block(&mut self, hash: BlockHash, block: BlockHeader) -> Option<BlockHeader> {
        if block.prev_blockhash == self.tip {
            self.height += 1;
            self.tip = hash;
            self.headers.insert(hash, block)
        } else {
            None
        }
    }

    fn insert_chain(&mut self, chain: Chain) {
        Vec::from(chain).into_iter().for_each(|(hash, header)| {
            self.insert_block(hash, header);
        })
    }

    /// Get a block by its hash.
    fn get_block(&self, hash: &BlockHash) -> Option<&BlockHeader> {
        self.headers.get(hash)
    }

    /// Iterate over the longest chain, starting from the tip.
    fn chain(&self) -> ChainIter {
        ChainIter {
            next: self.tip,
            headers: &self.headers,
        }
    }

    /// Return the height of the longest chain.
    fn height(&self) -> u64 {
        self.height
    }

    /// Return the tip of the longest chain.
    fn tip(&self) -> &BlockHash {
        &self.tip
    }
}

/// An iterator over `BlockHash` values, starting from the tip of a chain.
pub struct ChainIter<'a> {
    next: BlockHash,
    headers: &'a HashMap<BlockHash, BlockHeader>,
}

impl<'a> Iterator for ChainIter<'a> {
    type Item = &'a BlockHeader;

    fn next(&mut self) -> Option<Self::Item> {
        match self.headers.get(&self.next) {
            Some(header) => {
                self.next = header.prev_blockhash;
                Some(header)
            }
            None => None,
        }
    }
}
