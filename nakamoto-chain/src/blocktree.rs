use std::collections::HashMap;
use std::ops::Deref;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::hash_types::BlockHash;
use bitcoin::util::hash::BitcoinHash;

use nonempty::NonEmpty;
use thiserror::Error;

/// An error related to the block tree.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid chain")]
    InvalidChain,
    #[error("empty chain")]
    EmptyChain,
}

/// A valid hash-linked sequence of blocks.
#[derive(Debug)]
pub struct Chain(NonEmpty<(BlockHash, BlockHeader)>);

impl Chain {
    /// Create a `Chain` from a list of headers. Fails if the list is empty,
    /// or the headers are not hash-linked.
    pub fn try_from(blks: Vec<BlockHeader>) -> Result<Self, Error> {
        let blks = NonEmpty::from_vec(blks).ok_or(Error::EmptyChain)?;
        let head_hash = blks.head.bitcoin_hash();

        let mut chain =
            NonEmpty::from(((head_hash, blks.head), Vec::with_capacity(blks.tail.len())));
        let mut tail = blks.tail.into_iter();
        let mut prev_hash = head_hash;

        while let Some(blk) = tail.next() {
            if blk.prev_blockhash != prev_hash {
                return Err(Error::InvalidChain);
            }
            let hash = blk.bitcoin_hash();

            prev_hash = hash;
            chain.push((hash, blk));
        }
        Ok(Self(chain))
    }

    pub fn root(&self) -> (&BlockHash, &BlockHeader) {
        let (hash, header) = self.0.first();
        (hash, header)
    }
}

impl From<Chain> for NonEmpty<(BlockHash, BlockHeader)> {
    fn from(Chain(blks): Chain) -> Self {
        blks
    }
}

impl Deref for Chain {
    type Target = NonEmpty<(BlockHash, BlockHeader)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A representation of all known blocks that keeps track of the longest chain.
pub trait BlockTree {
    /// Import a block into the block tree.
    fn insert_block(&mut self, hash: BlockHash, block: BlockHeader) -> Option<BlockHeader>;
    /// Import a chain into the block tree.
    fn insert_chain(&mut self, chain: Chain) -> Result<(BlockHash, u64), Error>;
    /// Get a block by its hash.
    fn get_block(&self, hash: &BlockHash) -> Option<&BlockHeader>;
    /// Iterate over the longest chain, starting from the tip.
    fn chain(&self) -> ChainIter;
    /// Return the height of the longest chain.
    fn height(&self) -> u64;
    /// Return the tip of the longest chain.
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

    fn insert_chain(&mut self, chain: Chain) -> Result<(BlockHash, u64), Error> {
        NonEmpty::from(chain)
            .into_iter()
            .for_each(|(hash, header)| {
                self.insert_block(hash, header);
            });

        // TODO: Validate target difficulty
        // TODO: Validate proof of work
        // TODO: Validate timestamp

        Err(Error::InvalidChain)
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
