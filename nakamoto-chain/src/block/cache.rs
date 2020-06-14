#[cfg(test)]
pub mod test;

use std::collections::{HashMap, VecDeque};

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;
use bitcoin::network::constants::Network;
use bitcoin::util::hash::BitcoinHash;

use nonempty::NonEmpty;

use crate::block::store::Store;
use crate::checkpoints;

use crate::block::tree::{BlockTree, Branch, Error};
use crate::block::{self, Bits, CachedBlock, Height, Time};

/// A chain candidate, forking off the active chain.
#[derive(Debug)]
struct Candidate {
    tip: BlockHash,
    headers: Vec<BlockHeader>,
    fork_height: Height,
    fork_hash: BlockHash,
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
        // TODO: This should come from somewhere else. Also, it shouldn't
        // be used for networks other than Mainnet.
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

        // Insert genesis in the headers map, but skip it during iteration.
        headers.insert(chain.head.hash, 0);
        for result in store.iter().skip(1) {
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
        let tip = self.chain.last();

        // Block extends the active chain.
        if header.prev_blockhash == tip.hash {
            let height = tip.height + 1;
            let bits = if self.params.allow_min_difficulty_blocks {
                if header.time > tip.time + self.params.pow_target_spacing as Time * 2 {
                    BlockHeader::compact_target_from_u256(&self.params.pow_limit)
                } else {
                    self.next_min_difficulty_target(tip.bits, &self.params)
                }
            } else {
                self.next_difficulty_target(tip.height, tip.time, tip.bits, &self.params)
            };

            // TODO: Validate timestamp.
            let target = block::target_from_bits(bits);
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
            self.extend_chain(CachedBlock {
                height,
                hash,
                header,
            });
        } else if self.headers.contains_key(&hash) || self.orphans.contains_key(&hash) {
            return Err(Error::DuplicateBlock(hash));
        } else {
            self.orphans.insert(hash, header);
        }

        let candidates = self.chain_candidates();

        if candidates.is_empty()
            && !self.headers.contains_key(&header.prev_blockhash)
            && !self.orphans.contains_key(&header.prev_blockhash)
        {
            return Err(Error::BlockMissing(header.prev_blockhash));
        }

        // TODO(perf): Skip overlapping branches.
        for branch in candidates.iter() {
            let candidate_work = Branch(&branch.headers).work();
            let main_work = Branch(self.chain_suffix(branch.fork_height)).work();

            // TODO: Validate branch before switching to it.
            if candidate_work > main_work {
                self.switch_to_fork(branch);
            } else if self.params.network != Network::Bitcoin {
                if candidate_work == main_work {
                    // Nb. We intend here to compare the hashes as integers, and pick the lowest
                    // hash as the winner. However, the `PartialEq` on `BlockHash` is implemented on
                    // the underlying `[u8]` array, and does something different (lexographical
                    // comparison). Since this code isn't run on Mainnet, it's okay, as it serves
                    // its purpose of being determinstic when choosing the active chain.
                    if branch.tip < self.chain.last().hash {
                        self.switch_to_fork(branch);
                    }
                }
            }
        }
        Ok((self.chain.last().hash, self.height()))
    }

    fn chain_candidates(&self) -> Vec<Candidate> {
        let mut branches = Vec::new();

        for tip in self.orphans.keys() {
            if let Some(branch) = self.branch(tip) {
                branches.push(branch);
            }
        }
        branches
    }

    fn branch(&self, tip: &BlockHash) -> Option<Candidate> {
        let tip = *tip;

        let mut headers = VecDeque::new();
        let mut cursor = tip;

        while let Some(header) = self.orphans.get(&cursor) {
            cursor = header.prev_blockhash;
            headers.push_front(*header);
        }

        if let Some(height) = self.headers.get(&cursor) {
            Some(Candidate {
                tip,
                fork_height: *height,
                fork_hash: cursor,
                headers: headers.into(),
            })
        } else {
            None
        }
    }

    fn next_min_difficulty_target(&self, last_bits: Bits, params: &Params) -> Bits {
        let pow_limit = BlockHeader::compact_target_from_u256(&params.pow_limit);
        let mut bits = last_bits;

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

    /// Rollback active chain to the given height.
    fn rollback(&mut self, height: Height) {
        for block in self.chain.tail.drain(height as usize..) {
            self.headers.remove(&block.hash);
            self.orphans.insert(block.hash, block.header);
        }
    }

    /// Activate a fork candidate.
    fn switch_to_fork(&mut self, branch: &Candidate) {
        self.rollback(branch.fork_height);

        for (i, header) in branch.headers.iter().enumerate() {
            self.extend_chain(CachedBlock {
                height: branch.fork_height + i as Height + 1,
                hash: header.bitcoin_hash(),
                header: *header,
            });
        }
    }

    /// Extend the active chain with a block.
    fn extend_chain(&mut self, blk: CachedBlock) {
        assert_eq!(blk.header.prev_blockhash, self.chain.last().hash);

        self.headers.insert(blk.hash, blk.height);
        self.orphans.remove(&blk.hash);
        self.chain.push(blk);
    }

    // TODO: Doctest.
    fn chain_suffix(&self, height: Height) -> &[CachedBlock] {
        &self.chain.tail[height as usize..]
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

        Ok(result.unwrap_or((self.chain.last().hash, self.height())))
    }

    fn get_block(&self, hash: &BlockHash) -> Option<&BlockHeader> {
        self.headers
            .get(hash)
            .and_then(|height| self.chain.get(*height as usize))
            .map(|blk| &blk.header)
    }

    fn get_block_by_height(&self, height: Height) -> Option<&BlockHeader> {
        self.chain.get(height as usize).map(|b| &b.header)
    }

    fn tip(&self) -> (BlockHash, BlockHeader) {
        (self.chain.last().hash, self.chain.last().header)
    }

    fn genesis(&self) -> &BlockHeader {
        &self.chain.first().header
    }

    /// Iterate over the longest chain, starting from the tip.
    fn iter(&self) -> Box<dyn Iterator<Item = (Height, BlockHeader)>> {
        // TODO: Don't copy the whole chain!
        Box::new(
            self.chain
                .clone()
                .into_iter()
                .enumerate()
                .map(|(i, h)| (i as Height, h.header)),
        )
    }

    /// Return the height of the longest chain.
    fn height(&self) -> Height {
        self.chain.tail.len() as Height
    }
}
