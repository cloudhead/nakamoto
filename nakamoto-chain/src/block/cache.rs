#[cfg(test)]
pub mod test;

use std::collections::HashMap;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;
use bitcoin::util::hash::BitcoinHash;

use nonempty::NonEmpty;

use crate::block::store::Store;
use crate::checkpoints;

use crate::block::tree::{BlockTree, Error};
use crate::block::{self, CachedBlock, Height, Time, Bits};

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

    fn next_min_difficulty_target(
        &self,
        last_bits: Bits,
        params: &Params,
    ) -> Bits {
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
