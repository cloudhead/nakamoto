//! Ephemeral storage backend for blocks.
use bitcoin::blockdata::block::BlockHeader;
use nonempty::NonEmpty;

use nakamoto_common::block::store::{Error, Store};
use nakamoto_common::block::Height;

#[derive(Debug, Clone)]
pub struct Memory(NonEmpty<BlockHeader>);

impl Memory {
    pub fn new(chain: NonEmpty<BlockHeader>) -> Self {
        Self(chain)
    }
}

impl Store for Memory {
    /// Get the genesis block.
    fn genesis(&self) -> BlockHeader {
        *self.0.first()
    }

    /// Append a batch of consecutive block headers to the end of the chain.
    fn put<I: Iterator<Item = BlockHeader>>(&mut self, headers: I) -> Result<Height, Error> {
        self.0.tail.extend(headers);
        Ok(self.0.len() as Height - 1)
    }

    /// Get the block at the given height.
    fn get(&self, height: Height) -> Result<BlockHeader, Error> {
        match self.0.get(height as usize) {
            Some(header) => Ok(*header),
            None => Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "unexpected end of file",
            ))),
        }
    }

    /// Rollback the chain to the given height.
    fn rollback(&mut self, height: Height) -> Result<(), Error> {
        match height {
            0 => self.0.tail.clear(),
            h => self.0.tail.truncate(h as usize),
        }
        Ok(())
    }

    /// Synchronize the changes to disk.
    fn sync(&mut self) -> Result<(), Error> {
        Ok(())
    }

    /// Iterate over all headers in the store.
    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Height, BlockHeader), Error>>> {
        Box::new(
            self.0
                .clone()
                .into_iter()
                .enumerate()
                .map(|(i, h)| Ok((i as Height, h))),
        )
    }

    /// Return the number of headers in the store.
    fn len(&self) -> Result<usize, Error> {
        Ok(self.0.len())
    }

    /// Return the height of the store.
    fn height(&self) -> Result<Height, Error> {
        Ok(self.0.len() as Height - 1)
    }

    /// Check data integrity.
    fn check(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Heal data corruption.
    fn heal(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
