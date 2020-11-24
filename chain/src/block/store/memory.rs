//! Ephemeral storage backend for blocks.
use nonempty::NonEmpty;

use nakamoto_common::block::store::{Error, Store};
use nakamoto_common::block::Height;

/// In-memory block store.
#[derive(Debug, Clone)]
pub struct Memory<H>(NonEmpty<H>);

impl<H> Memory<H> {
    /// Create a new in-memory block store.
    pub fn new(chain: NonEmpty<H>) -> Self {
        Self(chain)
    }
}

impl<H: Default> Default for Memory<H> {
    fn default() -> Self {
        Self(NonEmpty::new(H::default()))
    }
}

impl<H: 'static + Copy + Clone> Store for Memory<H> {
    type Header = H;

    /// Get the genesis block.
    fn genesis(&self) -> H {
        *self.0.first()
    }

    /// Append a batch of consecutive block headers to the end of the chain.
    fn put<I: Iterator<Item = H>>(&mut self, headers: I) -> Result<Height, Error> {
        self.0.tail.extend(headers);
        Ok(self.0.len() as Height - 1)
    }

    /// Get the block at the given height.
    fn get(&self, height: Height) -> Result<H, Error> {
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
    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Height, H), Error>>> {
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
