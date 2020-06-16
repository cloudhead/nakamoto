//! Block storage.

pub mod io;
pub mod memory;

pub use io::File;
pub use memory::Memory;

use crate::block::Height;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::encode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error decoding block: {0}")]
    Decoding(#[from] encode::Error),
    #[error("error: the store data is corrupt")]
    Corruption,
}

pub trait Store {
    /// Get the genesis block.
    fn genesis(&self) -> Result<BlockHeader, Error> {
        self.get(0)
    }
    /// Append a batch of consecutive block headers to the end of the chain.
    fn put<I: Iterator<Item = BlockHeader>>(&mut self, headers: I) -> Result<Height, Error>;
    /// Get the block at the given height.
    fn get(&self, height: Height) -> Result<BlockHeader, Error>;
    /// Rollback the chain to the given height.
    fn rollback(&mut self, height: Height) -> Result<(), Error>;
    /// Synchronize the changes to disk.
    fn sync(&mut self) -> Result<(), Error>;
    /// Iterate over all headers in the store.
    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Height, BlockHeader), Error>>>;
    /// Return the number of headers in the store.
    fn len(&self) -> Result<usize, Error>;
    /// Check the store integrity.
    fn check(&self) -> Result<(), Error>;
    /// Heal data corruption.
    fn heal(&mut self) -> Result<(), Error>;
}
