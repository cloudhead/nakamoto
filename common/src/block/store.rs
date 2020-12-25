//! Block header storage.
#![allow(clippy::len_without_is_empty)]
use crate::block::Height;

use bitcoin::consensus::encode;
use thiserror::Error;

/// A block storage error.
#[derive(Debug, Error)]
pub enum Error {
    /// An I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// An error decoding block data.
    #[error("error decoding header: {0}")]
    Decoding(#[from] encode::Error),
    /// A data-corruption error.
    #[error("error: the store data is corrupt")]
    Corruption,
}

/// Represents objects that can store block headers.
pub trait Store {
    /// The type of header used in the store.
    type Header: Sized;

    /// Get the genesis block.
    fn genesis(&self) -> Self::Header;
    /// Append a batch of consecutive block headers to the end of the chain.
    fn put<I: Iterator<Item = Self::Header>>(&mut self, headers: I) -> Result<Height, Error>;
    /// Get the block at the given height.
    fn get(&self, height: Height) -> Result<Self::Header, Error>;
    /// Rollback the chain to the given height.
    fn rollback(&mut self, height: Height) -> Result<(), Error>;
    /// Synchronize the changes to disk.
    fn sync(&mut self) -> Result<(), Error>;
    /// Iterate over all headers in the store.
    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Height, Self::Header), Error>>>;
    /// Return the number of headers in the store.
    fn len(&self) -> Result<usize, Error>;
    /// Return the store block height.
    fn height(&self) -> Result<Height, Error>;
    /// Check the store integrity.
    fn check(&self) -> Result<(), Error>;
    /// Heal data corruption.
    fn heal(&mut self) -> Result<(), Error>;
}
