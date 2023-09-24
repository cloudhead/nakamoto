//! Block header storage.
#![allow(clippy::len_without_is_empty)]
use std::borrow::Borrow;

use crate::block::Height;

use bitcoin::bip158::BlockFilter;
use bitcoin::block::Header;
use bitcoin::consensus::encode;
use bitcoin::hash_types::FilterHash;
use bitcoin::{OutPoint, Script, ScriptBuf};
use thiserror::Error;

use crate::network::Network;
use crate::source;

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
    /// Operation was interrupted.
    #[error("the operation was interrupted")]
    Interrupted,
}

/// Represents an object (such as a header), that has a genesis.
pub trait Genesis {
    /// Create a genesis header.
    fn genesis(network: Network) -> Self;
}

/// Genesis implementation for `bitcoin`'s header.
impl Genesis for Header {
    fn genesis(network: Network) -> Self {
        network.genesis()
    }
}

/// Genesis implementation for `bitcoin`'s `FilterHash`.
impl Genesis for FilterHash {
    fn genesis(network: Network) -> Self {
        use bitcoin::hashes::Hash;

        let genesis = network.genesis_block();
        let filter = BlockFilter::new_script_filter(
            &genesis,
            |_| -> Result<ScriptBuf, bitcoin::bip158::Error> {
                panic!("{}: genesis block should have no inputs", source!())
            },
        )
        .unwrap();

        FilterHash::hash(&filter.content)
    }
}

/// Genesis implementation for `bitcoin`'s `BlockFilter`.
impl Genesis for BlockFilter {
    fn genesis(network: Network) -> Self {
        let genesis = network.genesis_block();

        BlockFilter::new_script_filter(&genesis, |_| -> Result<ScriptBuf, bitcoin::bip158::Error> {
            panic!("{}: genesis block should have no inputs", source!())
        })
        .unwrap()
    }
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
    fn heal(&self) -> Result<(), Error>;
}
