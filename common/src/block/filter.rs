//! Compact block filter core types and traits.
#![warn(missing_docs)]

use std::ops::RangeInclusive;

use thiserror::Error;

pub use bitcoin::hash_types::{FilterHash, FilterHeader};
pub use bitcoin::util::bip158::BlockFilter;

use super::Height;
use crate::block::store::{self, Genesis};
use crate::network::Network;

impl Genesis for FilterHeader {
    /// Filter header for the genesis block.
    ///
    /// ```
    /// use nakamoto_common::block::filter::{FilterHash, FilterHeader};
    /// use nakamoto_common::block::store::Genesis as _;
    /// use nakamoto_common::network::Network;
    /// use bitcoin_hashes::{hex::FromHex, sha256d};
    ///
    /// let genesis = FilterHeader::genesis(Network::Testnet);
    ///
    /// assert_eq!(
    ///     genesis.as_hash(),
    ///     sha256d::Hash::from_hex(
    ///         "21584579b7eb08997773e5aeff3a7f932700042d0ed2a6129012b7d7ae81b750"
    ///     ).unwrap()
    /// );
    /// ```
    fn genesis(network: Network) -> Self {
        let filter = BlockFilter::genesis(network);
        filter.filter_header(&FilterHeader::default())
    }
}

/// An error related to the filters access.
#[derive(Debug, Error)]
pub enum Error {
    /// Filter or header at given height not found.
    #[error("filter at height {0} not found")]
    NotFound(Height),
    /// A storage error occured.
    #[error("storage error: {0}")]
    Store(#[from] store::Error),
}

/// A trait for types that provide read/write access to compact block filters, and filter headers.
pub trait Filters {
    /// Get filter headers given a block height range.
    fn get_headers(&self, range: RangeInclusive<Height>) -> Vec<(FilterHash, FilterHeader)>;
    /// Get the filter header at the given height. Includes the hash of the filter itself.
    fn get_header(&self, height: Height) -> Option<(FilterHash, FilterHeader)>;
    /// Import filter headers.
    fn import_headers(&mut self, headers: Vec<(FilterHash, FilterHeader)>)
        -> Result<Height, Error>;
    /// Get the tip of the filter header chain.
    fn tip(&self) -> (&FilterHash, &FilterHeader);
    /// Get the height of the filter header chain.
    fn height(&self) -> Height;
    /// Get the filter header previous to the given height.
    fn get_prev_header(&self, height: Height) -> Option<FilterHeader> {
        if height == 0 {
            // If the start height is `0` (genesis), we return the zero hash as the parent.
            Some(FilterHeader::default())
        } else {
            self.get_header(height - 1).map(|(_, h)| h)
        }
    }
    /// Rollback chain by the given number of headers.
    fn rollback(&mut self, n: usize) -> Result<(), Error>;
    /// Truncate the filter header chain to zero.
    fn clear(&mut self) -> Result<(), Error>;
}
