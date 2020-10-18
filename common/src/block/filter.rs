//! Compact block filter core types and traits.
#![warn(missing_docs)]

use std::io;
use std::ops::Range;

use thiserror::Error;

use bitcoin::consensus::encode;
use bitcoin::consensus::{Decodable, Encodable};

pub use bitcoin::hash_types::FilterHash;
pub use bitcoin::util::bip158::BlockFilter;

use super::Height;
use crate::block::store;
use crate::network::Network;

/// A filter header.
///
/// This type is used to distinguish hashes from headers, which are concatenations of filter
/// hashes.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FilterHeader {
    hash: FilterHash,
}

impl Encodable for FilterHeader {
    fn consensus_encode<W: io::Write>(&self, e: W) -> Result<usize, encode::Error> {
        self.hash.consensus_encode(e)
    }
}

impl Decodable for FilterHeader {
    fn consensus_decode<D: io::Read>(d: D) -> Result<Self, encode::Error> {
        let hash = FilterHash::consensus_decode(d)?;
        Ok(FilterHeader { hash })
    }
}

impl AsRef<[u8]> for FilterHeader {
    fn as_ref(&self) -> &[u8] {
        self.hash.as_ref()
    }
}

impl From<FilterHash> for FilterHeader {
    fn from(hash: FilterHash) -> Self {
        FilterHeader { hash }
    }
}

impl From<FilterHeader> for FilterHash {
    fn from(header: FilterHeader) -> Self {
        header.hash
    }
}

impl Default for FilterHeader {
    fn default() -> Self {
        FilterHeader {
            hash: FilterHash::default(),
        }
    }
}

impl FilterHeader {
    /// Create a new filter header from the filter hash and the previous header.
    ///
    /// *BIP 157: The canonical hash of a block filter is the double-SHA256 of the serialized
    /// filter.  Filter headers are 32-byte hashes derived for each block filter. They are computed
    /// as the double-SHA256 of the concatenation of the filter hash with the previous filter
    /// header.*
    ///
    pub fn new(filter_hash: FilterHash, prev_header: &FilterHeader) -> Self {
        use bitcoin_hashes::Hash;

        let mut header_bytes = [0u8; 64];

        header_bytes[0..32].copy_from_slice(&filter_hash[..]);
        header_bytes[32..64].copy_from_slice(&prev_header.as_ref()[..]);

        Self {
            hash: FilterHash::hash(&header_bytes),
        }
    }

    /// Filter header for the genesis block.
    ///
    /// ```
    /// use nakamoto_common::block::filter::{FilterHash, FilterHeader};
    /// use nakamoto_common::network::Network;
    /// use bitcoin_hashes::hex::FromHex;
    ///
    /// let genesis = FilterHeader::genesis(Network::Testnet);
    ///
    /// assert_eq!(
    ///     FilterHash::from(genesis),
    ///     FilterHash::from_hex(
    ///         "21584579b7eb08997773e5aeff3a7f932700042d0ed2a6129012b7d7ae81b750"
    ///     ).unwrap()
    /// );
    /// ```
    pub fn genesis(network: Network) -> Self {
        let genesis = network.genesis_block();
        let filter = BlockFilter::new_script_filter(&genesis, |_| {
            panic!("FilterHeader::genesis: genesis block should have no inputs")
        })
        .unwrap();

        FilterHeader {
            hash: filter.filter_id(&FilterHash::default()),
        }
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
    /// Get filters, given a block height range.
    fn get_filters(&self, range: Range<Height>) -> Result<Vec<BlockFilter>, Error>;
    /// Import the filter for the given block height.
    fn import_filter(&mut self, height: Height, filter: BlockFilter) -> Result<(), Error>;
    /// Get filter headers given a block height range.
    fn get_headers(&self, range: Range<Height>) -> Result<Vec<(FilterHash, FilterHeader)>, Error>;
    /// Get the filter header at the given height. Includes the hash of the filter itself.
    fn get_header(&self, height: Height) -> Result<(FilterHash, FilterHeader), Error>;
    /// Import filter headers.
    fn import_headers(&mut self, headers: Vec<(FilterHash, FilterHeader)>) -> Result<(), Error>;
    /// Get the tip of the filter header chain.
    fn tip(&self) -> &(FilterHash, FilterHeader);
    /// Get the height of the filter header chain.
    fn height(&self) -> Height;
    /// Get the filter header previous to the given height.
    fn get_prev_header(&self, height: Height) -> Result<FilterHeader, Error> {
        if height == 0 {
            // If the start height is `0` (genesis), we return the zero hash as the parent.
            Ok(FilterHeader::default())
        } else {
            let (_, header) = self.get_header(height - 1)?;
            Ok(header)
        }
    }
    /// Rollback headers to the given height.
    fn rollback(&mut self, height: Height) -> Result<(), Error>;
}
