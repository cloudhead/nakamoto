#![allow(dead_code)]
//! Compact block filter cache.

use std::io;
use std::ops::Range;

use bitcoin::consensus::{encode, Decodable, Encodable};

pub use nakamoto_common::block::filter::{
    self, BlockFilter, Error, FilterHash, FilterHeader, Filters,
};
pub use nakamoto_common::block::store::Store;

use nakamoto_common::block::Height;
use nakamoto_common::network::Network;

#[derive(Debug, Clone, Copy, Default)]
pub struct StoredHeader {
    hash: FilterHash,
    header: FilterHeader,
}

impl Encodable for StoredHeader {
    fn consensus_encode<W: io::Write>(&self, mut e: W) -> Result<usize, encode::Error> {
        let mut len = 0;

        len += self.hash.consensus_encode(&mut e)?;
        len += self.header.consensus_encode(&mut e)?;

        Ok(len)
    }
}

impl Decodable for StoredHeader {
    fn consensus_decode<D: io::Read>(mut d: D) -> Result<Self, encode::Error> {
        let hash = FilterHash::consensus_decode(&mut d)?;
        let header = FilterHeader::consensus_decode(&mut d)?;

        Ok(StoredHeader { hash, header })
    }
}

impl StoredHeader {
    pub fn genesis(network: Network) -> Self {
        Self {
            hash: filter::genesis_hash(network),
            header: FilterHeader::genesis(network),
        }
    }
}

pub struct FilterCache<S> {
    header_store: S,
}

impl<S: Store<Header = StoredHeader>> FilterCache<S> {
    pub fn new(header_store: S) -> Self {
        Self { header_store }
    }
}

#[allow(unused_variables)]
impl<S: Store<Header = StoredHeader>> Filters for FilterCache<S> {
    fn get_filters(&self, range: Range<Height>) -> Result<Vec<BlockFilter>, Error> {
        todo!()
    }
    fn import_filter(&mut self, height: Height, filter: BlockFilter) -> Result<(), Error> {
        todo!()
    }
    fn get_header(&self, height: Height) -> Result<(FilterHash, FilterHeader), Error> {
        todo!()
    }
    fn get_headers(&self, range: Range<Height>) -> Result<Vec<(FilterHash, FilterHeader)>, Error> {
        todo!()
    }
    fn import_headers(
        &mut self,
        headers: Vec<(FilterHash, FilterHeader)>,
    ) -> Result<Height, Error> {
        self.header_store
            .put(
                headers
                    .into_iter()
                    .map(|(hash, header)| StoredHeader { hash, header }),
            )
            .map_err(Error::from)
    }
    fn tip(&self) -> &(FilterHash, FilterHeader) {
        todo!()
    }
    fn height(&self) -> Height {
        0
    }
    fn rollback(&mut self, n: usize) -> Result<(), Error> {
        todo!()
    }
}
