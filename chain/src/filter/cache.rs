#![allow(dead_code)]
//! Compact block filter cache.

use std::io;
use std::ops::Range;

use nonempty::NonEmpty;

use bitcoin::consensus::{encode, Decodable, Encodable};

pub use nakamoto_common::block::filter::{
    self, BlockFilter, Error, FilterHash, FilterHeader, Filters,
};
pub use nakamoto_common::block::store::Store;

use nakamoto_common::block::Height;
use nakamoto_common::network::Network;

use crate::filter::store;

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
    headers: NonEmpty<StoredHeader>,
    header_store: S,
}

impl<S: Store<Header = StoredHeader>> FilterCache<S> {
    pub fn from(header_store: S) -> Result<Self, nakamoto_common::block::store::Error> {
        let mut headers = NonEmpty::new(header_store.genesis());

        for result in header_store.iter().skip(1) {
            let (_, header) = result?;
            headers.push(header);
        }

        Ok(Self {
            header_store,
            headers,
        })
    }
}

impl<S> FilterCache<S> {
    /// Verify the filter header chain. Returns `true` if the chain is valid.
    pub fn verify(&self) -> Result<(), store::Error> {
        let prev_header = FilterHeader::default();

        for stored_header in self.headers.iter() {
            let expected = FilterHeader::new(stored_header.hash, &prev_header);
            let actual = stored_header.header;

            if actual != expected {
                return Err(store::Error::Integrity);
            }
        }
        Ok(())
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

    fn get_header(&self, height: Height) -> Option<(FilterHash, FilterHeader)> {
        self.headers
            .get(height as usize)
            .map(|s| (s.hash, s.header))
    }

    fn get_headers(&self, range: Range<Height>) -> Vec<(FilterHash, FilterHeader)> {
        self.headers
            .iter()
            .skip(range.start as usize)
            .take(range.end as usize - range.start as usize)
            .map(|h| (h.hash, h.header))
            .collect()
    }

    fn import_headers(
        &mut self,
        headers: Vec<(FilterHash, FilterHeader)>,
    ) -> Result<Height, Error> {
        let iter = headers
            .into_iter()
            .map(|(hash, header)| StoredHeader { hash, header });

        self.headers.tail.extend(iter.clone());
        self.header_store.put(iter).map_err(Error::from)
    }

    fn tip(&self) -> (&FilterHash, &FilterHeader) {
        let StoredHeader { hash, header } = self.headers.last();
        (hash, header)
    }

    fn height(&self) -> Height {
        self.headers.tail.len() as Height
    }

    fn rollback(&mut self, n: usize) -> Result<(), Error> {
        // Height to rollback to.
        let height = self.height() - n as Height;

        self.header_store.rollback(height)?;
        self.headers.tail.truncate(height as usize);

        Ok(())
    }
}
