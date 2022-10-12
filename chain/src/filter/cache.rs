#![allow(dead_code)]
//! Compact block filter cache.

use std::io;
use std::ops::RangeInclusive;

use nakamoto_common::bitcoin::consensus::{encode, Decodable, Encodable};

use nakamoto_common::bitcoin_hashes::Hash;
pub use nakamoto_common::block::filter::{
    self, BlockFilter, Error, FilterHash, FilterHeader, Filters,
};
pub use nakamoto_common::block::store::Store;

use nakamoto_common::block::store::Genesis;
use nakamoto_common::block::Height;
use nakamoto_common::network::Network;
use nakamoto_common::nonempty::NonEmpty;

use crate::filter::store;

#[derive(Debug, Clone, Copy)]
pub struct StoredHeader {
    pub hash: FilterHash,
    pub header: FilterHeader,
}

impl Default for StoredHeader {
    fn default() -> Self {
        Self {
            hash: FilterHash::all_zeros(),
            header: FilterHeader::all_zeros(),
        }
    }
}

impl Encodable for StoredHeader {
    fn consensus_encode<W: io::Write + ?Sized>(&self, e: &mut W) -> Result<usize, io::Error> {
        let mut len = 0;

        len += self.hash.consensus_encode(e)?;
        len += self.header.consensus_encode(e)?;

        Ok(len)
    }
}

impl Decodable for StoredHeader {
    fn consensus_decode<D: io::Read + ?Sized>(d: &mut D) -> Result<Self, encode::Error> {
        let hash = FilterHash::consensus_decode(d)?;
        let header = FilterHeader::consensus_decode(d)?;

        Ok(StoredHeader { hash, header })
    }
}

impl Genesis for StoredHeader {
    fn genesis(network: Network) -> Self {
        Self {
            hash: FilterHash::genesis(network),
            header: FilterHeader::genesis(network),
        }
    }
}

pub struct FilterCache<S> {
    headers: NonEmpty<StoredHeader>,
    header_store: S,
}

impl<S: Store<Header = StoredHeader>> FilterCache<S> {
    pub fn load(header_store: S) -> Result<Self, nakamoto_common::block::store::Error> {
        Self::load_with(header_store, |_| true)
    }

    pub fn load_with(
        header_store: S,
        progress: impl Fn(Height) -> bool,
    ) -> Result<Self, nakamoto_common::block::store::Error> {
        let mut headers = NonEmpty::new(header_store.genesis());

        for (height, result) in header_store.iter().enumerate().skip(1) {
            let (_, header) = result?;
            headers.push(header);

            if !progress(height as Height) {
                return Err(nakamoto_common::block::store::Error::Interrupted);
            }
        }

        Ok(Self {
            header_store,
            headers,
        })
    }
}

impl<S> FilterCache<S> {
    /// Verify the filter header chain. Returns `true` if the chain is valid.
    pub fn verify(&self, network: Network) -> Result<(), store::Error> {
        self.verify_with(network, |_| true)
    }

    pub fn verify_with(
        &self,
        network: Network,
        progress: impl Fn(Height) -> bool,
    ) -> Result<(), store::Error> {
        let mut prev_header = FilterHeader::all_zeros();

        if self.headers.first().header != FilterHeader::genesis(network) {
            return Err(store::Error::Integrity);
        }

        for (height, stored_header) in self.headers.iter().enumerate() {
            let expected = stored_header.hash.filter_header(&prev_header);
            let actual = stored_header.header;

            if actual != expected {
                return Err(store::Error::Integrity);
            }
            prev_header = actual;

            if !progress(height as Height) {
                return Err(store::Error::Interrupted);
            }
        }
        Ok(())
    }
}

#[allow(unused_variables)]
impl<S: Store<Header = StoredHeader>> Filters for FilterCache<S> {
    fn get_header(&self, height: Height) -> Option<(FilterHash, FilterHeader)> {
        self.headers
            .get(height as usize)
            .map(|s| (s.hash, s.header))
    }

    fn get_headers(&self, range: RangeInclusive<Height>) -> Vec<(FilterHash, FilterHeader)> {
        let (start, end) = (*range.start(), *range.end());

        self.headers
            .iter()
            .skip(start as usize)
            .take(end as usize - start as usize + 1)
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

    fn rollback(&mut self, height: Height) -> Result<(), Error> {
        self.header_store.rollback(height)?;
        self.headers.tail.truncate(height as usize);

        Ok(())
    }

    fn clear(&mut self) -> Result<(), Error> {
        self.header_store.rollback(0)?;
        self.headers.tail.clear();

        Ok(())
    }
}
