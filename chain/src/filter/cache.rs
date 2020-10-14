#![allow(dead_code)]
//! Compact block filter cache.

use std::ops::Range;

pub use nakamoto_common::block::filter::{BlockFilter, Error, FilterHash, FilterHeader, Filters};
pub use nakamoto_common::block::store::Store;

use nakamoto_common::block::Height;

pub struct FilterCache<S> {
    header_store: S,
}

impl<S: Store<Header = FilterHeader>> FilterCache<S> {
    pub fn new(header_store: S) -> Self {
        Self { header_store }
    }
}

#[allow(unused_variables)]
impl<S: Store<Header = FilterHeader>> Filters for FilterCache<S> {
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
    fn import_headers(&mut self, headers: Vec<(FilterHash, FilterHeader)>) -> Result<(), Error> {
        todo!()
    }
    fn tip(&self) -> &(FilterHash, FilterHeader) {
        todo!()
    }
    fn height(&self) -> Height {
        todo!()
    }
}
