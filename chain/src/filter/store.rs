//! Compact block filter store.

pub use nakamoto_common::block::filter::{BlockFilter, FilterHash, FilterHeader, Filters};
pub use nakamoto_common::block::store::Store;

pub type File = crate::store::io::File<FilterHeader>;
pub type Memory = crate::store::memory::Memory<FilterHeader>;
