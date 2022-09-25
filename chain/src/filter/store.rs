//! Compact block filter store.

use thiserror::Error;

pub use nakamoto_common::block::filter::{BlockFilter, FilterHash, FilterHeader, Filters};
pub use nakamoto_common::block::store::Store;

pub type File = crate::store::io::File<FilterHeader>;
pub type Memory = crate::store::memory::Memory<FilterHeader>;

/// A filter error occuring in the store.  Can happen if the store is corrupted.
#[derive(Debug, Error)]
pub enum Error {
    #[error("filter store is corrupted")]
    Integrity,
    #[error("the operation was interrupted")]
    Interrupted,
}
