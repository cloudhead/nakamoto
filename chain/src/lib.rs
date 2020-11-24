//! Functionality around proof-of-work chains.
#[allow(clippy::len_without_is_empty)]
#[allow(clippy::collapsible_if)]
#[allow(clippy::type_complexity)]
#[deny(
    unsafe_code,
    missing_docs,
    missing_debug_implementations,
    missing_copy_implementations
)]
pub mod block;
pub use block::*;

pub mod filter;

#[cfg(test)]
mod tests;
