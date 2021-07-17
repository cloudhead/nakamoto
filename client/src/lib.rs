//! Nakamoto's client library.
#![allow(clippy::inconsistent_struct_constructor)]
#![allow(clippy::type_complexity)]
#![deny(missing_docs, unsafe_code)]
pub mod client;
pub mod error;
pub mod handle;
pub mod peer;
pub mod txnmgr;

pub use client::*;

#[cfg(test)]
mod tests;
