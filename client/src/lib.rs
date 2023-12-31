//! Nakamoto's client library.
#![allow(clippy::inconsistent_struct_constructor)]
#![allow(clippy::type_complexity)]
#![deny(missing_docs, unsafe_code)]
mod client;
mod error;
#[allow(hidden_glob_reexports)]
mod event;
mod peer;
mod service;

pub use client::*;
pub mod handle;

#[cfg(test)]
mod tests;
