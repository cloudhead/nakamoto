//! Library of common Bitcoin functionality shared by all crates.
#![allow(clippy::type_complexity)]
#![deny(missing_docs, unsafe_code)]
pub mod block;
pub mod collections;
pub mod network;
pub mod p2p;

pub use bitcoin;
pub use bitcoin_hashes;
pub use nakamoto_net as net;
pub use nonempty;

/// Return the function path at the current source location.
#[macro_export]
macro_rules! source {
    () => {{
        fn f() {}
        fn type_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_of(f);
        &name[..name.len() - 3]
    }};
}
