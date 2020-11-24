//! Nakamoto's peer-to-peer library.
#![allow(clippy::type_complexity)]
#![allow(clippy::new_without_default)]
#![allow(clippy::single_match)]
#![allow(clippy::comparison_chain)]
#![deny(missing_docs, unsafe_code)]
pub mod address_book;
pub mod error;
pub mod event;
pub mod protocol;
pub mod reactor;
pub use bitcoin;

pub use protocol::PeerId;

#[cfg(test)]
mod fallible;

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

/// Makes a function randomly fail with the given error.
#[macro_export]
macro_rules! fallible {
    ($err:expr) => {
        #[cfg(test)]
        {
            let fallible = fallible::FALLIBLE.lock().unwrap();

            if let Some(p) = *fallible {
                let r = fastrand::f64();

                if r <= p {
                    return Err($err.into());
                }
            }
        }
    };
}
