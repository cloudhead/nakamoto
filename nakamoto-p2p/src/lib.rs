#![allow(clippy::type_complexity)]
#![allow(clippy::new_without_default)]
#![allow(clippy::single_match)]
pub mod address_book;
pub mod checkpoints;
pub mod error;
pub mod event;
pub mod protocol;
pub mod reactor;
pub use bitcoin;

#[cfg(test)]
mod fallible;

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

#[macro_export]
macro_rules! fallible {
    ($err:expr) => {
        #[cfg(test)]
        {
            let fallible = fallible::FALLIBLE.lock().unwrap();

            if let Some(p) = *fallible {
                let r = fastrand::u64(0..100);

                if r <= (p * 100.) as u64 {
                    return Err($err.into());
                }
            }
        }
    };
}
