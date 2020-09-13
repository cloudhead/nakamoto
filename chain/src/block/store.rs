//! Block storage backends.

pub use nakamoto_common::block::store::*;

pub mod io;
pub mod memory;

pub use io::File;
pub use memory::Memory;
