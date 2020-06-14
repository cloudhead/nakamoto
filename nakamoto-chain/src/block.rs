pub mod cache;
pub mod store;
pub mod tree;

pub use bitcoin::blockdata::block::BlockHeader;
pub use bitcoin::hash_types::BlockHash;

use std::ops::Deref;

/// Difficulty target of a block.
pub type Target = bitcoin::util::uint::Uint256;

/// Block work.
pub type Work = bitcoin::util::uint::Uint256;

/// Compact difficulty bits (target) of a block.
pub type Bits = u32;

/// Height of a block.
pub type Height = u64;

/// Block timestamp.
pub type Time = u32;

#[derive(Debug, Clone, Copy)]
pub struct CachedBlock {
    height: Height,
    hash: BlockHash,
    header: BlockHeader,
}

impl Deref for CachedBlock {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

impl tree::Header for CachedBlock {
    fn work(&self) -> Work {
        self.header.work()
    }
}

// Convert a compact difficulty representation to 256-bits.
// Taken from `BlockHeader::target` from the `bitcoin` library.
fn target_from_bits(bits: u32) -> Target {
    let (mant, expt) = {
        let unshifted_expt = bits >> 24;
        if unshifted_expt <= 3 {
            ((bits & 0xFFFFFF) >> (8 * (3 - unshifted_expt as usize)), 0)
        } else {
            (bits & 0xFFFFFF, 8 * ((bits >> 24) - 3))
        }
    };

    // The mantissa is signed but may not be negative
    if mant > 0x7FFFFF {
        Default::default()
    } else {
        Target::from_u64(mant as u64).unwrap() << (expt as usize)
    }
}
