pub mod genesis;
pub mod iter;
pub mod store;
pub mod time;
pub mod tree;

pub use bitcoin::blockdata::block::{Block, BlockHeader};
pub use bitcoin::blockdata::transaction::Transaction;
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
    pub height: Height,
    pub hash: BlockHash,
    pub header: BlockHeader,
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

/// Get the locator indexes starting from a given height, and going backwards, exponentially
/// backing off.
///
/// ```
/// use nakamoto_common::block;
///
/// assert_eq!(block::locators_indexes(0), vec![0]);
/// assert_eq!(block::locators_indexes(8), vec![8, 7, 6, 5, 4, 3, 2, 1, 0]);
/// assert_eq!(block::locators_indexes(99), vec![
///     99, 98, 97, 96, 95, 94, 93, 92, 91, 89, 85, 77, 61, 29, 0
/// ]);
/// ```
pub fn locators_indexes(mut from: Height) -> Vec<Height> {
    let mut indexes = Vec::new();
    let mut step = 1;

    while from > 0 {
        // For the first 8 blocks, don't skip any heights.
        if indexes.len() >= 8 {
            step *= 2;
        }
        indexes.push(from as Height);
        from = from.saturating_sub(step);
    }
    // Always include genesis.
    indexes.push(0);
    indexes
}

/// Convert a compact difficulty representation to 256-bits.
/// Taken from `BlockHeader::target` from the `bitcoin` library.
pub fn target_from_bits(bits: u32) -> Target {
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

/// Get the proof-of-work limit for the network, in bits.
pub fn pow_limit_bits(network: &bitcoin::Network) -> Bits {
    match network {
        bitcoin::Network::Bitcoin => 0x1d00ffff,
        bitcoin::Network::Testnet => 0x1d00ffff,
        bitcoin::Network::Regtest => 0x207fffff,
    }
}
