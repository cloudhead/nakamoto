//! Block-related types and functions.
pub mod checkpoints;
pub mod filter;
pub mod genesis;
pub mod iter;
pub mod store;
pub mod time;
pub mod tree;

pub use bitcoin::blockdata::block::{Block, BlockHeader};
pub use bitcoin::blockdata::transaction::Transaction;
pub use bitcoin::hash_types::BlockHash;

/// Difficulty target of a block.
pub type Target = bitcoin::util::uint::Uint256;

/// Block work.
pub type Work = bitcoin::util::uint::Uint256;

/// Compact difficulty bits (target) of a block.
pub type Bits = u32;

/// Height of a block.
pub type Height = u64;

/// Block time (seconds since Epoch).
pub type BlockTime = u32;

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

/// Get the proof-of-work limit for the network, in bits.
pub fn pow_limit_bits(network: &bitcoin::Network) -> Bits {
    match network {
        bitcoin::Network::Bitcoin => 0x1d00ffff,
        bitcoin::Network::Testnet => 0x1d00ffff,
        bitcoin::Network::Regtest => 0x207fffff,
    }
}
