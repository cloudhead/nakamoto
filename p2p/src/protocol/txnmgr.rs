//!
//! Manages transaction confirmation.
//!
#![warn(missing_docs)]
use bitcoin::{BlockHash, Txid};
use nakamoto_common::block::Height;

/// The status of a transaction.
pub enum TransactionStatus {
    /// The transaction was sent to one or more peers on the network, and is unconfirmed.
    Pending {
        /// Number of announcements of this transaction from other peers.
        announcements: usize,
    },
    /// The transaction was included in a block.
    Confirmed {
        /// Height of the block.
        height: Height,
        /// Hash of the block.
        block: BlockHash,
    },
    /// The transaction is not likely to confirm, as another transaction has spent its output.
    Stale {
        /// Replaced by transaction ID.
        replaced_by: Txid,
    },
}
