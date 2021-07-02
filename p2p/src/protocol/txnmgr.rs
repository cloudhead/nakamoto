//!
//! Manages transaction confirmation.
//!
#![warn(missing_docs)]
use std::collections::HashSet;

use bitcoin::{BlockHash, Transaction, Txid};
use nakamoto_common::block::Height;

/// The status of a transaction.
#[derive(Clone, Debug)]
pub enum TransactionStatus {
    /// The transaction was sent to one or more peers on the network, and is unconfirmed.
    Pending {
        /// Number of announcements of this transaction from other peers.
        announcements: usize,
    },
    /// The transaction is not likely to confirm, as another transaction has spent its output.
    Stale {
        /// Stale transaction.
        replaced_by: Txid,
    },
}

impl std::fmt::Display for TransactionStatus {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionStatus::Pending { announcements } => {
                write!(
                    fmt,
                    "Pending transaction announced by {} peers",
                    announcements
                )
            }

            TransactionStatus::Stale { replaced_by } => {
                write!(fmt, "Stale transaction replaced by {}", replaced_by)
            }

            TransactionStatus::Confirmed { height, block } => {
                write!(
                    fmt,
                    "Confirmed transaction on height {} and block hash {:?}",
                    height, block
                )
            }
        }
    }
}

/// The transaction manager state.
#[derive(Debug)]
pub struct TransactionManager {
    /// Transaction IDs to be tracked.
    transactions: HashSet<Transaction>,
}
