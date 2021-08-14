//! Client events.
use std::fmt;

use bitcoin::{Transaction, Txid};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};

use crate::spv::TxStatus;

/// Event emitted by the client.
#[derive(Debug, Clone)]
pub enum Event {
    /// The transaction manager is starting to listen on events.
    Ready {
        /// The tip of the block header chain.
        tip: Height,
    },
    /// A block was added to the main chain.
    BlockConnected {
        /// Hash of the block.
        hash: BlockHash,
        /// Height of the block.
        height: Height,
    },
    /// One of the blocks of the main chain was reverted, due to a re-org.
    /// These events will fire from the latest block starting from the tip, to the earliest.
    /// Mark all transactions belonging to this block as *unconfirmed*.
    BlockDisconnected {
        /// Hash of the block.
        hash: BlockHash,
        /// Height of the block when it was part of the main chain.
        height: Height,
    },
    /// A block has matched one of the filters and is ready to be processed.
    /// This event usually precedes [`Event::TxStatusChanged`] events.
    BlockMatched {
        /// Hash of the matching block.
        hash: BlockHash,
        /// Block header.
        header: BlockHeader,
        /// Block height.
        height: Height,
        /// Transactions in this block.
        transactions: Vec<Transaction>,
    },
    /// A filter was processed. If it matched any of the scripts in the watchlist,
    /// the corresponding block was scheduled for download, and a [`Event::BlockMatched`]
    /// event will eventually be fired.
    FilterProcessed {
        /// Corresponding block hash.
        block: BlockHash,
        /// Filter height (same as block).
        height: Height,
        /// Whether or not this filter matched any of the watched scripts.
        matched: bool,
    },
    /// The status of a transaction has changed.
    TxStatusChanged {
        /// The Transaction ID.
        txid: Txid,
        /// The new transaction status.
        status: TxStatus,
    },
    /// Compact filters have been synced and processed up to this point and matching blocks have
    /// been fetched.
    ///
    /// If filters have been processed up to the last block in the client's header chain, `height`
    /// and `tip` will be equal.
    Synced {
        /// Height up to which we are synced.
        height: Height,
        /// Tip of our block header chain.
        tip: Height,
    },
}

impl fmt::Display for Event {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready { .. } => {
                write!(fmt, "ready to process events and commands")
            }
            Self::BlockConnected { hash, height } => {
                write!(fmt, "block {} connected at height {}", hash, height)
            }
            Self::BlockDisconnected { hash, height } => {
                write!(fmt, "block {} disconnected at height {}", hash, height)
            }
            Self::BlockMatched { hash, height, .. } => {
                write!(
                    fmt,
                    "block {} ready to be processed at height {}",
                    hash, height
                )
            }
            Self::FilterProcessed {
                height, matched, ..
            } => {
                write!(
                    fmt,
                    "filter processed at height {} (match = {})",
                    height, matched
                )
            }
            Self::TxStatusChanged { txid, status } => {
                write!(fmt, "transaction {} status changed: {}", txid, status)
            }
            Self::Synced { height, .. } => write!(fmt, "filters synced up to height {}", height),
        }
    }
}
