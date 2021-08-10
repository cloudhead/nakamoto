//! Client events.
use std::fmt;

use bitcoin::{Transaction, Txid};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};

use crate::spv::TxStatus;

/// Event emitted by the SPV client.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub enum Event {
    /// The transaction manager is starting to listen on events.
    Ready {
        /// The tip of the block header chain.
        tip: Height,
    },
    /// A block was discovered that extends the main chain.
    BlockConnected { hash: BlockHash, height: Height },
    /// One of the blocks of the main chain was disconnected, due to a re-org.
    /// These events will fire from the latest block to the earliest.
    /// Mark all transactions belonging to this block as *unconfirmed*.
    BlockDisconnected { hash: BlockHash },
    /// A block's transactions where scanned for matching inputs and outputs.
    /// This event usually precedes [`Event::TxStatusChanged`] events.
    Block {
        hash: BlockHash,
        header: BlockHeader,
        height: Height,
        transactions: Vec<Transaction>,
    },
    /// A filter was processed. If it matched any of the scripts in the watchlist,
    /// the corresponding block was scheduled for download.
    FilterProcessed {
        block: BlockHash,
        height: Height,
        matched: bool,
    },
    /// The status of a transaction has changed.
    TxStatusChanged { txid: Txid, status: TxStatus },
    /// Compact filters have been synced and processed up to this point and matching blocks have
    /// been scanned.
    Synced { height: Height, tip: Height },
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
            Self::BlockDisconnected { hash } => write!(fmt, "block {} disconnected", hash),
            Self::Block { hash, height, .. } => {
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
                    "filter processed at height {} (match={})",
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
