use std::fmt;

use bitcoin::{Transaction, Txid};
use nakamoto_common::block::{Block, BlockHash, Height};

use super::TxStatus;

/// Event emitted by the SPV client.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub enum Event {
    /// A block was discovered that extends the main chain, and its transactions
    /// where scanned. This event usually precedes [`Event::TxStatusChanged`] events.
    BlockConnected {
        hash: BlockHash,
        height: Height,
        block: Block,
    },
    /// One of the blocks of the main chain was disconnected, due to a re-org.
    /// These events will fire from the latest block to the earliest.
    /// Mark all transactions belonging to this block as *unconfirmed*.
    BlockDisconnected { hash: BlockHash },
    /// The status of a transaction has changed.
    TxStatusChanged { txid: Txid, status: TxStatus },
    /// A transaction output has been redeemed in a block. One of the registered UTXOs was spent.
    TxRedeemed {
        transaction: Transaction,
        block: BlockHash,
        height: Height,
        value: u64,
    },
    /// A transaction that pays to a registered address was confirmed.
    TxReceived {
        transaction: Transaction,
        block: BlockHash,
        height: Height,
        value: u64,
    },
    /// Compact filters have been synced and processed up to this point.
    /// This should match the output of the [`super::handle::Handle::tip`] method.
    Synced { height: Height, block: BlockHash },
}

impl fmt::Display for Event {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockConnected { hash, height, .. } => {
                write!(fmt, "block {} connected at height {}", hash, height)
            }
            Self::BlockDisconnected { hash } => write!(fmt, "block {} disconnected", hash),
            Self::TxStatusChanged { txid, status } => {
                write!(fmt, "transaction {} status changed: {}", txid, status)
            }
            Self::TxRedeemed {
                transaction, block, ..
            } => write!(
                fmt,
                "transaction {} redeemed a registered output in block {}",
                transaction.txid(),
                block
            ),
            Self::TxReceived {
                transaction, block, ..
            } => write!(
                fmt,
                "transaction {} payed to a registered address in block {}",
                transaction.txid(),
                block
            ),
            Self::Synced { height, .. } => write!(fmt, "filters synced up to height {}", height),
        }
    }
}
