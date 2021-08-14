//! SPV event mapper.
#![allow(clippy::manual_range_contains, clippy::new_without_default)]

pub mod utxos;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::{fmt, net};

use p2p::event::Emitter;

use bitcoin::{Block, Txid};

use nakamoto_common::block::{BlockHash, Height};
use nakamoto_p2p as p2p;
use p2p::protocol;

use crate::client::Event;

/// Transaction status of a given transaction.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum TxStatus {
    /// This is the initial state of a transaction after it has been announced by the
    /// client.
    Unconfirmed,
    /// Transaction was acknowledged by a peer.
    ///
    /// This is the case when a peer requests the transaction data from us after an inventory
    /// announcement. It does not mean the transaction is considered valid by the peer.
    Acknowledged {
        /// Peer acknowledging the transaction.
        peer: net::SocketAddr,
    },
    /// Transaction was included in a block. This event is fired after
    /// a block from the main chain is scanned.
    Confirmed {
        /// Height at which it was included.
        height: Height,
        /// Hash of the block in which it was included.
        block: BlockHash,
    },
    /// A transaction that was previously confirmed, and is now reverted due to a
    /// re-org. Note that this event can only fire if the originally confirmed tx
    /// is still in memory.
    Reverted,
    /// Transaction was replaced by another transaction, and will probably never
    /// be included in a block. This can happen if an RBF transaction is replaced by one with
    /// a higher fee, or if a transaction is reverted and a conflicting transaction replaces
    /// it. In this case it would be preceded by a [`TxStatus::Reverted`] status.
    Stale {
        /// Transaction replacing the given transaction and causing it to be stale.
        replaced_by: Txid,
        /// Block of the included transaction.
        block: BlockHash,
    },
}

impl fmt::Display for TxStatus {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfirmed => write!(fmt, "transaction is unconfirmed"),
            Self::Acknowledged { peer } => {
                write!(fmt, "transaction was acknowledged by peer {}", peer)
            }
            Self::Confirmed { height, block } => write!(
                fmt,
                "transaction was included in block {} at height {}",
                block, height
            ),
            Self::Reverted => write!(fmt, "transaction has been reverted"),
            Self::Stale { replaced_by, block } => write!(
                fmt,
                "transaction was replaced by {} in block {}",
                replaced_by, block
            ),
        }
    }
}

/// Event mapper for SPV and client events.
/// Consumes protocol events and emits [`Event`].
pub struct Mapper {
    /// Best height known.
    tip: Height,
    /// The height up to which we've processed filters and matching blocks.
    sync_height: Height,
    /// The height up to which we've processed filters.
    /// This is usually going to be greater than `sync_height`.
    filter_height: Height,
    /// The height up to which we've processed matching blocks.
    /// This is always going to be lesser or equal to `filter_height`.
    block_height: Height,
    /// Filter heights that have been matched, and for which we are awaiting a block to process.
    pending: HashSet<Height>,
}

impl Mapper {
    /// Create a new SPV event mapper.
    pub fn new() -> Self {
        let tip = 0;
        let sync_height = 0;
        let filter_height = 0;
        let block_height = 0;
        let pending = HashSet::new();

        Self {
            tip,
            sync_height,
            filter_height,
            block_height,
            pending,
        }
    }

    /// Process protocol event and map it to client event(s).
    pub fn process(&mut self, event: protocol::Event, emitter: &Emitter<Event>) {
        use p2p::protocol::{cbfmgr, invmgr, syncmgr};

        match event {
            protocol::Event::SyncManager(syncmgr::Event::Synced(_, height)) => {
                self.tip = height;
            }
            protocol::Event::SyncManager(syncmgr::Event::BlockConnected { hash, height }) => {
                emitter.emit(Event::BlockConnected { hash, height });
            }
            protocol::Event::SyncManager(syncmgr::Event::BlockDisconnected { hash, height }) => {
                emitter.emit(Event::BlockDisconnected { hash, height });
            }
            protocol::Event::InventoryManager(invmgr::Event::BlockProcessed { block, height }) => {
                self.process_block(block, height, emitter);
            }
            protocol::Event::InventoryManager(invmgr::Event::Confirmed {
                transaction,
                height,
                block,
            }) => {
                emitter.emit(Event::TxStatusChanged {
                    txid: transaction.txid(),
                    status: TxStatus::Confirmed { height, block },
                });
            }
            protocol::Event::InventoryManager(invmgr::Event::Acknowledged { txid, peer }) => {
                emitter.emit(Event::TxStatusChanged {
                    txid,
                    status: TxStatus::Acknowledged { peer },
                });
            }
            protocol::Event::FilterManager(cbfmgr::Event::FilterProcessed {
                block,
                height,
                matched,
            }) => {
                self.process_filter(block, height, matched, emitter);
            }
            _ => {}
        }
        assert!(
            self.block_height <= self.filter_height,
            "Filters are processed before blocks"
        );
        assert!(
            self.sync_height <= self.filter_height,
            "Filters are processed before we are done"
        );

        // If we have no blocks left to process, we are synced to the height of the last
        // processed filter. Otherwise, we're synced up to the last processed block.
        let height = if self.pending.is_empty() {
            self.filter_height
        } else {
            self.block_height
        };

        // Ensure we only broadcast sync events when the sync height has changed.
        if height > self.sync_height {
            self.sync_height = height;

            emitter.emit(Event::Synced {
                height,
                tip: self.tip,
            });
        }
    }

    // PRIVATE METHODS /////////////////////////////////////////////////////////

    fn process_block(&mut self, block: Block, height: Height, emitter: &Emitter<Event>) {
        if !self.pending.remove(&height) {
            // Received unexpected block.
            return;
        }
        let hash = block.block_hash();

        log::debug!("Received block {} at height {}", hash, height);
        debug_assert!(height >= self.block_height);

        self.block_height = height;

        emitter.emit(Event::BlockMatched {
            height,
            hash,
            header: block.header,
            transactions: block.txdata,
        });
    }

    fn process_filter(
        &mut self,
        block: BlockHash,
        height: Height,
        matched: bool,
        emitter: &Emitter<Event>,
    ) {
        debug_assert!(height >= self.filter_height);

        if matched {
            log::debug!("Filter matched for block #{}", height);
            self.pending.insert(height);
        }
        self.filter_height = height;

        emitter.emit(Event::FilterProcessed {
            height,
            matched,
            block,
        });
    }
}
