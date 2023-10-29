//! State machine events.
use std::sync::Arc;
use std::{fmt, io, net};

use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::{Transaction, Txid};
use nakamoto_common::block::filter::BlockFilter;
use nakamoto_common::block::{Block, BlockHash, BlockHeader, Height};
use nakamoto_common::p2p::peer::Source;
use nakamoto_net::Disconnect;

use crate::fsm;
use crate::fsm::fees::FeeEstimate;
use crate::fsm::{Link, LocalTime, PeerId};

/// Event emitted by the client, after the "loading" phase is over.
#[derive(Debug, Clone)]
pub enum Event {
    /// The node is initializing its state machine and about to start network activity.
    Initializing,
    /// Ready to process peer events and start receiving commands.
    /// Note that this isn't necessarily the first event emitted.
    Ready {
        /// The tip of the block header chain.
        tip: Height,
        /// The tip of the filter header chain.
        filter_tip: Height,
        /// Local time.
        time: LocalTime,
    },
    /// Peer connected. This is fired when the physical TCP/IP connection
    /// is established. Use [`Event::PeerNegotiated`] to know when the P2P handshake
    /// has completed.
    PeerConnected {
        /// Peer address.
        addr: PeerId,
        /// Connection link.
        link: Link,
    },
    /// Outbound peer connection initiated.
    PeerConnecting {
        /// Peer address.
        addr: PeerId,
        /// Address source.
        source: Source,
        /// Peer services.
        services: ServiceFlags,
    },
    /// Peer disconnected after successful connection.
    PeerDisconnected {
        /// Peer address.
        addr: PeerId,
        /// Reason for disconnection.
        reason: Disconnect<fsm::DisconnectReason>,
    },
    /// Peer timed out when waiting for response.
    /// This usually leads to a disconnection.
    PeerTimedOut {
        /// Peer address.
        addr: PeerId,
    },
    /// Connection was never established and timed out or failed.
    PeerConnectionFailed {
        /// Peer address.
        addr: PeerId,
        /// Connection error.
        error: Arc<io::Error>,
    },
    /// Peer handshake completed. The peer connection is fully functional from this point.
    PeerNegotiated {
        /// Peer address.
        addr: PeerId,
        /// Connection link.
        link: Link,
        /// Peer services.
        services: ServiceFlags,
        /// Peer height.
        height: Height,
        /// Peer user agent.
        user_agent: String,
        /// Negotiated protocol version.
        version: u32,
    },
    /// The best known height amongst connected peers has been updated.
    /// Note that there is no guarantee that this height really exists;
    /// peers don't have to follow the protocol and could send a bogus
    /// height.
    PeerHeightUpdated {
        /// Best block height known.
        height: Height,
    },
    /// A peer misbehaved.
    PeerMisbehaved {
        /// Peer address.
        addr: PeerId,
    },
    /// A block was added to the main chain.
    BlockConnected {
        /// Block header.
        header: BlockHeader,
        /// Height of the block.
        height: Height,
    },
    /// One of the blocks of the main chain was reverted, due to a re-org.
    /// These events will fire from the latest block starting from the tip, to the earliest.
    /// Mark all transactions belonging to this block as *unconfirmed*.
    BlockDisconnected {
        /// Header of the block.
        header: BlockHeader,
        /// Height of the block when it was part of the main chain.
        height: Height,
    },
    /// Block downloaded and processed by inventory manager.
    BlockProcessed {
        /// The full block.
        block: Block,
        /// The block height.
        height: Height,
        /// The fee estimate for this block.
        fees: Option<FeeEstimate>,
    },
    /// A block has matched one of the filters and is ready to be processed.
    /// This event usually precedes [`Event::TxStatusChanged`] events.
    BlockMatched {
        /// Block height.
        height: Height,
        /// Matching block.
        block: Block,
    },
    /// Block header chain is in sync with network.
    BlockHeadersSynced {
        /// Block height.
        height: Height,
        /// Chain tip.
        hash: BlockHash,
    },
    /// Transaction fee rate estimated for a block.
    FeeEstimated {
        /// Block hash of the estimate.
        block: BlockHash,
        /// Block height of the estimate.
        height: Height,
        /// Fee estimate.
        fees: FeeEstimate,
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
        /// Whether or not this filter is valid.
        // TODO: Do not emit event for invalid filter.
        valid: bool,
        /// Filter was cached.
        cached: bool,
    },
    /// A filter was received.
    FilterReceived {
        /// Peer we received from.
        from: PeerId,
        /// The received filter.
        filter: BlockFilter,
        /// Filter height.
        height: Height,
        /// Hash of corresponding block.
        block: BlockHash,
    },
    /// A filter rescan has started.
    FilterRescanStarted {
        /// Start height.
        start: Height,
        /// End height.
        stop: Option<Height>,
    },
    /// A filter rescan has stopped.
    FilterRescanStopped {
        /// Stop height.
        height: Height,
    },
    /// Filter headers synced up to block header height.
    FilterHeadersSynced {
        /// Block height.
        height: Height,
    },
    /// The status of a transaction has changed.
    TxStatusChanged {
        /// The Transaction ID.
        txid: Txid,
        /// The new transaction status.
        status: TxStatus,
    },
    /// Address book exhausted.
    AddressBookExhausted,
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
    /// An error occured.
    Error {
        /// Error message.
        message: String,
        /// Error source.
        source: Arc<dyn std::error::Error + 'static + Sync + Send>,
    },
}

impl fmt::Display for Event {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initializing => {
                write!(fmt, "initializing")
            }
            Self::Ready { .. } => {
                write!(fmt, "ready to process events and commands")
            }
            Self::BlockHeadersSynced { height, hash } => {
                write!(fmt, "chain tip updated to {hash} at height {height}")
            }
            Self::BlockConnected { header, height, .. } => {
                write!(
                    fmt,
                    "block {} connected at height {}",
                    header.block_hash(),
                    height
                )
            }
            Self::BlockDisconnected { header, height, .. } => {
                write!(
                    fmt,
                    "block {} disconnected at height {}",
                    header.block_hash(),
                    height
                )
            }
            Self::BlockProcessed { block, height, .. } => {
                write!(
                    fmt,
                    "block {} processed at height {}",
                    block.block_hash(),
                    height
                )
            }
            Self::BlockMatched { height, .. } => {
                write!(fmt, "block matched at height {}", height)
            }
            Self::FeeEstimated { fees, height, .. } => {
                write!(
                    fmt,
                    "transaction median fee rate for block #{} is {} sat/vB",
                    height, fees.median,
                )
            }
            Self::FilterRescanStarted {
                start,
                stop: Some(stop),
            } => {
                write!(fmt, "rescan started from height {start} to {stop}")
            }
            Self::FilterRescanStarted { start, stop: None } => {
                write!(fmt, "rescan started from height {start}")
            }
            Self::FilterRescanStopped { height } => {
                write!(fmt, "rescan completed at height {height}")
            }
            Self::FilterHeadersSynced { height } => {
                write!(fmt, "filter headers synced up to height {height}")
            }
            Self::FilterReceived { from, block, .. } => {
                write!(fmt, "filter for block {block} received from {from}")
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
            Self::PeerConnected { addr, link } => {
                write!(fmt, "peer {} connected ({:?})", &addr, link)
            }
            Self::PeerConnectionFailed { addr, error } => {
                write!(
                    fmt,
                    "peer connection attempt to {} failed with {}",
                    &addr, error
                )
            }
            Self::PeerHeightUpdated { height } => {
                write!(fmt, "peer height updated to {}", height)
            }
            Self::PeerMisbehaved { addr } => {
                write!(fmt, "peer {addr} misbehaved")
            }
            Self::PeerDisconnected { addr, reason } => {
                write!(fmt, "disconnected from {} ({})", &addr, reason)
            }
            Self::PeerTimedOut { addr } => {
                write!(fmt, "peer {addr} timed out")
            }
            Self::PeerConnecting { addr, .. } => {
                write!(fmt, "connecting to peer {addr}")
            }
            Self::PeerNegotiated {
                addr,
                height,
                services,
                ..
            } => write!(
                fmt,
                "peer {} negotiated with services {} and height {}..",
                addr, services, height
            ),
            Self::AddressBookExhausted => {
                write!(
                    fmt,
                    "address book exhausted.. fetching new addresses from peers"
                )
            }
            Self::Error { message, source } => {
                write!(fmt, "error: {message}: {source}")
            }
        }
    }
}

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
    Reverted {
        /// The reverted transaction.
        transaction: Transaction,
    },
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
            Self::Reverted { transaction } => {
                write!(fmt, "transaction {} has been reverted", transaction.txid())
            }
            Self::Stale { replaced_by, block } => write!(
                fmt,
                "transaction was replaced by {} in block {}",
                replaced_by, block
            ),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nakamoto_common::bitcoin_hashes::Hash;
    use nakamoto_test::block::gen;

    #[test]
    fn test_tx_status_ordering() {
        assert!(
            TxStatus::Unconfirmed
                < TxStatus::Acknowledged {
                    peer: ([0, 0, 0, 0], 0).into()
                }
        );
        assert!(
            TxStatus::Acknowledged {
                peer: ([0, 0, 0, 0], 0).into()
            } < TxStatus::Confirmed {
                height: 0,
                block: BlockHash::all_zeros(),
            }
        );
        assert!(
            TxStatus::Confirmed {
                height: 0,
                block: BlockHash::all_zeros(),
            } < TxStatus::Reverted {
                transaction: gen::transaction(&mut fastrand::Rng::new())
            }
        );
        assert!(
            TxStatus::Reverted {
                transaction: gen::transaction(&mut fastrand::Rng::new())
            } < TxStatus::Stale {
                replaced_by: Txid::all_zeros(),
                block: BlockHash::all_zeros()
            }
        );
    }
}
