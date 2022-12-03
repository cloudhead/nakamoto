//! State machine events.
use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::bitcoin::BlockHash;

use crate::fsm::{self, Height, LocalTime, PeerId};

/// A peer-to-peer event.
#[derive(Debug, Clone)]
pub enum Event {
    /// The node is initializing its state machine and about to start network activity.
    Initializing,
    /// The node is initialized and ready to receive commands.
    Ready {
        /// Block header height.
        height: Height,
        /// Block header hash.
        hash: BlockHash,
        /// Filter header height.
        filter_height: Height,
        /// Local time.
        time: LocalTime,
    },
    /// Received a message from a peer.
    Received(PeerId, NetworkMessage),
    /// An address manager event.
    Address(fsm::AddressEvent),
    /// A sync manager event.
    Chain(fsm::ChainEvent),
    /// A peer manager event.
    Peer(fsm::PeerEvent),
    /// A CBF manager event.
    Filter(fsm::FilterEvent),
    /// An inventory manager event.
    Inventory(fsm::InventoryEvent),
    /// A ping manager event.
    Ping(fsm::PingEvent),
}

impl From<fsm::ChainEvent> for Event {
    fn from(e: fsm::ChainEvent) -> Self {
        Self::Chain(e)
    }
}

impl From<fsm::PeerEvent> for Event {
    fn from(e: fsm::PeerEvent) -> Self {
        Self::Peer(e)
    }
}

impl From<fsm::FilterEvent> for Event {
    fn from(e: fsm::FilterEvent) -> Self {
        Self::Filter(e)
    }
}

impl From<fsm::AddressEvent> for Event {
    fn from(e: fsm::AddressEvent) -> Self {
        Self::Address(e)
    }
}

impl From<fsm::InventoryEvent> for Event {
    fn from(e: fsm::InventoryEvent) -> Self {
        Self::Inventory(e)
    }
}

impl From<fsm::PingEvent> for Event {
    fn from(e: fsm::PingEvent) -> Self {
        Self::Ping(e)
    }
}
