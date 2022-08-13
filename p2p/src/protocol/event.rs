//! Protocol events.
use nakamoto_common::bitcoin::network::message::NetworkMessage;

use crate::protocol::{self, Height, LocalTime, PeerId};

/// A peer-to-peer event.
#[derive(Debug, Clone)]
pub enum Event {
    /// The node is initializing its state machine and about to start network activity.
    Initializing,
    /// The node is initialized and ready to receive commands.
    Ready {
        /// Block header height.
        height: Height,
        /// Filter header height.
        filter_height: Height,
        /// Local time.
        time: LocalTime,
    },
    /// Received a message from a peer.
    Received(PeerId, NetworkMessage),
    /// An address manager event.
    Address(protocol::AddressEvent),
    /// A sync manager event.
    Chain(protocol::ChainEvent),
    /// A peer manager event.
    Peer(protocol::PeerEvent),
    /// A CBF manager event.
    Filter(protocol::FilterEvent),
    /// An inventory manager event.
    Inventory(protocol::InventoryEvent),
}
