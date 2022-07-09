//! Protocol events.
use std::net;

use nakamoto_common::bitcoin::network::message::NetworkMessage;

use crate::event::Broadcast;
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
    /// The node is now listening for incoming connections.
    Listening(net::SocketAddr),
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

/// Any type that is able to publish events.
pub trait Publisher: Send + Sync {
    /// Publish an event.
    fn publish(&mut self, event: Event);
}

impl<T: Clone + Send + Sync> Publisher for Broadcast<Event, T> {
    /// Publish a message to all subscribers.
    fn publish(&mut self, event: Event) {
        self.broadcast(event)
    }
}
