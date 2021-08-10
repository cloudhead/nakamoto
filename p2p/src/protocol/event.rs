//! Protocol events.
use std::net;

use bitcoin::network::message::NetworkMessage;

use crate::event::Broadcast;
use crate::protocol::PeerId;
use crate::protocol::{addrmgr, cbfmgr, connmgr, invmgr, peermgr, syncmgr};

/// A peer-to-peer event.
#[derive(Debug, Clone)]
pub enum Event {
    /// The node is now listening for incoming connections.
    Listening(net::SocketAddr),
    /// Received a message from a peer.
    Received(PeerId, NetworkMessage),
    /// An address manager event.
    AddrManager(addrmgr::Event),
    /// A sync manager event.
    SyncManager(syncmgr::Event),
    /// A connection manager event.
    ConnManager(connmgr::Event),
    /// A peer manager event.
    PeerManager(peermgr::Event),
    /// A CBF manager event.
    FilterManager(cbfmgr::Event),
    /// An inventory manager event.
    InventoryManager(invmgr::Event),
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
