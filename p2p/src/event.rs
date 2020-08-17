use std::net;

use nakamoto_common::block::tree::ImportResult;

use crate::protocol::{Link, PeerId};

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Event<M> {
    /// The node is now listening for incoming connections.
    Listening(net::SocketAddr),
    /// The node is connecting to the network and isn't ready to start syncing.
    Connecting,
    /// The node started syncing with the network.
    Syncing,
    /// The node has finished syncing and is ready to accept
    /// connections and process commands.
    Synced,
    /// A new peer has connected and is ready to accept messages.
    /// This event is triggered *after* the peer handshake
    /// has successfully completed.
    Connected(PeerId, Link),
    /// A peer has been disconnected.
    Disconnected(PeerId),
    /// Received a message from a peer.
    Received(PeerId, M),
    /// Headers have been imported into the block store.
    HeadersImported(ImportResult),
}
