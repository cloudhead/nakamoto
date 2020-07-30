use crate::protocol::PeerId;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Event<M> {
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
    Connected(PeerId),
    /// A peer has been disconnected.
    Disconnected(PeerId),
    /// Received a message from a peer.
    Received(PeerId, M),
}
