use crate::protocol::PeerId;

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum DisconnectReason {
    Error,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Event<M> {
    /// The node has finished syncing and is ready to accept
    /// connections and process commands.
    Ready,
    /// A new peer has connected and is ready to accept messages.
    /// This event is triggered *after* the peer handshake
    /// has successfully completed.
    Connected(PeerId),
    /// A peer has been disconnected.
    Disconnected(PeerId, DisconnectReason),
    /// Received a message from a peer.
    Received(PeerId, M),
}
