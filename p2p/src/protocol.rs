pub mod bitcoin;
pub use self::bitcoin::Bitcoin;

use crate::event::Event;

use std::fmt::Debug;
use std::net;

use crossbeam_channel as chan;

use nakamoto_common::block::time::{LocalDuration, LocalTime};

/// Identifies a peer.
pub type PeerId = net::SocketAddr;

/// A timeout.
pub type Timeout = LocalDuration;

/// Link direction of the peer connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
    /// Inbound conneciton.
    Inbound,
    /// Outbound connection.
    Outbound,
}

impl Link {
    pub fn is_outbound(&self) -> bool {
        *self == Link::Outbound
    }
}

/// A message that can be sent to a peer.
pub trait Message: Send + Sync + 'static {
    /// The message payload.
    type Payload: Clone + Debug;

    /// Construct a message from a payload and magic.
    fn from_parts(payload: Self::Payload, magic: u32) -> Self;
    /// Retrieve the message payload.
    fn payload(&self) -> &Self::Payload;
    /// Retrieve the message magic.
    fn magic(&self) -> u32;
    /// Display the message.
    fn display(&self) -> &'static str;
}

/// Timeout source descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutSource {
    /// Header sync.
    Synch(Option<PeerId>),
    /// Peer connect.
    Connect,
    /// Peer handshake.
    Handshake(PeerId),
    /// Peer ping.
    Ping(PeerId),
}

/// A protocol input event, parametrized over the network message type.
/// These are input events generated outside of the protocol.
#[derive(Debug, Clone)]
pub enum Input<M, C> {
    /// New connection with a peer.
    Connected {
        /// Remote peer id.
        addr: PeerId,
        /// Local peer id.
        local_addr: PeerId,
        /// Link direction.
        link: Link,
    },
    /// Disconnected from peer.
    Disconnected(PeerId),
    /// Received a message from a remote peer.
    Received(PeerId, M),
    /// Sent a message to a remote peer, of the given size.
    Sent(PeerId, usize),
    /// An external command has been received.
    Command(C),
    /// A timeout has been reached.
    Timeout(TimeoutSource),
}

/// Output of a state transition (step) of the `Protocol` state machine.
#[derive(Debug)]
pub enum Out<M: Message> {
    /// Send a message to a peer.
    Message(PeerId, M),
    /// Connect to a peer.
    Connect(PeerId, Timeout),
    /// Disconnect from a peer.
    Disconnect(PeerId),
    /// Set a timeout associated with a peer.
    SetTimeout(TimeoutSource, Timeout),
    /// An event has occured.
    Event(Event<M::Payload>),
    /// Shutdown protocol.
    Shutdown,
}

impl<M: Message> From<Event<M::Payload>> for Out<M> {
    fn from(event: Event<M::Payload>) -> Self {
        Out::Event(event)
    }
}

/// Protocol builder. A type implementing this trait is able to construct a new
/// protocol instance.
pub trait ProtocolBuilder {
    /// The message type of the underlying protocol.
    type Message: Message;
    /// The protocol constructed by this builder.
    type Protocol: Protocol<Self::Message>;

    /// Build a new protocol instance, given an outbound channel. Protocol events
    /// and outputs are sent over this channel.
    fn build(self, tx: chan::Sender<Out<Self::Message>>) -> Self::Protocol;
}

/// A finite-state machine that can advance one step at a time, given an input event.
/// Parametrized over the message type.
pub trait Protocol<M: Message> {
    /// A command to query or control the protocol.
    type Command;

    /// Initialize the protocol. Called once before any event is sent to the state machine.
    fn initialize(&mut self, time: LocalTime);

    /// Process the next event and advance the state-machine by one step.
    /// Returns messages destined for peers.
    fn step(&mut self, event: Input<M, Self::Command>, local_time: LocalTime);
}
