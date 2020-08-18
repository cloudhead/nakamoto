pub mod bitcoin;
pub use self::bitcoin::Bitcoin;

use crate::event::Event;

use std::fmt::Debug;
use std::net;

use nakamoto_common::block::time::{LocalDuration, LocalTime};

/// Identifies a peer.
pub type PeerId = net::SocketAddr;

/// Link direction of the peer connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
    /// Inbound conneciton.
    Inbound,
    /// Outbound connection.
    Outbound,
}

/// A message that can be sent to a peer.
pub trait Message: Send + Sync + 'static {
    /// The message payload.
    type Payload: Clone + Debug;

    /// Retrieve the message payload.
    fn payload(&self) -> &Self::Payload;
    /// Display the message.
    fn display(&self) -> &'static str;
}

/// Component of the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Component {
    SyncManager,
    HandshakeManager,
    PingManager,
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
    /// Clock is ticking! Used to signify time passing.
    Tick(LocalTime),
    /// A timeout on a peer has been reached.
    Timeout(PeerId, Component),
}

impl<M: Message, C: Clone> Input<M, C> {
    pub fn payload(&self) -> Input<M::Payload, C> {
        use Input::*;

        match self {
            Connected {
                addr,
                local_addr,
                link,
            } => Connected {
                addr: *addr,
                local_addr: *local_addr,
                link: *link,
            },
            Disconnected(p) => Disconnected(*p),
            Received(p, m) => Received(*p, Message::payload(m).clone()),
            Sent(p, n) => Sent(*p, *n),
            Command(c) => Command(c.clone()),
            Tick(t) => Tick(*t),
            Timeout(p, c) => Timeout(*p, *c),
        }
    }
}

/// Output of a state transition (step) of the `Protocol` state machine.
#[derive(Debug, Eq, PartialEq)]
pub enum Output<M: Message> {
    /// Send a message to a peer.
    Message(PeerId, M),
    /// Connect to a peer.
    Connect(PeerId),
    /// Disconnect from a peer.
    Disconnect(PeerId),
    /// Set a timeout associated with a peer.
    SetTimeout(PeerId, Component, LocalDuration),
    /// An event has occured.
    Event(Event<M::Payload>),
    /// Shutdown protocol.
    Shutdown,
}

impl<M: Message> From<Event<M::Payload>> for Output<M> {
    fn from(event: Event<M::Payload>) -> Self {
        Output::Event(event)
    }
}

impl<M: Message> Output<M> {
    pub fn address(&self) -> Option<PeerId> {
        match self {
            Self::Message(addr, _) => Some(*addr),
            Self::Connect(addr) => Some(*addr),
            Self::Disconnect(addr) => Some(*addr),
            Self::SetTimeout(addr, _, _) => Some(*addr),
            Self::Event(_) => None,
            Self::Shutdown => None,
        }
    }
}

/// A finite-state machine that can advance one step at a time, given an input event.
/// Parametrized over the message type.
pub trait Protocol<M: Message> {
    /// Duration of inactivity before timing out a peer.
    const IDLE_TIMEOUT: LocalDuration;

    /// A command to query or control the protocol.
    type Command;

    /// Initialize the protocol. Called once before any event is sent to the state machine.
    fn initialize(&mut self, time: LocalTime) -> Vec<Output<M>>;

    /// Process the next event and advance the state-machine by one step.
    /// Returns messages destined for peers.
    fn step(&mut self, event: Input<M, Self::Command>) -> Vec<Output<M>>;
}
