pub mod bitcoin;
pub use self::bitcoin::Bitcoin;

use std::net;

use nakamoto_chain::block::time::{LocalDuration, LocalTime};

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
    type Payload: Clone;

    /// Retrieve the message payload.
    fn payload(&self) -> &Self::Payload;
}

/// A protocol event, parametrized over the network message type.
#[derive(Debug, Clone)]
pub enum Event<M, C> {
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
    /// Nothing has happened in some time.. This event is useful for checking
    /// timeouts or running periodic tasks.
    Idle,
    /// A timeout on a peer has been reached.
    Timeout(PeerId),
}

impl<M: Message, C: Clone> Event<M, C> {
    pub fn payload(&self) -> Event<M::Payload, C> {
        use Event::*;

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
            Idle => Idle,
            Timeout(p) => Timeout(p.clone()),
        }
    }
}

/// Output of a state transition (step) of the `Protocol` state machine.
#[derive(Debug, Eq, PartialEq)]
pub enum Output<M> {
    /// Send a message to a peer.
    Message(PeerId, M),
    /// Connect to a peer.
    Connect(PeerId),
    /// Disconnect from a peer.
    Disconnect(PeerId),
    /// Set a timeout associated with a peer.
    SetTimeout(PeerId, LocalDuration),
}

impl<M> Output<M> {
    pub fn address(&self) -> PeerId {
        match self {
            Self::Message(addr, _) => *addr,
            Self::Connect(addr) => *addr,
            Self::Disconnect(addr) => *addr,
            Self::SetTimeout(addr, _) => *addr,
        }
    }
}

/// A finite-state machine that can advance one step at a time, given an input event.
/// Parametrized over the message type.
pub trait Protocol<M> {
    /// Duration of inactivity before timing out a peer.
    const IDLE_TIMEOUT: LocalDuration;
    /// How long to wait between sending pings.
    const PING_INTERVAL: LocalDuration;

    /// A command to query or control the protocol.
    type Command;

    /// Initialize the protocol. Called once before any event is sent to the state machine.
    fn initialize(&mut self, time: LocalTime) -> Vec<Output<M>>;

    /// Process the next event and advance the state-machine by one step.
    /// Returns messages destined for peers.
    fn step(&mut self, event: Event<M, Self::Command>, time: LocalTime) -> Vec<Output<M>>;
}
