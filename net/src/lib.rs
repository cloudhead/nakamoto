//! Peer-to-peer networking core types.
#![allow(clippy::type_complexity)]
use std::sync::Arc;
use std::{fmt, io, net};

use crossbeam_channel as chan;

pub mod error;
pub mod event;
pub mod time;

pub use event::Publisher;
pub use time::{LocalDuration, LocalTime};

/// Link direction of the peer connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Link {
    /// Inbound conneciton.
    Inbound,
    /// Outbound connection.
    Outbound,
}

impl Link {
    /// Check whether the link is outbound.
    pub fn is_outbound(&self) -> bool {
        *self == Link::Outbound
    }

    /// Check whether the link is inbound.
    pub fn is_inbound(&self) -> bool {
        *self == Link::Inbound
    }
}

/// Output of a state transition of the `Protocol` state machine.
#[derive(Debug)]
pub enum Io<E, D> {
    /// There are some bytes ready to be sent to a peer.
    Write(net::SocketAddr),
    /// Connect to a peer.
    Connect(net::SocketAddr),
    /// Disconnect from a peer.
    Disconnect(net::SocketAddr, DisconnectReason<D>),
    /// Ask for a wakeup in a specified amount of time.
    Wakeup(LocalDuration),
    /// Emit an event.
    Event(E),
}

/// Disconnect reason.
#[derive(Debug, Clone)]
pub enum DisconnectReason<T> {
    /// Error with the underlying connection.
    ConnectionError(Arc<std::io::Error>),
    /// Peer was disconnected for another reason.
    Protocol(T),
}

impl<T: fmt::Display> fmt::Display for DisconnectReason<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionError(err) => write!(f, "connection error: {}", err),
            Self::Protocol(reason) => write!(f, "{}", reason),
        }
    }
}

/// A protocol state-machine.
///
/// Network protocols must implement this trait to be drivable by the reactor.
pub trait Protocol {
    /// Events emitted by the protocol.
    type Event: fmt::Debug;
    /// Reason a peer was disconnected.
    type DisconnectReason: fmt::Debug + fmt::Display;
    /// User commands handled by protocol.
    type Command;
    /// Return type of [`Protocol::drain`].
    type Drain: IntoIterator<Item = Io<Self::Event, Self::DisconnectReason>>;

    /// Initialize the protocol. Called once before any event is sent to the state machine.
    fn initialize(&mut self, _time: LocalTime) {
        // "He was alone. He was unheeded, happy and near to the wild heart of life. He was alone
        // and young and wilful and wildhearted, alone amid a waste of wild air and brackish waters
        // and the sea-harvest of shells and tangle and veiled grey sunlight and gayclad lightclad
        // figures of children and girls and voices childish and girlish in the air." -JJ
    }
    /// Received bytes from a peer.
    fn received_bytes(&mut self, addr: &net::SocketAddr, bytes: &[u8]);
    /// Connection attempt underway.
    ///
    /// This is only encountered when an outgoing connection attempt is made,
    /// and is always called before [`Protocol::connected`].
    ///
    /// For incoming connections, [`Protocol::connected`] is called directly.
    fn attempted(&mut self, addr: &net::SocketAddr);
    /// New connection with a peer.
    fn connected(&mut self, addr: net::SocketAddr, local_addr: &net::SocketAddr, link: Link);
    /// Disconnected from peer.
    fn disconnected(
        &mut self,
        addr: &net::SocketAddr,
        reason: DisconnectReason<Self::DisconnectReason>,
    );
    /// An external command has been received.
    fn command(&mut self, cmd: Self::Command);
    /// Used to update the protocol's internal clock.
    ///
    /// "a regular short, sharp sound, especially that made by a clock or watch, typically
    /// every second."
    fn tick(&mut self, local_time: LocalTime);
    /// Used to advance the state machine after some timer rings.
    fn wake(&mut self);
    /// Drain all protocol outputs since the last call.
    fn drain(&mut self) -> Self::Drain;
    /// Write the peer's output buffer to the given writer.
    ///
    /// May return [`io::ErrorKind::WriteZero`] if it isn't able to write the entire buffer.
    fn write<W: io::Write>(&mut self, addr: &net::SocketAddr, writer: W) -> io::Result<()>;
}

/// Any network reactor that can drive the light-client protocol.
pub trait Reactor {
    /// The type of waker this reactor uses.
    type Waker: Send + Clone;

    /// Create a new reactor, initializing it with a publisher for protocol events,
    /// a channel to receive commands, and a channel to shut it down.
    fn new(
        shutdown: chan::Receiver<()>,
        listening: chan::Sender<net::SocketAddr>,
    ) -> Result<Self, io::Error>
    where
        Self: Sized;

    /// Run the given protocol state machine with the reactor.
    fn run<P: Protocol, E: Publisher<P::Event>>(
        &mut self,
        listen_addrs: &[net::SocketAddr],
        protocol: P,
        publisher: E,
        commands: chan::Receiver<P::Command>,
    ) -> Result<(), error::Error>;

    /// Used to wake certain types of reactors.
    fn wake(waker: &Self::Waker) -> io::Result<()>;

    /// Return a new waker.
    fn waker(&self) -> Self::Waker;
}
