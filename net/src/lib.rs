//! Peer-to-peer networking core types.
#![allow(clippy::type_complexity)]
use std::borrow::Cow;
use std::hash::Hash;
use std::sync::Arc;
use std::{fmt, io, net};

use crossbeam_channel as chan;

pub mod error;
pub mod event;
pub mod simulator;
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

/// Output of a state transition of the state machine.
#[derive(Debug)]
pub enum Io<M, E, D, Id: PeerId = net::SocketAddr> {
    /// There are some bytes ready to be sent to a peer.
    Write(Id, M),
    /// Connect to a peer.
    Connect(Id),
    /// Disconnect from a peer.
    Disconnect(Id, D),
    /// Ask for a wakeup in a specified amount of time.
    SetTimer(LocalDuration),
    /// Emit an event.
    Event(E),
}

/// Disconnect reason.
#[derive(Debug, Clone)]
pub enum DisconnectReason<T> {
    /// Error while dialing the remote. This error occures before a connection is
    /// even established. Errors of this kind are usually not transient.
    DialError(Arc<std::io::Error>),
    /// Error with an underlying established connection. Sometimes, reconnecting
    /// after such an error is possible.
    ConnectionError(Arc<std::io::Error>),
    /// Peer was disconnected for another reason.
    StateMachine(T),
}

impl<T> DisconnectReason<T> {
    pub fn is_dial_err(&self) -> bool {
        matches!(self, Self::DialError(_))
    }

    pub fn is_connection_err(&self) -> bool {
        matches!(self, Self::ConnectionError(_))
    }
}

impl<T: fmt::Display> fmt::Display for DisconnectReason<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DialError(err) => write!(f, "{}", err),
            Self::ConnectionError(err) => write!(f, "{}", err),
            Self::StateMachine(reason) => write!(f, "{}", reason),
        }
    }
}

/// Remote peer id, which must be convertible into a [`net::SocketAddr`]
pub trait PeerId: Eq + Ord + Clone + Hash + fmt::Debug + From<net::SocketAddr> {
    fn to_socket_addr(&self) -> net::SocketAddr;
}

impl<T> PeerId for T
where
    T: Eq + Ord + Clone + Hash + fmt::Debug,
    T: Into<net::SocketAddr>,
    T: From<net::SocketAddr>,
{
    fn to_socket_addr(&self) -> net::SocketAddr {
        self.clone().into()
    }
}

/// A network service.
///
/// Network protocols must implement this trait to be drivable by the reactor.
pub trait Service<Id: PeerId = net::SocketAddr>: StateMachine<Id, Message = [u8]> {
    /// User commands handled by service.
    type Command;

    /// An external command has been received.
    fn command(&mut self, cmd: Self::Command);
}

/// A service state-machine.
pub trait StateMachine<Id: PeerId = net::SocketAddr>:
    Iterator<Item = Io<<Self::Message as ToOwned>::Owned, Self::Event, Self::DisconnectReason, Id>>
{
    /// Message type sent between peers.
    type Message: fmt::Debug + ToOwned + ?Sized;
    /// Events emitted by the state machine.
    type Event: fmt::Debug;
    /// Reason a peer was disconnected.
    type DisconnectReason: fmt::Debug
        + fmt::Display
        + Into<DisconnectReason<Self::DisconnectReason>>;

    /// Initialize the state machine. Called once before any event is sent to the state machine.
    fn initialize(&mut self, _time: LocalTime) {
        // "He was alone. He was unheeded, happy and near to the wild heart of life. He was alone
        // and young and wilful and wildhearted, alone amid a waste of wild air and brackish waters
        // and the sea-harvest of shells and tangle and veiled grey sunlight and gayclad lightclad
        // figures of children and girls and voices childish and girlish in the air." -JJ
    }
    /// Received message from a peer.
    fn received(&mut self, addr: &Id, message: Cow<Self::Message>);
    /// Connection attempt underway.
    ///
    /// This is only encountered when an outgoing connection attempt is made,
    /// and is always called before [`StateMachine::connected`].
    ///
    /// For incoming connections, [`StateMachine::connected`] is called directly.
    fn attempted(&mut self, addr: &Id);
    /// New connection with a peer.
    fn connected(&mut self, addr: Id, local_addr: &net::SocketAddr, link: Link);
    /// Disconnected from peer.
    fn disconnected(&mut self, addr: &Id, reason: DisconnectReason<Self::DisconnectReason>);
    /// Used to update the state machine's internal clock.
    ///
    /// "a regular short, sharp sound, especially that made by a clock or watch, typically
    /// every second."
    fn tick(&mut self, local_time: LocalTime);
    /// A timer set with [`Io::SetTimer`] has expired.
    fn timer_expired(&mut self);
}

/// Used by certain types of reactors to wake the event loop.
pub trait Waker: Send + Sync + Clone {
    /// Wake up! Call this after sending a command to make sure the command is processed
    /// in a timely fashion.
    fn wake(&self) -> io::Result<()>;
}

/// Any network reactor that can drive the light-client service.
pub trait Reactor<Id: PeerId = net::SocketAddr> {
    /// The type of waker this reactor uses.
    type Waker: Waker;

    /// Create a new reactor, initializing it with a publisher for service events,
    /// a channel to receive commands, and a channel to shut it down.
    fn new(
        shutdown: chan::Receiver<()>,
        listening: chan::Sender<net::SocketAddr>,
    ) -> Result<Self, io::Error>
    where
        Self: Sized;

    /// Run the given service with the reactor.
    fn run<S, E>(
        &mut self,
        listen_addrs: &[net::SocketAddr],
        service: S,
        publisher: E,
        commands: chan::Receiver<S::Command>,
    ) -> Result<(), error::Error>
    where
        S: Service<Id>,
        S::DisconnectReason: Into<DisconnectReason<S::DisconnectReason>>,
        E: Publisher<S::Event>;

    /// Return a new waker.
    fn waker(&self) -> Self::Waker;
}
