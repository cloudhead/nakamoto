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
pub enum ConnDirection {
    /// Inbound conneciton.
    Inbound,
    /// Outbound connection.
    Outbound,
}

impl ConnDirection {
    /// Check whether the link is outbound.
    pub fn is_outbound(&self) -> bool {
        *self == ConnDirection::Outbound
    }

    /// Check whether the link is inbound.
    pub fn is_inbound(&self) -> bool {
        *self == ConnDirection::Inbound
    }
}

/// Instructions received from a network protocol state machine and dispatched
/// by the reactor.
#[derive(Debug)]
pub enum ReactorDispatch<M, E, D, Id: PeerId = net::SocketAddr> {
    /// There are some bytes ready to be sent to a peer.
    SendPeer(Id, M),
    /// Connect to a peer.
    ConnectPeer(Id),
    /// Disconnect from a peer.
    DisconnectPeer(Id, D),
    /// Ask for a single timer-based wakeup.
    SetTimer(LocalDuration),
    /// Emit an event to all subscribers from the user threads.
    NotifySubscribers(E),
}

/// Disconnect reason originating either from the network interface or provided
/// by the network protocol state machine in form of
/// [`ReactorDispatch::DisconnectPeer`] instruction.
#[derive(Debug, Clone)]
pub enum DisconnectReason<T> {
    /// Error while dialing the remote. This error occures before a connection is
    /// even established. Errors of this kind are usually not transient.
    DialError(Arc<io::Error>),
    /// Error with an underlying established connection. Sometimes, reconnecting
    /// after such an error is possible.
    ConnectionError(Arc<io::Error>),
    /// Peer was disconnected due to a request from the network protocol
    /// business logic.
    OnDemand(T),
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
            Self::OnDemand(reason) => write!(f, "{}", reason),
        }
    }
}

impl<T> From<T> for DisconnectReason<T> {
    fn from(r: T) -> Self {
        DisconnectReason::OnDemand(r)
    }
}

/// Remote peer id, which must be interconvertible with [`net::SocketAddr`].
///
/// Automatically implemented for all types which can be constructed from and
/// converted into [`net::SocketAddr`].
// TODO: Investigate a problem that a PeerId can't be constructed with the remote
//       peer public key from just a socket address upon `accept`.
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

/// Peer network service which may be controlled from multiple user threads
/// user thread outside of the network event loop.
///
/// Network protocols must implement this trait to be drivable by the reactor.
pub trait PeerService<Id: PeerId = net::SocketAddr>: PeerProtocol<Id, PeerMessage = [u8]> {
    /// Commands handled by service. These commands should originate from an
    /// external "user" thread. They are passed through crossbeam channel provided
    /// in the `commands_channel` argument to the [`Reactor::run`] method. The
    /// commands are processed by the reactor calling [`Service::command_received`]
    /// method.
    type Command;

    /// A method that is called each time the service receives the command from
    /// the user thread
    fn command_received(&mut self, cmd: Self::Command);
}

/// Peer network protocol business logic.
///
/// This trait defines API for connecting specific protocol business logic to the
/// reactor implementation. It is parametrized by a peer id, which is the id
/// provided to the business logic from the reactor.
///
/// State machine generates instructions to the reactor by operating as an
/// iterator over .
pub trait PeerProtocol<Id: PeerId = net::SocketAddr>:
    Iterator<Item = ReactorDispatch<<Self::PeerMessage as ToOwned>::Owned, Self::Notification, Self::DisconnectSubreason, Id>>
{
    /// Message type sent between peers.
    type PeerMessage: ToOwned + ?Sized;

    /// Notifications which are sent by the reactor from the protocol state machine to
    /// the user thread via publisher provided to the reactor.
    type Notification;

    /// Reason a peer was disconnected in case the disconnection was caused by
    /// a state machine-specific reason.
    type DisconnectSubreason;

    /// Initialize the state machine. Called once before any event is sent to the state machine.
    fn initialize(&mut self, _time: LocalTime) {
        // "He was alone. He was unheeded, happy and near to the wild heart of life. He was alone
        // and young and wilful and wildhearted, alone amid a waste of wild air and brackish waters
        // and the sea-harvest of shells and tangle and veiled grey sunlight and gayclad lightclad
        // figures of children and girls and voices childish and girlish in the air." -JJ
    }

    /// Called by reactor upon receiving message from the remote peer.
    fn received(&mut self, remote_peer: &Id, message: Cow<Self::PeerMessage>);

    /// Connection attempt underway.
    ///
    /// This is only encountered when an outgoing connection attempt is made,
    /// and is always called before [`StateMachine::connected`].
    ///
    /// For incoming connections, [`StateMachine::connected`] is called directly.
    fn attempted(&mut self, remote_peer: &Id);

    /// Called whenever a new connection with a peer is established.
    fn connected(&mut self, remote_peer: Id, local_addr: &net::SocketAddr, link: ConnDirection);

    /// Called whenever remote peer got disconnected, either because of the
    /// network event or due to a local instruction from this state machine in
    /// form of [`ReactorDispatch::Disconnect`]
    fn disconnected(&mut self, remote_peer: &Id, reason: DisconnectReason<Self::DisconnectSubreason>);

    /// Called by the reactor every time the event loop gets data from the network.
    ///
    /// Used to update the state machine's internal clock.
    ///
    /// "a regular short, sharp sound, especially that made by a clock or watch, typically
    /// every second."
    fn tick(&mut self, local_time: LocalTime);

    /// Called by the reactor after a timeout whenever an Io::Wakeup was received
    /// by the reactor from this state machine iterator. Used to advance the state
    /// machine after some timer rings.
    ///
    /// NB: called together with [`StateMachine::wake`]
    fn wake(&mut self);
}

/// Used by certain types of reactors to wake the event loop to receive a user
/// command.
pub trait Waker: Send + Sync + Clone {
    /// Wake up! Call this after sending a command to make sure the command is processed
    /// in a timely fashion.
    fn wake(&self) -> io::Result<()>;
}

/// Any network reactor that can drive the light-client service.
pub trait Reactor<Id: PeerId = net::SocketAddr> {
    /// The type of [`Waker`] this reactor provides.
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
    ///
    /// # Arguments
    ///
    /// - `listen_addrs`: list of IP sockets to bind to;
    /// - `service`: a concrete network protocol implementation to run in the
    ///   reactor event loop;
    /// - `notification_publisher`: a concrete implementation of multiple subscribers single
    ///   publisher channel, used by the reactor to provide user threads
    ///   (subscribers) with the notifications of type `S::Notification`, emitted
    ///   by the network `service` business logic;
    /// - `commands_receiver`: the receiver part of the channel with the user thread
    ///   used to process commands from outside of the event loop.
    fn run<N, C>(
        &mut self,
        listen_addrs: &[net::SocketAddr],
        service: impl PeerService<Id, Notification = N, Command = C>,
        notification_publisher: impl Publisher<N>,
        commands_receiver: chan::Receiver<C>,
    ) -> Result<(), error::Error>;

    /// Construct a new instance of the reactor waker.
    ///
    /// Reactor can provide multiple wakers such that multiple user threads will
    /// be able to send a command to it.
    fn waker(&self) -> Self::Waker;
}
