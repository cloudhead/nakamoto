//! P2P related traits.
use std::{io, net};

use crossbeam_channel as chan;
use nakamoto_common::block::time::LocalTime;

use crate::error::Error;
use crate::protocol::event::Publisher;
use crate::protocol::{Command, DisconnectReason, Io, Link};

/// A protocol state-machine.
///
/// This trait is implemented by the core P2P protocol in [`crate::protocol::Protocol`].
pub trait Protocol {
    /// Return type of [`Protocol::drain`].
    type Drain: Iterator<Item = Io>;

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
    fn disconnected(&mut self, addr: &net::SocketAddr, reason: DisconnectReason);
    /// An external command has been received.
    fn command(&mut self, cmd: Command);
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

/// A type that can establish connections with peers.
/// Custom dialers can for example implement proxied connections.
pub trait Dialer: Default {
    /// Connect to a remote peer and return a TCP stream.
    /// *This function must not block.*
    fn dial(&mut self, addr: &net::SocketAddr) -> Result<net::TcpStream, io::Error>;
}

/// Used to wake certain types of reactors to wake the event loop.
pub trait Waker: Send + Sync + Clone {
    /// Wake the waker.
    fn wake(&self) -> io::Result<()>;
}

/// Any network reactor that can drive the light-client protocol.
pub trait Reactor<E: Publisher> {
    /// The type of waker this reactor uses.
    type Waker: Waker;

    /// Create a new reactor, initializing it with a publisher for protocol events,
    /// a channel to receive commands, and a channel to shut it down.
    fn new(
        publisher: E,
        commands: chan::Receiver<Command>,
        shutdown: chan::Receiver<()>,
    ) -> Result<Self, io::Error>
    where
        Self: Sized;

    /// Run the given protocol state machine with the reactor.
    fn run<P: Protocol, D: Dialer>(
        &mut self,
        listen_addrs: &[net::SocketAddr],
        protocol: P,
        dialer: D,
    ) -> Result<(), Error>;

    /// Return a new waker.
    fn waker(&self) -> Self::Waker;
}
