//! P2P related traits.
use std::{io, net};

use nakamoto_common::block::time::LocalTime;

use crate::error::Error;
use crate::protocol::channel::chan;
use crate::protocol::event::Publisher;
use crate::protocol::{Command, Input, Out};

/// A protocol state-machine.
///
/// This trait is implemented by the core P2P protocol in [`crate::protocol::Protocol`].
pub trait Protocol {
    /// Initialize the protocol. Called once before any event is sent to the state machine.
    fn initialize(&mut self, _time: LocalTime) {
        // "He was alone. He was unheeded, happy and near to the wild heart of life. He was alone
        // and young and wilful and wildhearted, alone amid a waste of wild air and brackish waters
        // and the sea-harvest of shells and tangle and veiled grey sunlight and gayclad lightclad
        // figures of children and girls and voices childish and girlish in the air." -JJ
    }
    /// Process the next input and advance the state machine by one step.
    fn step(&mut self, input: Input, local_time: LocalTime);
}

/// Any network reactor that can drive the light-client protocol.
pub trait Reactor<E: Publisher> {
    /// The type of waker this reactor uses.
    type Waker: Send + Clone;

    /// Create a new reactor, initializing it with a publisher for protocol events,
    /// a channel to receive commands, and a context.
    fn new(publisher: E, commands: chan::Receiver<Command>) -> Result<Self, io::Error>
    where
        E: Publisher,
        Self: Sized;

    /// Run the given protocol state machine with the reactor.
    ///
    /// The protocol is supplied via a "builder" function that takes the protocol output
    /// channel as its only parameter.
    fn run<B, P>(&mut self, listen_addrs: &[net::SocketAddr], builder: B) -> Result<(), Error>
    where
        P: Protocol,
        B: FnOnce(chan::Sender<Out>) -> P;

    /// Used to wake certain types of reactors.
    fn wake(waker: &Self::Waker) -> io::Result<()>;

    /// Return a new waker.
    fn waker(&self) -> Self::Waker;
}
