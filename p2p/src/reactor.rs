//! Reactor trait.
use std::{io, net};

use crossbeam_channel as chan;

use crate::error::Error;
use crate::event::Event;
use crate::protocol::{Command, Machine, Out};

/// Any network reactor that can drive the light-client protocol.
pub trait Reactor {
    /// The type of waker this reactor uses.
    type Waker: Send;

    /// Create a new reactor, initializing it with a channel to send protocol events on, and
    /// a channel to receive commands.
    fn new(
        subscriber: chan::Sender<Event>,
        commands: chan::Receiver<Command>,
    ) -> Result<Self, io::Error>
    where
        Self: Sized;

    /// Run the given protocol state machine with the reactor.
    fn run<F, M>(&mut self, listen_addrs: &[net::SocketAddr], machine: F) -> Result<(), Error>
    where
        F: FnOnce(chan::Sender<Out>) -> M,
        M: Machine;

    /// Subscribe to events.
    fn subscribe<F>(&mut self, callback: F)
    where
        F: Fn(Event) + Send + Sync + 'static;

    /// Used to wake certain types of reactors.
    fn wake(waker: &Self::Waker) -> io::Result<()>;

    /// Return a new waker.
    fn waker(&self) -> Self::Waker;
}
