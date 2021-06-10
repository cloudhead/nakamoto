//! Reactor trait.
use std::{io, net};

use crossbeam_channel as chan;

use crate::error::Error;
use crate::event::Publisher;
use crate::protocol::{Command, Machine, Out};

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
    fn run<F, M>(&mut self, listen_addrs: &[net::SocketAddr], machine: F) -> Result<(), Error>
    where
        F: FnOnce(chan::Sender<Out>) -> M,
        M: Machine;

    /// Used to wake certain types of reactors.
    fn wake(waker: &Self::Waker) -> io::Result<()>;

    /// Return a new waker.
    fn waker(&self) -> Self::Waker;
}
