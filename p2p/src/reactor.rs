//! Reactor trait.
use std::{io, net};

use crossbeam_channel as chan;

use nakamoto_common::block::filter::Filters;
use nakamoto_common::block::tree::BlockTree;

use crate::error::Error;
use crate::event::Event;
use crate::protocol::{self, Command};

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

    /// Run the given protocol with the reactor.
    fn run<T: BlockTree, F: Filters>(
        &mut self,
        builder: protocol::Builder<T, F>,
        listen_addrs: &[net::SocketAddr],
    ) -> Result<(), Error>;

    /// Used to wake certain types of reactors.
    fn wake(waker: &Self::Waker) -> io::Result<()>;

    /// Return a new waker.
    fn waker(&mut self) -> Self::Waker;
}
