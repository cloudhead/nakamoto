//! Reactor trait.
use std::{io, net};

use crossbeam_channel as chan;

use nakamoto_common::block::filter::Filters;
use nakamoto_common::block::tree::BlockTree;
use nakamoto_common::p2p::peer;

use crate::error::Error;
use crate::protocol::event::Publisher;
use crate::protocol::{Command, Out, Protocol};

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
    fn run<B, T: BlockTree, F: Filters, P: peer::Store>(
        &mut self,
        listen_addrs: &[net::SocketAddr],
        builder: B,
    ) -> Result<(), Error>
    where
        B: FnOnce(chan::Sender<Out>) -> Protocol<T, F, P>;

    /// Used to wake certain types of reactors.
    fn wake(waker: &Self::Waker) -> io::Result<()>;

    /// Return a new waker.
    fn waker(&self) -> Self::Waker;
}
