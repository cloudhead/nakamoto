use std::net;

use crossbeam_channel as chan;
use thiserror::Error;

use nakamoto_chain::block::{Block, BlockHash, BlockHeader, Transaction};
use nakamoto_p2p::protocol::Link;

#[derive(Error, Debug)]
pub enum Error {
    #[error("command channel disconnected")]
    Disconnected,
    #[error("the operation timed out")]
    Timeout,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<chan::RecvError> for Error {
    fn from(_: chan::RecvError) -> Self {
        Self::Disconnected
    }
}

impl<T> From<chan::SendError<T>> for Error {
    fn from(_: chan::SendError<T>) -> Self {
        Self::Disconnected
    }
}

/// A handle for communicating with a node process.
pub trait Handle {
    /// Node event generated during protocol operation.
    type Event;
    /// The message payload exchanged between nodes in the network.
    type Message;

    /// Get the tip of the chain.
    fn get_tip(&self) -> Result<BlockHeader, Error>;
    /// Get a full block from the network.
    fn get_block(&self, hash: &BlockHash) -> Result<Block, Error>;
    /// Connect to the designated peer address.
    fn connect(&self, addr: net::SocketAddr) -> Result<Link, Error>;
    /// Submit a transaction to the network.
    fn submit_transaction(&self, tx: Transaction) -> Result<(), Error>;
    /// Have the node receive a message as if it was coming from the given peer in the network.
    fn receive(&self, from: net::SocketAddr, msg: Self::Message) -> Result<(), Error>;
    /// Wait for the given predicate to be fulfilled.
    fn wait<F: Fn(Self::Event) -> Option<T>, T>(&self, f: F) -> Result<T, Error>;
    /// Wait for a given number of peers to be connected.
    fn wait_for_peers(&self, count: usize) -> Result<(), Error>;
    /// Wait for the node to be ready and in sync with the blockchain.
    fn wait_for_ready(&self) -> Result<(), Error>;
    /// Shutdown the node process.
    fn shutdown(self) -> Result<(), Error>;
}
