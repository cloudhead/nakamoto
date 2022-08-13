//! Node error module.
use std::io;

use crossbeam_channel as chan;
use thiserror::Error;

use nakamoto_chain as chain;
use nakamoto_common as common;
use nakamoto_p2p as p2p;

use p2p::protocol::Command;

/// A client error.
#[derive(Error, Debug)]
pub enum Error {
    /// An error occuring from a client handle.
    #[error(transparent)]
    Handle(#[from] crate::handle::Error),
    /// An error coming from the networking sub-system.
    #[error(transparent)]
    Net(#[from] nakamoto_net::error::Error),
    /// A chain-related error.
    #[error(transparent)]
    Chain(#[from] common::block::tree::Error),
    /// An I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// An error coming from the block store.
    #[error(transparent)]
    BlockStore(#[from] common::block::store::Error),
    /// An error coming from the filter store.
    #[error(transparent)]
    FilterStore(#[from] chain::filter::store::Error),
    /// An error coming from the peer store.
    #[error("error loading peers: {0}")]
    PeerStore(io::Error),
    /// A communication channel error.
    #[error("command channel disconnected")]
    Channel,
}

impl From<chan::SendError<Command>> for Error {
    fn from(_: chan::SendError<Command>) -> Self {
        Self::Channel
    }
}

impl From<chan::RecvError> for Error {
    fn from(_: chan::RecvError) -> Self {
        Self::Channel
    }
}
