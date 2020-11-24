//! Node error module.
use std::io;

use crossbeam_channel as chan;
use thiserror::Error;

use nakamoto_common as common;
use nakamoto_p2p as p2p;

use p2p::protocol::bitcoin::Command;

/// A node error.
#[derive(Error, Debug)]
pub enum Error {
    /// An error coming from the peer-to-peer sub-system.
    #[error(transparent)]
    P2p(#[from] p2p::error::Error),
    /// A chain-related error.
    #[error(transparent)]
    Chain(#[from] common::block::tree::Error),
    /// An I/O error.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// An address-book error.
    #[error("Error loading address book: {0}")]
    AddressBook(io::Error),
    /// An error coming from the block store.
    #[error(transparent)]
    BlockStore(#[from] common::block::store::Error),
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
