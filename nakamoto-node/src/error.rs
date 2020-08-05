use std::io;

use crossbeam_channel as chan;
use thiserror::Error;

use nakamoto_common as common;
use nakamoto_p2p as p2p;

use p2p::protocol::bitcoin::Command;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    P2p(#[from] p2p::error::Error),
    #[error(transparent)]
    Chain(#[from] common::block::tree::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("Error loading address book: {0}")]
    AddressBook(io::Error),
    #[error(transparent)]
    BlockStore(#[from] common::block::store::Error),
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
