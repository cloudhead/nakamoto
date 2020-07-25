use std::io;

use crossbeam_channel as chan;
use thiserror::Error;

use crate::node::Command;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(io::Error),

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
