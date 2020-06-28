use bitcoin::consensus::encode;

use std::fmt::Debug;
use std::io;
use std::time;

use crossbeam_channel as crossbeam;

use thiserror::Error;

use nakamoto_chain::block::tree;

/// An error occuring in peer-to-peer networking code.
#[derive(Error, Debug)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("timeout error: {0:?}")]
    Timeout(time::Duration),

    #[error("chain validation error: {0}")]
    BlockImport(#[from] tree::Error),

    #[error("encode/decode error: {0}")]
    Encode(#[from] encode::Error),

    #[error("not connected to the peer network")]
    NotConnected,

    #[error("send event error: {0}")]
    SendEvent(String),

    #[error("channel error: {0}")]
    Channel(Box<dyn std::error::Error + Send + Sync>),
}

impl<T: Debug + Send + Sync + 'static> From<crossbeam::SendError<T>> for Error {
    fn from(err: crossbeam::SendError<T>) -> Self {
        Self::Channel(Box::new(err))
    }
}

impl From<crossbeam::RecvError> for Error {
    fn from(err: crossbeam::RecvError) -> Self {
        Self::Channel(Box::new(err))
    }
}

impl From<crossbeam::RecvTimeoutError> for Error {
    fn from(err: crossbeam::RecvTimeoutError) -> Self {
        Self::Channel(Box::new(err))
    }
}
