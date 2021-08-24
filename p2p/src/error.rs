//! Peer-to-peer protocol errors.

use bitcoin::consensus::encode;

use std::fmt::Debug;
use std::io;

use crossbeam_channel as crossbeam;

use thiserror::Error;

/// An error occuring in peer-to-peer networking code.
#[derive(Error, Debug)]
pub enum Error {
    /// An I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    /// An encoding/decoding error.
    #[error("encode/decode error: {0}")]
    Encode(#[from] encode::Error),

    /// A channel send or receive error.
    #[error("channel error: {0}")]
    Channel(Box<dyn std::error::Error + Send + Sync + 'static>),
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
