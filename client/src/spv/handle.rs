use std::net;
use std::ops::RangeBounds;

use bitcoin::{Script, Transaction};
use thiserror::Error;

use nakamoto_common::block::Height;
use nakamoto_common::nonempty::NonEmpty;

use super::event::Event;

use crate::client::{self, chan};

#[derive(Error, Debug)]
pub enum Error {
    #[error("command channel disconnected")]
    Disconnected,
    #[error("the operation timed out")]
    Timeout,
    #[error("client command error: {0}")]
    Command(#[from] client::CommandError),
    #[error("client handle error: {0}")]
    Client(#[from] client::handle::Error),
}

impl From<chan::RecvError> for Error {
    fn from(_: chan::RecvError) -> Self {
        Self::Disconnected
    }
}

impl From<chan::RecvTimeoutError> for Error {
    fn from(err: chan::RecvTimeoutError) -> Self {
        match err {
            chan::RecvTimeoutError::Timeout => Self::Timeout,
            chan::RecvTimeoutError::Disconnected => Self::Disconnected,
        }
    }
}

impl<T> From<chan::SendError<T>> for Error {
    fn from(_: chan::SendError<T>) -> Self {
        Self::Disconnected
    }
}

/// SPV client handle.
pub trait Handle {
    /// Submit transactions to the network.
    fn submit(
        &mut self,
        txs: impl IntoIterator<Item = Transaction>,
    ) -> Result<NonEmpty<net::SocketAddr>, Error>;

    /// Subscribe to SPV-related events.
    fn events(&mut self) -> chan::Receiver<Event>;

    /// Rescan the blockchain for matching addresses and outputs.
    fn rescan(
        &mut self,
        range: impl RangeBounds<Height>,
        watchlist: impl Iterator<Item = Script>,
    ) -> Result<(), Error>;

    /// Shutdown the transaction manager. Blocks until ongoing tasks have completed.
    fn shutdown(self) -> Result<(), Error>;
}
