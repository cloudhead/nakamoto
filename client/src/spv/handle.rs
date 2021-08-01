use std::net;
use std::ops::RangeBounds;

use bitcoin::{Address, BlockHash, Script, Transaction};
use nakamoto_common::block::Height;
use nakamoto_common::nonempty::NonEmpty;

use super::event::Event;
use crate::client::{self, chan};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("command channel disconnected")]
    Disconnected,
    #[error("the operation timed out")]
    Timeout,
    #[error("client command error: {0}")]
    Client(#[from] client::CommandError),
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
    /// Get the current height and block hash up to which all filters have been synced and
    /// processed. Note that if a greater than zero starting height was specified, this is
    /// effectively treated as if blocks parent to that height were already processed.
    ///
    /// To track sync progress, the [`Event::Synced`] event may be used instead.
    fn tip(&self) -> Result<(Height, BlockHash), Error>;
    /// Submit transactions to the network.
    fn submit(
        &mut self,
        txs: impl IntoIterator<Item = Transaction>,
    ) -> Result<NonEmpty<net::SocketAddr>, Error>;
    /// Subscribe to SPV-related events.
    fn events(&mut self) -> chan::Receiver<Event>;
    /// Rescan the blockchain for matching addresses and outputs.
    fn rescan(&mut self, range: impl RangeBounds<Height>) -> Result<(), Error>;
    /// Watch an address.
    ///
    /// Returns `true` if the address was added to the watch list.
    fn watch_address(&self, address: Address) -> bool;
    /// Watch scripts.
    ///
    /// Returns `true` if the script was added to the watch list.
    fn watch_scripts(&self, scripts: impl IntoIterator<Item = Script>);
    /// Stop watching an address.
    fn unwatch_address(&self, address: &Address) -> bool;
    /// Stop watching scripts.
    fn unwatch_scripts<'a>(&self, scripts: impl IntoIterator<Item = &'a Script> + 'a);
    /// Shutdown the transaction manager. Blocks until ongoing tasks have completed.
    fn shutdown(self) -> Result<(), Error>;
}
