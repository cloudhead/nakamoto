use crossbeam_channel as chan;
use thiserror::Error;

use nakamoto_chain::block::{Block, BlockHash, BlockHeader, Transaction};

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
    /// Get the tip of the chain.
    fn get_tip(&self) -> Result<BlockHeader, Error>;
    /// Get a full block from the network.
    fn get_block(&self, hash: &BlockHash) -> Result<Block, Error>;
    /// Submit a transaction to the network.
    fn submit_transaction(&self, tx: Transaction) -> Result<(), Error>;
    /// Wait for a given number of peers to be connected.
    fn wait_for_peers(&self, count: usize) -> Result<(), Error>;
    /// Wait for the node to be ready and in sync with the blockchain.
    fn wait_for_ready(&self) -> Result<(), Error>;
    /// Shutdown the node process.
    fn shutdown(self) -> Result<(), Error>;
}
