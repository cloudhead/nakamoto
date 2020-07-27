use crossbeam_channel as chan;

use nakamoto_chain::block::{Block, BlockHash, BlockHeader, Transaction};

use crate::error::Error;

/// A handle for communicating with a node process.
pub trait Handle {
    /// Get the tip of the chain.
    fn get_tip(&self) -> Result<BlockHeader, Error>;
    /// Get a full block from the network.
    fn get_block(&self, hash: &BlockHash) -> Result<Block, Error>;
    /// Submit a transaction to the network.
    fn submit_transaction(&self, tx: Transaction) -> Result<(), Error>;
    /// Return a channel to wait for a given number of peers to be connected.
    fn wait_for_peers(&self, count: usize) -> Result<chan::Receiver<()>, Error>;
    /// Return a channel to wait for the node to be ready and in sync with the blockchain.
    fn wait_for_ready(&self) -> Result<chan::Receiver<()>, Error>;
    /// Shutdown the node process.
    fn shutdown(self) -> Result<(), Error>;
}
