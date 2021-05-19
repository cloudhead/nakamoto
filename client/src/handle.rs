//! Node handles are created from nodes by users of the library, to communicate with the underlying
//! protocol instance.
use std::net;
use std::ops::Range;

use crossbeam_channel as chan;
use thiserror::Error;

use nakamoto_common::block::filter::BlockFilter;
use nakamoto_common::block::tree::ImportResult;
use nakamoto_common::block::{self, Block, BlockHash, BlockHeader, Height, Transaction};
use nakamoto_p2p::{bitcoin::network::message::NetworkMessage, event::Event, protocol::Link};

/// An error resulting from a handle method.
#[derive(Error, Debug)]
pub enum Error {
    /// The command channel disconnected.
    #[error("command channel disconnected")]
    Disconnected,
    /// Invalid block height specified.
    #[error("invalid block height")]
    InvalidBlockHeight,
    /// The operation timed out.
    #[error("the operation timed out")]
    Timeout,
    /// An I/O error occured.
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
    fn get_tip(&self) -> Result<(Height, BlockHeader), Error>;
    /// Get a full block from the network.
    fn get_block(
        &self,
        hash: &BlockHash,
        channel: chan::Sender<(Block, Height)>,
    ) -> Result<(), Error>;
    /// Get compact filters from the network.
    fn get_filters(
        &self,
        range: Range<Height>,
        channel: chan::Sender<(BlockFilter, BlockHash, Height)>,
    ) -> Result<(), Error>;
    /// Broadcast a message to all *outbound* peers.
    fn broadcast(&self, msg: NetworkMessage) -> Result<(), Error>;
    /// Send a message to a random *outbound* peer. Return the chosen
    /// peer or nothing if no peer was available.
    fn query(&self, msg: NetworkMessage) -> Result<Option<net::SocketAddr>, Error>;
    /// Connect to the designated peer address.
    fn connect(&self, addr: net::SocketAddr) -> Result<Link, Error>;
    /// Disconnect from the designated peer address.
    fn disconnect(&self, addr: net::SocketAddr) -> Result<(), Error>;
    /// Submit a transaction to the network.
    fn submit_transaction(&self, tx: Transaction) -> Result<(), Error>;
    /// Import block headers into the node.
    /// This may cause the node to broadcast header or inventory messages to its peers.
    fn import_headers(
        &self,
        headers: Vec<BlockHeader>,
    ) -> Result<Result<ImportResult, block::tree::Error>, Error>;
    /// Wait for the given predicate to be fulfilled.
    fn wait<F: FnMut(Event) -> Option<T>, T>(&self, f: F) -> Result<T, Error>;
    /// Wait for a given number of peers to be connected.
    fn wait_for_peers(&self, count: usize) -> Result<(), Error>;
    /// Wait for the node to be ready and in sync with the blockchain.
    fn wait_for_ready(&self) -> Result<(), Error>;
    /// Wait for the node's active chain to reach a certain height. The hash at that height
    /// is returned.
    fn wait_for_height(&self, h: Height) -> Result<BlockHash, Error>;
    /// Listen on events.
    fn events(&self) -> &chan::Receiver<Event>;
    /// Shutdown the node process.
    fn shutdown(self) -> Result<(), Error>;
}
