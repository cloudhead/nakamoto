//! Node handles are created from nodes by users of the library, to communicate with the underlying
//! protocol instance.
use std::net;
use std::ops::{RangeBounds, RangeInclusive};

use crossbeam_channel as chan;
use thiserror::Error;

use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::network::Address;
use nakamoto_common::bitcoin::util::uint::Uint256;
use nakamoto_common::bitcoin::{Script, Txid};

use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::block::filter::BlockFilter;
use nakamoto_common::block::tree::{BlockReader, ImportResult};
use nakamoto_common::block::{self, Block, BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_p2p::fsm::Link;
use nakamoto_p2p::fsm::{self, Command, CommandError, Event, GetFiltersError, Peer};

/// An error resulting from a handle method.
#[derive(Error, Debug)]
pub enum Error {
    /// The command channel disconnected.
    #[error("command channel disconnected")]
    Disconnected,
    /// The command returned an error.
    #[error("command failed: {0}")]
    Command(#[from] CommandError),
    /// Failed to fetch filters.
    #[error("failed to get filters: {0}")]
    GetFilters(#[from] GetFiltersError),
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

/// A handle for communicating with a node process.
pub trait Handle: Sized + Send + Sync + Clone {
    /// Get the tip of the active chain. Returns the height of the chain, the header,
    /// and the total accumulated work.
    fn get_tip(&self) -> Result<(Height, BlockHeader, Uint256), Error>;
    /// Get a block header from the block header cache.
    fn get_block(&self, hash: &BlockHash) -> Result<Option<(Height, BlockHeader)>, Error>;
    /// Get a block header by height, from the block header cache.
    fn get_block_by_height(&self, height: Height) -> Result<Option<BlockHeader>, Error>;
    /// Query the local block tree using the given function. To return results from
    /// the query function, a [channel](`crate::chan`) may be used.
    fn query_tree(
        &self,
        query: impl Fn(&dyn BlockReader) + Send + Sync + 'static,
    ) -> Result<(), Error>;
    /// Find a branch from the active chain to the given (stale) block.
    ///
    /// See [BlockReader::find_branch](`nakamoto_common::block::tree::BlockReader::find_branch`).
    fn find_branch(&self, to: &BlockHash)
        -> Result<Option<(Height, NonEmpty<BlockHeader>)>, Error>;

    /// Request a full block from the network. The block will be sent over the channel created
    /// by [`Handle::blocks`] once received.
    fn request_block(&self, hash: &BlockHash) -> Result<(), Error>;
    /// Request compact filters from the network. The filters will be sent over the channel created
    /// by [`Handle::filters`] as they are received.
    fn request_filters(&self, range: RangeInclusive<Height>) -> Result<(), Error>;

    /// Subscribe to blocks received.
    fn blocks(&self) -> chan::Receiver<(Block, Height)>;
    /// Subscribe to compact filters received.
    fn filters(&self) -> chan::Receiver<(BlockFilter, BlockHash, Height)>;
    /// Subscribe to client events.
    fn events(&self) -> chan::Receiver<Event>;

    /// Send a command to the client.
    fn command(&self, cmd: Command) -> Result<(), Error>;
    /// Rescan the blockchain for matching scripts.
    ///
    /// If a "reorg" takes place, filters up to the start of the provided range
    /// will be re-fetched and scanned.
    fn rescan(
        &self,
        range: impl RangeBounds<Height>,
        watch: impl Iterator<Item = Script>,
    ) -> Result<(), Error> {
        // TODO: Handle invalid/empty ranges.

        let from = range.start_bound().cloned();
        let to = range.end_bound().cloned();

        self.command(Command::Rescan {
            from,
            to,
            watch: watch.collect(),
        })?;

        Ok(())
    }
    /// Update the watchlist with the provided scripts.
    ///
    /// Note that this won't trigger a rescan of any existing blocks. To avoid
    /// missing matching blocks, always watch scripts before sharing their
    /// corresponding address.
    fn watch(&self, watch: impl Iterator<Item = Script>) -> Result<(), Error> {
        self.command(Command::Watch {
            watch: watch.collect(),
        })?;

        Ok(())
    }
    /// Broadcast a message to peers matching the predicate.
    /// To only broadcast to outbound peers, use [`Peer::is_outbound`].
    fn broadcast(
        &self,
        msg: NetworkMessage,
        predicate: fn(Peer) -> bool,
    ) -> Result<Vec<net::SocketAddr>, Error>;
    /// Connect to the designated peer address.
    fn connect(&self, addr: net::SocketAddr) -> Result<Link, Error>;
    /// Disconnect from the designated peer address.
    fn disconnect(&self, addr: net::SocketAddr) -> Result<(), Error>;
    /// Submit a transaction to the network.
    ///
    /// Returns the peer(s) the transaction was announced to, or an error if no peers were found.
    fn submit_transaction(&self, tx: Transaction) -> Result<NonEmpty<net::SocketAddr>, Error>;
    /// Return a transaction that was propagated by the client.
    fn get_submitted_transaction(&self, txid: &Txid) -> Result<Option<Transaction>, Error>;
    /// Import block headers into the node.
    /// This may cause the node to broadcast header or inventory messages to its peers.
    fn import_headers(
        &self,
        headers: Vec<BlockHeader>,
    ) -> Result<Result<ImportResult, block::tree::Error>, Error>;
    /// Import peer addresses into the node's address book.
    fn import_addresses(&self, addrs: Vec<Address>) -> Result<(), Error>;
    /// Wait for the given predicate to be fulfilled.
    fn wait<F: FnMut(fsm::Event) -> Option<T>, T>(&self, f: F) -> Result<T, Error>;
    /// Wait for a given number of peers to be connected with the given services.
    fn wait_for_peers(
        &self,
        count: usize,
        required_services: impl Into<ServiceFlags>,
    ) -> Result<Vec<(net::SocketAddr, Height, ServiceFlags)>, Error>;
    /// Wait for the node's active chain to reach a certain height. The hash at that height
    /// is returned.
    fn wait_for_height(&self, h: Height) -> Result<BlockHash, Error>;
    /// Shutdown the node process.
    fn shutdown(self) -> Result<(), Error>;
}
