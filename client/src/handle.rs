//! Node handles are created from nodes by users of the library, to communicate with the underlying
//! protocol instance.
use std::net;
use std::ops::{RangeBounds, RangeInclusive};

use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::Address;
use bitcoin::Script;
use crossbeam_channel as chan;
use thiserror::Error;

use nakamoto_common::block::filter::BlockFilter;
use nakamoto_common::block::tree::ImportResult;
use nakamoto_common::block::{self, Block, BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::network::Network;
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_p2p::protocol::{Command, CommandError, GetFiltersError, Peer};
use nakamoto_p2p::{bitcoin::network::message::NetworkMessage, event::Event, protocol::Link};

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
    /// Get the Bitcoin network the client is configured for.
    fn network(&self) -> Network;
    /// Get the tip of the chain.
    fn get_tip(&self) -> Result<(Height, BlockHeader), Error>;
    /// Get a full block from the network.
    fn get_block(&self, hash: &BlockHash) -> Result<(), Error>;
    /// Get compact filters from the network.
    fn get_filters(&self, range: RangeInclusive<Height>) -> Result<(), Error>;
    /// Subscribe to blocks received.
    fn blocks(&self) -> chan::Receiver<(Block, Height)>;
    /// Subscribe to compact filters received.
    fn filters(&self) -> chan::Receiver<(BlockFilter, BlockHash, Height)>;
    /// Send a command to the client.
    fn command(&self, cmd: Command) -> Result<(), Error>;
    /// Rescan the blockchain for matching addresses and outputs.
    fn rescan(
        &self,
        range: impl RangeBounds<Height>,
        watch: impl Iterator<Item = Script>,
    ) -> Result<(), Error> {
        use std::ops::Bound;

        // Nb. Can be replaced with `Bound::cloned()` when available in stable rust.
        let from = match range.start_bound() {
            Bound::Included(n) => Bound::Included(*n),
            Bound::Excluded(n) => Bound::Excluded(*n),
            Bound::Unbounded => Bound::Unbounded,
        };
        let to = match range.end_bound() {
            Bound::Included(n) => Bound::Included(*n),
            Bound::Excluded(n) => Bound::Excluded(*n),
            Bound::Unbounded => Bound::Unbounded,
        };

        self.command(Command::Rescan {
            from,
            to,
            watch: watch.collect(),
        })
    }
    /// Broadcast a message to peers matching the predicate.
    ///
    /// To only broadcast to peers that have completed the handshake, filter
    /// using [`Peer::is_negotiated`].
    ///
    /// To only broadcast to outbound peers, use [`Peer::is_outbound`].
    fn broadcast(
        &self,
        msg: NetworkMessage,
        predicate: fn(Peer) -> bool,
    ) -> Result<Vec<net::SocketAddr>, Error>;
    /// Send a message to a random *outbound* peer. Return the chosen
    /// peer or nothing if no peer was available.
    fn query(&self, msg: NetworkMessage) -> Result<Option<net::SocketAddr>, Error>;
    /// Connect to the designated peer address.
    fn connect(&self, addr: net::SocketAddr) -> Result<Link, Error>;
    /// Disconnect from the designated peer address.
    fn disconnect(&self, addr: net::SocketAddr) -> Result<(), Error>;
    /// Submit transactions to the network.
    ///
    /// Returns the peer(s) the transaction was announced to, or an error
    /// if no peers were found.
    fn submit_transactions(
        &self,
        txs: Vec<Transaction>,
    ) -> Result<NonEmpty<net::SocketAddr>, Error>;
    /// Import block headers into the node.
    /// This may cause the node to broadcast header or inventory messages to its peers.
    fn import_headers(
        &self,
        headers: Vec<BlockHeader>,
    ) -> Result<Result<ImportResult, block::tree::Error>, Error>;
    /// Import peer addresses into the node's address book.
    fn import_addresses(&self, addrs: Vec<Address>) -> Result<(), Error>;
    /// Wait for the given predicate to be fulfilled.
    fn wait<F: FnMut(Event) -> Option<T>, T>(&self, f: F) -> Result<T, Error>;
    /// Wait for a given number of peers to be connected with the given services.
    fn wait_for_peers(
        &self,
        count: usize,
        required_services: impl Into<ServiceFlags>,
    ) -> Result<(), Error>;
    /// Wait for the node to be ready and in sync with the blockchain.
    fn wait_for_ready(&self) -> Result<(), Error>;
    /// Wait for the node's active chain to reach a certain height. The hash at that height
    /// is returned.
    fn wait_for_height(&self, h: Height) -> Result<BlockHash, Error>;
    /// Listen on events.
    fn events(&self) -> chan::Receiver<Event>;
    /// Shutdown the node process.
    fn shutdown(self) -> Result<(), Error>;
}
