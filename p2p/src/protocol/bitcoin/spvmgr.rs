//! Simple Payment Verification (SPV) Manager.
//!
//! Manages BIP 157/8 compact block filter sync.
//!

// TODO: Make sure to only call `get_cfheaders` on peers with support.
// use bitcoin::network::constants::ServiceFlags;

use thiserror::Error;

use bitcoin::network::message_filter::{CFHeaders, CFilter, GetCFHeaders, GetCFilters};

use nakamoto_common::block::filter::{self, BlockFilter, FilterHeader, Filters};
use nakamoto_common::block::tree::BlockTree;
use nakamoto_common::block::{BlockHash, Height};

use super::{PeerId, Timeout};

/// Maximum filter headers to be expected in a message.
const MAX_CFHEADERS: usize = 1000;

/// An error originating in the SPV manager.
#[derive(Error, Debug)]
pub enum Error {
    /// The request was ignored. This happens if we're not able to fulfill the reuqest.
    #[error("ignoring `{0}` message from {1}")]
    Ignored(&'static str, PeerId),
    /// Error due to an invalid peer message.
    #[error("invalid message received from peer")]
    InvalidMessage(PeerId),
    /// Error with the underlying filters datastore.
    #[error("filters error: {0}")]
    Filters(#[from] filter::Error),
}

/// An event originating in the SPV manager.
#[derive(Debug)]
pub enum Event {}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "")
    }
}

/// Compact filter synchronization.
pub trait SyncFilters {
    /// Get compact filter headers from peer, starting at the start height, and ending at the
    /// stop hash.
    fn get_cfheaders(&self, start_height: Height, stop_hash: BlockHash, timeout: Timeout);
    /// Get compact filters from a peer.
    fn get_cfilters(&self, start_height: Height, stop_hash: BlockHash, timeout: Timeout);
    /// Send compact filter headers to a peer.
    fn send_cfheaders(&self, addr: PeerId, headers: CFHeaders);
    /// Send a compact filter to a peer.
    fn send_cfilter(&self, addr: PeerId, filter: CFilter);
}

/// The ability to emit SPV related events.
pub trait Events {
    /// Emit an SPV-related event.
    fn event(&self, event: Event);
}

/// A compact block filter manager.
#[derive(Debug)]
pub struct SpvManager<F, U> {
    filters: F,
    upstream: U,
}

impl<F: Filters, U: SyncFilters + Events> SpvManager<F, U> {
    /// Create a new filter manager.
    pub fn new(filters: F, upstream: U) -> Self {
        Self { upstream, filters }
    }

    /// Called periodically. Triggers syncing if necessary.
    #[track_caller]
    pub fn idle<T: BlockTree>(&mut self, tree: &T) {
        let filter_height = self.filters.height();
        let block_height = tree.height();

        if filter_height < block_height {
            // We need to sync the filter header chain.
            // TODO
        } else if filter_height > block_height {
            panic!("SpvManager::idle: filter chain is longer than header chain!");
        }
    }

    /// Rollback filter header chain by a given number of headers.
    pub fn rollback(&mut self, n: usize) -> Result<(), filter::Error> {
        self.filters.rollback(n)
    }

    /// Handle a `cfheaders` message from a peer.
    pub fn received_cfheaders<T: BlockTree>(
        &mut self,
        addr: &PeerId,
        msg: CFHeaders,
        tree: &T,
    ) -> Result<(), Error> {
        if msg.filter_type != 0x0 {
            return Err(Error::InvalidMessage(*addr));
        }

        let prev_header: FilterHeader = msg.previous_filter.into();
        let (_, header) = self.filters.tip();

        if header != &prev_header {
            return Err(Error::InvalidMessage(*addr));
        }

        let start_height = self.filters.height();
        let stop_height = if let Some((height, _)) = tree.get_block(&msg.stop_hash) {
            height
        } else {
            return Err(Error::InvalidMessage(*addr));
        };
        let hashes = msg.filter_hashes;

        if start_height > stop_height {
            return Err(Error::InvalidMessage(*addr));
        }

        if hashes.len() > MAX_CFHEADERS {
            return Err(Error::InvalidMessage(*addr));
        }

        if (stop_height - start_height) as usize != hashes.len() {
            return Err(Error::InvalidMessage(*addr));
        }

        // Ok, looks like everything's valid..

        let mut last_header = prev_header;
        let mut headers = Vec::with_capacity(hashes.len());

        // Create headers out of the hashes.
        for filter_hash in hashes {
            last_header = FilterHeader::new(filter_hash, &last_header);
            headers.push((filter_hash, last_header));
        }
        self.filters.import_headers(headers).map_err(Error::from)
    }

    /// Handle a `getcfheaders` message from a peer.
    pub fn received_getcfheaders<T: BlockTree>(
        &mut self,
        addr: &PeerId,
        msg: GetCFHeaders,
        tree: &T,
    ) -> Result<(), Error> {
        if msg.filter_type != 0x0 {
            return Err(Error::InvalidMessage(*addr));
        }

        let start_height = msg.start_height as Height;
        let stop_height = if let Some((height, _)) = tree.get_block(&msg.stop_hash) {
            height
        } else {
            // Can't handle this message, we don't have the stop block.
            return Err(Error::Ignored("getcfheaders", *addr));
        };

        if let Ok(headers) = self.filters.get_headers(start_height..stop_height) {
            let hashes = headers.iter().map(|(hash, _)| *hash);
            let prev_header = self.filters.get_prev_header(start_height)?;

            self.upstream.send_cfheaders(
                *addr,
                CFHeaders {
                    filter_type: msg.filter_type,
                    stop_hash: msg.stop_hash,
                    previous_filter: prev_header.into(),
                    filter_hashes: hashes.collect(),
                },
            );
            return Ok(());
        }
        // We must be syncing, since we have the block headers requested but
        // not the associated filter headers. Simply ignore the request.
        return Err(Error::Ignored("getcfheaders", *addr));
    }

    /// Handle a `cfilter` message.
    pub fn received_cfilter<T: BlockTree>(
        &mut self,
        addr: &PeerId,
        msg: CFilter,
        tree: &T,
    ) -> Result<(), Error> {
        if msg.filter_type != 0x0 {
            return Err(Error::Ignored("cfilter", *addr));
        }

        let height = if let Some((height, _)) = tree.get_block(&msg.block_hash) {
            height
        } else {
            // Can't handle this message, we don't have the block.
            return Err(Error::Ignored("cfilter", *addr));
        };

        // The expected hash for this block filter.
        let hash = if let Ok((hash, _)) = self.filters.get_header(height) {
            hash
        } else {
            // Can't handle this message, we don't have the header.
            return Err(Error::Ignored("cfilter", *addr));
        };

        // Note that in case this fails, we have a bug in our implementation, since filter
        // headers are supposed to be downloaded in-order.
        let prev_header = self.filters.get_prev_header(height)?;
        let filter = BlockFilter::new(&msg.filter);

        if filter.filter_id(&prev_header.into()) != hash {
            return Err(Error::InvalidMessage(*addr));
        }

        self.filters
            .import_filter(height, filter)
            .map_err(Error::from)
    }

    /// Handle `getcfilters` message.
    pub fn received_getcfilters<T: BlockTree>(
        &mut self,
        _addr: &PeerId,
        msg: GetCFilters,
        _tree: &T,
    ) {
        if msg.filter_type != 0x0 {
            return;
        }
        // TODO
    }
}
