//! Simple Payment Verification (SPV) Manager.
//!
//! Manages BIP 157/8 compact block filter sync.
//!

use std::ops::Range;

use nonempty::NonEmpty;
use thiserror::Error;

use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_filter::{CFHeaders, CFilter, GetCFHeaders, GetCFilters};

use nakamoto_common::block::filter::{self, BlockFilter, FilterHash, FilterHeader, Filters};
use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::BlockTree;
use nakamoto_common::block::{BlockHash, Height};
use nakamoto_common::collections::HashMap;

use super::channel::SetTimeout;
use super::{Link, PeerId, Timeout};

/// Idle timeout.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::BLOCK_INTERVAL;

/// Maximum filter headers to be expected in a message.
const MAX_MESSAGE_CFHEADERS: usize = 2000;

/// Maximum filters to be expected in a message.
#[allow(dead_code)]
const MAX_MESSAGE_CFILTERS: usize = 1000;

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
pub enum Event {
    /// Filters were imported successfully.
    FilterImported {
        /// Peer we received from.
        from: PeerId,
        /// Filter hash.
        hash: FilterHash,
    },
    /// Filter headers were imported successfully.
    FilterHeadersImported {
        /// Peer we received from.
        from: PeerId,
        /// Number of headers.
        count: usize,
        /// New filter header chain height.
        height: Height,
    },
    /// Started syncing filters with a peer.
    Syncing(PeerId),
    /// Finished syncing filters up to the specified height.
    Synced(Height),
    /// A peer has timed out responding to a filter request.
    TimedOut(PeerId),
    /// Block header chain rollback detected.
    RollbackDetected(Height),
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::TimedOut(addr) => write!(fmt, "Peer {} timed out", addr),
            Event::FilterImported { from, hash } => {
                write!(fmt, "Filter {} imported from {}", hash, from)
            }
            Event::FilterHeadersImported {
                from,
                count,
                height,
            } => {
                write!(
                    fmt,
                    "Imported {} filter headers from {}, height = {}",
                    count, from, height
                )
            }
            Event::Synced(height) => {
                write!(fmt, "Filter headers synced up to height = {}", height)
            }
            Event::Syncing(addr) => write!(fmt, "Syncing headers with {}", addr),
            Event::RollbackDetected(height) => {
                write!(
                    fmt,
                    "Rollback detected: discarding filters from height {}..",
                    height
                )
            }
        }
    }
}

/// Compact filter synchronization.
pub trait SyncFilters {
    /// Get compact filter headers from peer, starting at the start height, and ending at the
    /// stop hash.
    fn get_cfheaders(
        &self,
        addr: PeerId,
        start_height: Height,
        stop_hash: BlockHash,
        timeout: Timeout,
    );
    /// Get compact filters from a peer.
    fn get_cfilters(
        &self,
        addr: PeerId,
        start_height: Height,
        stop_hash: BlockHash,
        timeout: Timeout,
    );
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

/// SPV manager configuration.
#[derive(Debug)]
pub struct Config {
    /// How long to wait for a response from a peer.
    pub request_timeout: Timeout,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            request_timeout: Timeout::from_secs(30),
        }
    }
}

/// A SPV peer.
#[derive(Debug)]
struct Peer {
    height: Height,
    last_active: LocalTime,
}

/// A compact block filter manager.
#[derive(Debug)]
pub struct SpvManager<F, U> {
    config: Config,
    peers: HashMap<PeerId, Peer>,
    filters: F,
    upstream: U,
    /// Last time we idled.
    last_idle: Option<LocalTime>,
    rng: fastrand::Rng,
}

impl<F: Filters, U: SyncFilters + Events + SetTimeout> SpvManager<F, U> {
    /// Create a new filter manager.
    pub fn new(config: Config, rng: fastrand::Rng, filters: F, upstream: U) -> Self {
        let peers = HashMap::with_hasher(rng.clone().into());

        Self {
            config,
            peers,
            upstream,
            filters,
            last_idle: None,
            rng,
        }
    }

    /// Initialize the spv manager. Should only be called once.
    pub fn initialize<T: BlockTree>(&mut self, now: LocalTime, tree: &T) {
        self.idle(now, tree);
    }

    /// Called periodically. Triggers syncing if necessary.
    pub fn idle<T: BlockTree>(&mut self, now: LocalTime, tree: &T) {
        if now - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            let filter_height = self.filters.height();
            let block_height = tree.height();

            if filter_height < block_height {
                // We need to sync the filter header chain.
                self.sync(tree);
                self.last_idle = Some(now);
                self.upstream.set_timeout(IDLE_TIMEOUT);
            } else if filter_height > block_height {
                panic!("SpvManager::idle: filter chain is longer than header chain!");
            }
        }
    }

    /// Rollback filter header chain by a given number of headers.
    pub fn rollback(&mut self, n: usize) -> Result<(), filter::Error> {
        self.filters.rollback(n)
    }

    /// Send a `getcfilters` message to a random peer.
    pub fn get_cfilters<T: BlockTree>(&mut self, _range: Range<Height>, _tree: &T) {
        todo!()
    }

    /// Handle a `cfheaders` message from a peer.
    pub fn received_cfheaders<T: BlockTree>(
        &mut self,
        addr: &PeerId,
        msg: CFHeaders,
        tree: &T,
    ) -> Result<Height, Error> {
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
        let count = hashes.len();

        if start_height > stop_height {
            return Err(Error::InvalidMessage(*addr));
        }

        if count > MAX_MESSAGE_CFHEADERS {
            return Err(Error::InvalidMessage(*addr));
        }

        if (stop_height - start_height) as usize != count {
            return Err(Error::InvalidMessage(*addr));
        }

        // Ok, looks like everything's valid..

        let mut last_header = prev_header;
        let mut headers = Vec::with_capacity(count);

        // Create headers out of the hashes.
        for filter_hash in hashes {
            last_header = FilterHeader::new(filter_hash, &last_header);
            headers.push((filter_hash, last_header));
        }
        self.filters
            .import_headers(headers)
            .map(|height| {
                self.upstream.event(Event::FilterHeadersImported {
                    from: *addr,
                    count,
                    height,
                });

                if height == tree.height() {
                    self.upstream.event(Event::Synced(height));
                }
                height
            })
            .map_err(Error::from)
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

        let headers = self.filters.get_headers(start_height..stop_height);
        if !headers.is_empty() {
            let hashes = headers.iter().map(|(hash, _)| *hash);
            let prev_header = self
                .filters
                .get_prev_header(start_height)
                .expect("SpvManager::received_getcfheaders: all headers up to the tip must exist");

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
        Err(Error::Ignored("getcfheaders", *addr))
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
        let hash = if let Some((hash, _)) = self.filters.get_header(height) {
            hash
        } else {
            // Can't handle this message, we don't have the header.
            return Err(Error::Ignored("cfilter", *addr));
        };

        // Note that in case this fails, we have a bug in our implementation, since filter
        // headers are supposed to be downloaded in-order.
        let prev_header = self
            .filters
            .get_prev_header(height)
            .expect("SpvManager::received_cfilter: all headers up to the tip must exist");
        let filter = BlockFilter::new(&msg.filter);

        if filter.filter_id(&prev_header.into()) != hash {
            return Err(Error::InvalidMessage(*addr));
        }

        self.filters
            .import_filter(height, filter)
            .map(|_| {
                self.upstream
                    .event(Event::FilterImported { from: *addr, hash })
            })
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

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.peers.remove(id);
    }

    /// Called when a new peer was negotiated.
    pub fn peer_negotiated(
        &mut self,
        id: PeerId,
        height: Height,
        services: ServiceFlags,
        link: Link,
        clock: &impl Clock,
    ) {
        if !link.is_outbound() {
            return;
        }
        if !services.has(ServiceFlags::COMPACT_FILTERS) {
            return;
        }

        self.peers.insert(
            id,
            Peer {
                last_active: clock.local_time(),
                height,
            },
        );
    }

    /// Send a `getcfheaders` message to a random peer.
    pub fn send_getcfheaders<T: BlockTree>(&mut self, range: Range<Height>, tree: &T) {
        let count = range.end as usize - range.start as usize;

        debug_assert!(range.start < range.end);
        debug_assert!(!range.is_empty());

        if range.is_empty() {
            return;
        }

        // Cap request to `MAX_MESSAGE_CFHEADERS`.
        let stop_hash = if count > MAX_MESSAGE_CFHEADERS {
            let stop_height = range.start + MAX_MESSAGE_CFHEADERS as Height - 1;
            let stop_block = tree
                .get_block_by_height(stop_height)
                .expect("all headers up to the tip exist");

            stop_block.block_hash()
        } else {
            let (hash, _) = tree.tip();

            hash
        };

        if let Some(peers) = NonEmpty::from_vec(self.peers.keys().collect()) {
            let ix = self.rng.usize(..peers.len());
            let peer = peers.get(ix).unwrap(); // Can't fail.

            self.upstream.get_cfheaders(
                **peer,
                range.start,
                stop_hash,
                self.config.request_timeout,
            );
        }
    }

    ////////////////////////////////////////////////////////////////////////////

    /// Attempt to sync the filter header chain.
    fn sync<T: BlockTree>(&mut self, tree: &T) {
        let start_height = self.filters.height() + 1;
        let stop_height = tree.height();

        self.send_getcfheaders(start_height..stop_height + 1, tree);
    }
}
