//! Simple Payment Verification (SPV) Manager.
//!
//! Manages BIP 157/8 compact block filter sync.
//!

use std::ops::Range;

use nonempty::NonEmpty;
use thiserror::Error;

use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_filter::{CFHeaders, CFilter, GetCFHeaders};

use nakamoto_common::block::filter::{self, BlockFilter, Filters};
use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::BlockTree;
use nakamoto_common::block::{BlockHash, Height};
use nakamoto_common::collections::HashMap;
use nakamoto_common::source;

use super::channel::SetTimeout;
use super::{Link, PeerId, Timeout};

/// Idle timeout.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::BLOCK_INTERVAL;

/// Services required from peers for SPV functionality.
pub const REQUIRED_SERVICES: ServiceFlags = ServiceFlags::COMPACT_FILTERS;

/// Maximum filter headers to be expected in a message.
pub const MAX_MESSAGE_CFHEADERS: usize = 2000;

/// Maximum filters to be expected in a message.
pub const MAX_MESSAGE_CFILTERS: usize = 1000;

/// An error originating in the SPV manager.
#[derive(Error, Debug)]
pub enum Error {
    /// The request was ignored. This happens if we're not able to fulfill the request.
    #[error("ignoring `{msg}` message from {from}")]
    Ignored {
        /// Message that was ignored.
        msg: &'static str,
        /// Message sender.
        from: PeerId,
    },
    /// Error due to an invalid peer message.
    #[error("invalid message received from {from}: {reason}")]
    InvalidMessage {
        /// Message sender.
        from: PeerId,
        /// Reason why the message is invalid.
        reason: &'static str,
    },
    /// Error with the underlying filters datastore.
    #[error("filters error: {0}")]
    Filters(#[from] filter::Error),
}

/// An event originating in the SPV manager.
#[derive(Debug, Clone)]
pub enum Event {
    /// Filter was received and validated.
    FilterReceived {
        /// Peer we received from.
        from: PeerId,
        /// The received filter.
        filter: BlockFilter,
        /// Filter height.
        height: Height,
        /// Hash of corresponding block.
        block_hash: BlockHash,
    },
    /// Filter headers were imported successfully.
    FilterHeadersImported {
        /// New filter header chain height.
        height: Height,
        /// Block hash corresponding to the tip of the filter header chain.
        block_hash: BlockHash,
    },
    /// Started syncing filter headers with a peer.
    Syncing {
        /// The remote peer.
        peer: PeerId,
        /// The start height from which we're syncing.
        start_height: Height,
        /// The stop hash.
        stop_hash: BlockHash,
    },
    /// Request canceled.
    RequestCanceled {
        /// Reason for cancellation.
        reason: &'static str,
    },
    /// Finished syncing filter headers up to the specified height.
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
            Event::FilterReceived {
                from,
                height,
                block_hash,
                ..
            } => {
                write!(
                    fmt,
                    "Filter {} received for block {} from {}",
                    height, block_hash, from
                )
            }
            Event::FilterHeadersImported { height, .. } => {
                write!(fmt, "Imported filter header(s) up to height = {}", height,)
            }
            Event::Synced(height) => {
                write!(fmt, "Filter headers synced up to height = {}", height)
            }
            Event::Syncing {
                peer,
                start_height,
                stop_hash,
            } => write!(
                fmt,
                "Syncing filter headers with {}, start = {}, stop = {}",
                peer, start_height, stop_hash
            ),
            Event::RequestCanceled { reason } => {
                write!(fmt, "Request canceled: {}", reason)
            }
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

/// An error from attempting to get compact filters.
#[derive(Error, Debug)]
pub enum GetFiltersError {
    /// The specified range is invalid, eg. it is out of bounds.
    #[error("the specified range is invalid")]
    InvalidRange,
    /// Not connected to any compact filter peer.
    #[error("not connected to any peer with compact filters support")]
    NotConnected,
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
    /// Inflight requests.
    inflight: HashMap<BlockHash, LocalTime>,
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
            inflight: HashMap::with_hasher(rng.clone().into()),
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
            self.sync(tree, now);
            self.last_idle = Some(now);
            self.upstream.set_timeout(IDLE_TIMEOUT);
            self.inflight.clear();
        }
    }

    /// A tick was received.
    pub fn received_tick<T: BlockTree>(&mut self, now: LocalTime, tree: &T) {
        self.idle(now, tree);
    }

    /// Rollback filter header chain by a given number of headers.
    pub fn rollback(&mut self, n: usize) -> Result<(), filter::Error> {
        self.filters.rollback(n)
    }

    /// Send a `getcfilters` message to a random peer.
    ///
    /// If the range is greater than [`MAX_MESSAGE_CFILTERS`], requests filters from multiple
    /// peers.
    pub fn get_cfilters<T: BlockTree>(
        &mut self,
        range: Range<Height>,
        tree: &T,
    ) -> Result<(), GetFiltersError> {
        // TODO: Consolidate this code with the `get_cfheaders` code.
        if let Some(peers) = NonEmpty::from_vec(self.peers.keys().collect()) {
            let iter = HeightIterator {
                start: range.start,
                stop: range.end,
                step: MAX_MESSAGE_CFILTERS as Height,
            };
            for r in iter {
                let ix = self.rng.usize(..peers.len());
                let peer = *peers.get(ix).unwrap(); // Can't fail.

                let stop_hash = tree
                    .get_block_by_height(r.end - 1)
                    .ok_or(GetFiltersError::InvalidRange)?
                    .block_hash();
                let timeout = self.config.request_timeout;

                self.upstream
                    .get_cfilters(*peer, r.start, stop_hash, timeout);
            }
            Ok(())
        } else {
            Err(GetFiltersError::NotConnected)
        }
    }

    /// Handle a `cfheaders` message from a peer.
    pub fn received_cfheaders<T: BlockTree>(
        &mut self,
        from: &PeerId,
        msg: CFHeaders,
        tree: &T,
        time: LocalTime,
    ) -> Result<Height, Error> {
        let from = *from;

        if msg.filter_type != 0x0 {
            return Err(Error::InvalidMessage {
                from,
                reason: "cfheaders: invalid filter type",
            });
        }

        let prev_header = msg.previous_filter_header;
        let (_, tip) = self.filters.tip();

        // If the previous header doesn't match our tip, this could be a stale
        // message arriving too late. Ignore it.
        if tip != &prev_header {
            return Ok(self.filters.height());
        }

        let stop_hash = msg.stop_hash;
        let start_height = self.filters.height();
        let stop_height = if let Some((height, _)) = tree.get_block(&stop_hash) {
            height
        } else {
            return Err(Error::InvalidMessage {
                from,
                reason: "cfheaders: unknown stop hash",
            });
        };

        if self.inflight.remove(&stop_hash).is_none() {
            return Err(Error::Ignored {
                from,
                msg: "cfheaders: unsolicited message",
            });
        }

        let hashes = msg.filter_hashes;
        let count = hashes.len();

        if start_height > stop_height {
            return Err(Error::InvalidMessage {
                from,
                reason: "cfheaders: start height is greater than stop height",
            });
        }

        if count > MAX_MESSAGE_CFHEADERS {
            return Err(Error::InvalidMessage {
                from,
                reason: "cfheaders: header count exceeds maximum",
            });
        }

        if (stop_height - start_height) as usize != count {
            return Err(Error::InvalidMessage {
                from,
                reason: "cfheaders: header count does not match height range",
            });
        }

        // Ok, looks like everything's valid..

        let mut last_header = prev_header;
        let mut headers = Vec::with_capacity(count);

        // Create headers out of the hashes.
        for filter_hash in hashes {
            last_header = filter_hash.filter_header(&last_header);
            headers.push((filter_hash, last_header));
        }
        self.filters
            .import_headers(headers)
            .map(|height| {
                self.upstream.event(Event::FilterHeadersImported {
                    height,
                    block_hash: stop_hash,
                });
                assert!(height <= tree.height());

                if height == tree.height() {
                    self.upstream.event(Event::Synced(height));
                } else {
                    self.sync(tree, time);
                }
                height
            })
            .map_err(Error::from)
    }

    /// Handle a `getcfheaders` message from a peer.
    pub fn received_getcfheaders<T: BlockTree>(
        &mut self,
        from: &PeerId,
        msg: GetCFHeaders,
        tree: &T,
    ) -> Result<(), Error> {
        let from = *from;

        if msg.filter_type != 0x0 {
            return Err(Error::InvalidMessage {
                from,
                reason: "getcfheaders: invalid filter type",
            });
        }

        let start_height = msg.start_height as Height;
        let stop_height = if let Some((height, _)) = tree.get_block(&msg.stop_hash) {
            height
        } else {
            // Can't handle this message, we don't have the stop block.
            return Err(Error::Ignored {
                msg: "getcfheaders",
                from,
            });
        };

        let headers = self.filters.get_headers(start_height..stop_height);
        if !headers.is_empty() {
            let hashes = headers.iter().map(|(hash, _)| *hash);
            let prev_header = self
                .filters
                .get_prev_header(start_height)
                .expect("SpvManager::received_getcfheaders: all headers up to the tip must exist");

            self.upstream.send_cfheaders(
                from,
                CFHeaders {
                    filter_type: msg.filter_type,
                    stop_hash: msg.stop_hash,
                    previous_filter_header: prev_header,
                    filter_hashes: hashes.collect(),
                },
            );
            return Ok(());
        }
        // We must be syncing, since we have the block headers requested but
        // not the associated filter headers. Simply ignore the request.
        Err(Error::Ignored {
            msg: "getcfheaders",
            from,
        })
    }

    /// Handle a `cfilter` message.
    pub fn received_cfilter<T: BlockTree>(
        &mut self,
        from: &PeerId,
        msg: CFilter,
        tree: &T,
    ) -> Result<(), Error> {
        let from = *from;

        if msg.filter_type != 0x0 {
            return Err(Error::Ignored {
                msg: "cfilter",
                from,
            });
        }

        let height = if let Some((height, _)) = tree.get_block(&msg.block_hash) {
            height
        } else {
            // Can't handle this message, we don't have the block.
            return Err(Error::Ignored {
                msg: "cfilter",
                from,
            });
        };

        // The expected hash for this block filter.
        let header = if let Some((_, header)) = self.filters.get_header(height) {
            header
        } else {
            // Can't handle this message, we don't have the header.
            return Err(Error::Ignored {
                msg: "cfilter",
                from,
            });
        };

        // Note that in case this fails, we have a bug in our implementation, since filter
        // headers are supposed to be downloaded in-order.
        let prev_header = self
            .filters
            .get_prev_header(height)
            .expect("SpvManager::received_cfilter: all headers up to the tip must exist");
        let filter = BlockFilter::new(&msg.filter);

        if filter.filter_header(&prev_header) != header {
            return Err(Error::InvalidMessage {
                from,
                reason: "cfilter: filter hash doesn't match header",
            });
        }

        self.upstream.event(Event::FilterReceived {
            from,
            block_hash: msg.block_hash,
            height,
            filter,
        });

        Ok(())
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.peers.remove(id);
    }

    /// Called when a new peer was negotiated.
    pub fn peer_negotiated<T: BlockTree>(
        &mut self,
        id: PeerId,
        height: Height,
        services: ServiceFlags,
        link: Link,
        clock: &impl Clock,
        tree: &T,
    ) {
        if !link.is_outbound() {
            return;
        }
        if !services.has(REQUIRED_SERVICES) {
            return;
        }
        let time = clock.local_time();

        self.peers.insert(
            id,
            Peer {
                last_active: time,
                height,
            },
        );
        self.sync(tree, time);
    }

    /// Send a `getcfheaders` message to a random peer.
    pub fn send_getcfheaders<T: BlockTree>(
        &mut self,
        range: Range<Height>,
        tree: &T,
        time: LocalTime,
    ) -> Option<(PeerId, Height, BlockHash)> {
        let count = range.end as usize - range.start as usize;

        debug_assert!(range.start < range.end);
        debug_assert!(!range.is_empty());

        if range.is_empty() {
            return None;
        }
        let start_height = range.start;

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
        if self.inflight.contains_key(&stop_hash) {
            // Don't request the same thing twice.
            return None;
        }

        // TODO: We should select peers that are caught up to the requested height.
        if let Some(peers) = NonEmpty::from_vec(self.peers.keys().collect()) {
            let ix = self.rng.usize(..peers.len());
            let peer = *peers.get(ix).unwrap(); // Can't fail.

            self.upstream.get_cfheaders(
                *peer,
                start_height,
                stop_hash,
                self.config.request_timeout,
            );
            self.inflight.insert(stop_hash, time);

            return Some((*peer, start_height, stop_hash));
        } else {
            self.upstream.event(Event::RequestCanceled {
                reason: "no peers with required services",
            });
        }
        None
    }

    /// Attempt to sync the filter header chain.
    pub fn sync<T: BlockTree>(&mut self, tree: &T, time: LocalTime) {
        let filter_height = self.filters.height();
        let block_height = tree.height();

        if filter_height < block_height {
            // We need to sync the filter header chain.
            let start_height = self.filters.height() + 1;
            let stop_height = tree.height();

            if let Some((peer, start_height, stop_hash)) =
                self.send_getcfheaders(start_height..stop_height + 1, tree, time)
            {
                self.upstream.event(Event::Syncing {
                    peer,
                    start_height,
                    stop_hash,
                });
            }
        } else if filter_height > block_height {
            panic!("{}: filter chain is longer than header chain!", source!());
        }
    }
}

/// Iterator over height ranges.
struct HeightIterator {
    start: Height,
    stop: Height,
    step: Height,
}

impl Iterator for HeightIterator {
    type Item = Range<Height>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.stop {
            let start = self.start;
            let stop = self.stop.min(start + self.step - 1);

            self.start = stop + 1;

            Some(start..stop)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use bitcoin_hashes::hex::FromHex;
    use crossbeam_channel as chan;

    use nakamoto_chain::block::{cache::BlockCache, store};
    use nakamoto_chain::filter::cache::FilterCache;
    use nakamoto_common::block::filter::{FilterHash, FilterHeader};
    use nakamoto_common::network::Network;
    use nakamoto_test::BITCOIN_HEADERS;

    use crate::protocol::channel::Channel;
    use crate::protocol::PROTOCOL_VERSION;

    use super::*;

    const FILTER_HASHES: [&str; 15] = [
        "9acd599f31639d36b8e531d12afb430bb17e7cdd6e73c993c343e417cda1f299",
        "0bfdf66fef865ea20f1a3c4d12a9570685aa89cdd8a950755ef7e870520533ad",
        "155215e98328f097cf085f721edff6f4e9e1072e14012052b86297aa21085dcb",
        "227a8f6d137745df7445afcc5b1484c5a70bd1edb2f2886943dcb396803d1d85",
        "fb86fad94ad95c042894083c7dce973406481b0fd674163fde5d4f52a7bc074d",
        "37a8db7d504b65c63f0d5559ab616e586257b3d0672d574e7fcc7018eb45aa35",
        "a1a81f3571c98b30ce69ddf2f9e6a014074d73327d0e0d6cdc4d493fe64e3f2a",
        "a16c3a9a9da80a10999f73e88fbf5cd63a0266115c5f1f683ee1f1c534ad232d",
        "f52a72367e64fffdbd5239c00f380db0ac77901a8a8faa9c642d592b87b4b7ca",
        "81c4c5606d54107bfb9dccbaf23b7a2459f8444816623ba23e3de91f16a525da",
        "1f64677b953cbc851277f95edb29065c7859cae744ef905b5950f8e79ed97c8a",
        "8cde7d77626801155a891eea0688d7eb5c37ca74d84493254ff4e4c2a886de4a",
        "3eb61e435e1ed1675b5c1fcc4a89b4dba3695a8b159aabe4c03833ecd7c41704",
        "802221cd81ad57748b713d8055b5fc6d5f7cef71b9d59d690857ef835704cab8",
        "503adfa2634006e453900717f070ffc11a639ee1a0416e4e137f396c7706e6b7",
    ];

    const FILTERS: [&[u8]; 11] = [
        &[1, 127, 168, 128],
        &[1, 140, 59, 16],
        &[1, 140, 120, 216],
        &[1, 19, 255, 16],
        &[1, 63, 182, 112],
        &[1, 56, 58, 48],
        &[1, 12, 113, 176],
        &[1, 147, 204, 216],
        &[1, 117, 5, 160],
        &[1, 141, 61, 184],
        &[1, 155, 155, 152],
    ];

    #[test]
    fn test_receive_filters() {
        let network = Network::Mainnet;
        let peer = &([0, 0, 0, 0], 0).into();
        let time = LocalTime::now();
        let tree = {
            let genesis = network.genesis();
            let params = network.params();

            assert_eq!(genesis, BITCOIN_HEADERS.head);

            BlockCache::from(store::Memory::new(BITCOIN_HEADERS.clone()), params, &[]).unwrap()
        };
        let (sender, _receiver) = chan::unbounded();

        let mut spvmgr = {
            let rng = fastrand::Rng::new();
            let cache = FilterCache::from(store::memory::Memory::genesis(network)).unwrap();
            let upstream = Channel::new(network, PROTOCOL_VERSION, "test", sender);

            SpvManager::new(Config::default(), rng, cache, upstream)
        };

        // Import the headers.
        {
            let msg = CFHeaders {
                filter_type: 0,
                stop_hash: BlockHash::from_hex(
                    "00000000b3322c8c3ef7d2cf6da009a776e6a99ee65ec5a32f3f345712238473",
                )
                .unwrap(),
                previous_filter_header: FilterHeader::from_hex(
                    "02c2392180d0ce2b5b6f8b08d39a11ffe831c673311a3ecf77b97fc3f0303c9f",
                )
                .unwrap(),
                filter_hashes: FILTER_HASHES
                    .iter()
                    .map(|h| FilterHash::from_hex(h).unwrap())
                    .collect(),
            };
            spvmgr.inflight.insert(msg.stop_hash, time);
            spvmgr.received_cfheaders(peer, msg, &tree, time).unwrap();
        }

        assert_eq!(spvmgr.filters.height(), 15);
        spvmgr.filters.verify(network).unwrap();

        let cfilters = FILTERS
            .iter()
            .zip(BITCOIN_HEADERS.iter())
            .map(|(f, h)| CFilter {
                filter_type: 0x0,
                block_hash: h.block_hash(),
                filter: f.to_vec(),
            });

        // Now import the filters.
        for msg in cfilters {
            spvmgr.received_cfilter(peer, msg, &tree).unwrap();
        }
    }

    #[test]
    fn test_height_iterator() {
        let mut it = super::HeightIterator {
            start: 3,
            stop: 19,
            step: 5,
        };
        assert_eq!(it.next(), Some(3..7));
        assert_eq!(it.next(), Some(8..12));
        assert_eq!(it.next(), Some(13..17));
        assert_eq!(it.next(), Some(18..19));
        assert_eq!(it.next(), None);
    }
}
