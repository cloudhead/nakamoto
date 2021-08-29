//!
//! Manages header synchronization with peers.
//!
#![warn(missing_docs)]
use std::sync::Arc;
use std::time::SystemTime;

use bitcoin::consensus::params::Params;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_blockdata::Inventory;

use nakamoto_common::block::store;
use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};
use nakamoto_common::collections::{AddressBook, HashMap};
use nakamoto_common::nonempty::NonEmpty;

use super::channel::{Disconnect, SetTimeout};
use super::{DisconnectReason, Link, Locators, PeerId, Timeout};

/// How long to wait for a request, eg. `getheaders` to be fulfilled.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);
/// How long before the tip of the chain is considered stale. This takes into account
/// that the block timestamp may have been set sometime in the future.
pub const TIP_STALE_DURATION: LocalDuration = LocalDuration::from_mins(60 * 2);
/// Maximum number of headers sent in a `headers` message.
pub const MAX_MESSAGE_HEADERS: usize = 2000;
/// Idle timeout.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::BLOCK_INTERVAL;
/// Services required from peers for header sync.
pub const REQUIRED_SERVICES: ServiceFlags = ServiceFlags::NETWORK;

/// Maximum headers announced in a `headers` message, when unsolicited.
const MAX_UNSOLICITED_HEADERS: usize = 24;
/// How long to wait between checks for longer chains from peers.
const PEER_SAMPLE_INTERVAL: LocalDuration = LocalDuration::from_mins(60);

/// The ability to get and send headers.
pub trait SyncHeaders {
    /// Get headers from a peer.
    fn get_headers(&self, addr: PeerId, locators: Locators);
    /// Send headers to a peer.
    fn send_headers(&self, addr: PeerId, headers: Vec<BlockHeader>);
    /// Send initial post-negotiation messages, eg. `sendheaders`.
    fn negotiate(&self, addr: PeerId);
    /// Emit a sync-related event.
    fn event(&self, event: Event);
}

/// What to do if a timeout for a peer is received.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OnTimeout {
    /// Disconnect peer on timeout.
    Disconnect,
    /// Do nothing on timeout.
    Ignore,
}

/// State of a sync peer.
#[derive(Debug)]
struct Peer {
    height: Height,
    tip: BlockHash,
    link: Link,
    last_active: Option<LocalTime>,
    last_asked: Option<Locators>,
}

/// Sync manager configuration.
#[derive(Debug)]
pub struct Config {
    /// Maximum number of messages in a `headers` message.
    pub max_message_headers: usize,
    /// How long to wait for a response from a peer.
    pub request_timeout: LocalDuration,
    /// Consensus parameters.
    pub params: Params,
}

/// The sync manager state.
#[derive(Debug)]
pub struct SyncManager<U> {
    /// Sync manager configuration.
    pub config: Config,

    /// Sync-specific peer state.
    peers: AddressBook<PeerId, Peer>,
    /// Last time our tip was updated.
    last_tip_update: Option<LocalTime>,
    /// Last time we sampled our peers for their active chain.
    last_peer_sample: Option<LocalTime>,
    /// Last time we idled.
    last_idle: Option<LocalTime>,
    /// Random number generator.
    rng: fastrand::Rng,
    /// In-flight requests to peers.
    inflight: HashMap<PeerId, GetHeaders>,
    /// Upstream protocol channel.
    upstream: U,
}

/// An event emitted by the sync manager.
#[derive(Debug, Clone)]
pub enum Event {
    /// Headers received from a peer.
    HeadersReceived(PeerId, usize),
    /// Invalid headers received from a peer.
    InvalidHeadersReceived(PeerId, Arc<Error>),
    /// Unsolicited headers received.
    UnsolicitedHeadersReceived(PeerId, usize),
    /// A block was added to the main chain.
    BlockConnected {
        /// Block height.
        height: Height,
        /// Block header.
        header: BlockHeader,
    },
    /// A block was removed from the main chain.
    BlockDisconnected {
        /// Block height.
        height: Height,
        /// Block hash.
        hash: BlockHash,
    },
    /// A new block was discovered via a peer.
    BlockDiscovered(PeerId, BlockHash),
    /// Headers were imported successfully.
    HeadersImported(ImportResult),
    /// Started syncing with a peer.
    Syncing(PeerId),
    /// Synced up to the specified hash and height.
    Synced(BlockHash, Height),
    /// A peer has timed out responding to a header request.
    TimedOut(PeerId),
    /// Potential stale tip detected on the active chain.
    StaleTipDetected(LocalTime),
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::HeadersReceived(addr, count) => {
                write!(fmt, "{}: Received {} header(s)", addr, count)
            }
            Event::InvalidHeadersReceived(addr, error) => {
                write!(fmt, "{}: Received invalid headers: {}", addr, error)
            }
            Event::TimedOut(addr) => write!(fmt, "Peer {} timed out", addr),
            Event::UnsolicitedHeadersReceived(from, count) => {
                write!(fmt, "Received {} unsolicited headers from {}", count, from)
            }
            Event::HeadersImported(import_result) => {
                write!(fmt, "Headers imported: {:?}", &import_result)
            }
            Event::Synced(hash, height) => {
                write!(fmt, "Headers synced up to hash={} height={}", hash, height)
            }
            Event::Syncing(addr) => write!(fmt, "Syncing headers with {}", addr),
            Event::BlockConnected { height, header } => {
                write!(
                    fmt,
                    "Block {} connected at height {}",
                    header.block_hash(),
                    height
                )
            }
            Event::BlockDisconnected { height, hash } => {
                write!(fmt, "Block {} disconnected at height {}", hash, height)
            }
            Event::BlockDiscovered(from, hash) => {
                write!(fmt, "{}: Discovered new block: {}", from, &hash)
            }
            Event::StaleTipDetected(last_update) => {
                let elapsed = LocalTime::from(SystemTime::now()) - *last_update;

                write!(
                    fmt,
                    "Potential stale tip detected (last update is {} ago)",
                    elapsed
                )
            }
        }
    }
}

/// A `getheaders` request sent to a peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetHeaders {
    /// The remote peer.
    pub addr: PeerId,
    /// Locators hashes.
    pub locators: Locators,
    /// Request timeout.
    pub timeout: LocalDuration,

    /// Time at which the request was sent.
    sent_at: LocalTime,
    /// What to do if this request times out.
    on_timeout: OnTimeout,
}

/// `headers` broadcast.
#[derive(Debug)]
pub struct SendHeaders {
    /// Peers to send headers to.
    pub addrs: Vec<PeerId>,
    /// Headers to send.
    pub headers: Vec<BlockHeader>,
}

impl<U: SetTimeout + SyncHeaders + Disconnect> SyncManager<U> {
    /// Create a new sync manager.
    pub fn new(config: Config, rng: fastrand::Rng, upstream: U) -> Self {
        let peers = AddressBook::new(rng.clone());
        let last_tip_update = None;
        let last_peer_sample = None;
        let last_idle = None;
        let inflight = HashMap::with_hasher(rng.clone().into());

        Self {
            peers,
            config,
            last_tip_update,
            last_peer_sample,
            last_idle,
            rng,
            inflight,
            upstream,
        }
    }

    /// Initialize the sync manager. Should only be called once.
    pub fn initialize<T: BlockTree>(&mut self, time: LocalTime, tree: &T) {
        // TODO: `tip` should return the height.
        let (hash, _) = tree.tip();
        let height = tree.height();

        self.idle(time, tree);
        self.upstream.event(Event::Synced(hash, height));
    }

    /// Called periodically.
    pub fn idle<T: BlockTree>(&mut self, now: LocalTime, tree: &T) {
        if now - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            self.sync(now, tree);
            self.last_idle = Some(now);
            self.upstream.set_timeout(IDLE_TIMEOUT);
        }
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
        if link.is_outbound() && !services.has(REQUIRED_SERVICES) {
            return;
        }
        self.register(id, height, link);
        self.upstream.negotiate(id);
        self.sync(clock.local_time(), tree);
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.unregister(id);
    }

    /// Called when we received a `getheaders` message from a peer.
    pub fn received_getheaders<T: BlockTree>(
        &self,
        addr: &PeerId,
        (locator_hashes, stop_hash): Locators,
        tree: &T,
    ) {
        let max = self.config.max_message_headers;

        if self.is_syncing() || max == 0 {
            return;
        }
        let headers = tree.locate_headers(&locator_hashes, stop_hash, max);

        if headers.is_empty() {
            return;
        }
        self.upstream.send_headers(*addr, headers);
    }

    /// Import blocks into our block tree.
    pub fn import_blocks<T: BlockTree, I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        blocks: I,
        context: &C,
        tree: &mut T,
    ) -> Result<ImportResult, Error> {
        match tree.import_blocks(blocks, context) {
            Ok(ImportResult::TipChanged(header, tip, height, reverted, connected)) => {
                let result = ImportResult::TipChanged(
                    header,
                    tip,
                    height,
                    reverted.clone(),
                    connected.clone(),
                );

                for (height, hash) in reverted {
                    self.upstream
                        .event(Event::BlockDisconnected { height, hash });
                }
                for (height, header) in connected {
                    self.upstream
                        .event(Event::BlockConnected { height, header });
                }

                self.upstream.event(Event::Synced(tip, height));
                self.broadcast_tip(&tip, tree);

                Ok(result)
            }
            Ok(result @ ImportResult::TipUnchanged) => Ok(result),
            Err(err) => Err(err),
        }
    }

    /// Called when we receive headers from a peer.
    pub fn received_headers<T: BlockTree>(
        &mut self,
        from: &PeerId,
        headers: Vec<BlockHeader>,
        clock: &impl Clock,
        tree: &mut T,
    ) -> Result<ImportResult, store::Error> {
        let request = self.inflight.remove(from);
        let headers = if let Some(headers) = NonEmpty::from_vec(headers) {
            headers
        } else {
            return Ok(ImportResult::TipUnchanged);
        };

        let length = headers.len();

        if length > MAX_MESSAGE_HEADERS {
            self.upstream
                .event(Event::UnsolicitedHeadersReceived(*from, length));

            return Ok(ImportResult::TipUnchanged);
        }
        // When unsolicited, we don't want to process too many headers in case of a DoS.
        if length > MAX_UNSOLICITED_HEADERS && request.is_none() {
            self.upstream
                .event(Event::UnsolicitedHeadersReceived(*from, length));

            return Ok(ImportResult::TipUnchanged);
        }

        if let Some(peer) = self.peers.get_mut(from) {
            peer.last_active = Some(clock.local_time());
        } else {
            return Ok(ImportResult::TipUnchanged);
        }
        self.upstream
            .event(Event::HeadersReceived(*from, headers.len()));

        let root = headers.first().block_hash();
        let best = headers.last().block_hash();

        if tree.contains(&best) {
            return Ok(ImportResult::TipUnchanged);
        }

        match self.import_blocks(headers.into_iter(), clock, tree) {
            Ok(ImportResult::TipUnchanged) => {
                // Try to find a common ancestor that leads up to the first header in
                // the list we received.
                let locators = (tree.locator_hashes(tree.height()), root);
                let timeout = self.config.request_timeout;

                self.request(
                    *from,
                    locators,
                    clock.local_time(),
                    timeout,
                    OnTimeout::Ignore,
                );

                Ok(ImportResult::TipUnchanged)
            }
            Ok(ImportResult::TipChanged(header, tip, height, reverted, connected)) => {
                // Update peer height.
                if let Some(peer) = self.peers.get_mut(from) {
                    if height > peer.height {
                        peer.tip = tip;
                        peer.height = height;
                    }
                }
                // Keep track of when we last updated our tip. This is useful to check
                // whether our tip is stale.
                self.last_tip_update = Some(clock.local_time());

                // If we received less than the maximum number of headers, we must be in sync.
                // Otherwise, ask for the next batch of headers.
                if length < MAX_MESSAGE_HEADERS {
                    // If these headers were unsolicited, we may already be ready/synced.
                    // Otherwise, we're finally in sync.

                    self.broadcast_tip(&tip, tree);
                    self.sync(clock.local_time(), tree);
                } else {
                    // TODO: If we're already in the state of asking for this header, don't
                    // ask again.
                    // TODO: Should we use stop-hash for the single locator?
                    let locators = (vec![tip], BlockHash::default());
                    let timeout = self.config.request_timeout;

                    self.request(
                        *from,
                        locators,
                        clock.local_time(),
                        timeout,
                        OnTimeout::Disconnect,
                    );
                }

                Ok(ImportResult::TipChanged(
                    header, tip, height, reverted, connected,
                ))
            }
            Err(err) => self
                .handle_error(from, err)
                .map(|()| ImportResult::TipUnchanged),
        }
    }

    fn request(
        &mut self,
        addr: PeerId,
        locators: Locators,
        sent_at: LocalTime,
        timeout: Timeout,
        on_timeout: OnTimeout,
    ) {
        if let Some(peer) = self.peers.get_mut(&addr) {
            debug_assert!(peer.last_asked.as_ref() != Some(&locators));

            peer.last_asked = Some(locators.clone());

            let req = GetHeaders {
                addr,
                locators,
                timeout,
                sent_at,
                on_timeout,
            };

            self.inflight.insert(addr, req.clone());
            self.upstream.get_headers(req.addr, req.locators);
            self.upstream.set_timeout(req.timeout);
        }
    }

    /// Called when we received an `inv` message. This will happen if we are out of sync with a
    /// peer, and blocks are being announced. Otherwise, we expect to receive a `headers` message.
    pub fn received_inv<T: BlockTree, C>(
        &mut self,
        addr: PeerId,
        inv: Vec<Inventory>,
        clock: &C,
        tree: &T,
    ) where
        C: Clock,
    {
        if !self.peers.contains_key(&addr) {
            return;
        }
        let mut best_block = None;

        for i in &inv {
            if let Inventory::Block(hash) = i {
                // TODO: Update block availability for this peer.
                if !tree.is_known(hash) {
                    self.upstream.event(Event::BlockDiscovered(addr, *hash));
                    // The final block hash in the inventory should be the highest. Use
                    // that one for a `getheaders` call.
                    best_block = Some(hash);
                }
            }
        }

        if let Some(stop_hash) = best_block {
            let locators = (tree.locator_hashes(tree.height()), *stop_hash);
            let timeout = self.config.request_timeout;

            // Try to find headers leading up to the `inv` entry.

            self.request(
                addr,
                locators,
                clock.local_time(),
                timeout,
                OnTimeout::Ignore,
            );
        }
    }

    /// Called when we received a tick.
    pub fn received_tick<T: BlockTree>(&mut self, local_time: LocalTime, tree: &T) {
        let timeout = self.config.request_timeout;
        let timed_out = self
            .inflight
            .iter()
            .filter_map(|(peer, req)| {
                if local_time - req.sent_at >= timeout {
                    Some((*peer, req.on_timeout))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for (peer, on_timeout) in &timed_out {
            self.inflight.remove(peer);

            match on_timeout {
                OnTimeout::Disconnect => {
                    self.unregister(peer);
                    self.upstream
                        .disconnect(*peer, DisconnectReason::PeerTimeout("sync"));
                }
                OnTimeout::Ignore => {
                    // It's likely that the peer just didn't have the requested header.
                }
            }
            self.upstream.event(Event::TimedOut(*peer));
        }

        // If some of the requests timed out, force a sync, otherwise just idle.
        if timed_out.is_empty() {
            self.idle(local_time, tree);
        } else {
            self.sync(local_time, tree);
        }
    }

    /// Get the best known height out of all our peers.
    pub fn best_height(&self) -> Option<Height> {
        self.peers.iter().map(|(_, p)| p.height).max()
    }

    /// Are we currently syncing?
    pub fn is_syncing(&self) -> bool {
        !self.inflight.is_empty()
    }

    ///////////////////////////////////////////////////////////////////////////

    fn handle_error(&mut self, from: &PeerId, err: Error) -> Result<(), store::Error> {
        match err {
            // If this is an error with the underlying store, we have to propagate
            // this up, because we can't handle it here.
            Error::Store(e) => Err(e),

            // If we got a bad block from the peer, we can handle it here.
            Error::InvalidBlockPoW
            | Error::InvalidBlockTarget(_, _)
            | Error::InvalidBlockHash(_, _)
            | Error::InvalidBlockHeight(_)
            | Error::InvalidBlockTime(_, _) => {
                self.record_misbehavior(from);
                self.upstream
                    .event(Event::InvalidHeadersReceived(*from, Arc::new(err)));

                Ok(())
            }

            // Harmless errors can be ignored.
            Error::DuplicateBlock(_) | Error::BlockMissing(_) => Ok(()),

            // TODO: This will be removed.
            Error::BlockImportAborted(_, _, _) => Ok(()),
        }
    }

    fn record_misbehavior(&mut self, _peer: &PeerId) {
        // TODO
    }

    /// Check whether our current tip is stale.
    ///
    /// *Nb. This doesn't check whether we've already requested new blocks.*
    fn stale_tip<T: BlockTree>(&self, now: LocalTime, tree: &T) -> Option<LocalTime> {
        if let Some(last_update) = self.last_tip_update {
            if last_update
                < now - LocalDuration::from_secs(self.config.params.pow_target_spacing * 3)
            {
                return Some(last_update);
            }
        }
        // If we don't have the time of the last update, it's probably because we
        // are fresh, or restarted our node. In that case we check the last block time
        // instead.
        let (_, tip) = tree.tip();
        let time = LocalTime::from_block_time(tip.time);

        if time <= now - TIP_STALE_DURATION {
            return Some(time);
        }

        None
    }

    /// Register a new peer.
    fn register(&mut self, id: PeerId, height: Height, link: Link) {
        let last_active = None;
        let last_asked = None;
        let tip = BlockHash::default();

        self.peers.insert(
            id,
            Peer {
                height,
                tip,
                link,
                last_active,
                last_asked,
            },
        );
    }

    /// Unregister a peer.
    fn unregister(&mut self, id: &PeerId) {
        self.inflight.remove(id);
        self.peers.remove(id);
    }

    /// Check whether a peer can be synced with using the given locators.
    fn is_sync_candidate<T: BlockTree>(
        &self,
        addr: &PeerId,
        peer: &Peer,
        locators: &[BlockHash],
        tree: &T,
    ) -> bool {
        peer.height > tree.height() && self.is_request_candidate(addr, peer, locators)
    }

    /// Check whether a peer is a good request candidate.
    fn is_request_candidate(&self, addr: &PeerId, peer: &Peer, locators: &[BlockHash]) -> bool {
        !self.inflight.contains_key(addr)
            && peer.link.is_outbound()
            && peer.last_asked.as_ref().map_or(true, |l| l.0 != locators)
    }

    /// Check whether or not we are in sync with the network.
    fn is_synced<T: BlockTree>(&mut self, now: LocalTime, tree: &T) -> bool {
        if let Some(last_update) = self.stale_tip(now, tree) {
            self.upstream.event(Event::StaleTipDetected(last_update));

            return false;
        }
        let height = tree.height();

        // Find the peer with the longest chain and compare our height to it.
        if let Some(peer_height) = self.best_height() {
            return height >= peer_height;
        }

        // Assume we're out of sync.
        false
    }

    /// Check if we're currently syncing with these locators.
    fn syncing(&self, locators: &Locators) -> bool {
        self.inflight.values().any(|r| &r.locators == locators)
    }

    /// Start syncing if we're out of sync.
    /// Returns `true` if we started syncing, and `false` if we were up to date or not able to
    /// sync.
    fn sync<T: BlockTree>(&mut self, now: LocalTime, tree: &T) -> bool {
        if self.peers.is_empty() {
            return false;
        }
        if self.is_synced(now, tree) {
            let (tip, _) = tree.tip();
            let height = tree.height();

            self.upstream.event(Event::Synced(tip, height));
            self.sample_peers(now, tree);

            return false;
        }

        // ... It looks like we're out of sync ...

        let locators = (tree.locator_hashes(tree.height()), BlockHash::default());

        // If we're already fetching these headers, just wait.
        if self.syncing(&locators) {
            return false;
        }

        if let Some((addr, _)) = self
            .peers
            .sample_with(|a, p| self.is_sync_candidate(a, p, &locators.0, tree))
        {
            let timeout = self.config.request_timeout;
            let addr = *addr;

            self.request(addr, locators, now, timeout, OnTimeout::Ignore);
            self.upstream.event(Event::Syncing(addr));

            true
        } else {
            // TODO: No peer found to sync.. emit event.
            false
        }
    }

    /// Broadcast our best block header to connected peers who don't have it.
    fn broadcast_tip<T: BlockTree>(&mut self, hash: &BlockHash, tree: &T) {
        if let Some((height, best)) = tree.get_block(hash) {
            for (addr, peer) in &*self.peers {
                // TODO: Don't broadcast to peer that is currently syncing?
                if peer.link == Link::Inbound && height > peer.height {
                    self.upstream.send_headers(*addr, vec![*best]);
                }
            }
        }
    }

    /// Ask all our outbound peers whether they have better block headers.
    fn sample_peers<T: BlockTree>(&mut self, now: LocalTime, tree: &T) {
        if now - self.last_peer_sample.unwrap_or_default() < PEER_SAMPLE_INTERVAL {
            return;
        }
        self.last_peer_sample = Some(now);

        // If we think we're in sync and we haven't asked other peers in a while, then
        // sample their headers just to make sure we're on the right chain.
        let locators = tree.locator_hashes(tree.height());
        let addrs = self
            .peers
            .iter()
            .filter(|(a, p)| self.is_request_candidate(a, p, &locators))
            .map(|(a, _)| *a)
            .collect::<Vec<_>>();

        for addr in addrs {
            self.request(
                addr,
                (locators.clone(), BlockHash::default()),
                now,
                self.config.request_timeout,
                OnTimeout::Ignore,
            );
        }
    }
}
