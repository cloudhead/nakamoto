//!
//! Manages header synchronization with peers.
//!
#![warn(missing_docs)]
use std::collections::VecDeque;
use std::time::SystemTime;

use nonempty::NonEmpty;

use bitcoin::consensus::params::Params;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_blockdata::Inventory;

use nakamoto_common::block::store;
use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};
use nakamoto_common::collections::HashMap;

use super::channel::Disconnect;
use super::{Link, Locators, PeerId, Timeout};

/// How long to wait for a request, eg. `getheaders` to be fulfilled.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);
/// How long before the tip of the chain is considered stale. This takes into account
/// that the block timestamp may have been set sometime in the future.
pub const TIP_STALE_DURATION: LocalDuration = LocalDuration::from_mins(60 * 2);
/// Maximum number of headers sent in a `headers` message.
pub const MAX_MESSAGE_HEADERS: usize = 2000;
/// Idle timeout.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_mins(10);

/// Maximum headers announced in a `headers` message, when unsolicited.
const MAX_HEADERS_ANNOUNCED: usize = 8;
/// How long to wait between checks for longer chains from peers.
const PEER_SAMPLE_INTERVAL: LocalDuration = LocalDuration::from_mins(60);
/// How long to wait before evicting requests from the `recent` queue.
const RECENT_REQUEST_LINGER: LocalDuration = LocalDuration::from_mins(10);

/// The ability to get and send headers.
pub trait SyncHeaders {
    /// Get headers from a peer.
    fn get_headers(&self, addr: PeerId, locators: Locators, timeout: LocalDuration);
    /// Send headers to a peer.
    fn send_headers(&self, addr: PeerId, headers: Vec<BlockHeader>);
    /// Emit a sync-related event.
    fn event(&self, event: Event);
}

/// Ability to set idle timeout.
pub trait Idle {
    /// Set idle timeout.
    fn idle(&self, timeout: Timeout);
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
struct PeerState {
    id: PeerId,
    height: Height,
    tip: BlockHash,
    services: ServiceFlags,
    link: Link,
    last_active: Option<LocalTime>,
}

impl PeerState {
    /// Whether this is an outbound peer.
    fn is_outbound(&self) -> bool {
        self.link == Link::Outbound
    }
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
pub struct SyncManager<T, U> {
    /// Block tree.
    pub tree: T,

    /// Sync-specific peer state.
    peers: HashMap<PeerId, PeerState>,
    /// Sync manager configuration.
    config: Config,
    /// Last time our tip was updated.
    last_tip_update: Option<LocalTime>,
    /// Last time we sampled our peers for their active chain.
    last_peer_sample: Option<LocalTime>,
    /// Random number generator.
    rng: fastrand::Rng,
    /// In-flight requests to peers.
    inflight: HashMap<PeerId, GetHeaders>,
    /// Recently sent requests.
    recent: VecDeque<GetHeaders>,
    /// Upstream protocol channel.
    upstream: U,
}

/// An event emitted by the sync manager.
#[derive(Debug)]
pub enum Event {
    /// Headers received from a peer.
    ReceivedHeaders(PeerId, usize),
    /// Invalid headers received from a peer.
    ReceivedInvalidHeaders(PeerId, Error),
    /// Unsolicited headers received.
    ReceivedUnsolicitedHeaders(PeerId, usize),
    /// Headers were imported successfully.
    HeadersImported(ImportResult),
    /// A new block was discovered via a peer.
    BlockDiscovered(PeerId, BlockHash),
    /// Started syncing with a peer.
    Syncing(PeerId),
    /// Finished syncing up to the specified hash and height.
    Synced(BlockHash, Height),
    /// A peer has timed out responding to a header request.
    TimedOut(PeerId),
    /// Potential stale tip detected on the active chain.
    StaleTipDetected(LocalTime),
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::ReceivedHeaders(addr, count) => {
                write!(fmt, "{}: Received {} header(s)", addr, count)
            }
            Event::ReceivedInvalidHeaders(addr, error) => {
                write!(fmt, "{}: Received invalid headers: {}", addr, error)
            }
            Event::TimedOut(addr) => write!(fmt, "Peer {} timed out", addr),
            Event::ReceivedUnsolicitedHeaders(from, count) => {
                write!(fmt, "Received {} unsolicited headers from {}", count, from)
            }
            Event::HeadersImported(import_result) => {
                write!(fmt, "Headers imported: {:?}", &import_result)
            }
            Event::Synced(hash, height) => {
                write!(fmt, "Headers synced up to hash={} height={}", hash, height)
            }
            Event::Syncing(addr) => write!(fmt, "Syncing headers with {}", addr),
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

impl<T: BlockTree, U: SyncHeaders + Disconnect + Idle> SyncManager<T, U> {
    /// Create a new sync manager.
    pub fn new(tree: T, config: Config, rng: fastrand::Rng, upstream: U) -> Self {
        let peers = HashMap::with_hasher(rng.clone().into());
        let last_tip_update = None;
        let last_peer_sample = None;
        let inflight = HashMap::with_hasher(rng.clone().into());
        let recent = VecDeque::with_capacity(32);

        Self {
            tree,
            peers,
            config,
            last_tip_update,
            last_peer_sample,
            rng,
            inflight,
            recent,
            upstream,
        }
    }

    /// Initialize the sync manager. Should only be called once.
    pub fn initialize(&mut self, time: LocalTime) {
        self.idle(time);
    }

    /// Called periodically.
    pub fn idle(&mut self, now: LocalTime) {
        // Evict recent requests that are no longer recent.
        self.recent
            .retain(|r| now - r.sent_at <= RECENT_REQUEST_LINGER);

        self.sync(now);
        self.upstream.idle(IDLE_TIMEOUT);
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
        self.register(id, height, services, link);
        self.sync(clock.local_time());
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.unregister(id);
    }

    /// Called when we received a `getheaders` message from a peer.
    pub fn received_getheaders(&self, addr: &PeerId, (locator_hashes, stop_hash): Locators) {
        let max = self.config.max_message_headers;

        if self.is_syncing() || max == 0 {
            return;
        }
        let headers = self.tree.locate_headers(&locator_hashes, stop_hash, max);

        if headers.is_empty() {
            return;
        }
        self.upstream.send_headers(*addr, headers);
    }

    /// Import blocks into our block tree.
    pub fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        blocks: I,
        context: &C,
    ) -> Result<ImportResult, Error> {
        match self.tree.import_blocks(blocks, context) {
            Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                let result = ImportResult::TipChanged(tip, height, reverted);

                self.upstream.event(Event::HeadersImported(result.clone()));
                self.upstream.event(Event::Synced(tip, height));
                self.broadcast_tip(&tip);

                Ok(result)
            }
            Ok(result @ ImportResult::TipUnchanged) => {
                self.upstream.event(Event::HeadersImported(result.clone()));

                Ok(result)
            }
            Err(err) => Err(err),
        }
    }

    /// Called when we receive headers from a peer.
    pub fn received_headers(
        &mut self,
        from: &PeerId,
        headers: Vec<BlockHeader>,
        clock: &impl Clock,
    ) -> Result<ImportResult, store::Error> {
        let headers = if let Some(headers) = NonEmpty::from_vec(headers) {
            headers
        } else {
            return Ok(ImportResult::TipUnchanged);
        };
        self.upstream
            .event(Event::ReceivedHeaders(*from, headers.len()));

        let length = headers.len();
        let best = headers.last().block_hash();
        let peer = self.peers.get_mut(from).unwrap();

        peer.last_active = Some(clock.local_time());

        if self.tree.contains(&best) {
            return Ok(ImportResult::TipUnchanged);
        }

        match self.inflight.remove(from) {
            Some(GetHeaders { locators, .. })
                if headers
                    .iter()
                    .any(|h| locators.0.contains(&h.prev_blockhash)) =>
            {
                // Requested headers. These should extend our main chain.
                // Check whether the start of the header chain matches one of the locators we
                // supplied to the peer. Otherwise, we consider them unsolicited.

                let result = self.extend_chain(headers, clock);

                if let Ok(ref imported) = result {
                    self.upstream
                        .event(Event::HeadersImported(imported.clone()));
                }

                match result {
                    Ok(ImportResult::TipUnchanged) => Ok(ImportResult::TipUnchanged),
                    Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                        let peer = self.peers.get_mut(from).unwrap();

                        if height > peer.height {
                            peer.tip = tip;
                            peer.height = height;
                        }

                        // Keep track of when we last updated our tip. This is useful to check
                        // whether our tip is stale.
                        self.last_tip_update = Some(clock.local_time());

                        // If we received less than the maximum number of headers, we must be in sync.
                        // Otherwise, ask for the next batch of headers.
                        if length < self.config.max_message_headers {
                            // If these headers were unsolicited, we may already be ready/synced.
                            // Otherwise, we're finally in sync.

                            self.broadcast_tip(&tip);
                            self.sync(clock.local_time());
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

                        Ok(ImportResult::TipChanged(tip, height, reverted))
                    }
                    Err(err) => self
                        .handle_error(from, err)
                        .map(|()| ImportResult::TipUnchanged),
                }
            }
            // Header announcement.
            _ if length <= MAX_HEADERS_ANNOUNCED => {
                let root = headers.first().block_hash();

                match self.tree.import_blocks(headers.into_iter(), clock) {
                    Ok(import_result @ ImportResult::TipUnchanged) => {
                        self.upstream
                            .event(Event::HeadersImported(import_result.clone()));

                        // Try to find a common ancestor that leads up to the first header in
                        // the list we received.
                        let locators = (self.tree.locator_hashes(self.tree.height()), root);
                        let timeout = self.config.request_timeout;

                        self.request(
                            *from,
                            locators,
                            clock.local_time(),
                            timeout,
                            OnTimeout::Ignore,
                        );

                        Ok(import_result)
                    }
                    Ok(import_result) => {
                        self.upstream
                            .event(Event::HeadersImported(import_result.clone()));

                        Ok(import_result)
                    }
                    Err(err) => self
                        .handle_error(from, err)
                        .map(|()| ImportResult::TipUnchanged),
                }
            }
            // We've received a large number of unsolicited headers. This is more than the
            // typical headers sent during a header announcement, and we haven't asked
            // this peer for any headers. We choose to ignore it.
            _ => {
                self.upstream
                    .event(Event::ReceivedUnsolicitedHeaders(*from, length));

                Ok(ImportResult::TipUnchanged)
            }
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
        let req = GetHeaders {
            addr,
            locators,
            timeout,
            sent_at,
            on_timeout,
        };

        debug_assert!(!self
            .recent
            .iter()
            .any(|r| r.addr == req.addr && r.locators == req.locators));

        self.recent.push_front(req.clone());
        self.recent.truncate(32);

        self.inflight.insert(addr, req.clone());
        self.upstream
            .get_headers(req.addr, req.locators, req.timeout);
    }

    fn extend_chain(
        &mut self,
        headers: NonEmpty<BlockHeader>,
        clock: &impl Clock,
    ) -> Result<ImportResult, Error> {
        let mut import_result = ImportResult::TipUnchanged;

        for header in headers.into_iter() {
            match self.tree.extend_tip(header, clock) {
                Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                    debug_assert!(reverted.is_empty());

                    import_result = ImportResult::TipChanged(tip, height, vec![]);
                }
                Ok(ImportResult::TipUnchanged) => {
                    // We must have received headers from a different peer in the meantime,
                    // keep processing in case one of the headers extends our chain.
                    continue;
                }
                Err(err) => {
                    // TODO: Ask different peer.
                    // TODO: Transition peer.

                    return Err(err);
                }
            }
        }

        Ok(import_result)
    }

    /// Called when we received an `inv` message. This will happen if we are out of sync with a
    /// peer, and blocks are being announced. Otherwise, we expect to receive a `headers` message.
    pub fn received_inv<C>(&mut self, addr: PeerId, inv: Vec<Inventory>, clock: &C)
    where
        C: Clock,
    {
        let mut best_block = None;

        for i in &inv {
            if let Inventory::Block(hash) = i {
                // TODO: Update block availability for this peer.
                if !self.tree.is_known(hash) {
                    self.upstream.event(Event::BlockDiscovered(addr, *hash));
                    // The final block hash in the inventory should be the highest. Use
                    // that one for a `getheaders` call.
                    best_block = Some(hash);
                }
            }
        }

        if let Some(stop_hash) = best_block {
            let locators = (self.tree.locator_hashes(self.tree.height()), *stop_hash);
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

    /// Called when we received a timeout previously set on a peer.
    pub fn received_timeout(&mut self, src: Option<PeerId>, local_time: LocalTime) {
        if let Some(id) = src {
            let timeout = self.config.request_timeout;

            match self.inflight.remove(&id) {
                Some(GetHeaders {
                    locators,
                    sent_at,
                    on_timeout,
                    ..
                }) if local_time - sent_at >= timeout => {
                    match on_timeout {
                        OnTimeout::Disconnect => {
                            self.unregister(&id);
                            self.upstream.disconnect(id);
                        }
                        OnTimeout::Ignore => {
                            // It's likely that the peer just didn't have the requested header.
                        }
                    }
                    self.upstream.event(Event::TimedOut(id));

                    // TODO: We should check that we're still interested in these headers, or just
                    // call `sync` here, or simply not do anything.
                    if let Some(peer) = self.random_sync_candidate(&locators.0) {
                        let addr = peer.id;

                        self.request(addr, locators, local_time, timeout, OnTimeout::Ignore);
                    }
                }
                _ => {}
            }
        } else {
            self.idle(local_time);
        }
    }

    /// Get the best known height out of all our peers.
    pub fn best_height(&self) -> Height {
        self.peers
            .iter()
            .map(|(_, p)| p.height)
            .max()
            .unwrap_or_else(|| self.tree.height())
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
                    .event(Event::ReceivedInvalidHeaders(*from, err));

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
    fn stale_tip(&self, now: LocalTime) -> Option<LocalTime> {
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
        let (_, tip) = self.tree.tip();
        let time = LocalTime::from_block_time(tip.time);

        if time <= now - TIP_STALE_DURATION {
            return Some(time);
        }

        None
    }

    /// Are we currently syncing?
    fn is_syncing(&self) -> bool {
        !self.inflight.is_empty()
    }

    /// Register a new peer.
    fn register(&mut self, id: PeerId, height: Height, services: ServiceFlags, link: Link) {
        let last_active = None;
        let tip = BlockHash::default();

        self.peers.insert(
            id,
            PeerState {
                id,
                height,
                tip,
                services,
                link,
                last_active,
            },
        );
    }

    /// Unregister a peer.
    fn unregister(&mut self, id: &PeerId) {
        self.peers.remove(id);
    }

    /// Pick a random peer we could sync with using the given locators.
    fn random_sync_candidate(&self, locators: &[BlockHash]) -> Option<&PeerState> {
        let candidates = self
            .peers
            .values()
            .filter(|p| self.is_sync_candidate(p, locators));

        if let Some(peers) = NonEmpty::from_vec(candidates.collect()) {
            let ix = self.rng.usize(..peers.len());

            return peers.get(ix).cloned();
        }

        None
    }

    /// Check whether a peer can be synced with using the given locators.
    fn is_sync_candidate(&self, peer: &PeerState, locators: &[BlockHash]) -> bool {
        peer.is_outbound()
            && peer.height > self.tree.height()
            && !self.inflight.contains_key(&peer.id)
            && !self
                .recent
                .iter()
                .any(|r| r.addr == peer.id && r.locators.0 == locators)
    }

    /// Check whether or not we are in sync with the network.
    fn is_synced(&mut self, now: LocalTime) -> bool {
        if let Some(last_update) = self.stale_tip(now) {
            self.upstream.event(Event::StaleTipDetected(last_update));

            return false;
        }
        let height = self.tree.height();

        // Find the peer with the longest chain and compare our height to it.
        if let Some(peer_height) = self.peers.values().map(|p| p.height).max() {
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
    fn sync(&mut self, now: LocalTime) {
        if self.peers.is_empty() {
            return;
        }
        if self.is_synced(now) {
            let (tip, _) = self.tree.tip();
            let height = self.tree.height();

            self.upstream.event(Event::Synced(tip, height));

            // If we think we're in sync and we haven't asked other peers in a while, then
            // sample their headers just to make sure we're on the right chain.
            if self
                .last_peer_sample
                .map(|t| now.duration_since(t) >= PEER_SAMPLE_INTERVAL)
                .unwrap_or(true)
            {
                self.last_peer_sample = Some(now);

                self.sample_peers(now);
            }
            return;
        }
        // It looks like we're out of sync...

        let locators = (
            self.tree.locator_hashes(self.tree.height()),
            BlockHash::default(),
        );

        // If we're already fetching these headers, just wait.
        if self.syncing(&locators) {
            return;
        }

        if let Some(peer) = self.random_sync_candidate(&locators.0) {
            let timeout = self.config.request_timeout;
            let addr = peer.id;

            self.request(addr, locators, now, timeout, OnTimeout::Ignore);
            self.upstream.event(Event::Syncing(addr));
        } else {
            // TODO: No peer found to sync.. emit event.
        }
    }

    /// Broadcast our best block header to connected peers who don't have it.
    fn broadcast_tip(&mut self, hash: &BlockHash) {
        if let Some((height, best)) = self.tree.get_block(hash) {
            for (addr, peer) in &self.peers {
                // TODO: Don't broadcast to peer that is currently syncing?
                if peer.link == Link::Inbound && height > peer.height {
                    // addrs.push(*addr);
                    self.upstream.send_headers(*addr, vec![*best]);
                }
            }
        }
    }

    /// Ask all our outbound peers whether they have better block headers.
    fn sample_peers(&mut self, now: LocalTime) {
        let locators = self.tree.locator_hashes(self.tree.height());
        let addrs = self
            .peers
            .values()
            .filter(|p| self.is_sync_candidate(p, &locators))
            .map(|p| p.id)
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
