//!
//! Manages header synchronization with peers.
//!
use nakamoto_common::bitcoin::consensus::params::Params;
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::network::message_blockdata::Inventory;

use nakamoto_common::block::store;
use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{BlockReader, BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};
use nakamoto_common::collections::{AddressBook, HashMap};
use nakamoto_common::nonempty::NonEmpty;

use super::output::{Disconnect, Wakeup};
use super::{DisconnectReason, Link, Locators, PeerId, Socket};

/// How long to wait for a request, eg. `getheaders` to be fulfilled.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);
/// How long before the tip of the chain is considered stale. This takes into account
/// that the block timestamp may have been set sometime in the future.
pub const TIP_STALE_DURATION: LocalDuration = LocalDuration::from_mins(60 * 2);
/// Maximum number of headers sent in a `headers` message.
pub const MAX_MESSAGE_HEADERS: usize = 2000;
/// Maximum number of inventories sent in an `inv` message.
pub const MAX_MESSAGE_INVS: usize = 50000;
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
    fn get_headers(&mut self, addr: PeerId, locators: Locators);
    /// Send headers to a peer.
    fn send_headers(&mut self, addr: PeerId, headers: Vec<BlockHeader>);
    /// Send initial post-negotiation messages, eg. `sendheaders`.
    fn negotiate(&mut self, addr: PeerId);
    /// Emit a sync-related event.
    fn event(&self, event: Event);
}

/// What to do if a timeout for a peer is received.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum OnTimeout {
    /// Disconnect peer on timeout.
    Disconnect,
    /// Do nothing on timeout.
    Ignore,
    /// Retry with a different peer on timeout.
    Retry(usize),
}

/// State of a sync peer.
#[derive(Debug)]
struct Peer {
    height: Height,
    preferred: bool,
    tip: BlockHash,
    link: Link,
    last_active: Option<LocalTime>,
    last_asked: Option<Locators>,

    _socket: Socket,
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
pub struct SyncManager<U, C> {
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
    /// In-flight requests to peers.
    inflight: HashMap<PeerId, GetHeaders>,
    /// Upstream protocol channel.
    upstream: U,
    /// Clock.
    clock: C,
}

/// An event emitted by the sync manager.
#[derive(Debug, Clone)]
pub enum Event {
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
        /// Block header.
        header: BlockHeader,
    },
    /// A new block was discovered via a peer.
    BlockDiscovered(PeerId, BlockHash),
    /// Syncing headers.
    Syncing {
        /// Current block header height.
        current: Height,
        /// Best known block header height.
        best: Height,
    },
    /// Synced up to the specified hash and height.
    Synced(BlockHash, Height),
    /// Potential stale tip detected on the active chain.
    StaleTip(LocalTime),
    /// Peer misbehaved.
    PeerMisbehaved(PeerId),
    /// Peer height updated.
    PeerHeightUpdated {
        /// Best height known.
        height: Height,
    },
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::PeerMisbehaved(addr) => {
                write!(fmt, "{}: Peer misbehaved", addr)
            }
            Event::PeerHeightUpdated { height } => {
                write!(fmt, "Peer height updated to {}", height)
            }
            Event::Synced(hash, height) => {
                write!(
                    fmt,
                    "Headers synced up to height {} with hash {}",
                    height, hash
                )
            }
            Event::Syncing { current, best } => write!(fmt, "Syncing headers {}/{}", current, best),
            Event::BlockConnected { height, header } => {
                write!(
                    fmt,
                    "Block {} connected at height {}",
                    header.block_hash(),
                    height
                )
            }
            Event::BlockDisconnected { height, header } => {
                write!(
                    fmt,
                    "Block {} disconnected at height {}",
                    header.block_hash(),
                    height
                )
            }
            Event::BlockDiscovered(from, hash) => {
                write!(fmt, "{}: Discovered new block: {}", from, &hash)
            }
            Event::StaleTip(last_update) => {
                write!(
                    fmt,
                    "Potential stale tip detected (last update was {})",
                    last_update
                )
            }
        }
    }
}

/// A `getheaders` request sent to a peer.
#[derive(Clone, Debug, PartialEq, Eq)]
struct GetHeaders {
    /// Locators hashes.
    locators: Locators,
    /// Time at which the request was sent.
    sent_at: LocalTime,
    /// What to do if this request times out.
    on_timeout: OnTimeout,
}

impl<U: Wakeup + Disconnect + SyncHeaders, C: Clock> SyncManager<U, C> {
    /// Create a new sync manager.
    pub fn new(config: Config, rng: fastrand::Rng, upstream: U, clock: C) -> Self {
        let peers = AddressBook::new(rng.clone());
        let last_tip_update = None;
        let last_peer_sample = None;
        let last_idle = None;
        let inflight = HashMap::with_hasher(rng.into());

        Self {
            peers,
            config,
            last_tip_update,
            last_peer_sample,
            last_idle,
            inflight,
            upstream,
            clock,
        }
    }

    /// Initialize the sync manager. Should only be called once.
    pub fn initialize<T: BlockReader>(&mut self, tree: &T) {
        // TODO: `tip` should return the height.
        let (hash, _) = tree.tip();
        let height = tree.height();

        self.idle(tree);
        self.upstream.event(Event::Synced(hash, height));
    }

    /// Called periodically.
    pub fn idle<T: BlockReader>(&mut self, tree: &T) {
        let now = self.clock.local_time();
        // Nb. The idle timeout is very long: as long as the block interval.
        // This shouldn't be a problem, as the sync manager can make progress without it.
        if now - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            if !self.sync(tree) {
                self.sample_peers(tree);
            }
            self.last_idle = Some(now);
            self.upstream.wakeup(IDLE_TIMEOUT);
        }
    }

    /// Called when a new peer was negotiated.
    pub fn peer_negotiated<T: BlockReader>(
        &mut self,
        socket: Socket,
        height: Height,
        services: ServiceFlags,
        preferred: bool,
        link: Link,
        tree: &T,
    ) {
        if link.is_outbound() && !services.has(REQUIRED_SERVICES) {
            return;
        }

        if height > self.best_height().unwrap_or_else(|| tree.height()) {
            self.upstream.event(Event::PeerHeightUpdated { height });
        }

        self.upstream.negotiate(socket.addr);
        self.register(socket, height, preferred, link);
        self.sync(tree);
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.unregister(id);
    }

    /// Called when we received a `getheaders` message from a peer.
    pub fn received_getheaders<T: BlockReader>(
        &mut self,
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
    pub fn import_blocks<T: BlockTree, I: Iterator<Item = BlockHeader>>(
        &mut self,
        blocks: I,
        tree: &mut T,
    ) -> Result<ImportResult, Error> {
        match tree.import_blocks(blocks, &self.clock) {
            Ok(ImportResult::TipChanged(header, tip, height, reverted, connected)) => {
                let result = ImportResult::TipChanged(
                    header,
                    tip,
                    height,
                    reverted.clone(),
                    connected.clone(),
                );

                for (height, header) in reverted {
                    self.upstream
                        .event(Event::BlockDisconnected { height, header });
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
            log::debug!("Received more than maximum headers allowed from {}", from);

            self.record_misbehavior(from);
            self.upstream
                .disconnect(*from, DisconnectReason::PeerMisbehaving("too many headers"));

            return Ok(ImportResult::TipUnchanged);
        }
        // When unsolicited, we don't want to process too many headers in case of a DoS.
        if length > MAX_UNSOLICITED_HEADERS && request.is_none() {
            log::debug!("Received {} unsolicited headers from {}", length, from);

            return Ok(ImportResult::TipUnchanged);
        }

        if let Some(peer) = self.peers.get_mut(from) {
            peer.last_active = Some(clock.local_time());
        } else {
            return Ok(ImportResult::TipUnchanged);
        }
        log::debug!("[sync] Received {} block header(s) from {}", length, from);

        let root = headers.first().block_hash();
        let best = headers.last().block_hash();

        if tree.contains(&best) {
            return Ok(ImportResult::TipUnchanged);
        }

        match self.import_blocks(headers.into_iter(), tree) {
            Ok(ImportResult::TipUnchanged) => {
                // Try to find a common ancestor that leads up to the first header in
                // the list we received.
                let locators = (tree.locator_hashes(tree.height()), root);
                let timeout = self.config.request_timeout;

                self.request(*from, locators, timeout, OnTimeout::Ignore);

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
                    self.sync(tree);
                } else {
                    let locators = (vec![tip], BlockHash::default());
                    let timeout = self.config.request_timeout;

                    self.request(*from, locators, timeout, OnTimeout::Disconnect);
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
        timeout: LocalDuration,
        on_timeout: OnTimeout,
    ) {
        // Don't request more than once from the same peer.
        if self.inflight.contains_key(&addr) {
            return;
        }
        if let Some(peer) = self.peers.get_mut(&addr) {
            debug_assert!(peer.last_asked.as_ref() != Some(&locators));

            peer.last_asked = Some(locators.clone());

            let sent_at = self.clock.local_time();
            let req = GetHeaders {
                locators,
                sent_at,
                on_timeout,
            };

            self.inflight.insert(addr, req.clone());
            self.upstream.get_headers(addr, req.locators);
            self.upstream.wakeup(timeout);
        }
    }

    /// Called when we received an `inv` message. This will happen if we are out of sync with a
    /// peer, and blocks are being announced. Otherwise, we expect to receive a `headers` message.
    pub fn received_inv<T: BlockReader>(&mut self, addr: PeerId, inv: Vec<Inventory>, tree: &T) {
        // Don't try to fetch headers from `inv` message while syncing. It's not helpful.
        if self.is_syncing() {
            return;
        }
        // Ignore and disconnect peers misbehaving.
        if inv.len() > MAX_MESSAGE_INVS {
            return;
        }

        let peer = if let Some(peer) = self.peers.get_mut(&addr) {
            peer
        } else {
            return;
        };
        let mut best_block = None;

        for i in &inv {
            if let Inventory::Block(hash) = i {
                peer.tip = *hash;

                // "Headers-first is the primary method of announcement on the network. If a node
                // fell back to sending blocks by inv, it's probably for a re-org. The final block
                // hash provided should be the highest."

                if !tree.is_known(hash) {
                    self.upstream.event(Event::BlockDiscovered(addr, *hash));
                    best_block = Some(hash);
                }
            }
        }

        if let Some(stop_hash) = best_block {
            let locators = (tree.locator_hashes(tree.height()), *stop_hash);
            let timeout = self.config.request_timeout;

            // Try to find headers leading up to the `inv` entry.

            self.request(addr, locators, timeout, OnTimeout::Retry(3));
        }
    }

    /// Called when we received a tick.
    pub fn received_wake<T: BlockReader>(&mut self, tree: &T) {
        let local_time = self.clock.local_time();
        let timeout = self.config.request_timeout;
        let timed_out = self
            .inflight
            .iter()
            .filter_map(|(peer, req)| {
                if local_time - req.sent_at >= timeout {
                    Some((*peer, req.on_timeout, req.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut sync = false;
        for (peer, on_timeout, req) in timed_out {
            self.inflight.remove(&peer);

            match on_timeout {
                OnTimeout::Ignore => {
                    // It's likely that the peer just didn't have the requested header.
                }
                OnTimeout::Retry(0) | OnTimeout::Disconnect => {
                    self.upstream
                        .disconnect(peer, DisconnectReason::PeerTimeout("getheaders"));
                    sync = true;
                }
                OnTimeout::Retry(n) => {
                    if let Some((addr, _)) = self.peers.sample_with(|a, p| {
                        *a != peer && self.is_request_candidate(a, p, &req.locators.0)
                    }) {
                        let addr = *addr;
                        self.request(addr, req.locators, timeout, OnTimeout::Retry(n - 1));
                    }
                }
            }
        }

        // If some of the requests timed out, force a sync, otherwise just idle.
        if sync {
            self.sync(tree);
        } else {
            self.idle(tree);
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
                log::debug!("{}: Received invalid headers: {}", from, err);

                self.record_misbehavior(from);
                self.upstream
                    .disconnect(*from, DisconnectReason::PeerMisbehaving("invalid headers"));

                Ok(())
            }

            // Harmless errors can be ignored.
            Error::DuplicateBlock(_) | Error::BlockMissing(_) => Ok(()),

            // TODO: This will be removed.
            Error::BlockImportAborted(_, _, _) => Ok(()),
        }
    }

    fn record_misbehavior(&mut self, peer: &PeerId) {
        self.upstream.event(Event::PeerMisbehaved(*peer));
    }

    /// Check whether our current tip is stale.
    ///
    /// *Nb. This doesn't check whether we've already requested new blocks.*
    fn stale_tip<T: BlockReader>(&self, tree: &T) -> Option<LocalTime> {
        let now = self.clock.local_time();

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
    fn register(&mut self, socket: Socket, height: Height, preferred: bool, link: Link) {
        let last_active = None;
        let last_asked = None;
        let tip = BlockHash::default();

        self.peers.insert(
            socket.addr,
            Peer {
                height,
                tip,
                link,
                preferred,
                last_active,
                last_asked,
                _socket: socket,
            },
        );
    }

    /// Unregister a peer.
    fn unregister(&mut self, id: &PeerId) {
        self.inflight.remove(id);
        self.peers.remove(id);
    }

    /// Select a random preferred peer.
    fn preferred_peer<T: BlockReader>(&self, locators: &Locators, tree: &T) -> Option<PeerId> {
        let peers: Vec<_> = self.peers.shuffled().collect();
        let height = tree.height();
        let locators = &locators.0;

        peers
            .iter()
            .find(|(a, p)| {
                p.preferred && p.height > height && self.is_request_candidate(a, p, locators)
            })
            .or_else(|| {
                peers
                    .iter()
                    .find(|(a, p)| p.preferred && self.is_request_candidate(a, p, locators))
            })
            .or_else(|| {
                peers
                    .iter()
                    .find(|(a, p)| self.is_request_candidate(a, p, locators))
            })
            .map(|(a, _)| **a)
    }

    /// Check whether a peer is a good request candidate for the given locators.
    /// This function ensures that we don't ask the same peer twice for the same locators.
    fn is_request_candidate(&self, addr: &PeerId, peer: &Peer, locators: &[BlockHash]) -> bool {
        !self.inflight.contains_key(addr)
            && peer.link.is_outbound()
            && peer.last_asked.as_ref().map_or(true, |l| l.0 != locators)
    }

    /// Check whether or not we are in sync with the network.
    fn is_synced<T: BlockReader>(&self, tree: &T) -> bool {
        if let Some(last_update) = self.stale_tip(tree) {
            self.upstream.event(Event::StaleTip(last_update));

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
    fn sync<T: BlockReader>(&mut self, tree: &T) -> bool {
        if self.peers.is_empty() {
            return false;
        }
        if self.is_synced(tree) {
            let (tip, _) = tree.tip();
            let height = tree.height();

            // TODO: This event can fire multiple times if `sync` is called while we're already
            // in sync.
            self.upstream.event(Event::Synced(tip, height));

            return false;
        }

        // ... It looks like we're out of sync ...

        let locators = (tree.locator_hashes(tree.height()), BlockHash::default());

        // If we're already fetching these headers, just wait.
        if self.syncing(&locators) {
            return false;
        }

        if let Some(addr) = self.preferred_peer(&locators, tree) {
            let timeout = self.config.request_timeout;
            let current = tree.height();
            let best = self.best_height().unwrap_or(current);

            self.request(addr, locators, timeout, OnTimeout::Ignore);
            self.upstream.event(Event::Syncing { current, best });

            true
        } else {
            // TODO: No peer found to sync.. emit event.
            false
        }
    }

    /// Broadcast our best block header to connected peers who don't have it.
    fn broadcast_tip<T: BlockReader>(&mut self, hash: &BlockHash, tree: &T) {
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
    fn sample_peers<T: BlockReader>(&mut self, tree: &T) {
        let now = self.clock.local_time();

        if now - self.last_peer_sample.unwrap_or_default() < PEER_SAMPLE_INTERVAL {
            return;
        }
        if self.stale_tip(tree).is_none() {
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
                self.config.request_timeout,
                OnTimeout::Ignore,
            );
        }
    }
}
