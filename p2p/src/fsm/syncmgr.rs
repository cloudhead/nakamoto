//!
//! Manages header synchronization with peers.
//!
use nakamoto_common::bitcoin::consensus::params::Params;
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use nakamoto_common::bitcoin_hashes::Hash;
use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{BlockReader, BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};
use nakamoto_common::collections::{AddressBook, HashMap};
use nakamoto_common::nonempty::NonEmpty;

use super::output::{Io, Outbox};
use super::Event;
use super::{DisconnectReason, Link, Locators, PeerId};

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
pub struct SyncManager<C> {
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
    /// State-machine output.
    outbox: Outbox,
    /// Clock.
    clock: C,
}

impl<C> Iterator for SyncManager<C> {
    type Item = Io;

    fn next(&mut self) -> Option<Self::Item> {
        self.outbox.next()
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

impl<C: Clock> SyncManager<C> {
    /// Create a new sync manager.
    pub fn new(config: Config, rng: fastrand::Rng, clock: C) -> Self {
        let peers = AddressBook::new(rng.clone());
        let last_tip_update = None;
        let last_peer_sample = None;
        let last_idle = None;
        let inflight = HashMap::with_hasher(rng.into());
        let outbox = Outbox::default();

        Self {
            peers,
            config,
            last_tip_update,
            last_peer_sample,
            last_idle,
            inflight,
            outbox,
            clock,
        }
    }

    /// Initialize the sync manager. Should only be called once.
    pub fn initialize<T: BlockReader>(&mut self, tree: &T) {
        self.idle(tree);
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
            self.outbox.set_timer(IDLE_TIMEOUT);
        }
    }

    /// Event received.
    pub fn received_event<T: BlockTree>(&mut self, event: Event, tree: &mut T) {
        match event {
            Event::PeerNegotiated {
                addr,
                link,
                services,
                height,
                ..
            } => {
                self.peer_negotiated(addr, height, services, link, tree);
            }
            Event::PeerDisconnected { addr, .. } => {
                self.unregister(&addr);
            }
            Event::MessageReceived { from, message } => match message.as_ref() {
                NetworkMessage::Headers(headers) => {
                    self.received_headers(&from, headers, tree);
                }
                NetworkMessage::SendHeaders => {
                    // We adhere to `sendheaders` by default.
                }
                NetworkMessage::GetHeaders(GetHeadersMessage {
                    locator_hashes,
                    stop_hash,
                    ..
                }) => {
                    self.received_getheaders(&from, (locator_hashes.to_vec(), *stop_hash), tree);
                }
                NetworkMessage::Inv(inventory) => {
                    self.received_inv(from, inventory, tree);
                    // TODO: invmgr: Update block availability for this peer.
                }
                _ => {}
            },
            _ => {}
        }
    }

    /// Called when a new peer was negotiated.
    fn peer_negotiated<T: BlockReader>(
        &mut self,
        addr: PeerId,
        height: Height,
        services: ServiceFlags,
        link: Link,
        tree: &T,
    ) {
        if link.is_outbound() && !services.has(REQUIRED_SERVICES) {
            return;
        }

        if height > self.best_height().unwrap_or_else(|| tree.height()) {
            self.outbox.event(Event::PeerHeightUpdated { height });
        }

        self.register(
            addr,
            height,
            // We prefer if the peer doesn't have compact filters support,
            // leaving those peers free for fetching filters.
            !services.has(ServiceFlags::COMPACT_FILTERS),
            link,
        );
        self.sync(tree);
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
        self.outbox.headers(*addr, headers);
    }

    /// Import blocks into our block tree.
    pub fn import_blocks<T: BlockTree, I: Iterator<Item = BlockHeader>>(
        &mut self,
        blocks: I,
        tree: &mut T,
    ) -> Result<ImportResult, Error> {
        let result = tree.import_blocks(blocks, &self.clock);

        if let Ok(
            result @ ImportResult::TipChanged {
                hash,
                reverted,
                connected,
                ..
            },
        ) = &result
        {
            let reorg = !reverted.is_empty();

            for (height, header) in reverted.iter().cloned() {
                self.outbox
                    .event(Event::BlockDisconnected { height, header });
            }
            for (height, header) in connected.iter().cloned() {
                self.outbox.event(Event::BlockConnected { height, header });
            }
            self.outbox.event(Event::BlockHeadersImported {
                reorg,
                result: result.clone(),
            });
            self.broadcast_tip(hash, tree);
        }
        result
    }

    /// Called when we receive headers from a peer.
    pub fn received_headers<T: BlockTree>(
        &mut self,
        from: &PeerId,
        headers: &[BlockHeader],
        tree: &mut T,
    ) {
        let request = self.inflight.remove(from);
        let Some(headers) = NonEmpty::from_vec(headers.to_vec()) else {
            return;
        };
        let length = headers.len();

        if length > MAX_MESSAGE_HEADERS {
            log::debug!("Received more than maximum headers allowed from {from}");
            self.record_misbehavior(from, "invalid `headers` message");

            return;
        }
        // When unsolicited, we don't want to process too many headers in case of a DoS.
        if length > MAX_UNSOLICITED_HEADERS && request.is_none() {
            log::debug!("Received {} unsolicited headers from {}", length, from);

            return;
        }

        if let Some(peer) = self.peers.get_mut(from) {
            peer.last_active = Some(self.clock.local_time());
        } else {
            return;
        }
        log::debug!(target: "p2p", "Received {} block header(s) from {}", length, from);

        let root = headers.first().block_hash();
        let best = headers.last().block_hash();

        if tree.contains(&best) {
            return;
        }

        match self.import_blocks(headers.into_iter(), tree) {
            Ok(ImportResult::TipUnchanged) => {
                // Try to find a common ancestor that leads up to the first header in
                // the list we received.
                let locators = (tree.locator_hashes(tree.height()), root);
                let timeout = self.config.request_timeout;

                self.request(*from, locators, timeout, OnTimeout::Ignore);
            }
            Ok(ImportResult::TipChanged { hash, height, .. }) => {
                // Update peer height.
                if let Some(peer) = self.peers.get_mut(from) {
                    if height > peer.height {
                        peer.tip = hash;
                        peer.height = height;
                    }
                }
                // Keep track of when we last updated our tip. This is useful to check
                // whether our tip is stale.
                self.last_tip_update = Some(self.clock.local_time());

                // If we received less than the maximum number of headers, we must be in sync.
                // Otherwise, ask for the next batch of headers.
                if length < MAX_MESSAGE_HEADERS {
                    // If these headers were unsolicited, we may already be ready/synced.
                    // Otherwise, we're finally in sync.
                    self.broadcast_tip(&hash, tree);
                    self.sync(tree);
                } else {
                    let locators = (vec![hash], BlockHash::all_zeros());
                    let timeout = self.config.request_timeout;

                    self.request(*from, locators, timeout, OnTimeout::Disconnect);
                }
            }
            // If this is an error with the underlying store, we have to propagate
            // this up, because we can't handle it here.
            Err(Error::Store(e)) => self.outbox.error(e),
            // If we got a bad block from the peer, we can handle it here.
            Err(
                e @ Error::InvalidBlockPoW
                | e @ Error::InvalidBlockTarget(_, _)
                | e @ Error::InvalidBlockHash(_, _)
                | e @ Error::InvalidBlockHeight(_)
                | e @ Error::InvalidBlockTime(_, _),
            ) => {
                log::warn!(target: "p2p", "Received invalid headers from {from}: {e}");

                self.record_misbehavior(from, "invalid headers in `headers` message");
            }
            // Harmless errors can be ignored.
            Err(Error::DuplicateBlock(_) | Error::BlockMissing(_)) => {}
            // TODO: This will be removed.
            Err(Error::BlockImportAborted(_, _, _)) => {}
            // These shouldn't happen here.
            // TODO: Perhaps there's a better way to have this error not show up here.
            Err(Error::Interrupted | Error::GenesisMismatch) => {}
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
            self.outbox.get_headers(addr, req.locators);
            self.outbox.set_timer(timeout);
        }
    }

    /// Called when we received an `inv` message. This will happen if we are out of sync with a
    /// peer, and blocks are being announced. Otherwise, we expect to receive a `headers` message.
    pub fn received_inv<T: BlockReader>(&mut self, addr: PeerId, inv: &[Inventory], tree: &T) {
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

        for i in inv {
            if let Inventory::Block(hash) = i {
                peer.tip = *hash;

                // "Headers-first is the primary method of announcement on the network. If a node
                // fell back to sending blocks by inv, it's probably for a re-org. The final block
                // hash provided should be the highest."

                if !tree.is_known(hash) {
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
                    self.outbox
                        .disconnect(peer, DisconnectReason::PeerTimeout("getheaders"));
                    sync = true;
                }
                OnTimeout::Retry(n) => {
                    if let Some((addr, _)) = self.peers.sample_with(|a, p| {
                        // TODO: After the first retry, it won't be a request candidate anymore,
                        // since it will have `last_asked` set?
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

    fn record_misbehavior(&mut self, addr: &PeerId, reason: &'static str) {
        self.outbox.event(Event::PeerMisbehaved {
            addr: *addr,
            reason,
        });
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
    fn register(&mut self, addr: PeerId, height: Height, preferred: bool, link: Link) {
        let last_active = None;
        let last_asked = None;
        let tip = BlockHash::all_zeros();

        self.peers.insert(
            addr,
            Peer {
                height,
                tip,
                link,
                preferred,
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

    /// Select a random preferred peer.
    fn preferred_peer<T: BlockReader>(&self, locators: &Locators, tree: &T) -> Option<PeerId> {
        let peers: Vec<_> = self.peers.shuffled().collect();
        let height = tree.height();
        let locators = &locators.0;

        peers
            .iter()
            .filter(|(a, p)| self.is_request_candidate(a, p, locators))
            .find(|(_, p)| p.preferred && p.height > height)
            .or_else(|| peers.iter().find(|(_, p)| p.preferred))
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
    fn is_synced<T: BlockReader>(&mut self, tree: &T) -> bool {
        if self.stale_tip(tree).is_some() {
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
            self.outbox
                .event(Event::BlockHeadersSynced { hash: tip, height });

            return false;
        }

        // ... It looks like we're out of sync ...

        let locators = (tree.locator_hashes(tree.height()), BlockHash::all_zeros());

        // If we're already fetching these headers, just wait.
        if self.syncing(&locators) {
            return false;
        }

        if let Some(addr) = self.preferred_peer(&locators, tree) {
            let timeout = self.config.request_timeout;
            let current = tree.height();
            let best = self.best_height().unwrap_or(current);

            if best > current {
                self.request(addr, locators, timeout, OnTimeout::Ignore);
                return true;
            }
        }
        // TODO: No peer found to sync.. emit event.
        false
    }

    /// Broadcast our best block header to connected peers who don't have it.
    fn broadcast_tip<T: BlockReader>(&mut self, hash: &BlockHash, tree: &T) {
        if let Some((height, best)) = tree.get_block(hash) {
            for (addr, peer) in &*self.peers {
                // TODO: Don't broadcast to peer that is currently syncing?
                if peer.link == Link::Inbound && height > peer.height {
                    self.outbox.headers(*addr, vec![*best]);
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
                (locators.clone(), BlockHash::all_zeros()),
                self.config.request_timeout,
                OnTimeout::Ignore,
            );
        }
    }
}
