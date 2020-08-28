use std::collections::HashMap;

use nonempty::NonEmpty;

use bitcoin::consensus::params::Params;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::util::hash::BitcoinHash;

use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};

use super::{Link, Locators, PeerId};

/// How long to wait for a request, eg. `getheaders` to be fulfilled.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(8);
/// Maximum headers announced in a `headers` message, when unsolicited.
pub const MAX_HEADERS_ANNOUNCED: usize = 8;
/// How long before the tip of the chain is considered stale. This takes into account
/// that the block timestamp may have been set sometime in the future.
pub const TIP_STALE_DURATION: LocalDuration = LocalDuration::from_mins(60 * 2);

/// A timeout.
type Timeout = LocalDuration;

#[must_use]
#[derive(Debug)]
pub enum SyncResult<G, S> {
    GetHeaders(G),
    SendHeaders(S),
    Okay,
}

/// Synchronization states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Syncing {
    /// Not currently syncing. This is usually the starting and end state.
    Idle,
    /// Syncing. A `getheaders` message was sent and we are expecting a response.
    AwaitingHeaders(Locators),
}

impl Default for Syncing {
    fn default() -> Self {
        Self::Idle
    }
}

#[derive(Debug)]
struct PeerState {
    id: PeerId,
    height: Height,
    tip: BlockHash,
    services: ServiceFlags,
    link: Link,
    state: Syncing,
}

impl PeerState {
    fn is_candidate(&self) -> bool {
        matches!(self.state, Syncing::Idle)
    }

    fn is_outbound(&self) -> bool {
        self.link == Link::Outbound
    }

    fn transition(&mut self, state: Syncing) {
        if self.state == state {
            return;
        }
        self.state = state;
    }
}

#[derive(Debug)]
pub struct Config {
    pub max_message_headers: usize,
    pub request_timeout: LocalDuration,
    pub params: Params,
}

#[derive(Debug)]
pub struct SyncManager<T> {
    /// Block tree.
    pub tree: T,

    /// Sync-specific peer state.
    peers: HashMap<PeerId, PeerState>,
    /// Sync manager configuration.
    config: Config,
    /// Last time our tip was updated.
    last_tip_update: Option<LocalTime>,
    /// Random number generator.
    rng: fastrand::Rng,
    /// Output event queue.
    events: Vec<Event>,
}

#[derive(Debug)]
pub enum Event {
    ReceivedInvalidHeaders(PeerId, Error),
    ReceivedUnsolicitedHeaders(PeerId, usize),
    HeadersImported(ImportResult),
    BlockDiscovered(PeerId, BlockHash),
    Syncing(PeerId),
    Synced(BlockHash, Height),
}

#[must_use]
#[derive(Debug)]
pub struct GetHeaders {
    pub addr: PeerId,
    pub locators: Locators,
    pub timeout: LocalDuration,
}

impl GetHeaders {
    fn new(peer: &mut PeerState, locators: Locators, timeout: Timeout) -> Self {
        peer.transition(Syncing::AwaitingHeaders(locators.clone()));

        GetHeaders {
            addr: peer.id,
            locators,
            timeout,
        }
    }
}

#[must_use]
#[derive(Debug)]
pub struct SendHeaders {
    pub addrs: Vec<PeerId>,
    pub headers: Vec<BlockHeader>,
}

#[must_use]
#[derive(Debug)]
pub struct PeerTimeout;

impl<'a, T: 'a + BlockTree> SyncManager<T> {
    /// Create a new sync manager.
    pub fn new(tree: T, config: Config, rng: fastrand::Rng) -> Self {
        let peers = HashMap::new();
        let events = Vec::new();
        let last_tip_update = None;

        Self {
            tree,
            peers,
            config,
            last_tip_update,
            rng,
            events,
        }
    }

    /// Initialize the sync manager. Should only be called once.
    pub fn initialize(&mut self, time: LocalTime) -> impl Iterator<Item = GetHeaders> + 'a {
        self.tick(time)
    }

    /// Drain the event queue.
    pub fn events(&'a mut self) -> impl Iterator<Item = Event> + 'a {
        self.events.drain(..)
    }

    /// Called periodically.
    pub fn tick(&mut self, now: LocalTime) -> impl Iterator<Item = GetHeaders> + 'a {
        self.sync(now).into_iter()
    }

    /// Called when a new peer was negotiated.
    pub fn peer_negotiated(
        &mut self,
        id: PeerId,
        height: Height,
        tip: BlockHash,
        services: ServiceFlags,
        link: Link,
        clock: &impl Clock,
    ) -> Option<GetHeaders> {
        self.register(id, height, tip, services, link);
        self.sync(clock.local_time())
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.unregister(id);
    }

    /// Called when we received a `getheaders` message from a peer.
    pub fn received_getheaders(
        &self,
        addr: &PeerId,
        (locator_hashes, stop_hash): Locators,
        max: usize,
    ) -> Option<SendHeaders> {
        if locator_hashes.is_empty() || self.is_syncing() || max == 0 {
            return None;
        }
        let headers = self.get_headers(locator_hashes, stop_hash, max);

        if headers.is_empty() {
            return None;
        }

        Some(SendHeaders {
            addrs: vec![*addr],
            headers,
        })
    }

    /// Import blocks into our block tree.
    pub fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        blocks: I,
        context: &C,
    ) -> Result<(ImportResult, Option<SendHeaders>), Error> {
        match self.tree.import_blocks(blocks, context) {
            Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                let result = ImportResult::TipChanged(tip, height, reverted);

                self.emit(Event::HeadersImported(result.clone()));
                self.emit(Event::Synced(tip, height));

                Ok((result, self.broadcast_tip(&tip)))
            }
            Ok(result @ ImportResult::TipUnchanged) => {
                self.emit(Event::HeadersImported(result.clone()));
                Ok((result, None))
            }
            Err(err) => Err(err),
        }
    }

    /// Called when we receive headers from a peer.
    pub fn received_headers(
        &mut self,
        from: &PeerId,
        headers: NonEmpty<BlockHeader>,
        clock: &impl Clock,
    ) -> SyncResult<GetHeaders, SendHeaders> {
        let length = headers.len();
        let best = headers.last().bitcoin_hash();

        if self.tree.contains(&best) {
            // Ignore duplicate headers on the active chain.
            return SyncResult::Okay;
        }

        match &self.peers.get_mut(from).unwrap().state {
            // Requested headers. These should extend our main chain.
            // Check whether the start of the header chain matches one of the locators we
            // supplied to the peer. Otherwise, we consider them unsolicited.
            Syncing::AwaitingHeaders((locators, _))
                if headers.iter().any(|h| locators.contains(&h.prev_blockhash)) =>
            {
                let (mut tip, _) = self.tree.tip();
                let mut height = self.tree.height();

                for header in headers.into_iter() {
                    match self.tree.extend_tip(header, clock) {
                        Ok(ImportResult::TipChanged(t, h, _)) => {
                            tip = t;
                            height = h;
                        }
                        Ok(ImportResult::TipUnchanged) => {
                            // We must have received headers from a different peer in the meantime,
                            // keep processing in case one of the headers extends our chain.
                            continue;
                        }
                        Err(err) => {
                            self.record_misbehavior(from);
                            self.emit(Event::ReceivedInvalidHeaders(*from, err));

                            // TODO: Ask different peer.
                            // TODO: Transition peer.

                            return SyncResult::Okay;
                        }
                    }
                }

                let peer = self.peers.get_mut(from).unwrap();

                if height > peer.height {
                    peer.tip = tip;
                    peer.height = height;
                }

                // Keep track of when we last updated our tip. This is useful to check
                // whether our tip is stale.
                self.last_tip_update = Some(clock.local_time());

                let import_result = ImportResult::TipChanged(tip, height, vec![]);

                // If we received less than the maximum number of headers, we must be in sync.
                // Otherwise, ask for the next batch of headers.
                if length < self.config.max_message_headers {
                    // If these headers were unsolicited, we may already be ready/synced.
                    // Otherwise, we're finally in sync.
                    peer.transition(Syncing::Idle);
                    self.emit(Event::HeadersImported(import_result));
                    self.emit(Event::Synced(tip, height));

                    return self
                        .broadcast_tip(&tip)
                        .map(SyncResult::SendHeaders)
                        .unwrap_or(SyncResult::Okay);
                } else {
                    self.events.push(Event::HeadersImported(import_result));

                    // TODO: If we're already in the state of asking for this header, don't
                    // ask again.
                    let locators = (vec![tip], BlockHash::default());
                    let timeout = self.config.request_timeout;

                    return SyncResult::GetHeaders(GetHeaders::new(peer, locators, timeout));
                }
            }
            // Header announcement.
            _ if length <= MAX_HEADERS_ANNOUNCED => {
                let root = headers.first().bitcoin_hash();

                match self.tree.import_blocks(headers.into_iter(), clock) {
                    Ok(import_result @ ImportResult::TipUnchanged) => {
                        self.emit(Event::HeadersImported(import_result));

                        // Try to find a common ancestor that leads up to the first header in
                        // the list we received.
                        let locators = (self.tree.locator_hashes(self.tree.height()), root);

                        let peer = self.peers.get_mut(from).unwrap();
                        let timeout = self.config.request_timeout;

                        return SyncResult::GetHeaders(GetHeaders::new(peer, locators, timeout));
                    }
                    Ok(import_result) => {
                        self.emit(Event::HeadersImported(import_result));
                    }
                    Err(err) => {
                        self.record_misbehavior(from);
                        self.emit(Event::ReceivedInvalidHeaders(*from, err));
                    }
                }
            }
            _ => {
                // We've received a large number of unsolicited headers. This is more than the
                // typical headers sent during a header announcement, and we haven't asked
                // this peer for any headers. We choose to ignore it.
                self.emit(Event::ReceivedUnsolicitedHeaders(*from, length));
            }
        }

        SyncResult::Okay
    }

    /// Called when we received an `inv` message. This will happen if we are out of sync with a
    /// peer, and blocks are being announced. Otherwise, we expect to receive a `headers` message.
    pub fn received_inv(&mut self, addr: PeerId, inv: Vec<Inventory>) -> Option<GetHeaders> {
        let mut best_block = None;

        for i in &inv {
            if let Inventory::Block(hash) = i {
                // TODO: Update block availability for this peer.
                if !self.tree.is_known(hash) {
                    self.emit(Event::BlockDiscovered(addr, *hash));
                    // The final block hash in the inventory should be the highest. Use
                    // that one for a `getheaders` call.
                    best_block = Some(hash);
                }
            }
        }

        if let Some(stop_hash) = best_block {
            let locators = (self.tree.locator_hashes(self.tree.height()), *stop_hash);
            let timeout = self.config.request_timeout;
            let peer = self.peers.get_mut(&addr).unwrap();

            return Some(GetHeaders::new(peer, locators, timeout));
        }
        None
    }

    /// Called when we received a timeout previously set on a peer.
    pub fn received_timeout(&mut self, id: PeerId) -> (Option<PeerTimeout>, Option<GetHeaders>) {
        let peer = if let Some(peer) = self.peers.get_mut(&id) {
            peer
        } else {
            return (None, None);
        };

        match &peer.state {
            Syncing::AwaitingHeaders(locators) => {
                let locators = locators.clone();

                self.unregister(&id);

                // TODO: Get random peer. Random peer iterator?
                if let Some(peer) = self.peers.values_mut().find(|p| p.state == Syncing::Idle) {
                    let timeout = self.config.request_timeout;

                    return (
                        Some(PeerTimeout),
                        Some(GetHeaders::new(peer, locators, timeout)),
                    );
                }
                (Some(PeerTimeout), None)
            }
            _ => (None, None),
        }
    }

    ///////////////////////////////////////////////////////////////////////////

    fn record_misbehavior(&mut self, _peer: &PeerId) {
        todo!();
    }

    /// Check whether our current tip is stale.
    ///
    /// *Nb. This doesn't check whether we've already requested new blocks.*
    fn is_tip_stale(&self, now: LocalTime) -> bool {
        if let Some(last_update) = self.last_tip_update {
            if last_update
                < now - LocalDuration::from_secs(self.config.params.pow_target_spacing * 3)
            {
                return true;
            }
        }
        // If we don't have the time of the last update, it's probably because we
        // are fresh, or restarted our node. In that case we check the last block time
        // instead.
        let (_, tip) = self.tree.tip();
        let time = LocalTime::from_timestamp(tip.time);

        time <= now - TIP_STALE_DURATION
    }

    /// Are we currently syncing?
    fn is_syncing(&self) -> bool {
        self.peers
            .values()
            .any(|p| matches!(p.state, Syncing::AwaitingHeaders(_)))
    }

    fn emit(&mut self, event: Event) {
        self.events.push(event);
    }

    fn register(
        &mut self,
        id: PeerId,
        height: Height,
        tip: BlockHash,
        services: ServiceFlags,
        link: Link,
    ) {
        self.peers.insert(
            id,
            PeerState {
                id,
                height,
                tip,
                services,
                link,
                state: Syncing::default(),
            },
        );
    }

    fn unregister(&mut self, id: &PeerId) {
        self.peers.remove(id);
    }

    fn random_peer(&self) -> Option<&PeerState> {
        let height = self.tree.height();

        let candidates = self
            .peers
            .values()
            .filter(|p| p.is_candidate() && p.height > height);
        let prefered = candidates.clone().filter(|p| p.is_outbound());

        if let Some(peers) = NonEmpty::from_vec(prefered.collect())
            .or_else(|| NonEmpty::from_vec(candidates.collect()))
        {
            let ix = self.rng.usize(..peers.len());

            return peers.get(ix).cloned();
        }

        None
    }

    /// Check whether or not we are in sync with the network.
    /// TODO: Should return the minimum peer height, so that we can
    /// keep track of it in our state, while syncing to it.
    fn is_synced(&self, now: LocalTime) -> bool {
        if self.is_tip_stale(now) {
            return false;
        }
        let height = self.tree.height();

        // TODO: Check actual block hashes once we are caught up on height.
        if let Some(peer_height) = self.peers.values().map(|p| p.height).max() {
            return height >= peer_height;
        }

        false
    }

    /// Start syncing if we're out of sync.
    fn sync(&mut self, now: LocalTime) -> Option<GetHeaders> {
        if self.peers.is_empty() {
            return None;
        }
        if self.is_synced(now) {
            let (tip, _) = self.tree.tip();
            let height = self.tree.height();

            self.emit(Event::Synced(tip, height));

            return None;
        }
        self.force_sync()
    }

    /// Try to sync now without checking our peer state.
    fn force_sync(&mut self) -> Option<GetHeaders> {
        let locators = (
            self.tree.locator_hashes(self.tree.height()),
            BlockHash::default(),
        );

        // TODO: Factor this out when we have a `peermgr`.
        // TODO: Threshold should be a parameter.
        // TODO: Peer should be picked amongst lowest latency ones.

        if let Some(peer) = self.random_peer() {
            let timeout = self.config.request_timeout;
            let addr = peer.id;
            let peer = self.peers.get_mut(&addr).expect("the peer exists");
            let get_headers = GetHeaders::new(peer, locators, timeout);

            self.emit(Event::Syncing(addr));

            return Some(get_headers);
        }

        None
    }

    /// Broadcast our best block header to connected peers who don't have it.
    fn broadcast_tip(&self, hash: &BlockHash) -> Option<SendHeaders> {
        if let Some((height, best)) = self.tree.get_block(hash) {
            let mut addrs = Vec::new();

            for (addr, peer) in &self.peers {
                // TODO: Don't broadcast to peer that is currently syncing?
                if peer.link == Link::Inbound && height > peer.height {
                    addrs.push(*addr);
                    // TODO: Update peer inventory?
                }
            }
            return Some(SendHeaders {
                addrs,
                headers: vec![*best],
            });
        }
        None
    }

    fn get_headers(
        &self,
        locator_hashes: Vec<BlockHash>,
        // FIXME(syncmgr): Use this
        _stop_hash: BlockHash,
        max: usize,
    ) -> Vec<BlockHeader> {
        let tree = &self.tree;

        // Start from the highest locator hash that is on our active chain.
        // We don't respond with anything if none of the locators were found. Sorry!
        if let Some(hash) = locator_hashes.iter().find(|h| tree.contains(h)) {
            let (start_height, _) = self.tree.get_block(hash).unwrap();

            // TODO: Set this to highest locator hash. We can assume that the peer
            // is at this height if they know this hash.
            // TODO: If the height is higher than the previous peer height, also
            // set the peer tip.
            // peer.height = start_height;

            let start = start_height + 1;
            let end = Height::min(start + max as Height, tree.height() + 1);

            tree.range(start..end).collect()
        } else {
            vec![]
        }
    }

    /// Ask all our outbound peers whether they have better block headers.
    #[allow(dead_code)]
    fn sample_headers(&mut self) -> Vec<GetHeaders> {
        let height = self.tree.height();

        let mut messages = Vec::new();
        let locators = self.tree.locator_hashes(height);

        for peer in self.peers.values_mut() {
            if peer.is_candidate() && peer.is_outbound() {
                messages.push(GetHeaders::new(
                    peer,
                    (locators.clone(), BlockHash::default()),
                    self.config.request_timeout,
                ));
            }
        }
        messages
    }
}
