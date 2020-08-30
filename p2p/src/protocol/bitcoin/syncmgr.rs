use std::collections::HashMap;

use nonempty::NonEmpty;

use bitcoin::consensus::params::Params;
use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::util::hash::BitcoinHash;

use nakamoto_common::block::time::{Clock, LocalDuration, LocalTime};
use nakamoto_common::block::tree::{BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};

use super::{Link, Locators, PeerId, Timeout};

pub use result::Message;

/// How long to wait for a request, eg. `getheaders` to be fulfilled.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(30);
/// Maximum headers announced in a `headers` message, when unsolicited.
pub const MAX_HEADERS_ANNOUNCED: usize = 8;
/// How long before the tip of the chain is considered stale. This takes into account
/// that the block timestamp may have been set sometime in the future.
pub const TIP_STALE_DURATION: LocalDuration = LocalDuration::from_mins(60 * 2);
/// How long to wait between checks for longer chains from peers.
pub const PEER_SAMPLE_INTERVAL: LocalDuration = LocalDuration::from_mins(60);

mod result {
    use super::{GetHeaders, Request, SendHeaders};

    #[must_use]
    #[derive(Debug)]
    pub enum Message {
        GetHeaders(GetHeaders),
        SendHeaders(SendHeaders),
    }

    impl From<GetHeaders> for Message {
        fn from(other: GetHeaders) -> Self {
            Self::GetHeaders(other)
        }
    }

    impl From<SendHeaders> for Message {
        fn from(other: SendHeaders) -> Self {
            Self::SendHeaders(other)
        }
    }

    impl From<Request> for Message {
        fn from(other: Request) -> Self {
            Self::GetHeaders(GetHeaders::from(vec![other]))
        }
    }

    pub fn empty() -> Vec<Message> {
        vec![]
    }

    pub fn once(item: impl Into<Message>) -> Vec<Message> {
        vec![item.into()]
    }
}

/// What to do if a timeout for a peer is received.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum OnTimeout {
    Disconnect,
    Ignore,
}

/// Synchronization states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Syncing {
    /// Not currently syncing. This is usually the starting and end state.
    Idle,
    /// Syncing. A `getheaders` message was sent and we are expecting a response.
    AwaitingHeaders(Locators, LocalTime, OnTimeout),
    /// The peer didn't respond to the last request.
    NotResponding,
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
    fn is_idle(&self) -> bool {
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
    /// Last time we sampled our peers for their active chain.
    last_peer_sample: Option<LocalTime>,
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
    TimedOut(PeerId),
}

#[must_use]
#[derive(Debug)]
pub struct GetHeaders(std::vec::IntoIter<Request>);

#[derive(Debug)]
pub struct Request {
    pub addr: PeerId,
    pub locators: Locators,
    pub timeout: LocalDuration,
}

impl Request {
    fn new(
        peer: &mut PeerState,
        locators: Locators,
        local_time: LocalTime,
        timeout: Timeout,
        on_timeout: OnTimeout,
    ) -> Self {
        peer.transition(Syncing::AwaitingHeaders(
            locators.clone(),
            local_time,
            on_timeout,
        ));

        Request {
            addr: peer.id,
            locators,
            timeout,
        }
    }
}

impl GetHeaders {
    fn new(
        peer: &mut PeerState,
        locators: Locators,
        local_time: LocalTime,
        timeout: Timeout,
        on_timeout: OnTimeout,
    ) -> Self {
        peer.transition(Syncing::AwaitingHeaders(
            locators.clone(),
            local_time,
            on_timeout,
        ));

        GetHeaders(
            vec![Request {
                addr: peer.id,
                locators,
                timeout,
            }]
            .into_iter(),
        )
    }

    fn empty() -> Self {
        Self(vec![].into_iter())
    }
}

impl Iterator for GetHeaders {
    type Item = Request;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl From<Vec<Request>> for GetHeaders {
    fn from(other: Vec<Request>) -> Self {
        Self(other.into_iter())
    }
}

#[must_use]
#[derive(Debug)]
pub struct SendHeaders {
    pub addrs: Vec<PeerId>,
    pub headers: Vec<BlockHeader>,
}

impl<'a, T: 'a + BlockTree> SyncManager<T> {
    /// Create a new sync manager.
    pub fn new(tree: T, config: Config, rng: fastrand::Rng) -> Self {
        let peers = HashMap::new();
        let events = Vec::new();
        let last_tip_update = None;
        let last_peer_sample = None;

        Self {
            tree,
            peers,
            config,
            last_tip_update,
            last_peer_sample,
            rng,
            events,
        }
    }

    /// Initialize the sync manager. Should only be called once.
    pub fn initialize(&mut self, time: LocalTime) -> GetHeaders {
        self.tick(time)
    }

    /// Drain the event queue.
    pub fn events(&'a mut self) -> impl Iterator<Item = Event> + 'a {
        self.events.drain(..)
    }

    /// Called periodically.
    pub fn tick(&mut self, now: LocalTime) -> GetHeaders {
        for peer in self
            .peers
            .values_mut()
            .filter(|p| matches!(p.state, Syncing::NotResponding))
        {
            peer.transition(Syncing::Idle);
        }
        self.sync(now)
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
    ) -> GetHeaders {
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
        if self.is_syncing() || max == 0 {
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
    ) -> Result<(ImportResult, Vec<SendHeaders>), Error> {
        match self.tree.import_blocks(blocks, context) {
            Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                let result = ImportResult::TipChanged(tip, height, reverted);

                self.emit(Event::HeadersImported(result.clone()));
                self.emit(Event::Synced(tip, height));

                Ok((result, self.broadcast_tip(&tip)))
            }
            Ok(result @ ImportResult::TipUnchanged) => {
                self.emit(Event::HeadersImported(result.clone()));
                Ok((result, vec![]))
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
    ) -> Vec<result::Message> {
        let length = headers.len();
        let best = headers.last().bitcoin_hash();
        let peer = self.peers.get_mut(from).unwrap();

        if self.tree.contains(&best) {
            peer.transition(Syncing::Idle);
            return result::empty(); // Ignore duplicate headers on the active chain.
        }

        match &peer.state {
            // Requested headers. These should extend our main chain.
            // Check whether the start of the header chain matches one of the locators we
            // supplied to the peer. Otherwise, we consider them unsolicited.
            Syncing::AwaitingHeaders((locators, _), _, _)
                if headers.iter().any(|h| locators.contains(&h.prev_blockhash)) =>
            {
                let result = self.extend_chain(headers, clock);

                if let Ok(ref imported) = result {
                    self.emit(Event::HeadersImported(imported.clone()));
                }

                match result {
                    Ok(ImportResult::TipUnchanged) => {
                        self.peers.get_mut(from).unwrap().transition(Syncing::Idle);
                    }
                    Ok(ImportResult::TipChanged(tip, height, _)) => {
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
                            peer.transition(Syncing::Idle);

                            return self
                                .broadcast_tip(&tip)
                                .into_iter()
                                .map(Message::from)
                                .chain(self.sync(clock.local_time()).map(Message::from))
                                .collect();
                        } else {
                            // TODO: If we're already in the state of asking for this header, don't
                            // ask again.
                            // TODO: Should we use stop-hash for the single locator?
                            let locators = (vec![tip], BlockHash::default());
                            let timeout = self.config.request_timeout;

                            return result::once(GetHeaders::new(
                                peer,
                                locators,
                                clock.local_time(),
                                timeout,
                                OnTimeout::Disconnect,
                            ));
                        }
                    }
                    Err(err) => {
                        self.record_misbehavior(from);
                        self.emit(Event::ReceivedInvalidHeaders(*from, err));

                        return result::empty();
                    }
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

                        return result::once(GetHeaders::new(
                            peer,
                            locators,
                            clock.local_time(),
                            timeout,
                            OnTimeout::Ignore,
                        ));
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

        result::empty()
    }

    fn extend_chain(
        &mut self,
        headers: NonEmpty<BlockHeader>,
        clock: &impl Clock,
    ) -> Result<ImportResult, Error> {
        let mut import_result = ImportResult::TipUnchanged;

        for header in headers.into_iter() {
            match self.tree.extend_tip(header, clock) {
                Ok(ImportResult::TipChanged(tip, height, _)) => {
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
    pub fn received_inv<C>(&mut self, addr: PeerId, inv: Vec<Inventory>, clock: &C) -> GetHeaders
    where
        C: Clock,
    {
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

            // Try to find headers leading up to the `inv` entry.

            return GetHeaders::new(
                peer,
                locators,
                clock.local_time(),
                timeout,
                OnTimeout::Ignore,
            );
        }
        GetHeaders::empty()
    }

    /// Called when we received a timeout previously set on a peer.
    pub fn received_timeout(
        &mut self,
        id: PeerId,
        local_time: LocalTime,
    ) -> (OnTimeout, GetHeaders) {
        let peer = if let Some(peer) = self.peers.get_mut(&id) {
            peer
        } else {
            return (OnTimeout::Ignore, GetHeaders::empty());
        };
        let timeout = self.config.request_timeout;

        match peer.state.clone() {
            Syncing::AwaitingHeaders(locators, time, on_timeout)
                if local_time - time >= timeout =>
            {
                match on_timeout {
                    OnTimeout::Disconnect => self.unregister(&id),
                    OnTimeout::Ignore => peer.transition(Syncing::NotResponding),
                }
                self.emit(Event::TimedOut(id));

                // TODO: Get random peer. Random peer iterator?
                if let Some(peer) = self.peers.values_mut().find(|p| p.state == Syncing::Idle) {
                    return (
                        on_timeout,
                        GetHeaders::new(peer, locators, local_time, timeout, OnTimeout::Ignore),
                    );
                }
                (on_timeout, GetHeaders::empty())
            }
            _ => (OnTimeout::Ignore, GetHeaders::empty()),
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
            .any(|p| matches!(p.state, Syncing::AwaitingHeaders(_, _, _)))
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

    fn random_sync_candidate(&self) -> Option<&PeerState> {
        let candidates = self.peers.values().filter(|p| self.is_sync_candidate(p));

        if let Some(peers) = NonEmpty::from_vec(candidates.collect()) {
            let ix = self.rng.usize(..peers.len());

            return peers.get(ix).cloned();
        }

        None
    }

    fn is_sync_candidate(&self, peer: &PeerState) -> bool {
        self::is_sync_candidate(peer, self.tree.height())
    }

    /// Check whether or not we are in sync with the network.
    fn is_synced(&self, now: LocalTime) -> bool {
        if self.is_tip_stale(now) {
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
        for peer in self.peers.values() {
            match &peer.state {
                Syncing::AwaitingHeaders(l, _, _) if l == locators => {
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    /// Start syncing if we're out of sync.
    fn sync(&mut self, now: LocalTime) -> GetHeaders {
        if self.peers.is_empty() {
            return GetHeaders::empty();
        }
        if self.is_synced(now) {
            let (tip, _) = self.tree.tip();
            let height = self.tree.height();

            self.emit(Event::Synced(tip, height));

            // If we think we're in sync and we haven't asked other peers in a while, then
            // sample their headers just to make sure we're on the right chain.
            if self
                .last_peer_sample
                .map(|t| now.duration_since(t) >= PEER_SAMPLE_INTERVAL)
                .unwrap_or(true)
            {
                self.last_peer_sample = Some(now);

                return self.sample_peers(now);
            } else {
                return GetHeaders::empty();
            }
        }
        // It looks like we're out of sync...

        let locators = (
            self.tree.locator_hashes(self.tree.height()),
            BlockHash::default(),
        );

        // If we're already fetching these headers, just wait.
        if self.syncing(&locators) {
            return GetHeaders::empty();
        }

        if let Some(peer) = self.random_sync_candidate() {
            let timeout = self.config.request_timeout;
            let addr = peer.id;
            let peer = self.peers.get_mut(&addr).expect("the peer exists");
            let get_headers = GetHeaders::new(peer, locators, now, timeout, OnTimeout::Ignore);

            self.emit(Event::Syncing(addr));

            return get_headers;
        }

        GetHeaders::empty()
    }

    /// Broadcast our best block header to connected peers who don't have it.
    fn broadcast_tip(&self, hash: &BlockHash) -> Vec<SendHeaders> {
        if let Some((height, best)) = self.tree.get_block(hash) {
            let mut addrs = Vec::new();

            for (addr, peer) in &self.peers {
                // TODO: Don't broadcast to peer that is currently syncing?
                if peer.link == Link::Inbound && height > peer.height {
                    addrs.push(*addr);
                    // TODO: Update peer inventory?
                }
            }
            return vec![SendHeaders {
                addrs,
                headers: vec![*best],
            }];
        }

        vec![]
    }

    fn get_headers(
        &self,
        locator_hashes: Vec<BlockHash>,
        stop_hash: BlockHash,
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
            let stop = self
                .tree
                .get_block(&stop_hash)
                .map(|(h, _)| h)
                .unwrap_or(tree.height());
            let stop = Height::min(start + max as Height, stop + 1);

            return tree.range(start..stop).collect();
        }

        if let Some((_, header)) = self.tree.get_block(&stop_hash) {
            return vec![*header];
        }

        vec![]
    }

    /// Ask all our outbound peers whether they have better block headers.
    fn sample_peers(&mut self, now: LocalTime) -> GetHeaders {
        let height = self.tree.height();
        let locators = self.tree.locator_hashes(height);
        let timeout = self.config.request_timeout;

        let messages = self
            .peers
            .values_mut()
            .filter(|p| self::is_sync_candidate(p, height))
            .map(|peer| {
                Request::new(
                    peer,
                    (locators.clone(), BlockHash::default()),
                    now,
                    timeout,
                    OnTimeout::Ignore,
                )
            })
            .collect::<Vec<_>>();

        GetHeaders::from(messages)
    }
}

fn is_sync_candidate(peer: &PeerState, height: Height) -> bool {
    peer.is_outbound() && peer.is_idle() && peer.height > height
}
