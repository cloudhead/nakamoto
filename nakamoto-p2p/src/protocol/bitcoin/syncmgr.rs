use std::collections::HashMap;

use nonempty::NonEmpty;

use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::util::hash::BitcoinHash;

use nakamoto_common::block::time::{Clock, LocalDuration};
use nakamoto_common::block::tree::{BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};

use super::{Link, Locators, PeerId};

/// How long to wait for a request, eg. `getheaders` to be fulfilled.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_secs(8);

/// A timeout.
type Timeout = LocalDuration;

#[must_use]
#[derive(Debug)]
pub enum SyncResult<G, S> {
    GetHeaders(G),
    SendHeaders(S),
    Okay,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    WaitingForPeers,
    Syncing(PeerId),
    // TODO: Add confidence parameter to synced state.
    Synced(BlockHash, Height),
}

/// Synchronization states.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Syncing {
    /// Not currently syncing. This is usually the starting and end state.
    Idle,
    /// Syncing. A `getheaders` message was sent and we are expecting a response.
    AwaitingHeaders(Locators),
    /// The peer is not responding.
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
    fn is_candidate(&self) -> bool {
        // TODO
        true
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
}

#[derive(Debug)]
pub struct SyncManager<T> {
    /// Block tree.
    pub tree: T,

    /// Sync-specific peer state.
    peers: HashMap<PeerId, PeerState>,
    /// Sync manager configuration.
    config: Config,
    /// Sync state.
    state: State,
    /// Random number generator.
    rng: fastrand::Rng,
    /// Output event queue.
    events: Vec<Event>,
}

#[derive(Debug)]
pub enum Event {
    ReceivedInvalidHeaders(PeerId, Error),
    HeadersImported(ImportResult),
    BlockDiscovered(PeerId, BlockHash),
    Syncing(PeerId),
    Synced(BlockHash, Height),
    WaitingForPeers,
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

impl<T: BlockTree> SyncManager<T> {
    pub fn new(tree: T, config: Config, rng: fastrand::Rng) -> Self {
        let peers = HashMap::new();
        let state = State::WaitingForPeers;
        let events = Vec::new();

        Self {
            tree,
            peers,
            config,
            state,
            rng,
            events,
        }
    }

    pub fn events<'a>(&'a mut self) -> impl Iterator<Item = Event> + 'a {
        self.events.drain(..)
    }

    pub fn peer_connected(
        &mut self,
        id: PeerId,
        height: Height,
        tip: BlockHash,
        services: ServiceFlags,
        link: Link,
    ) -> Option<GetHeaders> {
        self.register(id, height, tip, services, link);
        self.sync()
    }

    pub fn peer_disconnected(&mut self, id: &PeerId) {
        self.unregister(id);
    }

    pub fn receive_get_headers(
        &self,
        addr: &PeerId,
        (locator_hashes, stop_hash): Locators,
        max: usize,
    ) -> Option<SendHeaders> {
        let headers = self.get_headers(locator_hashes, stop_hash, max);

        if headers.is_empty() {
            return None;
        }
        if !self.is_synced() {
            return None;
        }

        return Some(SendHeaders {
            addrs: vec![*addr],
            headers,
        });
    }

    pub fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        chain: I,
        context: &C,
    ) -> Result<(ImportResult, Option<SendHeaders>), Error> {
        match self.tree.import_blocks(chain, context) {
            Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                let result = ImportResult::TipChanged(tip, height, reverted);
                self.emit(Event::HeadersImported(result.clone()));

                Ok((result, self.broadcast_tip(&tip)))
            }
            Ok(result @ ImportResult::TipUnchanged) => {
                self.emit(Event::HeadersImported(result.clone()));
                Ok((result, None))
            }
            Err(err) => Err(err),
        }
    }

    pub fn receive_headers(
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

        let timeout = self.config.request_timeout;
        let peer = self.peers.get_mut(from).unwrap();

        // TODO: Before importing, we could check the headers against known checkpoints.
        // TODO: Check that headers form a chain.
        let result = self.tree.import_blocks(headers.into_iter(), clock);

        match result {
            Ok(import_result @ ImportResult::TipUnchanged) => {
                self.events.push(Event::HeadersImported(import_result));

                // Try to find a common ancestor.
                let locators = (
                    self.tree.locators_hashes(self.tree.height()),
                    BlockHash::default(),
                );

                return SyncResult::GetHeaders(GetHeaders::new(peer, locators, timeout));
            }
            Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                peer.tip = tip;
                peer.height = height;

                let import_result = ImportResult::TipChanged(tip, height, reverted);

                // TODO: Check that the headers received match the headers awaited.
                if let Syncing::AwaitingHeaders(_locators) = &peer.state {
                    // If we received less than the maximum number of headers, we must be in sync.
                    // Otherwise, ask for the next batch of headers.
                    if length < self.config.max_message_headers {
                        // If these headers were unsolicited, we may already be ready/synced.
                        // Otherwise, we're finally in sync.
                        peer.transition(Syncing::Idle);
                        self.emit(Event::HeadersImported(import_result));
                        self.transition(State::Synced(tip, height));

                        return self
                            .broadcast_tip(&tip)
                            .map(|msg| SyncResult::SendHeaders(msg))
                            .unwrap_or(SyncResult::Okay);
                    } else {
                        self.events.push(Event::HeadersImported(import_result));

                        // TODO: If we're already in the state of asking for this header, don't
                        // ask again.
                        let locators = (vec![tip], BlockHash::default());

                        return SyncResult::GetHeaders(GetHeaders::new(peer, locators, timeout));
                    }
                } else {
                    self.emit(Event::HeadersImported(import_result));
                }
            }
            Err(err) => {
                self.emit(Event::ReceivedInvalidHeaders(*from, err));
            }
        }
        SyncResult::Okay
    }

    /// Receive an `inv` message. This will happen if we are out of sync with a peer. And blocks
    /// are being announced. Otherwise, we expect to receive a `headers` message.
    pub fn receive_inv(&mut self, addr: PeerId, inv: Vec<Inventory>) -> Option<GetHeaders> {
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
            let locators = (self.locator_hashes(), *stop_hash);
            let timeout = self.config.request_timeout;
            let peer = self.peers.get_mut(&addr).unwrap();

            return Some(GetHeaders::new(peer, locators, timeout));
        }
        None
    }

    pub fn receive_timeout(&mut self, id: PeerId) -> (Option<PeerTimeout>, Option<GetHeaders>) {
        let peer = if let Some(peer) = self.peers.get_mut(&id) {
            peer
        } else {
            return (None, None);
        };

        match &peer.state {
            Syncing::AwaitingHeaders(locators) => {
                let locators = locators.clone();

                peer.transition(Syncing::NotResponding);

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

    /// Check whether or not we are in sync with the network.
    /// TODO: Should return the minimum peer height, so that we can
    /// keep track of it in our state, while syncing to it.
    fn is_synced(&self) -> bool {
        let height = self.tree.height();

        // TODO: Check actual block hashes once we are caught up on height.
        if let Some(peer_height) = self
            .peers
            .values()
            .filter(|p| p.is_candidate())
            .map(|p| p.height)
            .min()
        {
            height >= peer_height
        } else {
            true
        }
    }

    /// Start syncing with the given peer.
    fn sync(&mut self) -> Option<GetHeaders> {
        if self.peers.is_empty() {
            return None;
        }
        let locators = (self.locator_hashes(), BlockHash::default());

        if self.is_synced() {
            let (tip, _) = self.tree.tip();
            let height = self.tree.height();

            self.transition(State::Synced(tip, height));

            return None;
        }

        // TODO: Pick a peer whose `height` is high enough.
        // TODO: Factor this out when we have a `peermgr`.
        // TODO: Threshold should be a parameter.
        // TODO: Peer should be picked amongst lowest latency ones.
        // Wait for a certain connection threshold to make sure we choose the best
        // peer to sync from. For now, we choose a random peer.
        let ix = self.rng.usize(..self.peers.len());
        let (addr, peer) = self.peers.iter_mut().nth(ix).unwrap();
        let timeout = self.config.request_timeout;
        let addr = *addr;
        let get_headers = GetHeaders::new(peer, locators, timeout);

        self.transition(State::Syncing(addr));

        Some(get_headers)
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

    fn transition(&mut self, state: State) {
        let previous = self.state.clone();

        if state == previous {
            return;
        }

        match &state {
            State::WaitingForPeers => self.emit(Event::WaitingForPeers),
            State::Syncing(addr) => self.emit(Event::Syncing(*addr)),
            State::Synced(hash, height) => self.emit(Event::Synced(*hash, *height)),
        }
        self.state = state;
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

    #[allow(dead_code)]
    fn sample_headers(&mut self) -> Vec<GetHeaders> {
        let height = self.tree.height();
        let (tip, _) = self.tree.tip();

        let mut messages = Vec::new();

        if let Some(parent) = self.tree.get_block_by_height(height - 1) {
            let locators = (vec![parent.bitcoin_hash()], tip);

            for peer in self.peers.values_mut() {
                if peer.link == Link::Outbound {
                    messages.push(GetHeaders::new(
                        peer,
                        locators.clone(),
                        self.config.request_timeout,
                    ));
                }
            }
        }
        messages
    }

    fn locator_hashes(&self) -> Vec<BlockHash> {
        let (hash, _) = self.tree.tip();

        vec![hash]
    }
}
