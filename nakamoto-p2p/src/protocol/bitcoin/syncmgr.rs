use std::collections::HashMap;

use nonempty::NonEmpty;

use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::util::hash::BitcoinHash;

use nakamoto_common::block::time::Clock;
use nakamoto_common::block::tree::{BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};

use super::{Link, Locators, PeerId};

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
}

#[derive(Debug)]
pub enum Input {
    PeerConnected(PeerId, Height, BlockHash, ServiceFlags, Link),
    PeerDisconnected(PeerId),
    ReceivedHeaders(PeerId, NonEmpty<BlockHeader>),
    ReceivedInventory(PeerId, Vec<Inventory>),
    GetHeaders(PeerId, Locators),
}

#[must_use]
#[derive(Debug)]
pub enum Output {
    GetHeaders(PeerId, Locators),
    SendHeaders(Vec<PeerId>, Vec<BlockHeader>),
    HeadersImported(ImportResult),
    ReceivedInvalidHeaders(PeerId, Error),
    BlockDiscovered(PeerId, BlockHash),
    Syncing(PeerId),
    Synced(BlockHash, Height),
    PeerTimeout(PeerId),
    WaitingForPeers,
}

impl<T: BlockTree> SyncManager<T> {
    pub fn new(tree: T, config: Config, rng: fastrand::Rng) -> Self {
        let peers = HashMap::new();
        let state = State::WaitingForPeers;

        Self {
            tree,
            peers,
            config,
            state,
            rng,
        }
    }

    pub fn step<C: Clock>(&mut self, input: Input, clock: &C) -> Vec<Output> {
        match input {
            Input::PeerConnected(id, height, tip, services, link) => {
                self.register(id, height, tip, services, link);
            }
            Input::PeerDisconnected(id) => {
                self.unregister(&id);
            }
            Input::ReceivedHeaders(from, headers) => {
                return self.receive_headers(&from, headers, clock);
            }
            Input::ReceivedInventory(from, inv) => {
                return self.receive_inv(from, inv);
            }
            Input::GetHeaders(from, locators) => {
                return self.receive_get_headers(&from, locators, self.config.max_message_headers);
            }
        }
        vec![]
    }

    pub fn peer_connected(
        &mut self,
        id: PeerId,
        height: Height,
        tip: BlockHash,
        services: ServiceFlags,
        link: Link,
    ) -> Vec<Output> {
        self.register(id, height, tip, services, link);
        self.sync()
    }

    pub fn peer_disconnected(&mut self, id: &PeerId) -> Vec<Output> {
        self.unregister(id);
        vec![]
    }

    pub fn receive_get_headers(
        &self,
        addr: &PeerId,
        (locator_hashes, stop_hash): Locators,
        max: usize,
    ) -> Vec<Output> {
        let headers = self.get_headers(locator_hashes, stop_hash, max);

        if headers.is_empty() {
            return vec![];
        }
        return vec![Output::SendHeaders(vec![*addr], headers)];
    }

    pub fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        chain: I,
        context: &C,
    ) -> Result<(ImportResult, Vec<Output>), Error> {
        match self.tree.import_blocks(chain, context) {
            Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                let mut out = self.broadcast_tip(&tip);
                let result = ImportResult::TipChanged(tip, height, reverted);

                out.push(Output::HeadersImported(result.clone()));

                Ok((result, out))
            }
            Ok(result @ ImportResult::TipUnchanged) => {
                Ok((result.clone(), vec![Output::HeadersImported(result)]))
            }
            Err(err) => Err(err),
        }
    }

    pub fn receive_headers(
        &mut self,
        from: &PeerId,
        headers: NonEmpty<BlockHeader>,
        clock: &impl Clock,
    ) -> Vec<Output> {
        let length = headers.len();
        let best = headers.last().bitcoin_hash();

        if self.tree.contains(&best) {
            // Ignore duplicate headers on the active chain.
            return vec![];
        }

        let peer = self.peers.get_mut(from).unwrap();

        // TODO: Before importing, we could check the headers against known checkpoints.
        // TODO: Check that headers form a chain.
        match self.tree.import_blocks(headers.into_iter(), clock) {
            Ok(import_result @ ImportResult::TipUnchanged) => {
                // Try to find a common ancestor.
                let locators = (
                    self.tree.locators_hashes(self.tree.height()),
                    BlockHash::default(),
                );

                // TODO: What if we already knew these headers?
                peer.transition(Syncing::AwaitingHeaders(locators.clone()));

                vec![
                    Output::HeadersImported(import_result),
                    Output::GetHeaders(*from, locators),
                ]
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

                        let mut out = self.transition(State::Synced(tip, height));

                        out.push(Output::HeadersImported(import_result));
                        out.push(Output::Synced(tip, height));
                        out.extend(self.broadcast_tip(&tip));
                        out
                    } else {
                        // TODO: If we're already in the state of asking for this header, don't
                        // ask again.
                        let locators = (vec![tip], BlockHash::default());

                        peer.transition(Syncing::AwaitingHeaders(locators.clone()));

                        vec![
                            Output::HeadersImported(import_result),
                            Output::GetHeaders(*from, locators),
                        ]
                    }
                } else {
                    vec![Output::HeadersImported(import_result)]
                }
            }
            Err(err) => vec![Output::ReceivedInvalidHeaders(*from, err)],
        }
    }

    /// Receive an `inv` message. This will happen if we are out of sync with a peer. And blocks
    /// are being announced. Otherwise, we expect to receive a `headers` message.
    pub fn receive_inv(&mut self, addr: PeerId, inv: Vec<Inventory>) -> Vec<Output> {
        let mut best_block = None;
        let mut output = Vec::new();

        for i in &inv {
            if let Inventory::Block(hash) = i {
                // TODO: Update block availability for this peer.
                if !self.tree.is_known(hash) {
                    output.push(Output::BlockDiscovered(addr, *hash));
                    // The final block hash in the inventory should be the highest. Use
                    // that one for a `getheaders` call.
                    best_block = Some(hash);
                }
            }
        }

        if let Some(stop_hash) = best_block {
            output.push(Output::GetHeaders(
                addr,
                (self.locator_hashes(), *stop_hash),
            ));
        }
        output
    }

    pub fn handle_timeout(&mut self, id: PeerId) -> Vec<Output> {
        if let Some(peer) = self.peers.get(&id) {
            match peer.state {
                Syncing::AwaitingHeaders(_) => return vec![Output::PeerTimeout(id)],
                _ => {}
            }
        }
        vec![]
    }

    ///////////////////////////////////////////////////////////////////////////

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
    fn sync(&mut self) -> Vec<Output> {
        if self.peers.is_empty() {
            return vec![Output::WaitingForPeers];
        }
        let locators = (self.locator_hashes(), BlockHash::default());
        let mut out = Vec::new();

        if self.is_synced() {
            let (tip, _) = self.tree.tip();
            let height = self.tree.height();

            return self.transition(State::Synced(tip, height));
        }

        // TODO: Pick a peer whose `height` is high enough.
        // TODO: Factor this out when we have a `peermgr`.
        // TODO: Threshold should be a parameter.
        // TODO: Peer should be picked amongst lowest latency ones.
        // Wait for a certain connection threshold to make sure we choose the best
        // peer to sync from. For now, we choose a random peer.
        let ix = self.rng.usize(..self.peers.len());
        let (addr, peer) = self.peers.iter_mut().nth(ix).unwrap();
        let addr = *addr;

        peer.transition(Syncing::AwaitingHeaders(locators.clone()));

        out.extend(self.transition(State::Syncing(addr)));
        out.push(Output::GetHeaders(addr, locators));
        out
    }

    /// Broadcast our best block header to connected peers who don't have it.
    fn broadcast_tip(&self, hash: &BlockHash) -> Vec<Output> {
        if let Some((height, best)) = self.tree.get_block(hash) {
            let mut addrs = Vec::new();

            for (addr, peer) in &self.peers {
                // TODO: Don't broadcast to peer that is currently syncing?
                if peer.link == Link::Inbound && height > peer.height {
                    addrs.push(*addr);
                    // TODO: Update peer inventory?
                }
            }
            vec![Output::SendHeaders(addrs, vec![*best])]
        } else {
            vec![]
        }
    }

    fn transition(&mut self, state: State) -> Vec<Output> {
        let previous = self.state.clone();

        if state == previous {
            return vec![];
        }

        self.state = state;

        match &self.state {
            State::WaitingForPeers => vec![Output::WaitingForPeers],
            State::Syncing(addr) => vec![Output::Syncing(*addr)],
            State::Synced(hash, height) => vec![Output::Synced(*hash, *height)],
        }
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

    fn locator_hashes(&self) -> Vec<BlockHash> {
        let (hash, _) = self.tree.tip();

        vec![hash]
    }
}
