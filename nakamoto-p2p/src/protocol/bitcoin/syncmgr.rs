use std::collections::HashMap;

use log::*;
use nonempty::NonEmpty;

use bitcoin::network::constants::ServiceFlags;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::util::hash::BitcoinHash;

use nakamoto_common::block::time::Clock;
use nakamoto_common::block::tree::{BlockTree, Error, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height};

use super::{Locators, PeerId};

#[derive(Debug)]
pub enum ReceiveHeaders {
    Continue {
        import_result: ImportResult,
        locators: Locators,
    },
    Okay(ImportResult),
    Done(ImportResult),
    Failure(Error),
    Duplicate,
}

#[derive(Debug)]
pub enum Event {
    Syncing,
    Synced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Idle,
    Syncing(PeerId),
    Synced(Height),
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
    state: Syncing,
    ctx: &'static str,
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
        debug!(
            "[{}] {}: Syncing: {:?} -> {:?}",
            self.ctx, self.id, self.state, state
        );

        self.state = state;
    }
}

#[derive(Debug)]
pub struct Config {
    pub max_headers_received: usize,
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
    /// Read-only context passed by parent, used for logging.
    ctx: &'static str,
}

impl<T: BlockTree> SyncManager<T> {
    pub fn new(tree: T, config: Config, rng: fastrand::Rng, ctx: &'static str) -> Self {
        let peers = HashMap::new();
        let state = State::Idle;

        Self {
            tree,
            peers,
            config,
            state,
            rng,
            ctx,
        }
    }

    pub fn register(&mut self, id: PeerId, height: Height, tip: BlockHash, services: ServiceFlags) {
        self.peers.insert(
            id,
            PeerState {
                id,
                height,
                tip,
                services,
                state: Syncing::default(),
                ctx: self.ctx,
            },
        );
    }

    pub fn unregister(&mut self, peer: &PeerId) {
        self.peers.remove(peer);
    }

    /// Check whether or not we are in sync with the network.
    /// TODO: Should return the minimum peer height, so that we can
    /// keep track of it in our state, while syncing to it.
    pub fn is_synced(&self) -> bool {
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
    pub fn sync(&mut self) -> Option<(PeerId, Locators, Vec<Event>)> {
        let locators = (self.locator_hashes(), BlockHash::default());
        let mut events = Vec::new();

        // TODO: Pick a peer whose `height` is high enough.
        // TODO: Factor this out when we have a `peermgr`.
        let ix = self.rng.usize(..self.peers.len());
        let (addr, peer) = self.peers.iter_mut().nth(ix).unwrap();
        let addr = *addr;

        peer.transition(Syncing::AwaitingHeaders(locators.clone()));

        if self.transition(State::Syncing(addr)).is_some() {
            events.push(Event::Syncing);
        }

        Some((addr, locators, events))
    }

    pub fn height(&self) -> Height {
        self.tree.height()
    }

    pub fn get_headers(&self, locator_hashes: Vec<BlockHash>, max: usize) -> Vec<BlockHeader> {
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

    pub fn import_blocks<I: Iterator<Item = BlockHeader>, C: Clock>(
        &mut self,
        chain: I,
        context: &C,
    ) -> Result<ImportResult, Error> {
        self.tree.import_blocks(chain, context)
    }

    pub fn receive_headers(
        &mut self,
        peer: &PeerId,
        headers: NonEmpty<BlockHeader>,
        clock: &impl Clock,
    ) -> ReceiveHeaders {
        let length = headers.len();
        let best = headers.last().bitcoin_hash();

        if self.tree.contains(&best) {
            return ReceiveHeaders::Duplicate;
        }

        let peer = self.peers.get_mut(peer).unwrap();

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

                ReceiveHeaders::Continue {
                    import_result,
                    locators,
                }
            }
            Ok(ImportResult::TipChanged(tip, height, reverted)) => {
                peer.tip = tip;
                peer.height = height;

                let import_result = ImportResult::TipChanged(tip, height, reverted);

                // TODO: Check that the headers received match the headers awaited.
                if let Syncing::AwaitingHeaders(_locators) = &peer.state {
                    // If we received less than the maximum number of headers, we must be in sync.
                    // Otherwise, ask for the next batch of headers.
                    if length < self.config.max_headers_received {
                        // If these headers were unsolicited, we may already be ready/synced.
                        // Otherwise, we're finally in sync.
                        peer.transition(Syncing::Idle);

                        ReceiveHeaders::Done(import_result)
                    } else {
                        // TODO: If we're already in the state of asking for this header, don't
                        // ask again.
                        let locators = (vec![tip], BlockHash::default());

                        peer.transition(Syncing::AwaitingHeaders(locators.clone()));

                        ReceiveHeaders::Continue {
                            import_result,
                            locators,
                        }
                    }
                } else {
                    ReceiveHeaders::Okay(import_result)
                }
            }
            Err(err) => ReceiveHeaders::Failure(err),
        }
    }

    /// Receive an `inv` message. This will happen if we are out of sync with a peer. And blocks
    /// are being announced. Otherwise, we expect to receive a `headers` message.
    pub fn receive_inv(
        &mut self,
        addr: PeerId,
        inv: Vec<Inventory>,
    ) -> Result<Option<Locators>, Error> {
        let mut best_block = None;

        for i in &inv {
            if let Inventory::Block(hash) = i {
                // TODO: Update block availability for this peer.
                if !self.tree.is_known(hash) {
                    debug!("[{}] {}: Discovered new block: {}", self.ctx, addr, &hash);
                    // The final block hash in the inventory should be the highest. Use
                    // that one for a `getheaders` call.
                    best_block = Some(hash);
                }
            }
        }

        if let Some(stop_hash) = best_block {
            Ok(Some((self.locator_hashes(), *stop_hash)))
        } else {
            Ok(None)
        }
    }

    fn transition(&mut self, state: State) -> Option<State> {
        let previous = self.state.clone();

        if state == previous {
            return None;
        }
        debug!("[{}] state: {:?} -> {:?}", self.ctx, previous, state);

        self.state = state;

        Some(previous)
    }

    fn locator_hashes(&self) -> Vec<BlockHash> {
        let (hash, _) = self.tree.tip();

        vec![hash]
    }
}
