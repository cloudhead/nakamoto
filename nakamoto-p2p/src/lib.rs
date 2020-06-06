pub mod error;
pub mod peer;
pub mod tcp;

use nakamoto_chain::block::store::Store;
use nakamoto_chain::blocktree::{BlockCache, BlockTree, Height};

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net;
use std::sync::{mpsc, Arc, RwLock};

use log::*;

/// Number of blocks out of sync we have to be to trigger an initial sync.
pub const SYNC_THRESHOLD: Height = 144;
/// Minimum number of peers to be connected to.
pub const PEER_CONNECTION_THRESHOLD: usize = 3;

/// Identifies a peer.
pub type PeerId = net::SocketAddr;

/// A mapping between peer identifiers and peers.
type Peers<R> = HashMap<PeerId, Peer<R>>;

/// A network peer.
struct Peer<R: Read>(peer::Connection<R>);

impl<R: Read + Write> Peer<R> {
    fn run<S: Store>(
        addr: net::SocketAddr,
        peer: peer::Connection<R>,
        cache: Arc<RwLock<BlockCache<S>>>,
        peers: Arc<RwLock<Peers<R>>>,
        events: mpsc::Sender<peer::Event>,
    ) -> Result<(), error::Error> {
        use std::collections::hash_map::Entry;

        let mut peers = peers.write().expect("lock has not been poisoned");

        match peers.entry(addr) {
            Entry::Vacant(v) => {
                let Peer(ref mut peer) = v.insert(Peer(peer));
                peer.run(cache, events)?;
            }
            Entry::Occupied(_) => {}
        }
        Ok(())
    }

    fn height(&self) -> Height {
        self.0.height
    }
}

pub enum NetworkState {
    /// Connecting to the network. Syncing hasn't started yet.
    Connecting,
    /// Initial syncing (IBD) has started with the designated peer.
    InitialSync(PeerId),
    /// We're in sync.
    Synced,
}

pub struct Network<S: Store, R: Read> {
    peer_config: peer::Config,
    peers: Arc<RwLock<Peers<R>>>,
    block_cache: Arc<RwLock<BlockCache<S>>>,
    state: NetworkState,

    connected: HashSet<PeerId>,
    handshaked: HashSet<PeerId>,
    disconnected: HashSet<PeerId>,
}

impl<S: Store, R: Read + Write> Network<S, R> {
    pub fn new(peer_config: peer::Config, block_cache: Arc<RwLock<BlockCache<S>>>) -> Self {
        let peers = Arc::new(RwLock::new(Peers::new()));
        let state = NetworkState::Connecting;

        Self {
            state,
            peer_config,
            peers,
            block_cache,
            connected: HashSet::new(),
            handshaked: HashSet::new(),
            disconnected: HashSet::new(),
        }
    }

    /// Start initial block header sync.
    pub fn initial_sync(&mut self, peer: PeerId) {
        // TODO: Notify peer that it should sync.
        self.state = NetworkState::InitialSync(peer);
    }

    /// Check whether or not we are in sync with the network.
    pub fn is_synced(&self) -> Result<bool, error::Error> {
        let cache = self.block_cache.read().expect("lock is not poisoned");
        let height = cache.height();

        let peers = self.peers.read().expect("lock is not poisoned");

        // TODO: Make sure we only consider connected peers?
        // TODO: Check actual block hashes once we are caught up on height.
        if let Some(peer_height) = peers.values().map(|p| p.height()).min() {
            Ok(height >= peer_height || peer_height - height <= SYNC_THRESHOLD)
        } else {
            Err(error::Error::NotConnected)
        }
    }

    pub fn listen(&mut self, rx: mpsc::Receiver<peer::Event>) -> Result<(), error::Error> {
        loop {
            let event = rx.recv_timeout(peer::PING_INTERVAL);

            match event {
                Ok(peer::Event::Connected(peer)) => {
                    self.disconnected.remove(&peer);
                    self.connected.insert(peer);
                }
                Ok(peer::Event::Handshaked(peer)) => {
                    self.handshaked.insert(peer);
                }
                Ok(peer::Event::Disconnected(peer)) => {
                    self.connected.remove(&peer);
                    self.handshaked.remove(&peer);
                    self.disconnected.insert(peer);
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // This always means that all senders have been dropped.
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }

            // TODO: Find out who to ping.

            if self.handshaked.len() >= PEER_CONNECTION_THRESHOLD {
                match self.is_synced() {
                    Ok(is_synced) => {
                        if is_synced {
                            self.state = NetworkState::Synced;
                        } else {
                            let ix = fastrand::usize(..self.handshaked.len());
                            let peer = *self.handshaked.iter().nth(ix).unwrap();

                            self.initial_sync(peer);
                        }
                    }
                    Err(error::Error::NotConnected) => self.state = NetworkState::Connecting,
                    Err(err) => panic!(err.to_string()),
                }
            }
        }
        Ok(())
    }
}
