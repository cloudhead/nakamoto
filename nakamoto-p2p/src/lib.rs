pub mod address_book;
pub mod checkpoints;
pub mod error;
pub mod peer;
pub mod tcp;

pub mod prototype;

use nakamoto_chain::block::{time::AdjustedTime, tree::BlockTree, Height};

use std::collections::{HashMap, HashSet};
use std::net;
use std::sync::{mpsc, Arc, Mutex, RwLock};

use log::*;

/// Number of blocks out of sync we have to be to trigger an initial sync.
pub const SYNC_THRESHOLD: Height = 144;
/// Minimum number of peers to be connected to.
pub const PEER_CONNECTION_THRESHOLD: usize = 3;
/// Maximum time adjustment between network and local time (70 minutes).
pub const MAX_TIME_ADJUSTMENT: peer::TimeOffset = 70 * 60;

/// Identifies a peer.
pub type PeerId = net::SocketAddr;

/// A mapping between peer identifiers and peers.
type Peers<T> = HashMap<PeerId, T>;

pub trait Peer {
    // TODO: The interface here should be `receive` or `tick`.
    fn run(&mut self, events: mpsc::Sender<peer::Event>) -> Result<(), error::Error>;
    fn height(&self) -> Height;
    fn time_offset(&self) -> peer::TimeOffset;
}

fn run<P: Peer>(
    addr: net::SocketAddr,
    peer: P,
    peers: Arc<RwLock<Peers<P>>>,
    events: mpsc::Sender<peer::Event>,
) -> Result<(), error::Error> {
    use std::collections::hash_map::Entry;

    let mut peers = peers.write().expect("lock has not been poisoned");

    match peers.entry(addr) {
        Entry::Vacant(v) => {
            let peer = v.insert(peer);
            peer.run(events)?;
        }
        Entry::Occupied(_) => {}
    }
    Ok(())
}

pub enum NetworkState {
    /// Connecting to the network. Syncing hasn't started yet.
    Connecting,
    /// Initial syncing (IBD) has started with the designated peer.
    InitialSync(PeerId),
    /// We're in sync.
    Synced,
}

pub struct Network<T: BlockTree, P: Peer> {
    peer_config: peer::Config,
    peers: Arc<RwLock<Peers<P>>>,
    block_cache: Arc<RwLock<T>>,
    state: NetworkState,
    adjusted_time: Arc<Mutex<AdjustedTime<PeerId>>>,

    connected: HashSet<PeerId>,
    handshaked: HashSet<PeerId>,
    disconnected: HashSet<PeerId>,
}

impl<T: BlockTree, P: Peer> Network<T, P> {
    pub fn new(
        peer_config: peer::Config,
        block_cache: Arc<RwLock<T>>,
        adjusted_time: Arc<Mutex<AdjustedTime<PeerId>>>,
    ) -> Self {
        let peers = Arc::new(RwLock::new(Peers::new()));
        let state = NetworkState::Connecting;

        Self {
            state,
            peer_config,
            peers,
            block_cache,
            adjusted_time,
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
                Ok(peer::Event::Handshaked(peer, offset)) => {
                    self.handshaked.insert(peer);
                    self.adjusted_time
                        .lock()
                        .expect("lock is not poisoned")
                        .add_sample(peer, offset);
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
