pub mod error;
pub mod peer;

use nakamoto_chain::blocktree::{BlockCache, BlockTree, Height};

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use log::*;

/// Number of blocks out of sync we have to be to trigger an initial sync.
pub const SYNC_THRESHOLD: Height = 144;
/// Minimum number of peers to be connected to.
pub const PEER_CONNECTION_THRESHOLD: usize = 3;

type Peers<R> = HashMap<PeerId, Peer<R>>;

/// Identifies a peer.
pub type PeerId = net::SocketAddr;

/// Peer state.
struct Peer<R: Read + Write> {
    conn: peer::Connection<R>,
}

impl Peer<net::TcpStream> {
    fn run(
        addr: net::SocketAddr,
        config: peer::Config,
        cache: Arc<RwLock<BlockCache>>,
        peers: Arc<RwLock<Peers<net::TcpStream>>>,
        events: mpsc::Sender<peer::Event>,
    ) -> Result<(), error::Error> {
        use std::collections::hash_map::Entry;

        debug!("Connecting to {}...", &addr);

        let conn = peer::Connection::new(&addr, config)?;
        let addr = conn.address;

        debug!("Connected to {}", &addr);
        trace!("{:#?}", conn);

        let mut peers = peers.write().expect("lock has not been poisoned");

        match peers.entry(addr) {
            Entry::Vacant(v) => {
                let peer = v.insert(Peer { conn });
                peer.conn.run(cache, events)?;
            }
            Entry::Occupied(_) => {}
        }
        debug!("Disconnected from {}", &addr);

        Ok(())
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

pub struct Network {
    peer_config: peer::Config,
    peers: Arc<RwLock<Peers<net::TcpStream>>>,
    block_cache: Arc<RwLock<BlockCache>>,
    state: NetworkState,

    connected: HashSet<PeerId>,
    handshaked: HashSet<PeerId>,
    disconnected: HashSet<PeerId>,
}

impl Network {
    pub fn new(peer_config: peer::Config, block_cache: Arc<RwLock<BlockCache>>) -> Self {
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

    pub fn connect(&mut self, addrs: &[net::SocketAddr]) -> Result<Vec<()>, error::Error> {
        let (tx, rx) = mpsc::channel();
        let mut spawned = Vec::new();

        for addr in addrs.iter() {
            let cache = self.block_cache.clone();
            let config = self.peer_config.clone();
            let peers = self.peers.clone();
            let addr = addr.clone();
            let tx = tx.clone();

            let handle = thread::spawn(move || match Peer::run(addr, config, cache, peers, tx) {
                Ok(result) => return Ok(result),
                Err(err) => panic!(err.to_string()),
            });
            spawned.push(handle);
        }
        // We don't need this transmitting end, since we've supplied all transmitters to peers.
        drop(tx);

        loop {
            let event = rx.recv_timeout(peer::PING_INTERVAL);

            match event {
                Ok(peer::Event::Connected(peer)) => {
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

        spawned
            .into_iter()
            .flat_map(thread::JoinHandle::join)
            .collect()
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
        if let Some(peer_height) = peers.values().map(|p| p.conn.height).min() {
            Ok(height >= peer_height || peer_height - height <= SYNC_THRESHOLD)
        } else {
            Err(error::Error::NotConnected)
        }
    }
}
