pub mod address_book;
pub mod checkpoints;
pub mod error;
pub mod peer;
pub mod tcp;

use nakamoto_chain::block::{tree::BlockTree, Height, Time};

use std::collections::{HashMap, HashSet};
use std::net;
use std::sync::{mpsc, Arc, RwLock};

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
    fn run<T: BlockTree>(
        &mut self,
        cache: Arc<RwLock<T>>,
        events: mpsc::Sender<peer::Event>,
    ) -> Result<(), error::Error>;

    fn height(&self) -> Height;
    fn time_offset(&self) -> peer::TimeOffset;
}

fn run<T: BlockTree, P: Peer>(
    addr: net::SocketAddr,
    peer: P,
    cache: Arc<RwLock<T>>,
    peers: Arc<RwLock<Peers<P>>>,
    events: mpsc::Sender<peer::Event>,
) -> Result<(), error::Error> {
    use std::collections::hash_map::Entry;

    let mut peers = peers.write().expect("lock has not been poisoned");

    match peers.entry(addr) {
        Entry::Vacant(v) => {
            let peer = v.insert(peer);
            peer.run(cache, events)?;
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

    connected: HashSet<PeerId>,
    handshaked: HashSet<PeerId>,
    disconnected: HashSet<PeerId>,
}

impl<T: BlockTree, P: Peer> Network<T, P> {
    pub fn new(peer_config: peer::Config, block_cache: Arc<RwLock<T>>) -> Self {
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

    /// *Network-adjusted time* is the median timestamp of all connected peers.
    /// Since we store only time offsets for each peer, the network-adjusted time is
    /// the local time plus the median offset of all connected peers.
    ///
    /// Nb. Network time is never adjusted more than 70 minutes from local system time.
    pub fn network_adjusted_time(&self, local_time: Time) -> Time {
        let peers = self.peers.read().expect("lock is not poisoned");
        let mut offsets = peers.values().map(Peer::time_offset).collect::<Vec<_>>();
        let count = offsets.len();

        offsets.sort();

        let median_offset: peer::TimeOffset = if count % 2 == 0 {
            (offsets[count / 2 - 1] + offsets[count / 2]) / 2
        } else {
            offsets[count / 2]
        };

        let adjustment = median_offset.abs().min(MAX_TIME_ADJUSTMENT) as Time;

        if median_offset > 0 {
            local_time + adjustment
        } else {
            local_time - adjustment
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

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    use bitcoin::blockdata::constants;
    use nakamoto_chain::block::cache::model;

    struct TestPeer {
        time_offset: peer::TimeOffset,
    }

    impl Peer for TestPeer {
        fn run<T: BlockTree>(
            &mut self,
            _cache: Arc<RwLock<T>>,
            _events: mpsc::Sender<peer::Event>,
        ) -> Result<(), error::Error> {
            Ok(())
        }

        fn height(&self) -> Height {
            0
        }

        fn time_offset(&self) -> peer::TimeOffset {
            self.time_offset
        }
    }

    #[test]
    fn test_network_adjusted_time() {
        let network = bitcoin::Network::Bitcoin;
        let genesis = constants::genesis_block(network).header;
        let cache = model::Cache::new(genesis);
        let net: Network<_, TestPeer> =
            Network::new(Default::default(), Arc::new(RwLock::new(cache)));
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as Time;

        {
            let mut peers = net.peers.write().unwrap();
            peers.insert(([127, 0, 0, 1], 8333).into(), TestPeer { time_offset: 92 });
        }
        assert_eq!(net.network_adjusted_time(local_time), local_time + 92);

        {
            let mut peers = net.peers.write().unwrap();
            peers.insert(([127, 0, 0, 2], 8333).into(), TestPeer { time_offset: -32 });
        }
        assert_eq!(net.network_adjusted_time(local_time), local_time + 30);

        {
            let mut peers = net.peers.write().unwrap();
            peers.insert(([127, 0, 0, 3], 8333).into(), TestPeer { time_offset: 66 });
        }
        assert_eq!(net.network_adjusted_time(local_time), local_time + 66);

        {
            let mut peers = net.peers.write().unwrap();
            peers.insert(([127, 0, 0, 4], 8333).into(), TestPeer { time_offset: -78 });
        }
        assert_eq!(net.network_adjusted_time(local_time), local_time + 17);

        {
            let mut peers = net.peers.write().unwrap();
            peers.insert(([127, 0, 0, 5], 8333).into(), TestPeer { time_offset: -1 });
        }
        assert_eq!(net.network_adjusted_time(local_time), local_time - 1);

        {
            let mut peers = net.peers.write().unwrap();
            [
                (([127, 0, 0, 6], 8333), 43818),
                (([127, 0, 0, 7], 8333), 91234),
                (([127, 0, 0, 8], 8333), 38183),
                (([127, 0, 0, 9], 8333), 8961),
                (([127, 0, 0, 0], 8333), 9131),
            ]
            .iter()
            .cloned()
            .for_each(|(addr, time_offset)| {
                peers.insert(addr.into(), TestPeer { time_offset });
            });
        }
        assert_eq!(
            net.network_adjusted_time(local_time),
            local_time + MAX_TIME_ADJUSTMENT as Time
        );

        {
            let mut peers = net.peers.write().unwrap();
            [
                (([127, 0, 0, 6], 8333), -43818),
                (([127, 0, 0, 7], 8333), -91234),
                (([127, 0, 0, 8], 8333), -38183),
                (([127, 0, 0, 9], 8333), -8961),
                (([127, 0, 0, 0], 8333), -9131),
            ]
            .iter()
            .cloned()
            .for_each(|(addr, time_offset)| {
                peers.insert(addr.into(), TestPeer { time_offset });
            });
        }
        assert_eq!(
            net.network_adjusted_time(local_time),
            local_time - MAX_TIME_ADJUSTMENT as Time
        );
    }
}
