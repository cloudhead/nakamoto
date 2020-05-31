pub mod error;
pub mod peer;

use nakamoto_chain::blocktree::BlockCache;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;

use log::*;

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
    ) -> Result<(), error::Error> {
        use std::collections::hash_map::Entry;

        let conn = peer::Connection::new(&addr, config)?;
        let addr = conn.address;

        debug!("Connected to {}", &addr);
        trace!("{:#?}", conn);

        let mut peers = peers.write().expect("lock has not been poisoned");

        match peers.entry(addr) {
            Entry::Vacant(v) => {
                let peer = v.insert(Peer { conn });
                peer.conn.run(cache)?;
            }
            Entry::Occupied(_) => {}
        }
        debug!("Disconnected from {}", &addr);

        Ok(())
    }
}

pub struct Network {
    peer_config: peer::Config,
    peers: Arc<RwLock<Peers<net::TcpStream>>>,
    block_cache: Arc<RwLock<BlockCache>>,
}

impl Network {
    pub fn new(peer_config: peer::Config, block_cache: Arc<RwLock<BlockCache>>) -> Self {
        let peers = Arc::new(RwLock::new(Peers::new()));

        Self {
            peer_config,
            peers,
            block_cache,
        }
    }

    pub fn connect(&self, addrs: &[net::SocketAddr]) -> Result<Vec<()>, error::Error> {
        let mut spawned = Vec::new();

        for addr in addrs.iter() {
            let cache = self.block_cache.clone();
            let config = self.peer_config.clone();
            let peers = self.peers.clone();
            let addr = addr.clone();

            let handle = thread::spawn(move || match Peer::run(addr, config, cache, peers) {
                Ok(result) => return Ok(result),
                Err(err) => panic!(err.to_string()),
            });
            spawned.push(handle);
        }

        spawned
            .into_iter()
            .flat_map(thread::JoinHandle::join)
            .collect()
    }
}
