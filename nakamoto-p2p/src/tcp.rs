use nakamoto_chain::blocktree::BlockCache;

use std::net;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use log::*;

use crate::{error, peer};
use crate::{Network, Peer, Peers};

impl Peer<net::TcpStream> {
    /// Connect to a peer given a remote address.
    pub fn dial(
        addr: &net::SocketAddr,
        config: peer::Config,
    ) -> Result<peer::Connection<net::TcpStream>, error::Error> {
        let sock = net::TcpStream::connect(addr)?;

        sock.set_read_timeout(Some(peer::IDLE_TIMEOUT))?;
        sock.set_write_timeout(Some(peer::IDLE_TIMEOUT))?;

        let address = sock.peer_addr()?;
        let local_address = sock.local_addr()?;

        Ok(peer::Connection::from(sock, local_address, address, config))
    }

    fn thread(
        addr: net::SocketAddr,
        config: peer::Config,
        cache: Arc<RwLock<BlockCache>>,
        peers: Arc<RwLock<Peers<net::TcpStream>>>,
        events: mpsc::Sender<peer::Event>,
    ) -> Result<(), error::Error> {
        debug!("Connecting to {}...", &addr);

        let conn = Self::dial(&addr, config)?;
        let addr = conn.address;

        debug!("Connected to {}", &addr);
        trace!("{:#?}", conn);

        Peer::run(addr, conn, cache, peers, events)?;

        debug!("Disconnected from {}", &addr);

        Ok(())
    }
}

impl Network<net::TcpStream> {
    pub fn connect(&mut self, addrs: &[net::SocketAddr]) -> Result<Vec<()>, error::Error> {
        let (tx, rx) = mpsc::channel();
        let mut spawned = Vec::with_capacity(addrs.len());

        for addr in addrs.iter() {
            let cache = self.block_cache.clone();
            let config = self.peer_config.clone();
            let peers = self.peers.clone();
            let addr = addr.clone();
            let tx = tx.clone();

            let handle = thread::spawn(move || Peer::thread(addr, config, cache, peers, tx));

            spawned.push(handle);
        }
        drop(tx); // All transmitters have been given to peers.

        self.listen(rx)?;

        spawned
            .into_iter()
            .flat_map(thread::JoinHandle::join)
            .collect()
    }
}
