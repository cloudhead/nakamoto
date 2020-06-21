use nakamoto_chain::block::tree::BlockTree;

use std::net;
use std::sync::{mpsc, Arc, RwLock};
use std::thread;

use log::*;

use crate::address_book::AddressBook;
use crate::{error, peer};
use crate::{Network, Peers};

/// Stack size for spawned threads, in bytes.
/// Since we're creating a thread per peer, we want to keep the stack size small.
const THREAD_STACK_SIZE: usize = 1024 * 1024;

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

    Ok(peer::Connection::from(
        sock,
        local_address,
        address,
        peer::Link::Outbound,
        config,
    ))
}

fn thread<T: BlockTree>(
    addr: net::SocketAddr,
    config: peer::Config,
    cache: Arc<RwLock<T>>,
    peers: Arc<RwLock<Peers<peer::Connection<net::TcpStream>>>>,
    events: mpsc::Sender<peer::Event>,
) -> Result<(), error::Error> {
    debug!("Connecting to {}...", &addr);

    let conn = dial(&addr, config)?;
    let addr = conn.address;

    debug!("Connected to {}", &addr);
    trace!("{:#?}", conn);

    crate::run(addr, conn, cache, peers, events)?;

    debug!("Disconnected from {}", &addr);

    Ok(())
}

impl<T: BlockTree + Sync + Send + 'static> Network<T, peer::Connection<net::TcpStream>> {
    pub fn connect(&mut self, peers: AddressBook) -> Result<Vec<()>, error::Error> {
        let (tx, rx) = mpsc::channel();
        let mut spawned = Vec::with_capacity(peers.len());

        for addr in peers.iter() {
            let cache = self.block_cache.clone();
            let config = self.peer_config;
            let peers = self.peers.clone();
            let addr = *addr;
            let tx = tx.clone();

            let handle = thread::Builder::new()
                .name(addr.to_string())
                .stack_size(THREAD_STACK_SIZE)
                .spawn(move || self::thread(addr, config, cache, peers, tx))?;

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
