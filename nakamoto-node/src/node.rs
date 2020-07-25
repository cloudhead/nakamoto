use std::io;
use std::net;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use crossbeam_channel as chan;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store::{self, Store};
use nakamoto_chain::block::time::AdjustedTime;
use nakamoto_chain::block::{BlockHeader, Transaction};
use nakamoto_p2p as p2p;
use nakamoto_p2p::address_book::AddressBook;
use nakamoto_p2p::bitcoin::network::message::RawNetworkMessage;
use nakamoto_p2p::protocol::bitcoin::Command;
use nakamoto_p2p::protocol::bitcoin::{self, Network};
use nakamoto_p2p::reactor::poll::Waker;

use crate::error::Error;
use crate::handle::Handle;

/// Node configuration.
pub struct NodeConfig {
    pub discovery: bool,
    pub listen: Vec<net::SocketAddr>,
    pub network: Network,
    pub address_book: AddressBook,
}

/// A light-node process.
pub struct Node {
    commands: chan::Receiver<Command>,
    handle: chan::Sender<Command>,
    config: NodeConfig,
    reactor: nakamoto_p2p::reactor::poll::Reactor<net::TcpStream, RawNetworkMessage, Command>,
}

impl Node {
    /// Create a new node.
    pub fn new(config: NodeConfig) -> Result<Self, Error> {
        let reactor = p2p::reactor::poll::Reactor::new()?;
        let (handle, commands) = chan::unbounded::<Command>();

        Ok(Self {
            commands,
            handle,
            reactor,
            config,
        })
    }

    pub fn seed<S: net::ToSocketAddrs>(&mut self, _seeds: Vec<S>) {
        todo!()
    }

    /// Start the node process. This function is meant to be
    /// run in a background thread.
    pub fn run(mut self) -> Result<(), Error> {
        let cfg = bitcoin::Config::from(self.config.network);
        let genesis = cfg.network.genesis();
        let params = cfg.network.params();

        log::info!("Initializing daemon ({:?})..", cfg.network);
        log::info!("Genesis block hash is {}", cfg.network.genesis_hash());

        // FIXME: Get path from `NodeConfig`.
        let path = Path::new("headers.db");
        let mut store = match store::File::create(path, genesis) {
            Err(store::Error::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {
                log::info!("Found existing store {:?}", path);
                store::File::open(path, genesis)?
            }
            Err(err) => panic!(err.to_string()),
            Ok(store) => {
                log::info!("Initializing new block store {:?}", path);
                store
            }
        };
        if store.check().is_err() {
            log::warn!("Corruption detected in store, healing..");
            store.heal()?; // Rollback store to the last valid header.
        }
        log::info!("Store height = {}", store.height()?);
        log::info!("Loading blocks from store..");

        let local_time = SystemTime::now().into();
        let checkpoints = cfg.network.checkpoints().collect::<Vec<_>>();
        let clock = AdjustedTime::<net::SocketAddr>::new(local_time);
        let cache = BlockCache::from(store, params, &checkpoints)?;

        log::info!("{} peer(s) found..", self.config.address_book.len());
        log::debug!("{:?}", self.config.address_book);
        let protocol = p2p::protocol::Bitcoin::new(cache, self.config.address_book, clock, cfg);

        if self.config.listen.is_empty() {
            let port = protocol.config.port();
            self.reactor
                .run(protocol, self.commands, &[([0, 0, 0, 0], port).into()])?;
        } else {
            self.reactor
                .run(protocol, self.commands, &self.config.listen)?;
        }

        Ok(())
    }

    /// Create a new handle to communicate with the node.
    pub fn handle(&mut self) -> NodeHandle {
        NodeHandle {
            waker: self.reactor.waker(),
            commands: self.handle.clone(),
        }
    }
}

/// An instance of [`Handle`] for [`Node`].
pub struct NodeHandle {
    commands: chan::Sender<Command>,
    waker: Arc<Waker>,
}

impl Handle for NodeHandle {
    fn get_tip(&self) -> Result<BlockHeader, Error> {
        let (transmit, receive) = chan::bounded::<BlockHeader>(1);
        self.commands.send(Command::GetTip(transmit))?;
        self.waker.wake()?;

        Ok(receive.recv()?)
    }

    fn submit_transaction(&self, _tx: Transaction) -> Result<(), Error> {
        todo!()
    }

    fn wait_for_peers(&self, _count: usize) -> Result<chan::Receiver<()>, Error> {
        todo!()
    }

    fn wait_for_ready(&self) -> Result<chan::Receiver<()>, Error> {
        todo!()
    }

    fn shutdown(self) -> Result<(), Error> {
        todo!()
    }
}
