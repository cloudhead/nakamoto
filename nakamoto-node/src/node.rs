use std::io;
use std::net;
use std::path::Path;
use std::sync::Arc;
use std::time::{self, SystemTime};

use crossbeam_channel as chan;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store::{self, Store};
use nakamoto_chain::block::time::AdjustedTime;
use nakamoto_chain::block::tree::BlockTree;
use nakamoto_chain::block::{Block, BlockHash, BlockHeader, Transaction};

use nakamoto_p2p as p2p;
use nakamoto_p2p::address_book::AddressBook;
use nakamoto_p2p::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use nakamoto_p2p::bitcoin::util::hash::BitcoinHash;
use nakamoto_p2p::event::Event;
use nakamoto_p2p::protocol::bitcoin::Command;
use nakamoto_p2p::protocol::bitcoin::{self, Network};
use nakamoto_p2p::reactor::poll::Waker;

use crate::error::Error;
use crate::handle::{self, Handle};

/// Node configuration.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub discovery: bool,
    pub listen: Vec<net::SocketAddr>,
    pub network: Network,
    pub address_book: AddressBook,
    pub timeout: time::Duration,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            discovery: true,
            listen: vec![([127, 0, 0, 1], 0).into()],
            network: Network::default(),
            address_book: AddressBook::default(),
            timeout: time::Duration::from_secs(60),
        }
    }
}

/// A light-node process.
pub struct Node {
    pub config: NodeConfig,

    commands: chan::Receiver<Command>,
    handle: chan::Sender<Command>,
    events: chan::Receiver<Event<NetworkMessage>>,
    reactor: nakamoto_p2p::reactor::poll::Reactor<net::TcpStream, RawNetworkMessage, Command>,
}

impl Node {
    /// Create a new node.
    pub fn new(config: NodeConfig) -> Result<Self, Error> {
        let (handle, commands) = chan::unbounded::<Command>();
        let (subscriber, events) = chan::unbounded::<Event<NetworkMessage>>();
        let reactor = p2p::reactor::poll::Reactor::new(subscriber)?;

        Ok(Self {
            commands,
            events,
            handle,
            reactor,
            config,
        })
    }

    /// Seed the node's address book with peer addresses.
    pub fn seed<S: net::ToSocketAddrs>(&mut self, seeds: Vec<S>) -> Result<(), Error> {
        self.config.address_book.seed(seeds).map_err(Error::from)
    }

    /// Start the node process. This function is meant to be run in its own thread.
    pub fn run(mut self) -> Result<(), Error> {
        let cfg = bitcoin::Config::from(self.config.network, self.config.address_book);
        let genesis = cfg.network.genesis();
        let params = cfg.network.params();

        log::info!("Initializing node ({:?})..", cfg.network);
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

        log::info!("{} peer(s) found..", cfg.address_book.len());
        log::debug!("{:?}", cfg.address_book);
        let protocol = p2p::protocol::Bitcoin::new(cache, clock, cfg);

        if self.config.listen.is_empty() {
            let port = protocol.network.port();
            self.reactor
                .run(protocol, self.commands, &[([0, 0, 0, 0], port).into()])?;
        } else {
            self.reactor
                .run(protocol, self.commands, &self.config.listen)?;
        }

        Ok(())
    }

    /// Start the node process, supplying the block cache. This function is meant to be run in
    /// its own thread.
    pub fn run_with<T: BlockTree>(mut self, cache: T) -> Result<(), Error> {
        let cfg = bitcoin::Config::from(self.config.network, self.config.address_book);

        log::info!("Initializing node ({:?})..", cfg.network);
        log::info!("Genesis block hash is {}", cfg.network.genesis_hash());
        log::info!("Chain height is {}", cache.height());

        let local_time = SystemTime::now().into();
        let clock = AdjustedTime::<net::SocketAddr>::new(local_time);

        log::info!("{} peer(s) found..", cfg.address_book.len());
        log::debug!("{:?}", cfg.address_book);
        let protocol = p2p::protocol::Bitcoin::new(cache, clock, cfg);

        if self.config.listen.is_empty() {
            let port = protocol.network.port();
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
            events: self.events.clone(),
            timeout: self.config.timeout,
        }
    }
}

/// An instance of [`Handle`] for [`Node`].
pub struct NodeHandle {
    commands: chan::Sender<Command>,
    events: chan::Receiver<Event<NetworkMessage>>,
    waker: Arc<Waker>,
    timeout: time::Duration,
}

impl NodeHandle {
    /// Set the timeout for operations that wait on the network.
    pub fn set_timeout(&mut self, timeout: time::Duration) {
        self.timeout = timeout;
    }

    /// Send a command to the command channel, and wake up the event loop.
    fn command(&self, cmd: Command) -> Result<(), handle::Error> {
        self.commands.send(cmd)?;
        self.waker.wake()?;

        Ok(())
    }

    /// Subscribe to the event feed, and wait for the given function to return something,
    /// or timeout if the specified amount of time has elapsed.
    fn wait_for<F, T>(&self, f: F) -> Result<T, handle::Error>
    where
        F: Fn(Event<NetworkMessage>) -> Option<T>,
    {
        let start = time::Instant::now();
        let events = self.events.clone();

        loop {
            if let Some(timeout) = self.timeout.checked_sub(start.elapsed()) {
                match events.recv_timeout(timeout) {
                    Ok(event) => {
                        if let Some(t) = f(event) {
                            return Ok(t);
                        }
                    }
                    Err(chan::RecvTimeoutError::Disconnected) => {
                        return Err(handle::Error::Disconnected);
                    }
                    Err(chan::RecvTimeoutError::Timeout) => {
                        // Keep trying until our timeout reaches zero.
                        continue;
                    }
                }
            } else {
                return Err(handle::Error::Timeout);
            }
        }
    }
}

impl Handle for NodeHandle {
    fn get_tip(&self) -> Result<BlockHeader, handle::Error> {
        let (transmit, receive) = chan::bounded::<BlockHeader>(1);
        self.command(Command::GetTip(transmit))?;

        Ok(receive.recv()?)
    }

    fn get_block(&self, hash: &BlockHash) -> Result<Block, handle::Error> {
        self.command(Command::GetBlock(*hash))?;

        self.wait_for(|e| match e {
            Event::Received(_, NetworkMessage::Block(blk)) if &blk.bitcoin_hash() == hash => {
                Some(blk)
            }
            _ => None,
        })
    }

    fn submit_transaction(&self, _tx: Transaction) -> Result<(), handle::Error> {
        todo!()
    }

    fn wait_for_peers(&self, count: usize) -> Result<(), handle::Error> {
        use std::collections::HashSet;

        self.wait_for(|e| {
            let mut connected = HashSet::new();

            match e {
                Event::Connected(addr) => {
                    connected.insert(addr);

                    if connected.len() == count {
                        Some(())
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
    }

    fn wait_for_ready(&self) -> Result<(), handle::Error> {
        self.wait_for(|e| match e {
            Event::Synced => Some(()),
            _ => None,
        })
    }

    fn shutdown(self) -> Result<(), handle::Error> {
        todo!()
    }
}
