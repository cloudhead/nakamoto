//! Core nakamoto client functionality. Wraps all the other modules under a unified
//! interface.
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io;
use std::net;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::time::{self, SystemTime};

pub use crossbeam_channel as chan;

use nakamoto_chain::block::{store, Block};
use nakamoto_chain::filter;
use nakamoto_chain::filter::cache::FilterCache;
use nakamoto_chain::{block::cache::BlockCache, filter::BlockFilter};

use nakamoto_common::block::filter::Filters;
use nakamoto_common::block::store::{Genesis as _, Store as _};
use nakamoto_common::block::time::AdjustedTime;
use nakamoto_common::block::tree::{self, BlockTree, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_common::p2p::peer::{Source, Store as _};

pub use nakamoto_common::network::Network;
pub use nakamoto_common::p2p::Domain;

use nakamoto_p2p as p2p;
use nakamoto_p2p::bitcoin::network::constants::ServiceFlags;
use nakamoto_p2p::bitcoin::network::message::NetworkMessage;
use nakamoto_p2p::bitcoin::network::Address;
use nakamoto_p2p::protocol::Protocol;
use nakamoto_p2p::protocol::{self, Link};
use nakamoto_p2p::protocol::{cbfmgr, connmgr, invmgr, peermgr, syncmgr};

pub use nakamoto_p2p::event;
pub use nakamoto_p2p::protocol::{Command, CommandError, Peer};
pub use nakamoto_p2p::reactor::Reactor;

pub use crate::error::Error;
pub use crate::event::Event;
pub use crate::handle;
pub use crate::peer;
pub use crate::spv;

/// Client configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Client listen addresses.
    pub listen: Vec<net::SocketAddr>,
    /// Bitcoin network.
    pub network: Network,
    /// Peers to connect to.
    pub connect: Vec<net::SocketAddr>,
    /// Network domains to connect to.
    pub domains: Vec<Domain>,
    /// Target number of outbound peers to connect to.
    pub target_outbound_peers: usize,
    /// Maximum number of inbound peers supported.
    pub max_inbound_peers: usize,
    /// Timeout duration for client commands.
    pub timeout: time::Duration,
    /// Client home path, where runtime data is stored, eg. block headers and filters.
    pub root: PathBuf,
    /// Client name. Used for logging only.
    pub name: &'static str,
    /// Services offered by this node.
    pub services: ServiceFlags,
    /// Protocol hooks.
    pub hooks: protocol::Hooks,
}

impl Config {
    /// Add seeds to connect to.
    pub fn seed<T: net::ToSocketAddrs + std::fmt::Debug>(&mut self, seeds: &[T]) -> io::Result<()> {
        let connect = seeds
            .iter()
            .flat_map(|seed| match seed.to_socket_addrs() {
                Ok(addrs) => addrs.map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<io::Result<Vec<_>>>()?;

        self.connect.extend(connect);

        Ok(())
    }
}

impl From<Config> for p2p::protocol::Config {
    fn from(cfg: Config) -> Self {
        Self {
            network: cfg.network,
            target: cfg.name,
            connect: cfg.connect,
            target_outbound_peers: cfg.target_outbound_peers,
            max_inbound_peers: cfg.max_inbound_peers,
            ..Self::default()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: vec![([0, 0, 0, 0], 0).into()],
            network: Network::default(),
            connect: Vec::new(),
            domains: Domain::all(),
            timeout: time::Duration::from_secs(60),
            root: PathBuf::from(env::var("HOME").unwrap_or_default()),
            target_outbound_peers: p2p::protocol::connmgr::TARGET_OUTBOUND_PEERS,
            max_inbound_peers: p2p::protocol::connmgr::MAX_INBOUND_PEERS,
            services: ServiceFlags::NONE,
            name: "self",
            hooks: protocol::Hooks::default(),
        }
    }
}

/// The client's event publisher.
pub struct Publisher {
    publishers: Vec<Box<dyn protocol::event::Publisher>>,
}

impl Publisher {
    fn new() -> Self {
        Self {
            publishers: Vec::new(),
        }
    }

    fn register(mut self, publisher: impl protocol::event::Publisher + 'static) -> Self {
        self.publishers.push(Box::new(publisher));
        self
    }
}

impl protocol::event::Publisher for Publisher {
    fn publish(&mut self, e: protocol::Event) {
        for p in self.publishers.iter_mut() {
            p.publish(e.clone());
        }
    }
}

/// A light-client process.
pub struct Client<R: Reactor<Publisher>> {
    /// Client configuration.
    pub config: Config,

    handle: chan::Sender<Command>,
    events: event::Subscriber<protocol::Event>,
    blocks: event::Subscriber<(Block, Height)>,
    filters: event::Subscriber<(BlockFilter, BlockHash, Height)>,
    subscriber: event::Subscriber<Event>,

    reactor: R,
}

impl<R: Reactor<Publisher>> Client<R> {
    /// Create a new client.
    pub fn new(config: Config) -> Result<Self, Error> {
        let (handle, commands) = chan::unbounded::<Command>();
        let (event_pub, events) = event::broadcast(|e, p| p.emit(e));
        let (blocks_pub, blocks) = event::broadcast(|e, p| {
            if let protocol::Event::InventoryManager(invmgr::Event::BlockProcessed {
                block,
                height,
            }) = e
            {
                p.emit((block, height));
            }
        });
        let (filters_pub, filters) = event::broadcast(|e, p| {
            if let protocol::Event::FilterManager(cbfmgr::Event::FilterReceived {
                filter,
                block_hash,
                height,
                ..
            }) = e
            {
                p.emit((filter, block_hash, height));
            }
        });
        let (publisher, subscriber) = event::broadcast({
            // FIXME: Correct tip should be passed, or parameter should be removed.
            let mut spv = crate::spv::Client::new(Default::default());

            move |e, p| spv.process(e, p)
        });

        let publisher = Publisher::new()
            .register(event_pub)
            .register(blocks_pub)
            .register(filters_pub)
            .register(publisher);

        let reactor = R::new(publisher, commands)?;

        Ok(Self {
            events,
            handle,
            reactor,
            config,
            blocks,
            filters,
            subscriber,
        })
    }

    /// Seed the client's address book with peer addresses.
    pub fn seed<S: net::ToSocketAddrs>(&mut self, seeds: Vec<S>) -> Result<(), Error> {
        for seed in seeds.into_iter() {
            let addrs = seed.to_socket_addrs()?;
            self.config.connect.extend(addrs);
        }
        Ok(())
    }

    /// Start the client process. This function is meant to be run in its own thread.
    pub fn run(mut self) -> Result<(), Error> {
        let home = self.config.root.join(".nakamoto");
        let dir = home.join(self.config.network.as_str());
        let listen = self.config.listen.clone();

        fs::create_dir_all(&dir)?;

        let genesis = self.config.network.genesis();
        let params = self.config.network.params();

        log::info!("Initializing client ({:?})..", self.config.network);
        log::info!(
            "Genesis block hash is {}",
            self.config.network.genesis_hash()
        );

        let path = dir.join("headers.db");
        let store = match store::File::create(&path, genesis) {
            Ok(store) => {
                log::info!("Initializing new block store {:?}", path);
                store
            }
            Err(store::Error::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {
                log::info!("Found existing store {:?}", path);
                let store = store::File::open(path, genesis)?;

                if store.check().is_err() {
                    log::warn!("Corruption detected in header store, healing..");
                    store.heal()?; // Rollback store to the last valid header.
                }
                log::info!("Store height = {}", store.height()?);
                log::info!("Loading block headers from store..");

                store
            }
            Err(err) => return Err(err.into()),
        };

        let local_time = SystemTime::now().into();
        let checkpoints = self.config.network.checkpoints().collect::<Vec<_>>();
        let clock = AdjustedTime::<net::SocketAddr>::new(local_time);
        let cache = BlockCache::from(store, params, &checkpoints)?;
        let rng = fastrand::Rng::new();

        log::info!("Initializing block filters..");

        let cfheaders_genesis = filter::cache::StoredHeader::genesis(self.config.network);
        let cfheaders_path = dir.join("filters.db");
        let cfheaders_store = match store::File::create(&cfheaders_path, cfheaders_genesis) {
            Ok(store) => {
                log::info!("Initializing new filter header store {:?}", cfheaders_path);
                store
            }
            Err(store::Error::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {
                log::info!("Found existing store {:?}", cfheaders_path);
                let store = store::File::open(cfheaders_path, cfheaders_genesis)?;

                if store.check().is_err() {
                    log::warn!("Corruption detected in filter store, healing..");
                    store.heal()?; // Rollback store to the last valid header.
                }
                log::info!("Filters height = {}", store.height()?);
                log::info!("Loading filter headers from store..");

                store
            }
            Err(err) => return Err(err.into()),
        };

        let filters = FilterCache::from(cfheaders_store)?;
        log::info!("Verifying filter headers..");
        filters.verify(self.config.network)?; // Verify store integrity.

        log::info!("Loading peer addresses..");

        let peers_path = dir.join("peers.json");
        let mut peers = match peer::Cache::create(&peers_path) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                log::info!("Found existing peer cache {:?}", peers_path);
                let cache = peer::Cache::open(&peers_path).map_err(Error::PeerStore)?;
                log::info!("{} peer(s) found..", cache.len());

                cache
            }
            Err(err) => {
                return Err(Error::PeerStore(err));
            }
            Ok(cache) => {
                log::info!("Initializing new peer address cache {:?}", peers_path);
                cache
            }
        };

        log::trace!("{:#?}", peers);

        if self.config.connect.is_empty() && peers.is_empty() {
            log::info!("Address book is empty. Trying DNS seeds..");
            peers.seed(
                self.config
                    .network
                    .seeds()
                    .iter()
                    .map(|s| (*s, self.config.network.port())),
                Source::Dns,
            )?;
            peers.flush()?;

            log::info!("{} seeds added to address book", peers.len());
        }

        let cfg = p2p::protocol::Config {
            network: self.config.network,
            params: self.config.network.params(),
            target: self.config.name,
            connect: self.config.connect,
            domains: self.config.domains,
            target_outbound_peers: self.config.target_outbound_peers,
            max_inbound_peers: self.config.max_inbound_peers,
            services: self.config.services,
            hooks: self.config.hooks,
            ..p2p::protocol::Config::default()
        };

        self.reactor.run(&listen, move |upstream| {
            Protocol::new(cache, filters, peers, clock, rng, cfg, upstream)
        })?;

        Ok(())
    }

    /// Start the client process, supplying the block cache. This function is meant to be run in
    /// its own thread.
    pub fn run_with<T: BlockTree, F: Filters, P: peer::Store>(
        mut self,
        cache: T,
        filters: F,
        peers: P,
    ) -> Result<(), Error> {
        let cfg = p2p::protocol::Config {
            services: self.config.services,
            hooks: self.config.hooks,
            domains: self.config.domains,
            ..p2p::protocol::Config::from(
                self.config.name,
                self.config.network,
                self.config.connect,
            )
        };

        log::info!("Initializing client ({:?})..", cfg.network);
        log::info!("Genesis block hash is {}", cfg.network.genesis_hash());
        log::info!("Chain height is {}", cache.height());

        let local_time = SystemTime::now().into();
        let clock = AdjustedTime::<net::SocketAddr>::new(local_time);
        let rng = fastrand::Rng::new();

        log::info!("{} peer(s) found..", peers.len());

        self.reactor.run(&self.config.listen, |upstream| {
            Protocol::new(cache, filters, peers, clock, rng, cfg, upstream)
        })?;

        Ok(())
    }

    /// Create a new handle to communicate with the client.
    pub fn handle(&self) -> Handle<R> {
        Handle {
            network: self.config.network,
            events: self.events.clone(),
            waker: self.reactor.waker(),
            commands: self.handle.clone(),
            timeout: self.config.timeout,
            blocks: self.blocks.clone(),
            filters: self.filters.clone(),
            subscriber: self.subscriber.clone(),
        }
    }
}

/// An instance of [`handle::Handle`] for [`Client`].
pub struct Handle<R: Reactor<Publisher>> {
    network: Network,
    commands: chan::Sender<Command>,
    events: event::Subscriber<protocol::Event>,
    blocks: event::Subscriber<(Block, Height)>,
    filters: event::Subscriber<(BlockFilter, BlockHash, Height)>,
    subscriber: event::Subscriber<Event>,
    waker: R::Waker,
    timeout: time::Duration,
}

impl<R: Reactor<Publisher>> Clone for Handle<R>
where
    R::Waker: Sync,
{
    fn clone(&self) -> Self {
        Self {
            network: self.network,
            blocks: self.blocks.clone(),
            commands: self.commands.clone(),
            events: self.events.clone(),
            filters: self.filters.clone(),
            subscriber: self.subscriber.clone(),
            timeout: self.timeout,
            waker: self.waker.clone(),
        }
    }
}

impl<R: Reactor<Publisher>> Handle<R>
where
    R::Waker: Sync,
{
    /// Set the timeout for operations that wait on the network.
    pub fn set_timeout(&mut self, timeout: time::Duration) {
        self.timeout = timeout;
    }

    /// Get connected peers.
    pub fn get_peers(
        &self,
        services: impl Into<ServiceFlags>,
    ) -> Result<HashSet<SocketAddr>, handle::Error> {
        let (sender, recvr) = chan::bounded(1);
        self._command(Command::GetPeers(services.into(), sender))?;

        Ok(recvr.recv()?)
    }

    /// Get block by height.
    pub fn get_block_by_height(
        &self,
        height: Height,
    ) -> Result<Option<BlockHeader>, handle::Error> {
        let (sender, recvr) = chan::bounded(1);
        self._command(Command::GetBlockByHeight(height, sender))?;

        Ok(recvr.recv()?)
    }

    /// Send a command to the command channel, and wake up the event loop.
    fn _command(&self, cmd: Command) -> Result<(), handle::Error> {
        self.commands.send(cmd)?;
        R::wake(&self.waker)?;

        Ok(())
    }
}

impl<R: Reactor<Publisher>> handle::Handle for Handle<R>
where
    R::Waker: Sync,
{
    fn network(&self) -> Network {
        self.network
    }

    fn get_tip(&self) -> Result<(Height, BlockHeader), handle::Error> {
        let (transmit, receive) = chan::bounded::<(Height, BlockHeader)>(1);
        self.command(Command::GetTip(transmit))?;

        Ok(receive.recv()?)
    }

    fn get_block(&self, hash: &BlockHash) -> Result<(), handle::Error> {
        self.command(Command::GetBlock(*hash))?;

        Ok(())
    }

    fn get_filters(&self, range: RangeInclusive<Height>) -> Result<(), handle::Error> {
        assert!(
            !range.is_empty(),
            "client::Handle::get_filters: range cannot be empty"
        );
        let (transmit, receive) = chan::bounded(1);
        self.command(Command::GetFilters(range, transmit))?;

        receive.recv()?.map_err(handle::Error::GetFilters)
    }

    fn blocks(&self) -> chan::Receiver<(Block, Height)> {
        self.blocks.subscribe()
    }

    fn filters(&self) -> chan::Receiver<(BlockFilter, BlockHash, Height)> {
        self.filters.subscribe()
    }

    fn subscribe(&self) -> chan::Receiver<Event> {
        self.subscriber.subscribe()
    }

    fn command(&self, cmd: Command) -> Result<(), handle::Error> {
        self._command(cmd)
    }

    fn broadcast(
        &self,
        msg: NetworkMessage,
        predicate: fn(Peer) -> bool,
    ) -> Result<Vec<net::SocketAddr>, handle::Error> {
        let (transmit, receive) = chan::bounded(1);
        self.command(Command::Broadcast(msg, predicate, transmit))?;

        Ok(receive.recv()?)
    }

    fn query(&self, msg: NetworkMessage) -> Result<Option<net::SocketAddr>, handle::Error> {
        let (transmit, receive) = chan::bounded::<Option<net::SocketAddr>>(1);
        self.command(Command::Query(msg, transmit))?;

        Ok(receive.recv()?)
    }

    fn connect(&self, addr: net::SocketAddr) -> Result<Link, handle::Error> {
        let events = self.events();
        self.command(Command::Connect(addr))?;

        event::wait(
            &events,
            |e| match e {
                protocol::Event::ConnManager(connmgr::Event::Connected(a, link))
                    if a == addr || (addr.ip().is_unspecified() && a.port() == addr.port()) =>
                {
                    Some(link)
                }
                _ => None,
            },
            self.timeout,
        )
        .map_err(handle::Error::from)
    }

    fn disconnect(&self, addr: net::SocketAddr) -> Result<(), handle::Error> {
        let events = self.events();

        self.command(Command::Disconnect(addr))?;
        event::wait(
            &events,
            |e| match e {
                protocol::Event::ConnManager(connmgr::Event::Disconnected(a))
                    if a == addr || (addr.ip().is_unspecified() && a.port() == addr.port()) =>
                {
                    Some(())
                }
                _ => None,
            },
            self.timeout,
        )?;

        Ok(())
    }

    fn import_headers(
        &self,
        headers: Vec<BlockHeader>,
    ) -> Result<Result<ImportResult, tree::Error>, handle::Error> {
        let (transmit, receive) = chan::bounded::<Result<ImportResult, tree::Error>>(1);
        self.command(Command::ImportHeaders(headers, transmit))?;

        Ok(receive.recv()?)
    }

    fn import_addresses(&self, addrs: Vec<Address>) -> Result<(), handle::Error> {
        self.command(Command::ImportAddresses(addrs))?;

        Ok(())
    }

    fn submit_transactions(
        &self,
        txs: Vec<Transaction>,
    ) -> Result<NonEmpty<net::SocketAddr>, handle::Error> {
        let (transmit, receive) = chan::bounded(1);
        self.command(Command::SubmitTransactions(txs, transmit))?;

        receive.recv()?.map_err(handle::Error::Command)
    }

    fn wait<F, T>(&self, f: F) -> Result<T, handle::Error>
    where
        F: FnMut(protocol::Event) -> Option<T>,
    {
        let events = self.events();
        let result = event::wait(&events, f, self.timeout)?;

        Ok(result)
    }

    fn wait_for_peers(
        &self,
        count: usize,
        required_services: impl Into<ServiceFlags>,
    ) -> Result<(), handle::Error> {
        let events = self.events();
        let required_services = required_services.into();

        let mut negotiated = self.get_peers(required_services)?; // Get already connected peers.

        if negotiated.len() == count {
            return Ok(());
        }

        event::wait(
            &events,
            |e| match e {
                protocol::Event::PeerManager(peermgr::Event::PeerNegotiated { addr, services }) => {
                    if services.has(required_services) {
                        negotiated.insert(addr);
                    }

                    if negotiated.len() == count {
                        Some(())
                    } else {
                        None
                    }
                }
                _ => None,
            },
            self.timeout,
        )?;

        Ok(())
    }

    fn wait_for_ready(&self) -> Result<(), handle::Error> {
        let events = self.events();
        event::wait(
            &events,
            |e| match e {
                protocol::Event::SyncManager(syncmgr::Event::Synced(_, _)) => Some(()),
                _ => None,
            },
            self.timeout,
        )?;

        Ok(())
    }

    fn wait_for_height(&self, h: Height) -> Result<BlockHash, handle::Error> {
        let events = self.events();

        match self.get_block_by_height(h)? {
            Some(e) => Ok(e.block_hash()),
            None => event::wait(
                &events,
                |e| match e {
                    protocol::Event::SyncManager(syncmgr::Event::HeadersImported(
                        ImportResult::TipChanged(_, hash, height, _),
                    )) if height == h => Some(hash),

                    _ => None,
                },
                self.timeout,
            )
            .map_err(handle::Error::from),
        }
    }

    fn events(&self) -> chan::Receiver<protocol::Event> {
        self.events.subscribe()
    }

    fn shutdown(self) -> Result<(), handle::Error> {
        self.command(Command::Shutdown)?;

        Ok(())
    }
}
