//! Core nakamoto client functionality. Wraps all the other modules under a unified
//! interface.
use std::collections::HashMap;
use std::collections::HashSet;
use std::env;
use std::fs;
use std::io;
use std::net;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{self, SystemTime};

use crossbeam_channel as chan;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_chain::filter;
use nakamoto_chain::filter::cache::FilterCache;

use nakamoto_common::block::filter::{BlockFilter, Filters};
use nakamoto_common::block::store::{Genesis as _, Store as _};
use nakamoto_common::block::time::AdjustedTime;
use nakamoto_common::block::tree::{self, BlockTree, ImportResult};
use nakamoto_common::block::{Block, BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::p2p::peer::{Source, Store as _};

pub use nakamoto_common::network::Network;

use nakamoto_p2p as p2p;
use nakamoto_p2p::bitcoin::network::constants::ServiceFlags;
use nakamoto_p2p::bitcoin::network::message::NetworkMessage;
use nakamoto_p2p::protocol::Link;
use nakamoto_p2p::protocol::{connmgr, peermgr, spvmgr, syncmgr};
use nakamoto_p2p::protocol::{Command, Protocol};

pub use nakamoto_p2p::event::Event;
pub use nakamoto_p2p::reactor::Reactor;

use crate::error::Error;
use crate::handle;
use crate::peer;

/// Client configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Client listen addresses.
    pub listen: Vec<net::SocketAddr>,
    /// Bitcoin network.
    pub network: Network,
    /// Peers to connect to.
    pub connect: Vec<net::SocketAddr>,
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
            timeout: time::Duration::from_secs(60),
            root: PathBuf::from(env::var("HOME").unwrap_or_default()),
            target_outbound_peers: p2p::protocol::connmgr::TARGET_OUTBOUND_PEERS,
            max_inbound_peers: p2p::protocol::connmgr::MAX_INBOUND_PEERS,
            services: ServiceFlags::NONE,
            name: "self",
        }
    }
}

struct BlockSubscribers {
    subs: HashMap<BlockHash, Vec<chan::Sender<(Block, Height)>>>,
}

impl BlockSubscribers {
    fn new() -> Self {
        Self {
            subs: HashMap::new(),
        }
    }

    fn subscribe(&mut self, hash: BlockHash, channel: chan::Sender<(Block, Height)>) {
        self.subs.entry(hash).or_default().push(channel);
    }

    fn input(&self, block: Block, height: Height) {
        let hash = block.block_hash();

        for (h, subs) in self.subs.iter() {
            if *h == hash {
                for sub in subs {
                    // TODO: Can we avoid the extra clone here? Eg. if there's only one sub.
                    sub.send((block.clone(), height)).unwrap();
                }
            }
        }
    }
}

type FilterSubscriber = chan::Sender<(BlockFilter, BlockHash, Height)>;

struct FilterSubscribers {
    subs: HashMap<Range<Height>, Vec<FilterSubscriber>>,
}

impl FilterSubscribers {
    fn new() -> Self {
        Self {
            subs: HashMap::new(),
        }
    }

    fn subscribe(&mut self, range: Range<Height>, channel: FilterSubscriber) {
        self.subs.entry(range).or_default().push(channel);
    }

    fn input(&self, filter: BlockFilter, block_hash: BlockHash, height: Height) {
        for (range, subs) in self.subs.iter() {
            if range.contains(&height) {
                for sub in subs {
                    sub.send((filter.clone(), block_hash, height)).unwrap();
                }
            }
        }
    }
}

/// A light-client process.
pub struct Client<R> {
    /// Client configuration.
    pub config: Config,

    handle: chan::Sender<Command>,
    events: chan::Receiver<Event>,
    reactor: R,

    blocks: Arc<Mutex<BlockSubscribers>>,
    filters: Arc<Mutex<FilterSubscribers>>,
}

impl<R: Reactor> Client<R> {
    /// Create a new client.
    pub fn new(config: Config) -> Result<Self, Error> {
        let (handle, commands) = chan::unbounded::<Command>();
        let (subscriber, events) = chan::unbounded::<Event>();
        let reactor = R::new(subscriber, commands)?;
        let blocks = Arc::new(Mutex::new(BlockSubscribers::new()));
        let filters = Arc::new(Mutex::new(FilterSubscribers::new()));

        Ok(Self {
            events,
            handle,
            reactor,
            config,
            blocks,
            filters,
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
            target_outbound_peers: self.config.target_outbound_peers,
            max_inbound_peers: self.config.max_inbound_peers,
            services: self.config.services,
            ..p2p::protocol::Config::default()
        };

        self.reactor.on_event({
            let blocks = self.blocks;
            let filters = self.filters;

            move |event| Self::process_event(event, blocks.clone(), filters.clone())
        });
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

        self.reactor.on_event({
            let blocks = self.blocks;
            let filters = self.filters;

            move |event| Self::process_event(event, blocks.clone(), filters.clone())
        });
        self.reactor.run(&self.config.listen, |upstream| {
            Protocol::new(cache, filters, peers, clock, rng, cfg, upstream)
        })?;

        Ok(())
    }

    /// Create a new handle to communicate with the client.
    pub fn handle(&self) -> Handle<R> {
        Handle {
            waker: self.reactor.waker(),
            commands: self.handle.clone(),
            events: self.events.clone(),
            timeout: self.config.timeout,
            blocks: self.blocks.clone(),
            filters: self.filters.clone(),
        }
    }

    ////////////////////////////////////////////////////////////////////////////

    fn process_event(
        event: Event,
        blocks: Arc<Mutex<BlockSubscribers>>,
        filters: Arc<Mutex<FilterSubscribers>>,
    ) {
        match event {
            Event::SyncManager(syncmgr::Event::BlockReceived(_, block, height)) => {
                blocks.lock().unwrap().input(block, height);
            }
            Event::SpvManager(spvmgr::Event::FilterReceived {
                filter,
                block_hash,
                height,
                ..
            }) => {
                filters.lock().unwrap().input(filter, block_hash, height);
            }
            _ => {}
        }
    }
}

/// An instance of [`handle::Handle`] for [`Client`].
pub struct Handle<R: Reactor> {
    commands: chan::Sender<Command>,
    events: chan::Receiver<Event>,
    waker: R::Waker,
    timeout: time::Duration,

    blocks: Arc<Mutex<BlockSubscribers>>,
    filters: Arc<Mutex<FilterSubscribers>>,
}

impl<R: Reactor> Handle<R>
where
    R::Waker: Sync,
{
    /// Set the timeout for operations that wait on the network.
    pub fn set_timeout(&mut self, timeout: time::Duration) {
        self.timeout = timeout;
    }

    /// Get connected peers.
    pub fn get_peers(&self) -> Result<HashSet<SocketAddr>, handle::Error> {
        let (sender, recvr) = chan::bounded(1);
        self._command(Command::GetPeers(sender))?;

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

impl<R: Reactor> handle::Handle for Handle<R>
where
    R::Waker: Sync,
{
    fn get_tip(&self) -> Result<(Height, BlockHeader), handle::Error> {
        let (transmit, receive) = chan::bounded::<(Height, BlockHeader)>(1);
        self.command(Command::GetTip(transmit))?;

        Ok(receive.recv()?)
    }

    fn get_block(
        &self,
        hash: &BlockHash,
        channel: chan::Sender<(Block, Height)>,
    ) -> Result<(), handle::Error> {
        self.blocks.lock().unwrap().subscribe(*hash, channel);
        self.command(Command::GetBlock(*hash))?;

        Ok(())
    }

    fn get_filters(
        &self,
        range: Range<Height>,
        channel: FilterSubscriber,
    ) -> Result<(), handle::Error> {
        assert!(
            !range.is_empty(),
            "client::Handle::get_filters: range cannot be empty"
        );
        self.filters
            .lock()
            .unwrap()
            .subscribe(range.clone(), channel);
        self.command(Command::GetFilters(range))?;

        Ok(())
    }

    fn command(&self, cmd: Command) -> Result<(), handle::Error> {
        self._command(cmd)
    }

    fn broadcast(&self, msg: NetworkMessage) -> Result<(), handle::Error> {
        self.command(Command::Broadcast(msg))
    }

    fn query(&self, msg: NetworkMessage) -> Result<Option<net::SocketAddr>, handle::Error> {
        let (transmit, receive) = chan::bounded::<Option<net::SocketAddr>>(1);
        self.command(Command::Query(msg, transmit))?;

        Ok(receive.recv()?)
    }

    fn connect(&self, addr: net::SocketAddr) -> Result<Link, handle::Error> {
        self.command(Command::Connect(addr))?;
        self.wait(|e| match e {
            Event::ConnManager(connmgr::Event::Connected(a, link))
                if a == addr || (addr.ip().is_unspecified() && a.port() == addr.port()) =>
            {
                Some(link)
            }
            _ => None,
        })
    }

    fn disconnect(&self, addr: net::SocketAddr) -> Result<(), handle::Error> {
        self.command(Command::Disconnect(addr))?;
        self.wait(|e| match e {
            Event::ConnManager(connmgr::Event::Disconnected(a))
                if a == addr || (addr.ip().is_unspecified() && a.port() == addr.port()) =>
            {
                Some(())
            }
            _ => None,
        })
    }

    fn import_headers(
        &self,
        headers: Vec<BlockHeader>,
    ) -> Result<Result<ImportResult, tree::Error>, handle::Error> {
        let (transmit, receive) = chan::bounded::<Result<ImportResult, tree::Error>>(1);
        self.command(Command::ImportHeaders(headers, transmit))?;

        Ok(receive.recv()?)
    }

    fn submit_transaction(&self, tx: Transaction) -> Result<(), handle::Error> {
        self.command(Command::SubmitTransaction(tx))?;

        Ok(())
    }

    /// Subscribe to the event feed, and wait for the given function to return something,
    /// or timeout if the specified amount of time has elapsed.
    fn wait<F, T>(&self, mut f: F) -> Result<T, handle::Error>
    where
        F: FnMut(Event) -> Option<T>,
    {
        let start = time::Instant::now();

        loop {
            if let Some(timeout) = self.timeout.checked_sub(start.elapsed()) {
                match self.events.recv_timeout(timeout) {
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

    fn wait_for_peers(&self, count: usize) -> Result<(), handle::Error> {
        // Get already connected peers.
        let mut negotiated = self.get_peers()?;

        if negotiated.len() == count {
            return Ok(());
        }

        self.wait(|e| match e {
            Event::PeerManager(peermgr::Event::PeerNegotiated { addr }) => {
                negotiated.insert(addr);

                if negotiated.len() == count {
                    Some(())
                } else {
                    None
                }
            }
            _ => None,
        })
    }

    fn wait_for_ready(&self) -> Result<(), handle::Error> {
        self.wait(|e| match e {
            Event::SyncManager(syncmgr::Event::Synced(_, _)) => Some(()),
            _ => None,
        })
    }

    fn wait_for_height(&self, h: Height) -> Result<BlockHash, handle::Error> {
        match self.get_block_by_height(h)? {
            Some(e) => Ok(e.block_hash()),

            None => {
                self.wait(|e| match e {
                    Event::SyncManager(syncmgr::Event::HeadersImported(
                        ImportResult::TipChanged(hash, height, _),
                    )) if height == h => Some(hash),

                    _ => None,
                })
            }
        }
    }

    fn events(&self) -> &chan::Receiver<Event> {
        &self.events
    }

    fn shutdown(self) -> Result<(), handle::Error> {
        self.command(Command::Shutdown)?;

        Ok(())
    }
}
