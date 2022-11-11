//! Core nakamoto client functionality. Wraps all the other modules under a unified
//! interface.
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::net;
use std::ops::ControlFlow;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::time::{self, SystemTime};

pub use crossbeam_channel as chan;

use nakamoto_chain::block::{store, Block};
use nakamoto_chain::filter;
use nakamoto_chain::filter::cache::FilterCache;
use nakamoto_chain::filter::cache::StoredHeader;
use nakamoto_chain::{block::cache::BlockCache, filter::BlockFilter};

use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::bitcoin::network::Address;
use nakamoto_common::bitcoin::util::uint::Uint256;
use nakamoto_common::block::store::{Genesis as _, Store as _};
use nakamoto_common::block::time::{AdjustedTime, RefClock};
use nakamoto_common::block::tree::{self, BlockReader, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_common::p2p::peer::{Source, Store as _};
use nakamoto_p2p::fsm;

pub use nakamoto_common::network;
pub use nakamoto_common::network::Network;
pub use nakamoto_common::p2p::Domain;
pub use nakamoto_net::event;
pub use nakamoto_p2p::fsm::{Command, CommandError, Hooks, Limits, Link, Peer};

pub use crate::error::Error;
pub use crate::event::{Event, Loading};
pub use crate::handle;
pub use crate::service::Service;

use crate::event::Mapper;
use crate::peer;
use nakamoto_net::{Reactor, Waker};

/// Client configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Bitcoin network.
    pub network: Network,
    /// Connect via these network domains, eg. IPv4, IPv6.
    pub domains: Vec<Domain>,
    /// Peers to connect to instead of using the peer discovery mechanism.
    pub connect: Vec<net::SocketAddr>,
    /// Client listen addresses.
    pub listen: Vec<net::SocketAddr>,
    /// Client home path, where runtime data is stored, eg. block headers and filters.
    pub root: PathBuf,
    /// Verify on-disk data at load time.
    /// This can be set to `true` for additional checks, if for example data integrity
    /// of the file system is not guaranteed, or the file system is untrusted.
    pub verify: bool,
    /// User agent string.
    pub user_agent: &'static str,
    /// Client hooks.
    pub hooks: Hooks,
    /// Services offered by this node.
    pub services: ServiceFlags,
    /// Configured limits.
    pub limits: Limits,
}

/// Configuration for loading event handling.
#[derive(Default)]
pub enum LoadingHandler {
    /// Ignore events.
    #[default]
    Ignore,
    /// Send events to given channel.
    Channel(chan::Sender<Loading>),
}

impl From<chan::Sender<Loading>> for LoadingHandler {
    fn from(c: chan::Sender<Loading>) -> Self {
        Self::Channel(c)
    }
}

impl LoadingHandler {
    fn send(&self, event: Loading) -> ControlFlow<()> {
        match self {
            Self::Ignore => ControlFlow::Continue(()),
            Self::Channel(channel) => {
                if channel.send(event).is_ok() {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            }
        }
    }
}

impl Config {
    /// Create a new configuration for the given network.
    pub fn new(network: Network) -> Self {
        Self {
            network,
            ..Self::default()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: Network::default(),
            connect: Vec::new(),
            domains: Domain::all(),
            listen: vec![([0, 0, 0, 0], 0).into()],
            root: PathBuf::from(env::var("HOME").unwrap_or_default()),
            verify: false,
            user_agent: fsm::USER_AGENT,
            hooks: Hooks::default(),
            limits: Limits::default(),
            services: ServiceFlags::NONE,
        }
    }
}

/// The client's event publisher.
struct Publisher<E> {
    publishers: Vec<Box<dyn nakamoto_net::Publisher<E>>>,
}

impl<E> Publisher<E> {
    /// Register a publisher.
    pub fn register(mut self, publisher: impl nakamoto_net::Publisher<E> + 'static) -> Self {
        self.publishers.push(Box::new(publisher));
        self
    }
}

impl<E> Default for Publisher<E> {
    fn default() -> Self {
        Self {
            publishers: Vec::new(),
        }
    }
}

impl<E> nakamoto_net::Publisher<E> for Publisher<E>
where
    E: Clone,
{
    fn publish(&mut self, e: E) {
        for p in self.publishers.iter_mut() {
            p.publish(e.clone());
        }
    }
}

/// Runs a pre-loaded client.
pub struct ClientRunner<R> {
    service: Service<
        BlockCache<store::File<BlockHeader>>,
        FilterCache<store::File<StoredHeader>>,
        peer::Cache,
        RefClock<AdjustedTime<net::SocketAddr>>,
    >,
    listen: Vec<net::SocketAddr>,
    commands: chan::Receiver<Command>,
    publisher: Publisher<fsm::Event>,
    reactor: R,
}

impl<R: Reactor> ClientRunner<R> {
    /// Run a pre-loaded client.
    pub fn run(mut self) -> Result<(), Error> {
        self.reactor
            .run(&self.listen, self.service, self.publisher, self.commands)?;

        Ok(())
    }
}

/// A light-client process.
pub struct Client<R: Reactor> {
    handle: Handle<R::Waker>,
    commands: chan::Receiver<Command>,
    publisher: Publisher<fsm::Event>,
    reactor: R,
}

impl<R: Reactor> Client<R> {
    /// Create a new client.
    pub fn new() -> Result<Self, Error> {
        let (commands_tx, commands_rx) = chan::unbounded::<Command>();
        let (event_pub, events) = event::broadcast(|e, p| p.emit(e));
        let (blocks_pub, blocks) = event::broadcast(|e, p| {
            if let fsm::Event::Inventory(fsm::InventoryEvent::BlockProcessed {
                block,
                height,
                ..
            }) = e
            {
                p.emit((block, height));
            }
        });
        let (filters_pub, filters) = event::broadcast(|e, p| {
            if let fsm::Event::Filter(fsm::FilterEvent::FilterReceived {
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
            let mut mapper = Mapper::default();
            move |e, p| mapper.process(e, p)
        });

        let publisher = Publisher::default()
            .register(event_pub)
            .register(blocks_pub)
            .register(filters_pub)
            .register(publisher);

        let (shutdown, shutdown_recv) = chan::bounded(1);
        let (listening_send, listening) = chan::bounded(1);
        let reactor = <R as Reactor>::new(shutdown_recv, listening_send)?;
        let handle = Handle {
            commands: commands_tx,
            events,
            blocks,
            filters,
            subscriber,
            waker: reactor.waker(),
            timeout: time::Duration::from_secs(60),
            shutdown,
            listening,
        };

        Ok(Self {
            handle,
            commands: commands_rx,
            publisher,
            reactor,
        })
    }

    /// Load the client configuration. Takes a loading handler that can optionally receive
    /// loading events.
    pub fn load(
        self,
        config: Config,
        loading: impl Into<LoadingHandler>,
    ) -> Result<ClientRunner<R>, Error> {
        let loading = loading.into();
        let home = config.root.join(".nakamoto");
        let network = config.network;
        let dir = home.join(network.as_str());
        let listen = config.listen.clone();

        fs::create_dir_all(&dir)?;

        let genesis = network.genesis();
        let params = network.params();

        log::info!(target: "client", "Initializing client ({:?})..", network);
        log::info!(target: "client", "Genesis block hash is {}", network.genesis_hash());

        let path = dir.join("headers.db");
        let store = match store::File::create(&path, genesis) {
            Ok(store) => {
                log::info!(target: "client", "Initializing new block store {:?}", path);
                store
            }
            Err(store::Error::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {
                log::info!(target: "client", "Found existing store {:?}", path);
                let store = store::File::open(path, genesis)?;

                if store.check().is_err() {
                    log::warn!(target: "client", "Corruption detected in header store, healing..");
                    store.heal()?; // Rollback store to the last valid header.
                }
                log::info!(target: "client", "Store height = {}", store.height()?);

                store
            }
            Err(err) => return Err(err.into()),
        };

        let local_time = SystemTime::now().into();
        let checkpoints = network.checkpoints().collect::<Vec<_>>();
        let clock = AdjustedTime::<net::SocketAddr>::new(local_time);
        let rng = fastrand::Rng::new();

        log::info!(target: "client", "Loading block headers from store..");

        let cache = BlockCache::new(store, params, &checkpoints)?
            .load_with(|height| loading.send(Loading::BlockHeaderLoaded { height }))?;

        log::info!(target: "client", "Initializing block filters..");

        let cfheaders_genesis = filter::cache::StoredHeader::genesis(network);
        let cfheaders_path = dir.join("filters.db");
        let cfheaders_store = match store::File::create(&cfheaders_path, cfheaders_genesis) {
            Ok(store) => {
                log::info!(target: "client", "Initializing new filter header store {:?}", cfheaders_path);
                store
            }
            Err(store::Error::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {
                log::info!(target: "client", "Found existing store {:?}", cfheaders_path);
                let store = store::File::open(cfheaders_path, cfheaders_genesis)?;

                if store.check().is_err() {
                    log::warn!(target: "client", "Corruption detected in filter store, healing..");
                    store.heal()?; // Rollback store to the last valid header.
                }
                log::info!(target: "client", "Filters height = {}", store.height()?);

                store
            }
            Err(err) => return Err(err.into()),
        };
        log::info!(target: "client", "Loading filter headers from store..");

        let filters = FilterCache::load_with(cfheaders_store, |height| {
            loading.send(Loading::FilterHeaderLoaded { height })
        })?;

        if config.verify {
            log::info!(target: "client", "Verifying filter headers..");

            filters.verify_with(network, |height| {
                loading.send(Loading::FilterHeaderVerified { height })
            })?; // Verify store integrity.
        } else {
            log::info!(target: "client", "Skipping filter header verification (verify = false)")
        }

        log::info!(target: "client", "Loading peer addresses..");

        let peers_path = dir.join("peers.json");
        let mut peers = match peer::Cache::create(&peers_path) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                log::info!(target: "client", "Found existing peer cache {:?}", peers_path);
                let cache = peer::Cache::open(&peers_path).map_err(Error::PeerStore)?;
                let cfpeers = cache
                    .iter()
                    .filter(|(_, ka)| ka.addr.services.has(ServiceFlags::COMPACT_FILTERS))
                    .count();

                log::info!(
                    target: "client",
                    "{} peer(s) found.. {} with compact filters support",
                    cache.len(),
                    cfpeers
                );
                cache
            }
            Err(err) => {
                return Err(Error::PeerStore(err));
            }
            Ok(cache) => {
                log::info!(target: "client", "Initializing new peer address cache {:?}", peers_path);
                cache
            }
        };

        log::trace!(target: "client", "{:#?}", peers);

        if config.connect.is_empty() && peers.is_empty() {
            log::info!(target: "client", "Address book is empty. Trying DNS seeds..");
            peers.seed(
                network.seeds().iter().map(|s| (*s, network.port())),
                Source::Dns,
            )?;
            peers.flush()?;

            log::info!(target: "client", "{} seeds added to address book", peers.len());
        }

        Ok(ClientRunner {
            listen,
            commands: self.commands,
            publisher: self.publisher,
            reactor: self.reactor,
            service: Service::new(cache, filters, peers, RefClock::from(clock), rng, config),
        })
    }

    /// Start the client process. This function is meant to be run in its own thread.
    pub fn run(self, config: Config) -> Result<(), Error> {
        self.load(config, LoadingHandler::Ignore)?.run()
    }

    /// Start the client process, supplying the service manually.
    /// This function is meant to be run in its own thread.
    pub fn run_service<T>(mut self, listen: &[net::SocketAddr], service: T) -> Result<(), Error>
    where
        T: nakamoto_net::Service<Event = fsm::Event, Command = Command>,
    {
        self.reactor.run::<T, Publisher<fsm::Event>>(
            listen,
            service,
            self.publisher,
            self.commands,
        )?;

        Ok(())
    }

    /// Create a new handle to communicate with the client.
    pub fn handle(&self) -> Handle<R::Waker> {
        self.handle.clone()
    }
}

/// An instance of [`handle::Handle`] for [`Client`].
pub struct Handle<W: Waker> {
    commands: chan::Sender<Command>,
    events: event::Subscriber<fsm::Event>,
    blocks: event::Subscriber<(Block, Height)>,
    filters: event::Subscriber<(BlockFilter, BlockHash, Height)>,
    subscriber: event::Subscriber<Event>,
    waker: W,
    timeout: time::Duration,
    shutdown: chan::Sender<()>,
    listening: chan::Receiver<net::SocketAddr>,
}

impl<W: Waker> Clone for Handle<W> {
    fn clone(&self) -> Self {
        Self {
            blocks: self.blocks.clone(),
            commands: self.commands.clone(),
            events: self.events.clone(),
            filters: self.filters.clone(),
            subscriber: self.subscriber.clone(),
            timeout: self.timeout,
            waker: self.waker.clone(),
            shutdown: self.shutdown.clone(),
            listening: self.listening.clone(),
        }
    }
}

impl<W: Waker> Handle<W> {
    /// Wait for node to start listening for incoming connections.
    pub fn listening(&mut self) -> Result<net::SocketAddr, handle::Error> {
        Ok(self.listening.recv_timeout(self.timeout)?)
    }

    /// Set the timeout for operations that wait on the network.
    pub fn set_timeout(&mut self, timeout: time::Duration) {
        self.timeout = timeout;
    }

    /// Get connected peers.
    pub fn get_peers(&self, services: impl Into<ServiceFlags>) -> Result<Vec<Peer>, handle::Error> {
        let (sender, recvr) = chan::bounded(1);
        self._command(Command::GetPeers(services.into(), sender))?;

        Ok(recvr.recv()?)
    }

    /// Send a command to the command channel, and wake up the event loop.
    fn _command(&self, cmd: Command) -> Result<(), handle::Error> {
        self.commands.send(cmd)?;
        self.waker.wake()?;

        Ok(())
    }
}

impl<W: Waker> handle::Handle for Handle<W> {
    fn get_tip(&self) -> Result<(Height, BlockHeader, Uint256), handle::Error> {
        let (transmit, receive) = chan::bounded::<(Height, BlockHeader, Uint256)>(1);
        self._command(Command::GetTip(transmit))?;

        Ok(receive.recv()?)
    }

    fn get_block(&self, hash: &BlockHash) -> Result<Option<(Height, BlockHeader)>, handle::Error> {
        let (transmit, receive) = chan::bounded(1);
        self._command(Command::GetBlockByHash(*hash, transmit))?;

        Ok(receive.recv()?)
    }

    fn get_block_by_height(&self, height: Height) -> Result<Option<BlockHeader>, handle::Error> {
        let (sender, recvr) = chan::bounded(1);
        self._command(Command::GetBlockByHeight(height, sender))?;

        Ok(recvr.recv()?)
    }

    fn query_tree(
        &self,
        query: impl Fn(&dyn BlockReader) + Send + Sync + 'static,
    ) -> Result<(), handle::Error> {
        use std::sync::Arc;

        self.command(Command::QueryTree(Arc::new(query)))?;

        Ok(())
    }

    fn find_branch(
        &self,
        to: &BlockHash,
    ) -> Result<Option<(Height, NonEmpty<BlockHeader>)>, handle::Error> {
        let to = *to;
        let (transmit, receive) = chan::bounded(1);

        self.query_tree(move |t| {
            transmit.send(t.find_branch(&to)).ok();
        })?;

        Ok(receive.recv()?)
    }

    fn request_block(&self, hash: &BlockHash) -> Result<(), handle::Error> {
        self.command(Command::RequestBlock(*hash))?;

        Ok(())
    }

    fn request_filters(&self, range: RangeInclusive<Height>) -> Result<(), handle::Error> {
        assert!(
            !range.is_empty(),
            "client::Handle::request_filters: range cannot be empty"
        );
        let (transmit, receive) = chan::bounded(1);
        self.command(Command::RequestFilters(range, transmit))?;

        receive.recv()?.map_err(handle::Error::GetFilters)
    }

    fn blocks(&self) -> chan::Receiver<(Block, Height)> {
        self.blocks.subscribe()
    }

    fn filters(&self) -> chan::Receiver<(BlockFilter, BlockHash, Height)> {
        self.filters.subscribe()
    }

    fn events(&self) -> chan::Receiver<Event> {
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
        let events = self.events.subscribe();
        self.command(Command::Connect(addr))?;

        event::wait(
            &events,
            |e| match e {
                fsm::Event::Peer(fsm::PeerEvent::Connected(a, link))
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
        let events = self.events.subscribe();

        self.command(Command::Disconnect(addr))?;
        event::wait(
            &events,
            |e| match e {
                fsm::Event::Peer(fsm::PeerEvent::Disconnected(a, _))
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

    fn submit_transaction(
        &self,
        tx: Transaction,
    ) -> Result<NonEmpty<net::SocketAddr>, handle::Error> {
        let (transmit, receive) = chan::bounded(1);
        self.command(Command::SubmitTransaction(tx, transmit))?;

        receive.recv()?.map_err(handle::Error::Command)
    }

    fn wait<F, T>(&self, f: F) -> Result<T, handle::Error>
    where
        F: FnMut(fsm::Event) -> Option<T>,
    {
        let events = self.events.subscribe();
        let result = event::wait(&events, f, self.timeout)?;

        Ok(result)
    }

    fn wait_for_peers(
        &self,
        count: usize,
        required_services: impl Into<ServiceFlags>,
    ) -> Result<Vec<(net::SocketAddr, Height, ServiceFlags)>, handle::Error> {
        let events = self.events.subscribe();
        let required_services = required_services.into();

        let negotiated = self.get_peers(required_services)?;
        if negotiated.len() == count {
            return Ok(negotiated
                .into_iter()
                .map(|p| (p.addr, p.height, p.services))
                .collect());
        }

        let mut negotiated = negotiated
            .into_iter()
            .map(|p| (p.addr, (p.height, p.services)))
            .collect::<HashMap<_, _>>(); // Get already connected peers.

        event::wait(
            &events,
            |e| match e {
                fsm::Event::Peer(fsm::PeerEvent::Negotiated {
                    addr,
                    height,
                    services,
                    ..
                }) => {
                    if services.has(required_services) {
                        negotiated.insert(addr, (height, services));
                    }

                    if negotiated.len() == count {
                        Some(negotiated.iter().map(|(a, (h, s))| (*a, *h, *s)).collect())
                    } else {
                        None
                    }
                }
                _ => None,
            },
            self.timeout,
        )
        .map_err(handle::Error::from)
    }

    fn wait_for_height(&self, h: Height) -> Result<BlockHash, handle::Error> {
        let events = self.events.subscribe();

        match self.get_block_by_height(h)? {
            Some(e) => Ok(e.block_hash()),
            None => event::wait(
                &events,
                |e| match e {
                    fsm::Event::Chain(fsm::ChainEvent::Synced(hash, height)) if height == h => {
                        Some(hash)
                    }
                    _ => None,
                },
                self.timeout,
            )
            .map_err(handle::Error::from),
        }
    }

    fn shutdown(self) -> Result<(), handle::Error> {
        self.shutdown.send(())?;
        self.waker.wake()?;

        Ok(())
    }
}

/// Client traits re-exports.
pub mod traits {
    pub use crate::handle::Handle;
}
