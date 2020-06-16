use std::io;
use std::net;
use std::path::Path;
use std::sync::{Arc, RwLock};

use argh::FromArgs;

use nakamoto_chain as chain;
use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store::{self, Store};
use nakamoto_p2p as p2p;
use nakamoto_p2p::address_book::AddressBook;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    P2p(#[from] p2p::error::Error),
    #[error(transparent)]
    Chain(#[from] chain::block::tree::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("Error loading address book: {0}")]
    AddressBook(io::Error),
    #[error(transparent)]
    BlockStore(#[from] store::Error),
}

#[derive(FromArgs)]
/// A Bitcoin light client.
pub struct Options {
    #[argh(option)]
    /// connect to the specified peers only
    pub connect: Vec<net::SocketAddr>,

    #[argh(switch)]
    /// use the bitcoin test network (default: false)
    pub testnet: bool,

    #[argh(option, default = "log::LevelFilter::Info")]
    /// log level (default: info)
    pub log: log::LevelFilter,
}

impl Options {
    pub fn from_env() -> Self {
        argh::from_env()
    }
}

pub fn run(opts: Options) -> Result<(), Error> {
    log::info!("Initializing daemon..");

    let cfg = p2p::peer::Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();

    log::info!("Genesis block hash is {}", cfg.network.genesis_hash());

    let path = Path::new("headers.db");
    let mut store = match store::File::create(path, genesis) {
        Err(store::Error::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {
            log::info!("Found existing store {:?}", path);
            store::File::open(path)?
        }
        Err(err) => panic!(err.to_string()),
        Ok(store) => {
            log::info!("Initializing new block store {:?}", path);
            store
        }
    };
    log::info!("Loading blocks from store..");

    if store.check().is_err() {
        log::warn!("Corruption detected in store, healing..");
        // Rollback store to the last valid header.
        store.heal()?;
        // If, after healing, our store is empty (which could happen if the previous
        // process terminated while writing the genesis header), we have to try to
        // write it again.
        if store.len()? == 0 {
            store.put(std::iter::once(genesis))?;
        }
    }

    let checkpoints = cfg.network.checkpoints().collect::<Vec<_>>();
    let cache = BlockCache::from(store, params, &checkpoints)?;
    let block_cache = Arc::new(RwLock::new(cache));
    let mut net = p2p::Network::new(cfg, block_cache);

    let peers = if opts.connect.is_empty() {
        match AddressBook::load("peers") {
            Ok(peers) if peers.is_empty() => {
                log::info!("Address book is empty. Trying DNS seeds..");
                AddressBook::bootstrap(cfg.network)?
            }
            Ok(peers) => peers,
            Err(err) => {
                return Err(Error::AddressBook(err));
            }
        }
    } else {
        AddressBook::from(opts.connect.as_slice())?
    };

    log::info!("{} peer(s) found..", peers.len());
    log::debug!("{:?}", peers);

    net.connect(peers)?;

    Ok(())
}
