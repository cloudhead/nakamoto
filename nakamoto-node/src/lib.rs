use std::io;
use std::net;
use std::path::Path;

use nakamoto_chain as chain;
use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store::{self, Store};
use nakamoto_chain::block::time::AdjustedTime;
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

pub fn run(connect: &[net::SocketAddr]) -> Result<(), Error> {
    log::info!("Initializing daemon..");

    let cfg = p2p::protocol::bitcoin::Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();

    log::info!("Genesis block hash is {}", cfg.network.genesis_hash());

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
    log::info!("Loading blocks from store..");

    if store.check().is_err() {
        log::warn!("Corruption detected in store, healing..");
        store.heal()?; // Rollback store to the last valid header.
    }
    log::info!("Store height = {}", store.height()?);

    let checkpoints = cfg.network.checkpoints().collect::<Vec<_>>();
    let clock = AdjustedTime::<net::SocketAddr>::new();
    let cache = BlockCache::from(store, params, &checkpoints)?;

    let peers = if connect.is_empty() {
        match AddressBook::load("peers") {
            Ok(peers) if peers.is_empty() => {
                log::info!("Address book is empty. Trying DNS seeds..");
                AddressBook::bootstrap(cfg.network.seeds(), cfg.network.port())?
            }
            Ok(peers) => peers,
            Err(err) => {
                return Err(Error::AddressBook(err));
            }
        }
    } else {
        AddressBook::from(connect)?
    };

    log::info!("{} peer(s) found..", peers.len());
    log::debug!("{:?}", peers);

    let protocol = p2p::protocol::Bitcoin::new(cache, clock, cfg);

    p2p::reactor::threaded::run(protocol, peers)?;

    Ok(())
}
