//! SPV client.

#[allow(missing_docs)]
pub mod blockmgr;
#[allow(missing_docs)]
pub mod filtermgr;

#[allow(missing_docs)]
pub mod event;
#[allow(missing_docs)]
pub mod handle;
#[allow(missing_docs)]
pub mod watchlist;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::{fmt, net, time};

use thiserror::Error;

use bitcoin::{OutPoint, Transaction, TxOut, Txid};

use nakamoto_common::block::filter::BlockFilter;
use nakamoto_common::block::{BlockHash, Height};
use nakamoto_p2p as p2p;

use blockmgr::BlockManager;
use filtermgr::FilterManager;

use crate::client::{self, chan, Mempool};
use event::Event;
use watchlist::Watchlist;

type Utxos = HashMap<OutPoint, TxOut>;

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub enum TxStatus {
    /// This is the initial state of a transaction after it has been announced by the
    /// client.
    Unconfirmed,
    /// Transaction was acknowledged by a peer.
    /// This is the case when a peer requests the transaction data from us after an inventory
    /// announcement.
    Acknowledged { peer: net::SocketAddr },
    /// The transaction was included in a block. This event is fired after
    /// a block from the main chain is scanned.
    Confirmed { height: Height, block: BlockHash },
    /// A transaction that was previously confirmed, and is now reverted due to a
    /// re-org. Note that this event can only fire if the originally confirmed tx
    /// is still in memory.
    Reverted,
    /// After `Reverted` is received, the transaction could be double-spent in a
    /// conflicting block on the best chain. As with `Reverted`, this event can
    /// only fire of the confirmed transaction is still in memory.
    Stale { replaced_by: Txid, block: BlockHash },
}

impl fmt::Display for TxStatus {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfirmed => write!(fmt, "transaction is unconfirmed"),
            Self::Acknowledged { peer } => {
                write!(fmt, "transaction was acknowledged by peer {}", peer)
            }
            Self::Confirmed { height, block } => write!(
                fmt,
                "transaction was included in block {} at height {}",
                block, height
            ),
            Self::Reverted => write!(fmt, "transaction has been reverted"),
            Self::Stale { replaced_by, block } => write!(
                fmt,
                "transaction was replaced by {} in block {}",
                replaced_by, block
            ),
        }
    }
}

#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum Error {
    #[error("mempool lock is poisoned")]
    Mempool,

    #[error("watchlist lock is poisoned")]
    Watchlist,

    #[error("client handle error: {0}")]
    Client(#[from] client::handle::Error),
}

impl<'a> From<PoisonError<MutexGuard<'a, Mempool>>> for Error {
    fn from(_: PoisonError<MutexGuard<'a, Mempool>>) -> Self {
        Self::Mempool
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, Watchlist>>> for Error {
    fn from(_: PoisonError<MutexGuard<'a, Watchlist>>) -> Self {
        Self::Watchlist
    }
}

#[allow(missing_docs)]
pub enum Command {
    Rescan {
        from: Option<Height>,
        to: Option<Height>,
    },
    Submit {
        transactions: Vec<Transaction>,
    },
}

#[allow(missing_docs)]
pub struct Handle {
    commands: chan::Sender<Command>,
    subscriber: p2p::event::Subscriber<Event>,
    timeout: time::Duration,
    watchlist: Arc<Mutex<Watchlist>>,
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            subscriber: self.subscriber.clone(),
            timeout: self.timeout,
            watchlist: self.watchlist.clone(),
        }
    }
}

#[allow(missing_docs)]
pub struct Config {
    genesis: Height,
}

#[allow(missing_docs)]
#[allow(dead_code)]
pub struct Client<H: client::handle::Handle> {
    client: H,
    handle: Handle,
    publisher: p2p::event::Broadcast<Event, Event>,
    control: chan::Receiver<Command>,
    mempool: Arc<Mutex<Mempool>>,
    utxos: Utxos,
    config: Config,
    height: Height,

    blockmgr: BlockManager<H>,
    filtermgr: FilterManager<H>,
}

impl<H: client::handle::Handle> Client<H> {
    #[allow(missing_docs)]
    pub fn new(client: H, config: Config) -> Self {
        let (publisher, subscriber) = p2p::event::broadcast(Some);
        let (commands, control) = chan::unbounded::<Command>();
        let mempool = client.mempool();
        let timeout = time::Duration::from_secs(9);
        let watchlist = Arc::new(Mutex::new(Watchlist::new()));
        let handle = Handle {
            commands,
            subscriber,
            timeout,
            watchlist,
        };
        let height = config.genesis;
        let blockmgr = BlockManager::new(height, client.clone());
        let filtermgr = FilterManager::new(height, client.clone());
        let utxos = Utxos::new();

        Self {
            client,
            handle,
            mempool,
            utxos,
            publisher,
            control,
            config,
            height,
            blockmgr,
            filtermgr,
        }
    }

    #[allow(missing_docs)]
    pub fn run(mut self) -> Result<(), Error> {
        let events = self.client.events();
        let blocks = self.client.blocks();
        let filters = self.client.filters();

        loop {
            chan::select! {
                recv(events) -> event => {
                    // Forward to event subscribers.
                    if let Ok(event) = event {
                        self.process_event(event);
                    } else {
                        todo!()
                    }
                }
                recv(self.control) -> command => {
                    if let Ok(command) = command {
                        self.process_command(command);
                    } else {
                        todo!()
                    }
                }
                recv(filters) -> msg => {
                    if let Ok((filter, block_hash, height)) = msg {
                        self.process_filter(filter, block_hash, height)?;
                    } else {
                        todo!()
                    }
                }
                recv(blocks) -> msg => {
                    if let Ok((block, height)) = msg {
                        let watchlist = self.handle.watchlist.lock()?;
                        self.blockmgr.block_received(
                            block,
                            height,
                            &mut self.utxos,
                            &watchlist,
                            &self.publisher,
                        )?;
                    } else {
                        todo!()
                    }
                }
            }
        }
    }

    /// Calculate the balance of all UTXOs.
    pub fn balance(&self) -> u64 {
        self.utxos.values().map(|u| u.value).sum()
    }

    /// Create a new handle to the SPV client.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    // PRIVATE METHODS /////////////////////////////////////////////////////////

    /// Process client event.
    fn process_event(&mut self, event: client::Event) {
        use p2p::protocol::spvmgr;

        if let client::Event::SpvManager(spvmgr::Event::FilterHeadersImported {
            height,
            block_hash,
        }) = event
        {
            self.filtermgr.headers_imported(height, block_hash).unwrap();
        }
    }

    fn process_filter(
        &mut self,
        filter: BlockFilter,
        block_hash: BlockHash,
        height: Height,
    ) -> Result<(), Error> {
        self.filtermgr.filter_received(filter, block_hash, height);

        // TODO: Deal with unwrap.

        let watchlist = self.handle.watchlist.lock()?;
        while let Some((block_hash, height)) = self.filtermgr.process(&watchlist).unwrap() {
            log::info!("Filter matched at height {}", height);
            log::info!("Fetching block {}", block_hash);

            self.blockmgr.get(block_hash)?;
        }
        Ok(())
    }

    /// Process user command.
    fn process_command(&mut self, command: Command) {
        match command {
            Command::Rescan { .. } => {}
            Command::Submit { .. } => {}
        }
    }
}
