//! SPV client.
#![allow(clippy::manual_range_contains, clippy::new_without_default)]

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
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::{fmt, net, time};

use thiserror::Error;

use bitcoin::{Block, OutPoint, Transaction, TxOut, Txid};

use nakamoto_common::block::filter::BlockFilter;
use nakamoto_common::block::{BlockHash, Height};
use nakamoto_p2p as p2p;

use blockmgr::BlockManager;
use filtermgr::FilterManager;

use crate::client::{self, chan, Mempool};
use event::Event;
use watchlist::Watchlist;

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub struct Utxos {
    map: HashMap<OutPoint, TxOut>,
}

impl Deref for Utxos {
    type Target = HashMap<OutPoint, TxOut>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for Utxos {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl Utxos {
    /// Create a new empty UTXO set.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Calculate the balance of all UTXOs.
    pub fn balance(&self) -> u64 {
        self.map.values().map(|u| u.value).sum()
    }
}

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
#[derive(Debug, Clone)]
pub enum Command {
    Rescan {
        from: Option<Height>,
        to: Option<Height>,
    },
    Submit {
        transactions: Vec<Transaction>,
    },
    Shutdown(chan::Sender<()>),
}

#[allow(missing_docs)]
pub struct Handle {
    commands: chan::Sender<Command>,
    subscriber: p2p::event::Subscriber<Event>,
    timeout: time::Duration,
    watchlist: Arc<Mutex<Watchlist>>,
    utxos: Arc<Mutex<Utxos>>,
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            subscriber: self.subscriber.clone(),
            timeout: self.timeout,
            watchlist: self.watchlist.clone(),
            utxos: self.utxos.clone(),
        }
    }
}

#[allow(unused_variables)]
impl handle::Handle for Handle {
    fn tip(&self) -> Result<(Height, BlockHash), handle::Error> {
        todo!()
    }

    fn events(&mut self) -> chan::Receiver<Event> {
        self.subscriber.subscribe()
    }

    fn submit(&mut self, txs: impl IntoIterator<Item = Transaction>) {
        todo!()
    }

    fn rescan(&mut self, range: impl std::ops::RangeBounds<Height>) {
        todo!()
    }

    fn watch_address(address: bitcoin::Address) -> bool {
        todo!()
    }

    fn watch_scripts(scripts: impl IntoIterator<Item = bitcoin::ScriptHash>) -> bool {
        todo!()
    }

    fn unwatch_address(address: &bitcoin::Address) {
        todo!()
    }

    fn unwatch_scripts(scripts: impl Iterator<Item = bitcoin::ScriptHash>) {
        todo!()
    }

    fn shutdown(&self) -> Result<(), handle::Error> {
        let (sender, recvr) = chan::bounded(1);
        self.commands.send(Command::Shutdown(sender)).ok();

        Ok(recvr.recv()?)
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
    config: Config,
    height: Height,

    blockmgr: BlockManager<H>,
    filtermgr: FilterManager<H>,
}

impl<H: client::handle::Handle> Client<H> {
    #[allow(missing_docs)]
    pub fn new(client: H, watchlist: Watchlist, config: Config) -> Self {
        let (publisher, subscriber) = p2p::event::broadcast(Some);
        let (commands, control) = chan::unbounded::<Command>();
        let mempool = client.mempool();
        let timeout = time::Duration::from_secs(9);
        let height = config.genesis;
        let watchlist = Arc::new(Mutex::new(watchlist));
        let utxos = Arc::new(Mutex::new(Utxos::new()));
        let blockmgr = BlockManager::new(height, client.clone(), utxos.clone(), watchlist.clone());
        let filtermgr = FilterManager::new(height, client.clone(), watchlist.clone());
        let handle = Handle {
            commands,
            subscriber,
            timeout,
            watchlist,
            utxos,
        };

        Self {
            client,
            handle,
            mempool,
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

        log::debug!("Starting SPV client event loop..");

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
                        if let Command::Shutdown(reply) = command {
                            // Drain incoming block queue before shutting down.
                            // We don't drain the other channels, as they may create
                            // more work.
                            for (blk, h) in blocks.try_iter() {
                                self.process_block(blk, h)?;
                            }
                            reply.send(()).ok();

                            return Ok(());
                        }
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
                        self.process_block(block, height)?;
                    } else {
                        todo!()
                    }
                }
            }
        }
    }

    /// Create a new handle to the SPV client.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    // PRIVATE METHODS /////////////////////////////////////////////////////////

    /// Process client event.
    fn process_event(&mut self, event: client::Event) {
        log::debug!("Received event: {:?}", event);

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
        log::debug!("Received filter for block #{}", height);

        self.filtermgr.filter_received(filter, block_hash, height);

        // TODO: Deal with unwrap.

        let matches = self.filtermgr.process().unwrap();
        for (block_hash, height) in matches {
            log::info!("Filter matched for block #{}", height);
            log::info!("Fetching block #{} ({})", height, block_hash);

            self.blockmgr.get(block_hash, height)?;
        }

        // TODO: We should better define what "Synced" means, and have this only in
        // one place, eg. in the main loop.
        if self.blockmgr.remaining.is_empty() && self.filtermgr.is_synced() {
            self.publisher.broadcast(Event::Synced {
                height,
                block: block_hash,
            });
        }
        log::debug!("Finished processing filter for block #{}", height);

        Ok(())
    }

    fn process_block(&mut self, block: Block, height: Height) -> Result<(), Error> {
        let block_hash = block.block_hash();

        log::debug!("Received block {} at height {}", block_hash, height);

        self.blockmgr.block_received(block, height, &self.publisher);
        self.blockmgr.process(&self.publisher)?;

        Ok(())
    }

    /// Process user command.
    fn process_command(&mut self, command: Command) {
        log::debug!("Received command: {:?}", command);

        match command {
            Command::Rescan { .. } => {}
            Command::Submit { .. } => {}
            Command::Shutdown(_) => {
                // This command must be handled before this function is called.
                unreachable! {}
            }
        }
    }
}

/// Properties of the [`Client`] we'd like to test.
///
/// 1. The final output is invariant to the order in which `block` and `cfilter` messages are
///    received.
///
///    Rationale: Blocks and compact filters are often fetched from multiple peers in parallel.
///    Hence, it's important that the system be able to handle out-of-order receipt of this data,
///    and that it not affect the final outcome, eg. the balance of the UTXOs.
///
/// 2. The final output is invariant to the granularity of the the filter header chain updates.
///
///    Rationale: Filter header updates are received via the `cfheaders` message. These messages
///    can carry anywhere between 1 and [`nakamoto_p2p::protocol::spvmgr::MAX_MESSAGE_CFHEADERS`]
///    headers. The system should handle many small messages the same way as it handles a few
///    large ones.
///
/// 3. The final output is invariant to chain re-orgs.
///
///    Rationale: Chain re-organizations happen, and filters can be invalidated. The final output
///    of the system should always match the main chain at any given point in time.
///
/// 4. The final output is always a function of the input.
///
///    Rationale: Irrespective to how the system converges towards its final state, the final output
///    should always match the given input.
///
/// 5. The commands `watch_address`, `unwatch_address`, `watch_scripts`, `unwatch_scripts`,
///    `submit` are idempotent.
///
/// 6. The `rescan` command is always a no-op if the start of the range is equal or greater
///    than the current synced height plus one.
///
///    Rationale: Any re-scans for future blocks are equivalent to the default behavior of
///    scanning incoming blocks as they come.
///
/// 7. The system is *injective*, in the sense that for every input there is a unique, distinct
///    output.
///
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::thread;

    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use nakamoto_common::network::Network;
    use nakamoto_test::block::gen;
    use nakamoto_test::logger;

    use super::handle::Handle as _;
    use super::p2p::protocol::spvmgr;
    use super::*;

    use crate::tests::mock;

    #[ignore]
    #[quickcheck]
    fn prop_filter_headers_imported(birth: Height, count: usize, seed: u64) -> TestResult {
        if count < 1 || count > 16 {
            return TestResult::discard();
        }
        if birth >= count as Height {
            return TestResult::discard();
        }
        logger::init(log::Level::Debug);

        let mut rng = fastrand::Rng::with_seed(seed);
        let network = Network::Regtest;
        let genesis = network.genesis_block();
        let chain = gen::blockchain(genesis, count as Height + 1, &mut rng);
        let remote = ([99, 99, 99, 99], 8333).into();
        let delta = chain.tail.len() - birth as usize;

        log::info!(
            target: "test",
            "--- Test case with chain length of {}, height of {} and birth height of {} (delta={}) ---",
            chain.len(),
            chain.len() - 1,
            birth,
            delta,
        );

        // Build watchlist.
        let mut watchlist = Watchlist::new();
        let mut balance = 0;
        for (h, blk) in chain.iter().enumerate().skip(birth as usize) {
            // Randomly pick certain blocks.
            if rng.bool() {
                // Randomly pick a transaction and add its output to the watchlist.
                let tx = &blk.txdata[rng.usize(0..blk.txdata.len())];
                watchlist.insert_script(tx.output[0].script_pubkey.clone());
                balance += tx.output[0].value;

                log::info!(target: "test",
                    "Marking txid {} block #{} ({})", tx.txid(), h, blk.block_hash());
            }
        }

        // Construct list of filters to send to client, as well as set of matching block hashes,
        // including false-positives.
        let mut filters = Vec::with_capacity(count);
        let mut matching = HashSet::new();
        for h in birth as usize..chain.len() {
            let filter = gen::cfilter(&chain[h]);
            let block_hash = chain[h].block_hash();

            if watchlist.match_filter(&filter, &block_hash).unwrap() {
                matching.insert(block_hash);
            }
            filters.push((filter, block_hash, h as Height));
        }
        rng.shuffle(&mut filters); // Filters are received out of order

        log::info!(target: "test", "Marked {} block(s) for matching", matching.len());
        log::info!(target: "test", "Transaction balance is {}", balance);

        let config = Config { genesis: birth };
        let client = mock::Client::new(network);
        let spv = super::Client::new(client.handle(), watchlist, config);
        let handle = spv.handle();

        let t = thread::spawn(|| spv.run().unwrap());

        // Split the filter headers in random chunks to be received by the client.
        let mut chunks = Vec::new();
        {
            let mut remaining = delta;

            while remaining > 0 {
                let count = rng.usize(1..=remaining);
                chunks.push(count);
                remaining -= count;
            }
        }

        log::info!(
            "Splitting filter headers into {} chunk(s): {:?}",
            chunks.len(),
            chunks
        );

        // Send the filter headers to the client in chunks.
        {
            let mut height = birth;

            for chunk in chunks {
                let tip = height + chunk as Height;

                // The filter header chain has advanced by `chunk`.
                let event = client::Event::SpvManager(spvmgr::Event::FilterHeadersImported {
                    height: tip,
                    block_hash: chain[tip as usize].block_hash(),
                });
                client.events.send(event).unwrap();

                // We expect the client to fetch the corresponding filters from the network,
                // including the birth height.
                match client.commands.recv().unwrap() {
                    client::Command::GetFilters(range, reply) => {
                        assert_eq!(*range.end(), tip);
                        reply.send(Ok(())).unwrap();
                    }
                    _ => panic!("expected `GetFilters` command"),
                }
                height = tip;
            }
        }

        log::info!(target: "test", "Sending requested filters to client..");
        for filter in filters {
            client.filters.send(filter).unwrap();
        }

        log::info!(target: "test", "Waiting for client to fetch matching blocks..");

        let mut requested = Vec::new();
        while !matching.is_empty() {
            log::info!(target: "test", "Blocks remaining to fetch: {}", matching.len());

            match client.commands.recv().unwrap() {
                client::Command::GetBlock(hash, reply) => {
                    if !matching.remove(&hash) {
                        log::info!("Client matched false-positive {}", hash);
                    }
                    reply.send(Ok(remote)).unwrap();
                    requested.push(hash);
                }
                _ => panic!("expected `GetBlock` command"),
            }
        }
        log::info!(target: "test", "All matching blocks have been fetched");

        rng.shuffle(&mut requested);
        for hash in requested {
            let (height, blk) = chain
                .iter()
                .enumerate()
                .find(|(_, blk)| blk.block_hash() == hash)
                .unwrap();

            log::info!(target: "test", "Sending block #{} to client", height);
            client.blocks.send((blk.clone(), height as Height)).unwrap();
        }
        log::info!(target: "test", "Checking matches..");

        // Shutdown.
        handle.shutdown().ok();
        t.join().unwrap();

        assert_eq!(balance, handle.utxos.lock().unwrap().balance());
        log::info!(target: "test", "--- Balance of {} matched ---", balance);

        TestResult::passed()
    }
}
