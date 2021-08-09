//! SPV client.
#![allow(clippy::manual_range_contains, clippy::new_without_default)]

#[allow(missing_docs)]
pub mod event;
#[allow(missing_docs)]
pub mod handle;
#[allow(missing_docs)]
pub mod utxos;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::{MutexGuard, PoisonError};
use std::{fmt, net, time};

use thiserror::Error;

use bitcoin::{Block, Script, Transaction, Txid};

use nakamoto_common::block::{BlockHash, Height};
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_p2p as p2p;

use crate::client::{self, chan};
use crate::spv::utxos::Utxos;

pub use event::Event;

#[allow(missing_docs)]
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
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
    #[error("utxo lock is poisoned")]
    Utxos,

    #[error("client handle error: {0}")]
    Client(#[from] client::handle::Error),

    #[error("client channel is disconnected")]
    Disconnected,
}

impl<'a> From<PoisonError<MutexGuard<'a, Utxos>>> for Error {
    fn from(_: PoisonError<MutexGuard<'a, Utxos>>) -> Self {
        Self::Utxos
    }
}

#[allow(missing_docs)]
pub enum ControlFlow {
    Break,
    Continue,
}

#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub enum Command {
    Shutdown(chan::Sender<()>),
}

#[allow(missing_docs)]
pub struct Handle<C> {
    commands: chan::Sender<Command>,
    client: C,
    subscriber: p2p::event::Subscriber<Event>,
    timeout: time::Duration,
}

impl<C: Clone> Clone for Handle<C> {
    fn clone(&self) -> Self {
        Self {
            commands: self.commands.clone(),
            client: self.client.clone(),
            subscriber: self.subscriber.clone(),
            timeout: self.timeout,
        }
    }
}

#[allow(unused_variables)]
impl<C: client::handle::Handle> handle::Handle for Handle<C> {
    fn events(&mut self) -> chan::Receiver<Event> {
        self.subscriber.subscribe()
    }

    fn submit(
        &mut self,
        txs: impl IntoIterator<Item = Transaction>,
    ) -> Result<NonEmpty<net::SocketAddr>, handle::Error> {
        self.client
            .submit_transactions(txs.into_iter().collect())
            .map_err(handle::Error::from)
    }

    fn rescan(
        &mut self,
        range: impl std::ops::RangeBounds<Height>,
        watch: impl Iterator<Item = Script>,
    ) -> Result<(), handle::Error> {
        self.client
            .rescan(range, watch)
            .map_err(handle::Error::from)
    }

    fn shutdown(self) -> Result<(), handle::Error> {
        let (sender, recvr) = chan::bounded(1);
        self.commands.send(Command::Shutdown(sender)).ok();

        Ok(recvr.recv()?)
    }
}

#[allow(missing_docs)]
#[derive(Debug, Default, Clone)]
pub struct Config {}

#[allow(missing_docs)]
#[allow(dead_code)]
pub struct Client<C: client::handle::Handle> {
    client: C,
    publisher: p2p::event::Broadcast<Event, Event>,
    control: chan::Receiver<Command>,
    config: Config,
    // The height up to which we've processed filters and matching blocks.
    sync_height: Height,
    // The height up to which we've processed filters.
    // This is usually going to be greater than `sync_height`.
    filter_height: Height,
    // The height up to which we've processed matching blocks.
    // This is always going to be lesser or equal to `filter_height`.
    block_height: Height,
    // Filter heights that have been matched, and for which we are awaiting a block to process.
    pending: HashSet<Height>,

    // Attributes accessible via a [`Handle`].
    commands: chan::Sender<Command>,
    subscriber: p2p::event::Subscriber<Event>,
    timeout: time::Duration,
}

impl<C: client::handle::Handle> Client<C> {
    #[allow(missing_docs)]
    pub fn new(client: C, config: Config) -> Self {
        let (publisher, subscriber) = p2p::event::broadcast(Some);
        let (commands, control) = chan::unbounded::<Command>();
        let timeout = time::Duration::from_secs(9);
        let sync_height = 0;
        let filter_height = 0;
        let block_height = 0;
        let pending = HashSet::new();

        Self {
            client,
            publisher,
            control,
            config,
            sync_height,
            filter_height,
            block_height,
            pending,
            commands,
            subscriber,
            timeout,
        }
    }

    #[allow(missing_docs)]
    pub fn run(mut self) -> Result<(), Error> {
        let events = self.client.events();
        let blocks = self.client.blocks();

        log::debug!("Starting SPV client event loop..");

        let (mut tip, _) = self.client.get_tip()?;
        self.publisher.broadcast(Event::Ready { tip });

        loop {
            chan::select! {
                recv(events) -> event => {
                    // Forward to event subscribers.
                    if let Ok(event) = event {
                        self.process_event(event, &mut tip);
                    } else {
                        // If the events channel disconnects, it means our backing client
                        // is gone, and there's no point in continuing.
                        return Err(Error::Disconnected);
                    }
                }
                recv(self.control) -> command => {
                    if let Ok(command) = command {
                        if let ControlFlow::Break = self.process_command(command, &blocks)? {
                            break;
                        }
                    } else {
                        unreachable! {
                            // There is no way for this to happen, since `self` holds one of the
                            // [`chan::Sender`] references, the channel cannot disconnect
                            // unless this loop exits.
                        }
                    }
                }
                recv(blocks) -> msg => {
                    if let Ok((block, height)) = msg {
                        self.process_block(block, height)?;
                    } else {
                        return Err(Error::Disconnected);
                    }
                }
            }

            assert!(
                self.block_height <= self.filter_height,
                "Filters are processed before blocks"
            );
            assert!(
                self.sync_height <= self.filter_height,
                "Filters are processed before we are done"
            );

            // If we have no blocks left to process, we are synced to the height of the last
            // processed filter. Otherwise, we're synced up to the last processed block.
            let height = if self.pending.is_empty() {
                self.filter_height
            } else {
                self.block_height
            };

            // Ensure we only broadcast sync events when the sync height has changed.
            if height > self.sync_height {
                self.sync_height = height;
                self.publisher.broadcast(Event::Synced { height, tip });
            }
        }
        Ok(())
    }

    /// Create a new handle to the SPV client.
    pub fn handle(&self) -> Handle<C> {
        Handle {
            commands: self.commands.clone(),
            client: self.client.clone(),
            subscriber: self.subscriber.clone(),
            timeout: self.timeout,
        }
    }

    // PRIVATE METHODS /////////////////////////////////////////////////////////

    /// Process client event.
    fn process_event(&mut self, event: client::Event, tip: &mut Height) {
        log::debug!("Received event: {:?}", event);

        use p2p::protocol::{cbfmgr, invmgr, syncmgr};

        match event {
            client::Event::SyncManager(syncmgr::Event::Synced(_, height)) => {
                *tip = height;
            }
            client::Event::SyncManager(syncmgr::Event::HeadersImported(result)) => {
                use nakamoto_common::block::tree::ImportResult;

                if let ImportResult::TipChanged(_, _hash, _height, stale) = result {
                    if !stale.is_empty() {
                        for hash in stale {
                            self.publisher.broadcast(Event::BlockDisconnected { hash });
                        }
                    }
                    // TODO: To emit `BlockConnected` events we need the block hashes.
                }
            }
            client::Event::InventoryManager(invmgr::Event::Confirmed {
                transaction,
                height,
                block,
            }) => self.publisher.broadcast(Event::TxStatusChanged {
                txid: transaction.txid(),
                status: TxStatus::Confirmed { height, block },
            }),
            client::Event::InventoryManager(invmgr::Event::Acknowledged { txid, peer }) => {
                self.publisher.broadcast(Event::TxStatusChanged {
                    txid,
                    status: TxStatus::Acknowledged { peer },
                })
            }
            client::Event::FilterManager(cbfmgr::Event::FilterProcessed {
                height,
                matched,
                block,
            }) => {
                debug_assert!(height >= self.filter_height);

                if matched {
                    log::debug!("Filter matched for block #{}", height);
                    self.pending.insert(height);
                }
                self.filter_height = height;
                self.publisher.broadcast(Event::FilterProcessed {
                    height,
                    matched,
                    block,
                });
            }
            _ => {}
        }
    }

    fn process_block(&mut self, block: Block, height: Height) -> Result<(), Error> {
        if !self.pending.remove(&height) {
            // Received unexpected block.
            return Ok(());
        }
        let hash = block.block_hash();

        log::debug!("Received block {} at height {}", hash, height);
        debug_assert!(height >= self.block_height);

        self.block_height = height;
        self.publisher.broadcast(Event::Block {
            height,
            hash,
            header: block.header,
            transactions: block.txdata,
        });

        Ok(())
    }

    /// Process user command.
    fn process_command(
        &mut self,
        command: Command,
        blocks: &chan::Receiver<(Block, Height)>,
    ) -> Result<ControlFlow, Error> {
        log::debug!("Received command: {:?}", command);

        match command {
            Command::Shutdown(reply) => {
                // Drain incoming block queue before shutting down.
                // We don't drain the other channels, as they may create
                // more work.
                for (blk, h) in blocks.try_iter() {
                    self.process_block(blk, h)?;
                }
                reply.send(()).ok();

                Ok(ControlFlow::Break)
            }
        }
    }
}
