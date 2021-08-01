//! Manages and processes blocks in the context of transaction monitoring and client-side
//! filtering.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use nakamoto_common::block::{Block, BlockHash, Height};
use nakamoto_p2p as p2p;

use crate::client::{self, Mempool};
use crate::spv::event::Event;
use crate::spv::{Error, TxStatus, Utxos, Watchlist};

pub struct BlockManager<H: client::handle::Handle> {
    pub remaining: BTreeMap<Height, BlockHash>,
    pub pending: BTreeMap<Height, Block>,

    client: H,
    mempool: Arc<Mutex<Mempool>>,
    watchlist: Arc<Mutex<Watchlist>>,
    utxos: Arc<Mutex<Utxos>>,
}

impl<H: client::handle::Handle> BlockManager<H> {
    pub fn new(client: H, utxos: Arc<Mutex<Utxos>>, watchlist: Arc<Mutex<Watchlist>>) -> Self {
        let mempool = client.mempool();

        Self {
            remaining: BTreeMap::new(),
            pending: BTreeMap::new(),
            client,
            mempool,
            watchlist,
            utxos,
        }
    }

    pub fn get(&mut self, block: BlockHash, height: Height) -> Result<(), Error> {
        self.remaining.insert(height, block);
        self.client.get_block(&block)?;

        Ok(())
    }

    pub fn is_synced(&self) -> bool {
        self.remaining.is_empty() && self.pending.is_empty()
    }

    pub fn block_received(
        &mut self,
        block: Block,
        height: Height,
        _events: &p2p::event::Broadcast<Event, Event>,
    ) {
        let hash = block.block_hash();
        if self.remaining.remove(&height).is_none() {
            log::info!("Received unexpected block {} ({})", height, hash);
            return;
        }

        log::info!(
            "Received block #{} (remaining={})",
            height,
            self.remaining.len()
        );
        self.pending.insert(height, block);
    }

    pub fn process(&mut self, events: &p2p::event::Broadcast<Event, Event>) -> Result<(), Error> {
        // We want to process blocks in order in the same way filters are processed in-order.
        // Since blocks can arrive out-of-order, we add them to the `pending` set until we're
        // able to process them.
        //
        // However, unlike with filters, we can't simply increment the block height to know which
        // block to process next, as there are gaps: blocks are only downloaded for matching
        // filters.
        //
        // Therefore, we make sure that there is no earlier block in the set that is to be
        // downloaded (`remaining`), since all blocks in the `pending` set are first added
        // to the `remaining` set, and the `remaining` set is updated in-order.
        while let Some(height) = self.pending.keys().next().cloned() {
            // If an earlier block remains to be downloaded, don't do anything.
            if self.remaining.keys().next().map_or(false, |h| h < &height) {
                break;
            }
            if let Some(block) = self.pending.remove(&height) {
                let hash = block.block_hash();

                log::debug!("Processing block #{}", height);

                for tx in &block.txdata {
                    let txid = tx.txid();

                    // Attempt to remove confirmed transaction from mempool.
                    // TODO: If this block becomes stale, this tx should go back in the
                    // mempool.
                    if self.mempool.lock()?.remove(&txid).is_some() {
                        // The transaction was in the mempool.
                        events.broadcast(Event::TxStatusChanged {
                            txid,
                            status: TxStatus::Confirmed {
                                block: hash,
                                height,
                            },
                        });
                    }

                    // Look for matching transaction data, and update the UTXO set.
                    let mut utxos = self.utxos.lock()?;
                    self.watchlist.lock()?.match_transaction(tx, &mut utxos);
                }
                events.broadcast(Event::BlockProcessed {
                    block,
                    height,
                    hash,
                });
            }
        }
        Ok(())
    }
}
