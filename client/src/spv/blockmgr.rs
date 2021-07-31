//! Manages and processes blocks in the context of transaction monitoring and client-side
//! filtering.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use nakamoto_common::block::{Block, BlockHash, Height};
use nakamoto_p2p as p2p;

use crate::client::{self, Mempool};
use crate::spv::event::Event;
use crate::spv::{Error, TxStatus, Utxos, Watchlist};

pub struct BlockManager<H: client::handle::Handle> {
    pub remaining: HashSet<BlockHash>,
    pub height: Height,

    client: H,
    mempool: Arc<Mutex<Mempool>>,
    watchlist: Arc<Mutex<Watchlist>>,
    utxos: Arc<Mutex<Utxos>>,
}

impl<H: client::handle::Handle> BlockManager<H> {
    pub fn new(
        height: Height,
        client: H,
        utxos: Arc<Mutex<Utxos>>,
        watchlist: Arc<Mutex<Watchlist>>,
    ) -> Self {
        let mempool = client.mempool();

        Self {
            remaining: HashSet::new(),
            height,
            client,
            mempool,
            watchlist,
            utxos,
        }
    }

    pub fn get(&mut self, block: BlockHash) -> Result<(), Error> {
        self.remaining.insert(block);
        self.client.get_block(&block)?;

        Ok(())
    }

    pub fn block_received(
        &mut self,
        block: Block,
        height: Height,
        events: &p2p::event::Broadcast<Event, Event>,
    ) -> Result<(), Error> {
        let hash = block.block_hash();

        if !self.remaining.remove(&hash) {
            log::info!("Received unexpected block {} ({})", height, hash);
            return Ok(());
        }

        log::info!(
            "Received block #{} (remaining={})",
            height,
            self.remaining.len()
        );

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
            let mut utxos = self.utxos.lock().unwrap();
            self.watchlist.lock()?.match_transaction(tx, &mut utxos);
        }

        events.broadcast(Event::Synced {
            height,
            block: hash,
        });

        Ok(())
    }
}
