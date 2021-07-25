//! Manages and processes blocks in the context of transaction monitoring and client-side
//! filtering.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bitcoin::OutPoint;

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
}

impl<H: client::handle::Handle> BlockManager<H> {
    pub fn new(height: Height, client: H) -> Self {
        let mempool = client.mempool();

        Self {
            remaining: HashSet::new(),
            height,
            client,
            mempool,
        }
    }

    pub fn get(&mut self, block: BlockHash) -> Result<(), Error> {
        self.remaining.insert(block);
        self.client.get_block(&block)?;

        Ok(())
    }

    pub fn process(
        &mut self,
        block: Block,
        height: Height,
        utxos: &mut Utxos,
        watchlist: &Watchlist,
        events: &p2p::event::Broadcast<Event, Event>,
    ) -> Result<(), Error> {
        let hash = block.block_hash();

        self.remaining.remove(&hash);

        log::info!(
            "Received block {} (remaining={})",
            height,
            self.remaining.len()
        );

        for tx in block.txdata.iter() {
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

            // Look for outputs.
            for (vout, output) in tx.output.iter().enumerate() {
                // Received coin.
                if watchlist.contains(&output) {
                    let outpoint = OutPoint {
                        txid,
                        vout: vout as u32,
                    };
                    utxos.insert(outpoint, output.clone());
                }
            }
            // Look for inputs.
            for input in tx.input.iter() {
                // Spent coin.
                if utxos.remove(&input.previous_output).is_some() {
                    // TODO
                }
            }
        }
        Ok(())
    }
}
