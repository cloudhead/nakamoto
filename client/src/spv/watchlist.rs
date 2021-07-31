//! Watchlist.
//!
//! Keeps track of address, scripts and other things that need monitoring.
//!
use std::collections::{HashMap, HashSet};

use bitcoin::util::bip158;
use bitcoin::BlockHash;
use bitcoin::{Address, OutPoint, Script, Transaction, TxOut};

use nakamoto_chain::filter::BlockFilter;

use super::Utxos;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Watchlist {
    addresses: HashMap<Script, Address>,
    scripts: HashSet<Script>,
}

impl Watchlist {
    pub fn new() -> Self {
        Self {
            addresses: HashMap::new(),
            scripts: HashSet::new(),
        }
    }

    pub fn insert_script(&mut self, script: Script) -> bool {
        self.scripts.insert(script)
    }

    pub fn insert_address(&mut self, address: Address) -> bool {
        self.addresses
            .insert(address.script_pubkey(), address)
            .is_none()
    }

    pub fn is_empty(&self) -> bool {
        self.addresses.is_empty() && self.scripts.is_empty()
    }

    pub fn contains(&self, txout: &TxOut) -> bool {
        self.addresses.contains_key(&txout.script_pubkey)
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.addresses.keys().map(|k| k.as_bytes())
    }

    pub fn match_filter(
        &self,
        filter: &BlockFilter,
        block_hash: &BlockHash,
    ) -> Result<bool, bip158::Error> {
        if self.is_empty() {
            return Ok(false);
        }
        if filter.match_any(&block_hash, &mut self.iter())? {
            return Ok(true);
        }
        Ok(false)
    }

    pub fn match_transaction(&self, tx: &Transaction, utxos: &mut Utxos) {
        // Look for outputs.
        for (vout, output) in tx.output.iter().enumerate() {
            // Received coin.
            if self.contains(&output) {
                let txid = tx.txid();
                let outpoint = OutPoint {
                    txid,
                    vout: vout as u32,
                };
                utxos.insert(outpoint, output.clone());
                log::info!("Unspent output found (balance={})", utxos.balance());
            }
        }
        // Look for inputs.
        for input in tx.input.iter() {
            // Spent coin.
            if utxos.remove(&input.previous_output).is_some() {
                log::info!("Spent output found (balance={})", utxos.balance())
            }
        }
    }
}

impl Default for Watchlist {
    fn default() -> Self {
        Self::new()
    }
}
