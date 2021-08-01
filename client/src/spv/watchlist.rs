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

    pub fn insert_scripts(&mut self, scripts: impl IntoIterator<Item = Script>) {
        self.scripts.extend(scripts)
    }

    pub fn insert_transaction(&mut self, _tx: &Transaction) -> bool {
        // TODO: Insert tx outputs/inputs?
        todo!()
    }

    /// Tracks incoming and outgoing coins to and from this address.
    pub fn insert_address(&mut self, address: Address) -> bool {
        // Since incoming coins are outgoing coins that spend *to* this address, and
        // outgoing coins are incoming coins that spend *from* this address, we only
        // need to track one output per address.
        self.addresses
            .insert(address.script_pubkey(), address)
            .is_none()
    }

    pub fn remove_address(&mut self, address: &Address) -> bool {
        self.addresses.remove(&address.script_pubkey()).is_some()
    }

    pub fn remove_scripts<'a>(&mut self, scripts: impl IntoIterator<Item = &'a Script> + 'a) {
        for script in scripts {
            self.scripts.remove(&script);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.addresses.is_empty() && self.scripts.is_empty()
    }

    pub fn contains(&self, txout: &TxOut) -> bool {
        self.addresses.contains_key(&txout.script_pubkey)
            || self.scripts.contains(&txout.script_pubkey)
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.addresses
            .keys()
            .chain(self.scripts.iter())
            .map(|k| k.as_bytes())
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
