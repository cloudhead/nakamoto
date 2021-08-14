//! A simple UTXO set.
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use bitcoin::{OutPoint, Script, Transaction, TxOut};

/// A simple UTXO set.
#[derive(Debug, Clone)]
pub struct Utxos {
    map: HashMap<OutPoint, TxOut>,
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

    /// Apply a transaction to the UTXO set.
    pub fn apply(&mut self, tx: &Transaction, scripts: &[Script]) {
        // Look for outputs.
        for (vout, output) in tx.output.iter().enumerate() {
            // Received coin.
            if scripts.contains(&output.script_pubkey) {
                let txid = tx.txid();
                let outpoint = OutPoint {
                    txid,
                    vout: vout as u32,
                };
                self.insert(outpoint, output.clone());
                log::info!("Unspent output found (balance={})", self.balance());
            }
        }
        // Look for inputs.
        for input in tx.input.iter() {
            // Spent coin.
            if self.remove(&input.previous_output).is_some() {
                log::info!("Spent output found (balance={})", self.balance())
            }
        }
    }
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
