//! Watchlist.
//!
//! Keeps track of address, scripts and other things that need monitoring.
//!
#![allow(missing_docs)]
use std::collections::HashSet;

use bitcoin::{Address, Script, TxOut};

#[derive(Debug, Clone)]
pub struct Watchlist {
    scripts: HashSet<Script>,
}

impl Watchlist {
    pub fn new() -> Self {
        Self {
            scripts: HashSet::new(),
        }
    }

    pub fn insert_scripts(&mut self, scripts: impl IntoIterator<Item = Script>) {
        self.scripts.extend(scripts)
    }

    /// Tracks incoming and outgoing coins to and from this address.
    pub fn insert_address(&mut self, address: Address) -> bool {
        // Since incoming coins are outgoing coins that spend *to* this address, and
        // outgoing coins are incoming coins that spend *from* this address, we only
        // need to track one output per address.
        self.scripts.insert(address.script_pubkey())
    }

    pub fn remove_address(&mut self, address: &Address) -> bool {
        self.scripts.remove(&address.script_pubkey())
    }

    pub fn remove_scripts<'a>(&mut self, scripts: impl IntoIterator<Item = &'a Script> + 'a) {
        for script in scripts {
            self.scripts.remove(&script);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.scripts.is_empty()
    }

    pub fn contains(&self, txout: &TxOut) -> bool {
        self.scripts.contains(&txout.script_pubkey)
    }

    pub fn scripts(&self) -> HashSet<Script> {
        self.scripts.clone()
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.scripts.iter().map(|k| k.as_bytes())
    }
}

impl Default for Watchlist {
    fn default() -> Self {
        Self::new()
    }
}
