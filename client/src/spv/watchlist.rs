//! Watchlist.
//!
//! Keeps track of address, scripts and other things that need monitoring.
//!
use std::collections::{HashMap, HashSet};

use bitcoin::{Address, Script, TxOut};

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
}

impl Default for Watchlist {
    fn default() -> Self {
        Self::new()
    }
}
