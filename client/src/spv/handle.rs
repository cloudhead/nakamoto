use std::ops::RangeBounds;

use bitcoin::{Address, ScriptHash, Transaction};
use nakamoto_common::block::Height;

use super::event::Event;
use crate::client::chan;

/// SPV client handle.
pub trait Handle {
    /// Submit transactions to the network.
    fn submit(&mut self, txs: impl IntoIterator<Item = Transaction>);
    /// Subscribe to SPV-related events.
    fn events(&mut self) -> chan::Receiver<Event>;
    /// Rescan the blockchain for matching addresses and outputs.
    fn rescan(&mut self, range: impl RangeBounds<Height>);
    /// Watch an address.
    ///
    /// Returns `true` if the address was added to the watch list.
    fn watch_address(address: Address) -> bool;
    /// Watch scripts.
    ///
    /// Returns `true` if the script was added to the watch list.
    fn watch_scripts(scripts: impl IntoIterator<Item = ScriptHash>) -> bool;
    /// Stop watching an address.
    fn unwatch_address(address: &Address);
    /// Stop watching scripts.
    fn unwatch_scripts(scripts: impl Iterator<Item = ScriptHash>);
}
