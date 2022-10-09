pub mod db;
pub mod ui;

use std::collections::HashSet;
use std::io;
use std::ops::ControlFlow;
use std::ops::ControlFlow::*;

use crossbeam_channel as chan;
use termion::event::Event;

use nakamoto_client as client;
use nakamoto_client::handle::Handle;
use nakamoto_common::bitcoin::Address;
use nakamoto_common::bitcoin::{OutPoint, Script, Transaction, TxOut};
use nakamoto_common::block::Height;

use crate::error::Error;
use crate::input::Signal;

pub use db::Db;
pub use db::{Read as _, Write as _};
pub use ui::Ui;

pub type Utxos = Vec<(OutPoint, TxOut)>;

/// Wallet state.
pub struct Wallet<H> {
    client: H,
    db: Db,
    ui: Ui,
    network: client::Network,
    watch: HashSet<Address>,
}

impl<H: Handle> Wallet<H> {
    /// Create a new wallet.
    pub fn new(client: H, network: client::Network, db: Db) -> Self {
        Self {
            client,
            db,
            network,
            watch: HashSet::new(),
            ui: Ui::default(),
        }
    }

    /// Calculate the wallet balance.
    pub fn balance(&self) -> Result<u64, Error> {
        self.db.balance().map_err(Error::from)
    }

    /// Apply a transaction to the wallet's UTXO set.
    pub fn apply(&mut self, tx: &Transaction, scripts: &[Script]) {
        // Look for outputs.
        for (vout, output) in tx.output.iter().enumerate() {
            // Received coin. Mark the address as *used*, and update the balance for that
            // address.
            if scripts.contains(&output.script_pubkey) {
                // Update UTXOs.
                let txid = tx.txid();
                let addr =
                    Address::from_script(&output.script_pubkey, self.network.into()).unwrap();

                self.db
                    .add_utxo(txid, vout as u32, addr, output.value)
                    .unwrap();
            }
        }

        // Look for inputs.
        for input in tx.input.iter() {
            // Spent coin. Remove the address from the set, since it is no longer ours.
            if let Ok(Some((_, _output))) = self.db.remove_utxo(&input.previous_output) {
                // TODO: Handle change addresses?
            }
        }
    }

    /// Run the wallet loop until it exits.
    pub fn run<W: io::Write>(
        &mut self,
        birth: Height,
        inputs: chan::Receiver<Event>,
        signals: chan::Receiver<Signal>,
        loading: chan::Receiver<client::Loading>,
        events: chan::Receiver<client::Event>,
        mut term: W,
    ) -> Result<(), Error> {
        self.watch = self
            .db
            .addresses()?
            .into_iter()
            .take(16)
            .map(|r| r.address)
            .collect();

        // Convert our address list into scripts.
        let watch: Vec<_> = self.watch.iter().map(|a| a.script_pubkey()).collect();
        let balance = self.db.balance()?;

        self.ui.message = format!("Scanning from block height {}", birth);
        self.ui.reset(&mut term)?;
        self.ui.decorations(&mut term)?;
        self.ui.set_balance(balance);

        // Start a re-scan from the birht height, which keeps scanning as new blocks arrive.
        self.client.rescan(birth.., watch.iter().cloned())?;

        // Loading...
        loop {
            chan::select! {
                recv(inputs) -> input => {
                    let input = input?;

                    if let Break(()) = self.ui.handle_input_event(input)? {
                        return Ok(());
                    }
                }
                recv(signals) -> signal => {
                    let signal = signal?;

                    if let Break(()) = self.handle_signal(signal, &mut term)? {
                        return Ok(());
                    }
                }
                recv(loading) -> event => {
                    if let Ok(event) = event {
                        if let Break(()) = self.ui.handle_loading_event(event)? {
                            return Ok(());
                        }
                    } else {
                        break;
                    }
                }
            }
            ui::refresh(&mut self.ui, &self.db, &mut term)?;
        }

        // Running...
        loop {
            chan::select! {
                recv(inputs) -> input => {
                    let input = input?;

                    if let Break(()) = self.ui.handle_input_event(input)? {
                        return Ok(());
                    }
                }
                recv(signals) -> signal => {
                    let signal = signal?;

                    if let Break(()) = self.handle_signal(signal, &mut term)? {
                        return Ok(());
                    }
                }
                recv(events) -> event => {
                    let event = event?;

                    if let Break(()) = self.handle_client_event(event, &watch)? {
                        break;
                    }
                }
            }
            ui::refresh(&mut self.ui, &self.db, &mut term)?;
        }
        Ok(())
    }

    fn handle_signal<W: io::Write>(
        &mut self,
        signal: Signal,
        term: &mut W,
    ) -> Result<ControlFlow<()>, Error> {
        log::info!("Received signal: {:?}", signal);

        match signal {
            Signal::WindowResized => {
                self.ui.redraw(&self.db, term)?;
            }
            Signal::Interrupted => return Ok(Break(())),
        }
        Ok(Continue(()))
    }

    fn handle_client_event(
        &mut self,
        event: client::Event,
        watch: &[Script],
    ) -> Result<ControlFlow<()>, Error> {
        log::debug!("Received event: {}", event);

        match event {
            client::Event::Ready { tip, .. } => {
                self.ui.handle_ready(tip);
            }
            client::Event::PeerHeightUpdated { height } => {
                self.ui.handle_peer_height(height);
            }
            client::Event::FilterProcessed { height, .. } => {
                self.ui.handle_filter_processed(height);
            }
            client::Event::BlockMatched {
                transactions,
                height,
                ..
            } => {
                for t in &transactions {
                    self.apply(t, watch);
                }
                let balance = self.balance()?;
                self.ui.set_balance(balance);

                log::info!(
                    "Processed block at height #{} (balance = {})",
                    height,
                    balance,
                );
            }
            // TODO: This should be called `Scanned`.
            client::Event::Synced { height, tip } => {
                self.ui.handle_synced(height, tip);
            }
            _ => {}
        }
        Ok(ControlFlow::Continue(()))
    }
}
