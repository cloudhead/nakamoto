pub mod db;
pub mod hw;
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
pub use hw::Hw;
pub use ui::Ui;

pub type Utxos = Vec<(OutPoint, TxOut)>;

/// Wallet state.
pub struct Wallet<H> {
    client: H,
    db: Db,
    ui: Ui,
    hw: Hw,
    network: client::Network,
    watch: HashSet<Address>,
}

impl<H: Handle> Wallet<H> {
    /// Create a new wallet.
    pub fn new(client: H, network: client::Network, db: Db, hw: Hw) -> Self {
        Self {
            client,
            db,
            hw,
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
        offline: bool,
        mut term: W,
    ) -> Result<(), Error> {
        let addresses = self.db.addresses()?;
        if addresses.is_empty() {
            log::info!("No addresses found, requesting from hardware device..");

            match self.hw.request_addresses(0..16, hw::AddressFormat::P2WPKH) {
                Ok(addrs) => {
                    for (ix, addr) in addrs {
                        self.db.add_address(&addr, ix, None)?;
                        self.watch.insert(addr);
                    }
                }
                Err(err) => {
                    log::warn!("Failed to request addresses from hardware device: {err}");
                }
            }
        } else {
            for addr in addresses {
                self.watch.insert(addr.address);
            }
        }

        // TODO: Don't rescan if watch list is empty.

        // Convert our address list into scripts.
        let watch: Vec<_> = self.watch.iter().map(|a| a.script_pubkey()).collect();
        let balance = self.db.balance()?;

        self.ui.message = format!("Scanning from block height {}", birth);
        self.ui.reset(&mut term)?;
        self.ui.decorations(&mut term)?;
        self.ui.set_balance(balance);
        self.ui.offline(offline);

        if offline {
            ui::refresh(&mut self.ui, &self.db, &mut term)?;
        } else {
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
        }

        // Running...
        loop {
            chan::select! {
                recv(inputs) -> input => {
                    let input = input?;

                    if let Break(()) = self.handle_input(input)? {
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

                    if let Break(()) = self.handle_client_event(event, &watch, offline, &mut term)? {
                        break;
                    }
                }
            }
            ui::refresh(&mut self.ui, &self.db, &mut term)?;
        }
        Ok(())
    }

    fn handle_input(&mut self, input: Event) -> Result<ControlFlow<()>, Error> {
        use termion::event::Key;

        match input {
            Event::Key(Key::F(1)) => {
                self.hw.connect()?;
            }
            _ => return self.ui.handle_input_event(input).map_err(Error::from),
        }

        Ok(Continue(()))
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

    fn handle_client_event<W: io::Write>(
        &mut self,
        event: client::Event,
        watch: &[Script],
        offline: bool,
        term: &mut W,
    ) -> Result<ControlFlow<()>, Error> {
        log::debug!("Received event: {}", event);

        match event {
            client::Event::Ready { tip, .. } => {
                self.ui.handle_ready(tip, offline);
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
                self.ui.redraw(&self.db, term)?;

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
