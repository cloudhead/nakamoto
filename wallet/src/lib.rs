//! A watch-only wallet.
pub mod logger;

use thiserror::Error;

use std::collections::HashSet;
use std::{io, net, thread};

use nakamoto_common::bitcoin::Address;

use nakamoto_client::handle::{self, Handle};
use nakamoto_client::spv::utxos::Utxos;
use nakamoto_client::Network;
use nakamoto_client::{protocol, Client, Config, Event};
use nakamoto_common::block::Height;
use nakamoto_common::network::Services;

/// An error occuring in the wallet.
#[derive(Error, Debug)]
pub enum Error {
    #[error("client handle error: {0}")]
    Handle(#[from] handle::Error),

    #[error("client error: {0}")]
    Client(#[from] nakamoto_client::error::Error),

    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// A Bitcoin wallet.
pub struct Wallet<H> {
    client: H,
    addresses: HashSet<Address>,
    utxos: Utxos,
}

impl<H: Handle> Wallet<H> {
    /// Create a new wallet, given a client handle and a list of watch addresses.
    pub fn new(client: H, addresses: Vec<Address>) -> Self {
        Self {
            client,
            addresses: addresses.into_iter().collect(),
            utxos: Utxos::new(),
        }
    }

    /// Rescan the blockchain for matching transactions.
    pub fn rescan(&mut self, birth: Height) -> Result<(), Error> {
        // Convert our address list into scripts.
        let addresses: Vec<_> = self.addresses.iter().map(|a| a.script_pubkey()).collect();
        let events = self.client.subscribe();

        log::info!("Waiting for peers..");
        self.client.wait_for_peers(1, Services::Chain)?;

        // Start a re-scan from the birht height, which keeps scanning as new blocks arrive.
        log::info!("Starting re-scan from block height {}", birth);
        self.client.rescan(birth.., addresses.iter().cloned())?;

        while let Ok(event) = events.recv() {
            match event {
                Event::BlockMatched {
                    transactions,
                    height,
                    ..
                } => {
                    for t in &transactions {
                        self.utxos.apply(t, &addresses);
                    }
                    log::info!(
                        "Processed block at height #{} (balance = {})",
                        height,
                        self.balance()
                    );
                }
                Event::Synced { height, tip } => {
                    log::info!(
                        "Synced up to height {} ({:.1}%) ({} remaining)",
                        height,
                        height as f64 / tip as f64 * 100.,
                        tip - height
                    );
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn balance(&self) -> u64 {
        self.utxos.balance()
    }
}

/// The network reactor we're going to use.
type Reactor = nakamoto_net_poll::Reactor<net::TcpStream>;

/// Entry point for running the wallet.
pub fn run(addresses: Vec<Address>, birth: Height) -> Result<(), Error> {
    let cfg = Config {
        listen: vec![], // Don't listen for incoming connections.
        protocol: protocol::Config {
            network: Network::Mainnet,
            ..protocol::Config::default()
        },
        ..Config::default()
    };

    // Create a new client using `Reactor` for networking.
    let client = Client::<Reactor>::new()?;
    let handle = client.handle();

    // Create a new wallet and rescan the chain from the provided `birth` height for
    // matching addresses.
    let mut wallet = Wallet::new(handle.clone(), addresses);

    // Start the network client in the background.
    thread::spawn(|| client.run(cfg).unwrap());

    wallet.rescan(birth)?;

    log::info!("Balance is {} sats", wallet.balance());
    log::info!("Rescan complete.");

    handle.shutdown()?;

    Ok(())
}
