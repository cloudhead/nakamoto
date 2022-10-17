//! A TUI Bitcoin wallet.
pub mod error;
pub mod input;
pub mod logger;
pub mod wallet;

use std::path::Path;
use std::{io, net, thread};

use termion::raw::IntoRawMode;

use nakamoto_client::handle::Handle;
use nakamoto_client::Network;
use nakamoto_client::{Client, Config, Limits};
use nakamoto_common::bitcoin::util::bip32::DerivationPath;
use nakamoto_common::block::Height;

use crate::error::Error;
use crate::wallet::Db;
use crate::wallet::Hw;
use crate::wallet::Wallet;

/// The network reactor we're going to use.
type Reactor = nakamoto_net_poll::Reactor<net::TcpStream>;

/// Entry point for running the wallet.
pub fn run(
    wallet: &Path,
    birth: Height,
    connect: net::SocketAddr,
    hd_path: DerivationPath,
) -> Result<(), Error> {
    let network = Network::Mainnet;
    let cfg = Config {
        network,
        listen: vec![], // Don't listen for incoming connections.
        connect: vec![connect],
        limits: Limits {
            max_outbound_peers: 1,
            ..Limits::default()
        },
        ..Config::default()
    };

    // Create a new client using `Reactor` for networking.
    let client = Client::<Reactor>::new()?;
    let handle = client.handle();
    let client_recv = handle.subscribe();
    let loading_recv = handle.loading();

    log::info!("Opening wallet file `{}`..", wallet.display());

    let db = Db::open(wallet)?;
    let hw = Hw::new(hd_path);

    let (inputs_tx, inputs_rx) = crossbeam_channel::unbounded();
    let (exit_tx, exit_rx) = crossbeam_channel::bounded(1);
    let (signals_tx, signals_rx) = crossbeam_channel::unbounded();

    log::info!("Spawning client threads..");

    // Start the UI loop in the background.
    let t1 = thread::spawn(|| input::run(inputs_tx, exit_rx));
    // Start the signal handler thread.
    let t2 = thread::spawn(|| input::signals(signals_tx));
    // Start the network client in the background.
    let t3 = thread::spawn(|| client.run(cfg));

    log::info!("Switching to alternative screen..");

    let stdout = io::stdout().into_raw_mode()?;
    let term = termion::screen::AlternateScreen::from(termion::cursor::HideCursor::from(
        termion::input::MouseTerminal::from(stdout),
    ));

    // Run the main wallet loop. This will block until the wallet exits.
    log::info!("Running main wallet loop..");
    Wallet::new(handle.clone(), network, db, hw).run(
        birth,
        inputs_rx,
        signals_rx,
        loading_recv,
        client_recv,
        term,
    )?;

    // Tell other threads that they should exit.
    log::info!("Exiting..");
    exit_tx.send(()).unwrap();

    // Shutdown the client, since the main loop exited.
    log::info!("Shutting down client..");
    handle.shutdown()?;

    t1.join().unwrap()?;
    t2.join().unwrap()?;
    t3.join().unwrap()?;

    Ok(())
}
