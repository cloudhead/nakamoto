use std::net;
use std::path::PathBuf;

use argh::FromArgs;

use nakamoto_common::bitcoin::Address;

use nakamoto_common::block::Height;
use nakamoto_wallet::logger;

/// A Bitcoin wallet.
#[derive(FromArgs)]
pub struct Options {
    /// watch the following addresses
    #[argh(option)]
    pub addresses: Vec<Address>,
    /// wallet genesis height, from which to start scanning
    #[argh(option)]
    pub genesis: Height,
    /// connect to this node
    #[argh(option)]
    pub connect: net::SocketAddr,
    /// wallet file
    #[argh(option)]
    pub wallet: PathBuf,
    /// enable debug logging
    #[argh(switch)]
    pub debug: bool,
}

impl Options {
    pub fn from_env() -> Self {
        argh::from_env()
    }
}

fn main() {
    let opts = Options::from_env();

    let level = if opts.debug {
        log::Level::Debug
    } else {
        log::Level::Error
    };
    logger::init(level).expect("initializing logger for the first time");

    if let Err(err) = nakamoto_wallet::run(&opts.wallet, opts.genesis, opts.connect) {
        log::error!("Fatal: {}", err);
        std::process::exit(1);
    }
}
