use argh::FromArgs;

use bitcoin::Address;

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
        log::Level::Info
    };
    logger::init(level).expect("initializing logger for the first time");

    if opts.addresses.is_empty() {
        log::error!("Fatal: at least one address must be specified with `--addresses`");
        std::process::exit(1);
    }

    if let Err(err) = nakamoto_wallet::run(opts.addresses, opts.genesis) {
        log::error!("Fatal: {}", err);
        std::process::exit(1);
    }
}
