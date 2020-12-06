use std::net;

use argh::FromArgs;

use nakamoto_client::client::Network;
use nakamoto_node::logger;

#[derive(FromArgs)]
/// A Bitcoin light client.
pub struct Options {
    /// connect to the specified peers only
    #[argh(option)]
    pub connect: Vec<net::SocketAddr>,

    /// listen on one of these addresses for peer connections.
    #[argh(option)]
    pub listen: Vec<net::SocketAddr>,

    /// use the bitcoin test network (default: false)
    #[argh(switch)]
    pub testnet: bool,

    /// log level (default: info)
    #[argh(option, default = "log::Level::Info")]
    pub log: log::Level,
}

impl Options {
    pub fn from_env() -> Self {
        argh::from_env()
    }
}

fn main() {
    let opts = Options::from_env();

    logger::init(opts.log).expect("initializing logger for the first time");

    let network = if opts.testnet {
        Network::Testnet
    } else {
        Network::Mainnet
    };

    if let Err(err) = nakamoto_node::run(&opts.connect, &opts.listen, network) {
        log::error!("{}", err);
        std::process::exit(1);
    }
}
