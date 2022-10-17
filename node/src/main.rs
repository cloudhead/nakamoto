use std::net;
use std::path::PathBuf;

use argh::FromArgs;

use nakamoto_client::client::Network;
use nakamoto_node::{logger, Domain};

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

    /// only connect to IPv4 addresses (default: false)
    #[argh(switch, short = '4')]
    pub ipv4: bool,

    /// only connect to IPv6 addresses (default: false)
    #[argh(switch, short = '6')]
    pub ipv6: bool,

    /// log level (default: info)
    #[argh(option, default = "log::Level::Info")]
    pub log: log::Level,

    /// root directory for nakamoto files (default: ~)
    #[argh(option)]
    pub root: Option<PathBuf>,
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

    let domains = if opts.ipv4 && opts.ipv6 {
        vec![Domain::IPV4, Domain::IPV6]
    } else if opts.ipv4 {
        vec![Domain::IPV4]
    } else if opts.ipv6 {
        vec![Domain::IPV6]
    } else {
        vec![Domain::IPV4, Domain::IPV6]
    };

    if let Err(e) = nakamoto_node::run(&opts.connect, &opts.listen, opts.root, &domains, network) {
        log::error!(target: "node", "Exiting: {}", e);
        std::process::exit(1);
    }
}
