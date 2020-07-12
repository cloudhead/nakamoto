use std::net;

use argh::FromArgs;

use nakamoto_daemon::logger;

#[derive(FromArgs)]
/// A Bitcoin light client.
pub struct Options {
    #[argh(option)]
    /// connect to the specified peers only
    pub connect: Vec<net::SocketAddr>,

    #[argh(option)]
    /// listen on one of these addresses for peer connections.
    pub listen: Vec<net::SocketAddr>,

    #[argh(switch)]
    /// use the bitcoin test network (default: false)
    pub testnet: bool,

    #[argh(option, default = "log::Level::Info")]
    /// log level (default: info)
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

    if let Err(err) = nakamoto_node::run(&opts.connect, &opts.listen) {
        log::error!("{}", err);
        std::process::exit(1);
    }
}
