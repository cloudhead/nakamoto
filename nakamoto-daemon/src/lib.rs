use std::net;

use argh::FromArgs;

#[derive(FromArgs)]
/// A Bitcoin light client.
pub struct Options {
    #[argh(option)]
    /// connect to the specified peers only
    pub connect: Vec<net::SocketAddr>,

    #[argh(switch)]
    /// use the bitcoin test network (default: false)
    pub testnet: bool,

    #[argh(option, default = "log::LevelFilter::Info")]
    /// log level (default: info)
    pub log: log::LevelFilter,
}

impl Options {
    pub fn from_env() -> Self {
        argh::from_env()
    }
}
