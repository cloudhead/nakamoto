use std::{io, net};

use nakamoto_client::Dialer;
use nakamoto_client::Network;
use nakamoto_client::{protocol, Client, Config, Publisher};

/// Broken dialer always fails to dial.
#[derive(Default)]
struct BrokenDialer {}

impl Dialer for BrokenDialer {
    fn dial(&mut self, addr: &net::SocketAddr) -> Result<net::TcpStream, io::Error> {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!("can't connect to {}, this dialer is broken!", addr),
        ))
    }
}

fn main() {
    /// The underlying reactor.
    type Reactor = nakamoto_net_poll::Reactor<net::TcpStream, Publisher>;

    let cfg = Config {
        listen: vec![],
        protocol: protocol::Config {
            network: Network::Testnet,
            ..protocol::Config::default()
        },
        ..Config::default()
    };
    let client = Client::<Reactor, BrokenDialer>::new().unwrap();

    client.run(cfg).unwrap();
}
