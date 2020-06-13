//! Address book. Keeps track of known peers.
use std::fmt;
use std::io;
use std::net;

use crate::peer::Network;

pub struct AddressBook {
    pub addrs: Vec<net::SocketAddr>,
}

impl AddressBook {
    pub fn from<T: net::ToSocketAddrs + fmt::Debug>(seeds: &[T]) -> io::Result<Self> {
        let addrs = seeds
            .iter()
            .flat_map(|seed| match seed.to_socket_addrs() {
                Ok(addrs) => addrs.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<io::Result<_>>()?;

        Ok(Self { addrs })
    }

    pub fn bootstrap(network: Network) -> io::Result<Self> {
        match network {
            Network::Mainnet => {
                let seeds = network
                    .seeds()
                    .iter()
                    .map(|s| (*s, network.port()))
                    .collect::<Vec<_>>();

                AddressBook::from(&seeds)
            }
            _ => todo!(),
        }
    }
}
