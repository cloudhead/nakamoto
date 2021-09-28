//! Seed resolution.
use log::debug;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::network::Network;

/// DNS feature
#[derive(Debug, Copy, Clone)]
pub enum SeedFeature {
    /// x1, Full Node
    NODE_NETWORK,
    /// x5, all include in x1 plus Node network
    NODE_BLOOM,
    /// x9, all included in x1 plus Node Witness
    NODE_WITNESS,
    /// xd (aka x13), all included in x9 and x5
    FULL_NODE,
}

impl Default for SeedFeature {
    fn default() -> Self {
        Self::NODE_NETWORK
    }
}

impl SeedFeature {
    pub fn as_code(&self) -> &'static str {
        match self {
            Self::NODE_NETWORK => &"x1",
            Self::NODE_BLOOM => &"x5",
            Self::NODE_WITNESS => &"x9",
            Self::FULL_NODE => &"xd",
        }
    }
}

/// Seeds implementation
pub struct Seeds<'a> {
    /// The network to work load the dns
    network: Network,
    /// The default port of the node
    port: u16,
    /// List of feature supported by nakamoto
    features: &'a [SeedFeature],
}

impl<'a> Seeds<'a> {
    /// Create a new seed with the requirements
    pub fn new(network: Network, port: u16, features: &'a [SeedFeature]) -> Self {
        Seeds {
            network,
            port,
            features,
        }
    }

    pub fn load(&self) -> Vec<SocketAddr> {
        let seeds = self.from_network();
        self.filter_by_feature(seeds)
    }

    fn from_network(&self) -> &[&str] {
        match self.network {
            Network::Mainnet => &[
                "seed.bitcoin.sipa.be",          // Pieter Wuille
                "dnsseed.bluematt.me",           // Matt Corallo
                "dnsseed.bitcoin.dashjr.org",    // Luke Dashjr
                "seed.bitcoinstats.com",         // Christian Decker
                "seed.bitcoin.jonasschnelli.ch", // Jonas Schnelli
                "seed.btc.petertodd.org",        // Peter Todd
                "seed.bitcoin.sprovoost.nl",     // Sjors Provoost
                "dnsseed.emzy.de",               // Stephan Oeste
                "seed.bitcoin.wiz.biz",          // Jason Maurice
                "seed.cloudhead.io",             // Alexis Sellier
            ],
            Network::Testnet => &[
                "testnet-seed.bitcoin.jonasschnelli.ch",
                "seed.tbtc.petertodd.org",
                "seed.testnet.bitcoin.sprovoost.nl",
                "testnet-seed.bluematt.me",
            ],
            Network::Regtest => &[], // No seeds
        }
    }

    fn filter_by_feature(&self, seed_list: &[&str]) -> Vec<SocketAddr> {
        let mut seeds_addr = Vec::new();
        for feature in self.features {
            let feature_code = feature.as_code();
            debug!("Filtering by feature {}", feature_code);

            for seed in seed_list {
                let sub_seed = format!("{}.{}:{}", feature_code, seed, self.port);
                debug!("Sub seed {} checking ...", sub_seed);
                if let Ok(hosts) = (*sub_seed).to_socket_addrs() {
                    for host in hosts {
                        seeds_addr.push(host);
                    }
                } else {
                    // we admit the failure at this point because not all the seed
                    // support all the feature.
                    debug!("Seed {} doesn't support feature {}", seed, feature_code);
                }
            }
        }
        seeds_addr
    }
}
