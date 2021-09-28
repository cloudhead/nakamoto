//! Seed resolution.
use crate::network::{Network};

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

    pub fn as_code(&self) -> &'static str{
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
    /// List of seed with all the feature supported by nakamoto
    seeds: Vec<&'a str>,
}

impl<'a> Seeds<'a> {
    /// Create a new seed with the requirements
    pub fn new(network: Network, features: &[SeedFeature]) -> Self {
        Seeds {
            network: network,
            seeds: vec![],
        }
    }

    // FIXME(vincenzopalazzo): Return the iterator of the vetor
    // to make easy the change in the interface.
    pub fn iter(&self) -> std::slice::Iter<'a, &str> {
        self.seeds.iter()
    }

    pub fn load(&self) -> &[&str] {
        let seed = self.from_network();
        seed
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
}

