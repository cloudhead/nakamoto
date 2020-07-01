//! Bitcoin peer network. Eg. *Mainnet*.

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::params::Params;
use bitcoin::hash_types::BlockHash;
use bitcoin_hashes::hex::FromHex;

use bitcoin_hashes::sha256d;

use nakamoto_chain::block::Height;

/// Bitcoin peer network.
#[derive(Debug, Copy, Clone)]
pub enum Network {
    /// Bitcoin Mainnet.
    Mainnet,
    /// Bitcoin Testnet.
    Testnet,
    /// Bitcoin regression test net.
    Regtest,
}

impl From<Network> for bitcoin::Network {
    fn from(value: Network) -> Self {
        match value {
            Network::Mainnet => Self::Bitcoin,
            Network::Testnet => Self::Testnet,
            Network::Regtest => Self::Regtest,
        }
    }
}

impl Network {
    pub fn port(&self) -> u16 {
        match self {
            Network::Mainnet => 8333,
            Network::Testnet => 18333,
            Network::Regtest => 18334,
        }
    }

    /// Blockchain checkpoints.
    pub fn checkpoints(&self) -> Box<dyn Iterator<Item = (Height, BlockHash)>> {
        use crate::checkpoints;

        let iter = match self {
            Network::Mainnet => &checkpoints::MAINNET,
            Network::Testnet => &checkpoints::TESTNET,
            Network::Regtest => &checkpoints::REGTEST,
        }
        .iter()
        .cloned()
        .map(|(height, hash)| {
            let hash = BlockHash::from_hex(hash).unwrap();
            (height, hash)
        });

        Box::new(iter)
    }

    /// DNS seeds. Used to bootstrap the client's address book.
    pub fn seeds(&self) -> &[&str] {
        match self {
            Network::Mainnet => &[
                "seed.bitcoin.sipa.be",          // Pieter Wuille
                "dnsseed.bluematt.me",           // Matt Corallo
                "dnsseed.bitcoin.dashjr.org",    // Luke Dashjr
                "seed.bitcoinstats.com",         // Christian Decker
                "seed.bitcoin.jonasschnelli.ch", // Jonas Schnelli
                "seed.btc.petertodd.org",        // Peter Todd
                "seed.bitcoin.sprovoost.nl",     // Sjors Provoost
                "dnsseed.emzy.de",               // Stephan Oeste
            ],
            _ => &[],
        }
    }
}

impl Network {
    /// ```
    /// use nakamoto_p2p::protocol::bitcoin::Network;
    /// use bitcoin::util::hash::BitcoinHash;
    ///
    /// let network = Network::Mainnet;
    /// let genesis = network.genesis();
    ///
    /// assert_eq!(network.genesis_hash(), genesis.bitcoin_hash());
    /// ```
    pub fn genesis(&self) -> BlockHeader {
        use bitcoin::blockdata::constants;

        constants::genesis_block((*self).into()).header
    }

    pub fn genesis_hash(&self) -> BlockHash {
        use bitcoin_hashes::Hash;
        use nakamoto_chain::genesis;

        let hash = match self {
            Self::Mainnet => genesis::MAINNET,
            Self::Testnet => genesis::TESTNET,
            Self::Regtest => genesis::REGTEST,
        };
        BlockHash::from(
            sha256d::Hash::from_slice(hash)
                .expect("the genesis hash has the right number of bytes"),
        )
    }

    pub fn params(&self) -> Params {
        Params::new((*self).into())
    }

    pub fn magic(&self) -> u32 {
        bitcoin::Network::from(*self).magic()
    }
}
