//! A watch-only wallet.
pub mod logger;

use std::collections::{HashMap, HashSet};
use std::{fmt, net, thread};

use crossbeam_channel as chan;

use bitcoin::blockdata::script::Script;
use bitcoin::blockdata::transaction::{OutPoint, TxOut};
use bitcoin::Address;

use nakamoto_client::error::Error;
use nakamoto_client::handle::Handle;
use nakamoto_client::Network;
use nakamoto_client::{client, Client, Config};
use nakamoto_common::block::Height;
use nakamoto_common::network::Services;

/// Re-scan parameters.
pub struct Rescan {
    genesis: Height,
}

/// A Bitcoin wallet.
pub struct Wallet<H> {
    client: H,
    addresses: HashSet<Address>,
    utxos: HashMap<OutPoint, TxOut>,
}

impl<H: Handle> Wallet<H> {
    /// Create a new wallet, given a client handle and a list of watch addresses.
    pub fn new(client: H, addresses: Vec<Address>) -> Self {
        Self {
            client,
            addresses: addresses.into_iter().collect(),
            utxos: HashMap::new(),
        }
    }

    /// Rescan the blockchain for matching transactions.
    pub fn rescan(&mut self, options: Rescan) -> Result<(), Error> {
        // 1. Download block filters between `genesis` and `height` Filters can be downloaded in
        //    parallel, but should be processed in-order.
        // 2. As they are downloaded, check if there's a match. If so, add the block hash
        //    to `blocks_remaining`.
        // 3. Once all filters in the range are downloaded, check each for matching addresses, with
        //    `addresses`. For each matching filter, download the corresponding block.
        // 4. As blocks are downloaded and checked for txs, remove them from the block queue,
        //    and update the UTXO set.
        // 5. Once there are no more blocks in the queue and filters to check, exit.
        //
        let addresses: HashSet<Script> = self.addresses.iter().map(|a| a.script_pubkey()).collect();
        let query = self
            .addresses
            .iter()
            .map(|a| a.script_pubkey())
            .collect::<Vec<_>>();

        log::info!("Waiting for peers..");

        self.client.wait_for_peers(1, Services::All)?;
        self.client.wait_for_ready()?;

        let (height, _) = self.client.get_tip()?;

        if options.genesis > height {
            // If the wallet genesis is higher than the current block height, we need to wait
            // until we reach that height.
            log::info!("Waiting for height {}", options.genesis);

            self.client.wait_for_height(options.genesis)?;
        }
        let range = options.genesis..height;
        let count = (range.end - range.start) as usize;

        let blocks_recv = self.client.blocks();
        let filters_recv = self.client.filters();

        log::info!("Fetching filters in range {}..{}", range.start, range.end);
        self.client.get_filters(range)?;

        let mut filter_height = options.genesis;
        let mut blocks_remaining = HashSet::new();
        let mut filters_remaining = count;

        while !blocks_remaining.is_empty() || filters_remaining > 0 {
            chan::select! {
                recv(filters_recv) -> msg => {
                    if let Ok((filter, block_hash, height)) = msg {
                        // Process filters in-order.
                        if height == filter_height {
                            filter_height = height + 1;
                            filters_remaining -= 1;

                            if let Ok(true) =
                                filter.match_any(&block_hash, &mut query.iter().map(|s| s.as_bytes()))
                            {
                                log::info!("Filter matched at height {}", height);
                                log::info!("Fetching block {}", block_hash);

                                // TODO: For BIP32 wallets, add one more address to check, if the
                                // matching one was the highest-index one.
                                blocks_remaining.insert(block_hash);
                                self.client.get_block(&block_hash)?;

                            }
                        } else {
                            // TODO: If this condition triggers, we should just queue the filters
                            // for later processing.
                            panic!(
                                "Filter received is too far ahead: expected height={}, got height={}",
                                filter_height, height
                            );
                        }
                    }
                }
                recv(blocks_recv) -> msg => {
                    if let Ok((block, height)) = msg {
                        blocks_remaining.remove(&block.block_hash());

                        log::info!(
                            "Received block {} (remaining={})",
                            height,
                            blocks_remaining.len()
                        );

                        for tx in block.txdata.iter() {
                            // Look for outputs.
                            for (vout, output) in tx.output.iter().enumerate() {
                                // Received coin.
                                if addresses.contains(&output.script_pubkey) {
                                    let outpoint = OutPoint {
                                        txid: tx.txid(),
                                        vout: vout as u32,
                                    };
                                    self.utxos.insert(outpoint, output.clone());
                                    log::info!("Unspent output found (balance={})", self.balance());
                                }
                            }
                            // Look for inputs.
                            for input in tx.input.iter() {
                                // Spent coin.
                                if self.utxos.remove(&input.previous_output).is_some() {
                                    log::info!("Spent output found (balance={})", self.balance())
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn balance(&self) -> u64 {
        self.utxos.values().map(|u| u.value).sum()
    }
}

/// The network reactor we're going to use.
type Reactor = nakamoto_net_poll::Reactor<net::TcpStream, client::Publisher>;

/// Entry point for running the wallet.
pub fn run<S: net::ToSocketAddrs + fmt::Debug>(
    seed: S,
    addresses: Vec<Address>,
    genesis: Height,
) -> Result<(), Error> {
    let mut cfg = Config {
        listen: vec![], // Don't listen for incoming connections.
        network: Network::Mainnet,
        ..Config::default()
    };
    cfg.seed(&[seed])?;
    // TODO: This shouldn't have to be specified manually. We should have a "discovery mode"
    // that can be static or dynamic.
    cfg.target_outbound_peers = cfg.connect.len().min(8);

    // Create a new client using `Reactor` for networking.
    let client = Client::<Reactor>::new(cfg)?;
    let handle = client.handle();

    // Start the network client in the background.
    thread::spawn(|| client.run().unwrap());

    // Create a new wallet and rescan the chain from the provided `genesis` height for
    // matching addresses.
    let mut wallet = Wallet::new(handle, addresses);

    wallet.rescan(Rescan { genesis })?;

    log::info!("Balance is {} sats", wallet.balance());
    log::info!("Rescan complete.");

    Ok(())
}
