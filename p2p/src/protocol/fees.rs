//! Types and utilities related to transaction fees and fee rates.
use std::collections::VecDeque;

use bitcoin::blockdata::constants::WITNESS_SCALE_FACTOR;
use bitcoin::{Block, OutPoint, Transaction, TxOut};

use nakamoto_common::collections::HashMap;
use nakamoto_common::nonempty::NonEmpty;

use super::Height;

// TODO: Handle rollbacks in a more graceful way.
// TODO: Prune UTXO set so that it doesn't grow indefinitely.

/// Maximum depth of a re-org that we are able to handle.
pub const MAX_REORG_DEPTH: usize = 12;

/// Transaction fee rate in satoshis/vByte.
pub type FeeRate = u64;

/// Fee rate estimate for a single block.
/// Measured in satoshis/vByte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeEstimate {
    /// The lowest fee rate included in the block.
    pub low: FeeRate,
    /// The median fee rate of the block.
    pub median: FeeRate,
    /// The highest fee rate included in the block.
    pub high: FeeRate,
}

impl FeeEstimate {
    /// Calculate a fee estimate from a list of fees.
    /// Returns [`None`] if the list is empty.
    ///
    /// ```
    /// use nakamoto_p2p::protocol::fees::FeeEstimate;
    ///
    /// assert_eq!(
    ///     FeeEstimate::from(vec![3, 9, 2]),
    ///     Some(FeeEstimate { low: 2, median: 3, high: 9 }),
    /// );
    ///
    /// assert_eq!(
    ///     FeeEstimate::from(vec![4, 6]),
    ///     Some(FeeEstimate { low: 4, median: 5, high: 6 }),
    /// );
    ///
    /// assert_eq!(
    ///     FeeEstimate::from(vec![9, 2, 1, 7]),
    ///     Some(FeeEstimate { low: 1, median: 5, high: 9 }),
    /// );
    ///
    /// assert_eq!(
    ///     FeeEstimate::from(vec![3]),
    ///     Some(FeeEstimate { low: 3, median: 3, high: 3 }),
    /// );
    ///
    /// assert_eq!(FeeEstimate::from(vec![]), None);
    /// ```
    pub fn from(mut fees: Vec<FeeRate>) -> Option<Self> {
        fees.sort_unstable();

        NonEmpty::from_vec(fees).map(|fees| {
            let count = fees.len();
            let median = if count % 2 == 1 {
                fees[count / 2]
            } else {
                let left = fees[count / 2 - 1] as f64;
                let right = fees[count / 2] as f64;

                ((left + right) / 2.).round() as FeeRate
            };

            Self {
                low: *fees.first(),
                median,
                high: *fees.last(),
            }
        })
    }
}

/// Transaction fee rate estimator.
#[derive(Debug, Default)]
pub struct FeeEstimator {
    /// UTXO set.
    utxos: HashMap<OutPoint, TxOut>,
    /// Blocks recently processed. We keep these around to replay the transactions
    /// backwards in case of a re-org.
    processed: VecDeque<(Height, Block)>,
}

impl FeeEstimator {
    /// Process a block and get a fee estimate.  Returns [`None`] if the block is empty.
    pub fn process(&mut self, block: Block, height: Height) -> Option<FeeEstimate> {
        let mut fees = Vec::new();

        for tx in &block.txdata {
            if let Some(rate) = self.apply(tx) {
                fees.push(rate);
            }
        }

        self.processed.push_back((height, block));
        if self.processed.len() > MAX_REORG_DEPTH {
            self.processed.pop_front();
        }
        FeeEstimate::from(fees)
    }

    /// Rollback to a certain height.
    pub fn rollback(&mut self, _height: Height) {
        self.utxos.clear();
    }

    /// Apply the transaction to the UTXO set and calculate the fee rate.
    fn apply(&mut self, tx: &Transaction) -> Option<FeeRate> {
        let txid = tx.txid();
        let mut received = 0;
        let mut sent = 0;

        // TODO: Process outputs.
        if tx.is_coin_base() {
            return None;
        }

        // Look for outputs.
        for (vout, output) in tx.output.iter().enumerate() {
            let outpoint = OutPoint {
                txid,
                vout: vout as u32,
            };
            self.utxos.insert(outpoint, output.clone());
            sent += output.value;
        }

        // Look for inputs.
        //
        // Only if we have all inputs (ie. previous outputs) in our UTXO set can we calculate
        // the transaction fee. If one is missing, we have to bail.
        for input in tx.input.iter() {
            if let Some(out) = self.utxos.remove(&input.previous_output) {
                received += out.value;
            } else {
                return None;
            }
        }
        assert!(received >= sent, "you can't spend what you don't have",);

        let fee = received - sent;
        let weight = tx.get_weight();
        let rate = fee as f64 / (weight as f64 / WITNESS_SCALE_FACTOR as f64);

        Some(rate.round() as FeeRate)
    }
}
