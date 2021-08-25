//! Types and utilities related to transaction fees and fee rates.
use bitcoin::blockdata::constants::WITNESS_SCALE_FACTOR;
use bitcoin::{OutPoint, Transaction, TxOut};

use nakamoto_common::collections::HashMap;
use nakamoto_common::nonempty::NonEmpty;

use super::Height;

// TODO: Handle rollbacks in a more graceful way.
// TODO: Prune UTXO set so that it doesn't grow indefinitely.

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
    utxos: HashMap<OutPoint, TxOut>,
}

impl FeeEstimator {
    /// Get a fee estimate, given a transaction block.
    /// Returns [`None`] if the block is empty.
    pub fn get_estimate(&mut self, txs: &[Transaction]) -> Option<FeeEstimate> {
        let mut fees = Vec::new();

        for tx in txs {
            if let Some(rate) = self.fee_rate(tx) {
                fees.push(rate);
            }
        }
        FeeEstimate::from(fees)
    }

    /// Rollback to a certain height.
    pub fn rollback(&mut self, _height: Height) {
        self.utxos.clear();
    }

    /// Calculate the fee rate of a transaction.
    fn fee_rate(&mut self, tx: &Transaction) -> Option<FeeRate> {
        let txid = tx.txid();
        let mut received = 0;
        let mut sent = 0;

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
