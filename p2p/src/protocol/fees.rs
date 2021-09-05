//! Types and utilities related to transaction fees and fee rates.
use std::collections::VecDeque;

use bitcoin::blockdata::constants::WITNESS_SCALE_FACTOR;
use bitcoin::{Block, OutPoint, Transaction, TxOut};

use nakamoto_common::collections::HashMap;
use nakamoto_common::nonempty::NonEmpty;

use super::Height;

// TODO: Prune UTXO set so that it doesn't grow indefinitely.

/// Maximum depth of a re-org that we are able to handle.
pub const MAX_UTXO_SNAPSHOTS: usize = 12;

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

/// Set of unspent transaction outputs (UTXO).
type UtxoSet = HashMap<OutPoint, TxOut>;

/// Transaction fee rate estimator.
#[derive(Debug, Default)]
pub struct FeeEstimator {
    /// UTXO set.
    utxos: UtxoSet,
    /// Current (best) height.
    height: Height,
    /// UTXO set snapshots.
    /// These are used to return to a previous state in the case of a re-org.
    snapshots: VecDeque<(Height, UtxoSet)>,
}

impl FeeEstimator {
    /// Process a block and get a fee estimate.  Returns [`None`] if none of the transactions
    /// could be processed due to missing UTXOs, or the block height isn't greater than the
    /// current block height of the fee estimator.
    ///
    /// # Panics
    ///
    /// Panics if the block height is not greater than the current block height of the
    /// fee estimator.
    pub fn process(&mut self, block: Block, height: Height) -> Option<FeeEstimate> {
        let mut fees = Vec::new();
        let snapshot = self.utxos.clone();

        assert!(
            height > self.height,
            "Received block #{} must be higher than best block #{}",
            height,
            self.height,
        );

        for tx in &block.txdata {
            if let Some(rate) = self.apply(tx) {
                fees.push(rate);
            }
        }

        self.snapshots.push_back((self.height, snapshot));
        if self.snapshots.len() > MAX_UTXO_SNAPSHOTS {
            self.snapshots.pop_front();
        }
        self.height = height;

        FeeEstimate::from(fees)
    }

    /// Rollback to a certain height.
    pub fn rollback(&mut self, height: Height) {
        self.snapshots.retain(|(h, _)| h <= &height);

        if let Some((h, snapshot)) = self.snapshots.pop_back() {
            assert!(h <= height);

            self.utxos = snapshot;
            self.height = h;
        }
    }

    /// Apply the transaction to the UTXO set and calculate the fee rate.
    fn apply(&mut self, tx: &Transaction) -> Option<FeeRate> {
        let txid = tx.txid();
        let mut received = 0;
        let mut sent = 0;

        // Look for outputs.
        for (vout, output) in tx.output.iter().enumerate() {
            let outpoint = OutPoint {
                txid,
                vout: vout as u32,
            };
            self.utxos.insert(outpoint, output.clone());
            sent += output.value;
        }
        // Since coinbase transactions have no inputs, we only process the outputs.
        if tx.is_coin_base() {
            return None;
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

#[cfg(test)]
mod tests {
    use super::*;
    use nakamoto_test::assert_matches;
    use nakamoto_test::block::gen;

    #[test]
    fn test_rollback() {
        let mut fe = FeeEstimator::default();
        let mut rng = fastrand::Rng::new();
        let genesis = gen::genesis(&mut rng);
        let blocks = gen::blockchain(genesis, 21, &mut rng);

        let mut estimates = HashMap::with_hasher(rng.into());

        for (height, block) in blocks.iter().cloned().enumerate().skip(1) {
            let estimate = fe.process(block, height as Height);
            estimates.insert(height, estimate);
        }
        assert_eq!(fe.snapshots.len(), MAX_UTXO_SNAPSHOTS as usize);
        assert_eq!(fe.height, 21);
        assert_matches!(fe.snapshots.back(), Some((20, _)));

        fe.rollback(18);
        assert_eq!(fe.snapshots.len(), 9);
        assert_eq!(fe.height, 18);
        assert_matches!(fe.snapshots.back(), Some((17, _)));

        assert_eq!(
            fe.process(blocks[19].clone(), 19).as_ref().unwrap(),
            estimates[&19].as_ref().unwrap()
        );
        assert_eq!(fe.snapshots.len(), 10);
        assert_eq!(fe.height, 19);
        assert_matches!(fe.snapshots.back(), Some((18, _)));
    }

    #[test]
    fn test_rollback_missing_height() {
        let mut fe = FeeEstimator::default();
        let mut rng = fastrand::Rng::new();
        let genesis = gen::genesis(&mut rng);
        let blocks = gen::blockchain(genesis, 14, &mut rng);

        fe.process(blocks[8].clone(), 8);
        fe.process(blocks[9].clone(), 9);

        fe.process(blocks[13].clone(), 13);
        fe.process(blocks[14].clone(), 14);

        assert_eq!(fe.snapshots.len(), 4);

        fe.rollback(10); // Missing height

        assert_eq!(fe.snapshots.len(), 2);
        assert_eq!(fe.height, 9);
        assert_matches!(fe.snapshots.back(), Some((8, _)));

        fe.rollback(8);

        assert_eq!(fe.snapshots.len(), 1);
        assert_eq!(fe.height, 8);
        assert_matches!(fe.snapshots.back(), Some((0, _)));

        fe.rollback(4);

        assert_eq!(fe.snapshots.len(), 0);
        assert_eq!(fe.height, 0);
    }
}
