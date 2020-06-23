use std::collections::HashSet;
use std::hash::Hash;
use std::time::{SystemTime, UNIX_EPOCH};

use super::Time;

/// Maximum time adjustment between network and local time (70 minutes).
pub const MAX_TIME_ADJUSTMENT: TimeOffset = 70 * 60;

/// Minimum number of samples before we adjust local time.
pub const MIN_TIME_SAMPLES: usize = 5;

/// Maximum number of samples stored.
pub const MAX_TIME_SAMPLES: usize = 200;

/// A time offset, in seconds.
pub type TimeOffset = i64;

/// Network-adjusted time tracker.
///
/// *Network-adjusted time* is the median timestamp of all connected peers.
/// Since we store only time offsets for each peer, the network-adjusted time is
/// the local time plus the median offset of all connected peers.
///
/// Nb. Network time is never adjusted more than 70 minutes from local system time.
#[derive(Debug)]
pub struct AdjustedTime<K> {
    /// Sample sources. Prevents us from getting two samples from the same source.
    sources: HashSet<K>,
    /// Time offset samples.
    samples: Vec<TimeOffset>,
    /// Current time offset, based on our samples.
    offset: TimeOffset,
}

impl<K: Hash + Eq> AdjustedTime<K> {
    pub fn new(origin: K) -> Self {
        let offset = 0;

        let mut sources = HashSet::with_capacity(MAX_TIME_SAMPLES);
        sources.insert(origin);

        let mut samples = Vec::with_capacity(MAX_TIME_SAMPLES);
        samples.push(offset);

        Self {
            sources,
            samples,
            offset,
        }
    }

    /// Add a time sample to influence the network-adjusted time.
    pub fn add_sample(&mut self, source: K, sample: TimeOffset) {
        // Nb. This behavior is based on Bitcoin Core. An alternative is to truncate the
        // samples list, to never exceed `MAX_TIME_SAMPLES`, and allow new samples to be
        // added to the list, while the set of sample sources keeps growing. This has the
        // advantage that as new peers are discovered, the network time can keep adjusting,
        // while old samples get discarded. Such behavior is found in `btcd`.
        if self.sources.len() == MAX_TIME_SAMPLES {
            return;
        }
        if !self.sources.insert(source) {
            return;
        }
        self.samples.push(sample);

        let mut offsets = self.samples.clone();
        let count = offsets.len();

        offsets.sort();

        // Don't adjust if less than 5 samples exist.
        if count < MIN_TIME_SAMPLES {
            return;
        }

        // Only adjust when a true median is found.
        //
        // Note that this means the offset will *not* be adjusted when the last sample
        // is added, since `MAX_TIME_SAMPLES` is even. This is a known "bug" in Bitcoin Core
        // and we reproduce it here, since this code affects consensus.
        if count % 2 == 1 {
            let median_offset: TimeOffset = offsets[count / 2];

            // Don't let other nodes change our time by more than a certain amount.
            if median_offset.abs() <= MAX_TIME_ADJUSTMENT {
                self.offset = median_offset;
            } else {
                // TODO: Check whether other nodes have times similar to ours, otherwise
                // log a warning about our clock possibly being wrong.
                self.offset = 0;
            }
            log::debug!("Time offset adjusted to {} seconds", self.offset);
        };
    }

    pub fn offset(&self) -> TimeOffset {
        self.offset
    }

    pub fn from(&self, time: Time) -> Time {
        let adjustment = self.offset;

        if adjustment > 0 {
            time + adjustment as Time
        } else {
            time - adjustment.abs() as Time
        }
    }

    pub fn get(&self) -> Time {
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as Time;

        self.from(local_time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    #[test]
    fn test_adjusted_time() {
        let mut adjusted_time: AdjustedTime<SocketAddr> =
            AdjustedTime::new(([127, 0, 0, 0], 8333).into());
        assert_eq!(adjusted_time.offset(), 0); // samples = [0]

        adjusted_time.add_sample(([127, 0, 0, 1], 8333).into(), 42);
        assert_eq!(adjusted_time.offset(), 0); // samples = [0, 42]

        adjusted_time.add_sample(([127, 0, 0, 2], 8333).into(), 47);
        assert_eq!(adjusted_time.offset(), 0); // samples = [0, 42, 47]

        for i in 3.. {
            adjusted_time.add_sample(([127, 0, 0, i], 8333).into(), MAX_TIME_ADJUSTMENT + 1);

            if adjusted_time.samples.len() >= MIN_TIME_SAMPLES {
                break;
            }
        }
        assert_eq!(adjusted_time.offset(), 47); // samples = [0, 42, 47, 4201, 4201]

        adjusted_time.add_sample(([127, 0, 0, 5], 8333).into(), MAX_TIME_ADJUSTMENT + 1);
        assert_eq!(
            adjusted_time.offset(),
            47,
            "No change when sample count is even"
        ); // samples = [0, 42, 47, 4201, 4201, 4201]

        adjusted_time.add_sample(([127, 0, 0, 6], 8333).into(), MAX_TIME_ADJUSTMENT + 1);
        assert_eq!(
            adjusted_time.offset(),
            0,
            "A too large time adjustment reverts back to 0",
        ); // samples = [0, 42, 47, 4201, 4201, 4201, 4201]
    }

    #[test]
    fn test_adjusted_time_negative() {
        let local_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as Time;

        let mut adjusted_time: AdjustedTime<SocketAddr> =
            AdjustedTime::new(([127, 0, 0, 0], 8333).into());
        assert_eq!(adjusted_time.offset(), 0); // samples = [0]

        for i in 1..5 {
            adjusted_time.add_sample(([127, 0, 0, i], 8333).into(), 96);
        } // samples = [0, 96, 96, 96, 96]
        assert_eq!(adjusted_time.offset(), 96);
        assert_eq!(adjusted_time.from(local_time), local_time + 96);

        for i in 5..11 {
            adjusted_time.add_sample(([127, 0, 0, i], 8333).into(), -96);
        } // samples = [-96, -96, -96, -96, -96, -96, 0, 96, 96, 96, 96]
        assert_eq!(adjusted_time.offset(), -96);
        assert_eq!(adjusted_time.from(local_time), local_time - 96);
    }

    #[test]
    fn test_adjusted_time_max_samples() {
        let mut adjusted_time: AdjustedTime<SocketAddr> =
            AdjustedTime::new(([127, 0, 0, 0], 8333).into());
        assert_eq!(adjusted_time.offset(), 0); // samples = [0]

        for i in 1..(MAX_TIME_SAMPLES / 2) {
            adjusted_time.add_sample(([127, 0, 0, i as u8], 8333).into(), -1);
        }
        assert_eq!(adjusted_time.offset(), -1);

        for i in (MAX_TIME_SAMPLES / 2).. {
            adjusted_time.add_sample(([127, 0, 0, i as u8], 8333).into(), 1);

            if adjusted_time.samples.len() == MAX_TIME_SAMPLES {
                break;
            }
        }
        // We added an equal number of samples on each side of the initial sample.
        // There are 99 samples before, and 99 samples after.
        assert_eq!(adjusted_time.offset(), 0);

        adjusted_time.add_sample(([127, 0, 0, 255], 8333).into(), 1);
        assert_eq!(
            adjusted_time.samples.len(),
            MAX_TIME_SAMPLES,
            "Adding a sample after the maximum is reached, has no effect"
        );
    }
}
