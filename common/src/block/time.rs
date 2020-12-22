//! Block time and other time-related types.
use std::collections::HashSet;
use std::hash::Hash;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{BlockTime, Height};

/// Maximum time adjustment between network and local time (70 minutes).
pub const MAX_TIME_ADJUSTMENT: TimeOffset = 70 * 60;

/// Maximum a block timestamp can exceed the network-adjusted time before
/// it is considered invalid (2 hours).
pub const MAX_FUTURE_BLOCK_TIME: BlockTime = 60 * 60 * 2;

/// Number of previous blocks to look at when determining the median
/// block time.
pub const MEDIAN_TIME_SPAN: Height = 11;

/// Minimum number of samples before we adjust local time.
pub const MIN_TIME_SAMPLES: usize = 5;

/// Maximum number of samples stored.
pub const MAX_TIME_SAMPLES: usize = 200;

/// A time offset, in seconds.
pub type TimeOffset = i64;

/// Clock that tells the time.
pub trait Clock {
    /// Tell the time in block time.
    fn block_time(&self) -> BlockTime;
    /// Tell the time in local time.
    fn local_time(&self) -> LocalTime;
}

/// Local time.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Ord, PartialOrd)]
pub struct LocalTime {
    /// Milliseconds since Epoch.
    millis: u128,
}

impl std::fmt::Display for LocalTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.block_time())
    }
}

impl Default for LocalTime {
    fn default() -> Self {
        Self { millis: 0 }
    }
}

impl LocalTime {
    /// Construct a local time from the current system time.
    pub fn now() -> Self {
        Self::from(SystemTime::now())
    }

    /// Construct a local time from whole seconds since Epoch.
    pub const fn from_secs(secs: u64) -> Self {
        Self {
            millis: secs as u128 * 1000,
        }
    }

    /// Convert a block time into a local time.
    pub fn from_block_time(t: BlockTime) -> Self {
        Self::from_secs(t as u64)
    }

    /// Return the local time as seconds since Epoch.
    /// This is the same representation as used in block header timestamps.
    pub fn block_time(&self) -> BlockTime {
        use std::convert::TryInto;

        (self.millis / 1000).try_into().unwrap()
    }

    /// Get the duration since the given time.
    ///
    /// # Panics
    ///
    /// This function will panic if `earlier` is later than `self`.
    pub fn duration_since(&self, earlier: LocalTime) -> LocalDuration {
        LocalDuration::from_millis(
            self.millis
                .checked_sub(earlier.millis)
                .expect("supplied time is later than self"),
        )
    }
}

/// Convert a `SystemTime` into a local time.
impl From<SystemTime> for LocalTime {
    fn from(system: SystemTime) -> Self {
        let millis = system.duration_since(UNIX_EPOCH).unwrap().as_millis();

        Self { millis }
    }
}

/// Substract two local times. Yields a duration.
impl std::ops::Sub<LocalTime> for LocalTime {
    type Output = LocalDuration;

    fn sub(self, other: LocalTime) -> LocalDuration {
        LocalDuration(self.millis - other.millis)
    }
}

/// Substract a duration from a local time. Yields a local time.
impl std::ops::Sub<LocalDuration> for LocalTime {
    type Output = LocalTime;

    fn sub(self, other: LocalDuration) -> LocalTime {
        LocalTime {
            millis: self.millis - other.0,
        }
    }
}

/// Add a duration to a local time. Yields a local time.
impl std::ops::Add<LocalDuration> for LocalTime {
    type Output = LocalTime;

    fn add(self, other: LocalDuration) -> LocalTime {
        LocalTime {
            millis: self.millis + other.0,
        }
    }
}

/// Time duration as measured locally.
#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct LocalDuration(u128);

impl LocalDuration {
    /// The time interval between blocks. The "block time".
    pub const BLOCK_INTERVAL: LocalDuration = Self::from_mins(10);

    /// Create a new duration from whole seconds.
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs as u128 * 1000)
    }

    /// Create a new duration from whole minutes.
    pub const fn from_mins(mins: u64) -> Self {
        Self::from_secs(mins * 60)
    }

    /// Construct a new duration from milliseconds.
    pub const fn from_millis(millis: u128) -> Self {
        Self(millis)
    }

    /// Return the number of minutes in this duration.
    pub const fn as_mins(&self) -> u64 {
        self.as_secs() / 60
    }

    /// Return the number of seconds in this duration.
    pub const fn as_secs(&self) -> u64 {
        (self.0 / 1000) as u64
    }

    /// Return the number of milliseconds in this duration.
    pub const fn as_millis(&self) -> u128 {
        self.0
    }
}

impl std::fmt::Display for LocalDuration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.as_millis() < 1000 {
            write!(f, "{} millisecond(s)", self.as_millis())
        } else if self.as_secs() < 60 {
            write!(f, "{} second(s)", self.as_secs())
        } else {
            write!(f, "{} minute(s)", self.as_mins())
        }
    }
}

impl<'a> std::iter::Sum<&'a LocalDuration> for LocalDuration {
    fn sum<I: Iterator<Item = &'a LocalDuration>>(iter: I) -> LocalDuration {
        let mut total: u128 = 0;

        for entry in iter {
            total = total
                .checked_add(entry.0)
                .expect("iter::sum should not overflow");
        }
        Self(total)
    }
}

impl std::ops::Add<LocalDuration> for LocalDuration {
    type Output = LocalDuration;

    fn add(self, other: LocalDuration) -> LocalDuration {
        LocalDuration(self.0 + other.0)
    }
}

impl std::ops::Div<u32> for LocalDuration {
    type Output = LocalDuration;

    fn div(self, other: u32) -> LocalDuration {
        LocalDuration(self.0 / other as u128)
    }
}

impl From<LocalDuration> for std::time::Duration {
    fn from(other: LocalDuration) -> Self {
        std::time::Duration::from_millis(other.0 as u64)
    }
}

/// Network-adjusted time tracker.
///
/// *Network-adjusted time* is the median timestamp of all connected peers.
/// Since we store only time offsets for each peer, the network-adjusted time is
/// the local time plus the median offset of all connected peers.
///
/// Nb. Network time is never adjusted more than 70 minutes from local system time.
#[derive(Debug, Clone)]
pub struct AdjustedTime<K> {
    /// Sample sources. Prevents us from getting two samples from the same source.
    sources: HashSet<K>,
    /// Time offset samples.
    samples: Vec<TimeOffset>,
    /// Current time offset, based on our samples.
    offset: TimeOffset,
    /// Last known local time.
    local_time: LocalTime,
}

impl<K: Eq + Hash> Clock for AdjustedTime<K> {
    fn block_time(&self) -> BlockTime {
        self.get()
    }

    fn local_time(&self) -> LocalTime {
        self.local_time()
    }
}

impl<K: Hash + Eq> Default for AdjustedTime<K> {
    fn default() -> Self {
        Self::new(LocalTime::default())
    }
}

impl<K: Hash + Eq> AdjustedTime<K> {
    /// Create a new network-adjusted time tracker.
    /// Starts with a single sample of zero.
    pub fn new(local_time: LocalTime) -> Self {
        let offset = 0;

        let mut samples = Vec::with_capacity(MAX_TIME_SAMPLES);
        samples.push(offset);

        let sources = HashSet::with_capacity(MAX_TIME_SAMPLES);

        Self {
            sources,
            samples,
            offset,
            local_time,
        }
    }

    /// Add a time sample to influence the network-adjusted time.
    pub fn record_offset(&mut self, source: K, sample: TimeOffset) {
        // Nb. This behavior is based on Bitcoin Core. An alternative is to truncate the
        // samples list, to never exceed `MAX_TIME_SAMPLES`, and allow new samples to be
        // added to the list, while the set of sample sources keeps growing. This has the
        // advantage that as new peers are discovered, the network time can keep adjusting,
        // while old samples get discarded. Such behavior is found in `btcd`.
        //
        // Another quirk of this implementation is that the actual number of samples can
        // reach `MAX_TIME_SAMPLES + 1`, since there is always an initial `0` sample with
        // no associated source.
        //
        // Finally, we never remove sources. Even after peers disconnect. This is congruent
        // with Bitcoin Core behavior. I'm not sure why that is.
        if self.sources.len() == MAX_TIME_SAMPLES {
            return;
        }
        if !self.sources.insert(source) {
            return;
        }
        self.samples.push(sample);

        let mut offsets = self.samples.clone();
        let count = offsets.len();

        offsets.sort_unstable();

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
            #[cfg(feature = "log")]
            log::debug!("Time offset adjusted to {} seconds", self.offset);
        };
    }

    /// Get the median network time offset.
    pub fn offset(&self) -> TimeOffset {
        self.offset
    }

    /// Get the network-adjusted time given a local time.
    pub fn from(&self, time: BlockTime) -> BlockTime {
        let adjustment = self.offset;

        if adjustment > 0 {
            time + adjustment as BlockTime
        } else {
            time - adjustment.abs() as BlockTime
        }
    }

    /// Get the current network-adjusted time.
    pub fn get(&self) -> BlockTime {
        self.from(self.local_time.block_time())
    }

    /// Set the local time to the given value.
    pub fn set_local_time(&mut self, time: LocalTime) {
        self.local_time = time;
    }

    /// Get the last known local time.
    pub fn local_time(&self) -> LocalTime {
        self.local_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    #[test]
    fn test_adjusted_time() {
        let mut adjusted_time: AdjustedTime<SocketAddr> = AdjustedTime::default();
        assert_eq!(adjusted_time.offset(), 0); // samples = [0]

        adjusted_time.record_offset(([127, 0, 0, 1], 8333).into(), 42);
        assert_eq!(adjusted_time.offset(), 0); // samples = [0, 42]

        adjusted_time.record_offset(([127, 0, 0, 2], 8333).into(), 47);
        assert_eq!(adjusted_time.offset(), 0); // samples = [0, 42, 47]

        for i in 3.. {
            adjusted_time.record_offset(([127, 0, 0, i], 8333).into(), MAX_TIME_ADJUSTMENT + 1);

            if adjusted_time.samples.len() >= MIN_TIME_SAMPLES {
                break;
            }
        }
        assert_eq!(adjusted_time.offset(), 47); // samples = [0, 42, 47, 4201, 4201]

        adjusted_time.record_offset(([127, 0, 0, 5], 8333).into(), MAX_TIME_ADJUSTMENT + 1);
        assert_eq!(
            adjusted_time.offset(),
            47,
            "No change when sample count is even"
        ); // samples = [0, 42, 47, 4201, 4201, 4201]

        adjusted_time.record_offset(([127, 0, 0, 6], 8333).into(), MAX_TIME_ADJUSTMENT + 1);
        assert_eq!(
            adjusted_time.offset(),
            0,
            "A too large time adjustment reverts back to 0",
        ); // samples = [0, 42, 47, 4201, 4201, 4201, 4201]
    }

    #[test]
    fn test_adjusted_time_negative() {
        use std::time::SystemTime;

        let local_time = SystemTime::now().into();
        let mut adjusted_time: AdjustedTime<SocketAddr> = AdjustedTime::new(local_time);
        assert_eq!(adjusted_time.offset(), 0); // samples = [0]

        for i in 1..5 {
            adjusted_time.record_offset(([127, 0, 0, i], 8333).into(), 96);
        } // samples = [0, 96, 96, 96, 96]
        assert_eq!(adjusted_time.offset(), 96);
        assert_eq!(
            adjusted_time.from(local_time.block_time()),
            local_time.block_time() + 96
        );

        for i in 5..11 {
            adjusted_time.record_offset(([127, 0, 0, i], 8333).into(), -96);
        } // samples = [-96, -96, -96, -96, -96, -96, 0, 96, 96, 96, 96]
        assert_eq!(adjusted_time.offset(), -96);
        assert_eq!(
            adjusted_time.from(local_time.block_time()),
            local_time.block_time() - 96
        );
    }

    #[test]
    fn test_adjusted_time_max_samples() {
        let mut adjusted_time: AdjustedTime<SocketAddr> = AdjustedTime::default();
        assert_eq!(adjusted_time.offset(), 0); // samples = [0]

        for i in 1..(MAX_TIME_SAMPLES / 2) {
            adjusted_time.record_offset(([127, 0, 0, i as u8], 8333).into(), -1);
        }
        assert_eq!(adjusted_time.offset(), -1);

        for i in (MAX_TIME_SAMPLES / 2).. {
            adjusted_time.record_offset(([127, 0, 0, i as u8], 8333).into(), 1);

            if adjusted_time.samples.len() == MAX_TIME_SAMPLES {
                break;
            }
        }
        // We added an equal number of samples on each side of the initial sample.
        // There are 99 samples before, and 99 samples after.
        assert_eq!(adjusted_time.offset(), 0);

        adjusted_time.record_offset(([127, 0, 0, 253], 8333).into(), 1);
        adjusted_time.record_offset(([127, 0, 0, 254], 8333).into(), 2);
        adjusted_time.record_offset(([127, 0, 0, 255], 8333).into(), 3);
        assert_eq!(
            adjusted_time.sources.len(),
            MAX_TIME_SAMPLES,
            "Adding a sample after the maximum is reached, has no effect"
        );
    }
}
