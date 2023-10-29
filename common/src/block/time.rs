//! Block time and other time-related types.
use std::cell::RefCell;
use std::collections::HashSet;
use std::hash::Hash;
use std::rc::Rc;

use super::{BlockTime, Height};

pub use nakamoto_net::time::{LocalDuration, LocalTime};

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
pub trait Clock: Clone {
    /// Return the local time as seconds since Epoch.
    /// This is the same representation as used in block header timestamps.
    fn block_time(&self) -> BlockTime;
    /// Tell the time in local time.
    fn local_time(&self) -> LocalTime;
    /// Create a clock from a block time.
    fn from_block_time(t: BlockTime) -> Self;
}

/// A network-adjusted clock.
pub trait AdjustedClock<K>: Clock {
    /// Record a peer offset.
    fn record_offset(&mut self, source: K, sample: TimeOffset);
    /// Set the local time.
    fn set(&mut self, local_time: LocalTime);
}

impl<K: Eq + Clone + Hash> AdjustedClock<K> for AdjustedTime<K> {
    fn record_offset(&mut self, source: K, sample: TimeOffset) {
        AdjustedTime::record_offset(self, source, sample)
    }

    fn set(&mut self, local_time: LocalTime) {
        AdjustedTime::set_local_time(self, local_time)
    }
}

/// Clock with interior mutability.
#[derive(Debug, Clone)]
pub struct RefClock<T: Clock> {
    inner: Rc<RefCell<T>>,
}

impl<T: Clock> std::ops::Deref for RefClock<T> {
    type Target = Rc<RefCell<T>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl RefClock<LocalTime> {
    /// Elapse time.
    pub fn elapse(&self, duration: LocalDuration) {
        self.inner.borrow_mut().elapse(duration)
    }
}

impl<K: Eq + Clone + Hash> RefClock<AdjustedTime<K>> {
    /// Elapse time.
    pub fn elapse(&self, duration: LocalDuration) {
        self.inner.borrow_mut().elapse(duration)
    }
}

impl<K: Eq + Clone + Hash> AdjustedClock<K> for RefClock<AdjustedTime<K>> {
    fn record_offset(&mut self, source: K, sample: TimeOffset) {
        self.inner.borrow_mut().record_offset(source, sample);
    }

    fn set(&mut self, local_time: LocalTime) {
        self.inner.borrow_mut().set_local_time(local_time);
    }
}

impl<T: Clock> From<T> for RefClock<T> {
    fn from(other: T) -> Self {
        Self {
            inner: Rc::new(RefCell::new(other)),
        }
    }
}

impl<T: Clock> Clock for RefClock<T> {
    fn block_time(&self) -> BlockTime {
        self.inner.borrow().block_time()
    }

    fn local_time(&self) -> LocalTime {
        self.inner.borrow().local_time()
    }

    fn from_block_time(t: BlockTime) -> Self {
        RefClock::from(T::from_block_time(t))
    }
}

impl Clock for LocalTime {
    fn block_time(&self) -> BlockTime {
        self.as_secs() as u32
    }

    fn local_time(&self) -> LocalTime {
        *self
    }

    fn from_block_time(t: BlockTime) -> Self {
        LocalTime::from_secs(t as u64)
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

impl<K: Eq + Clone + Hash> Clock for AdjustedTime<K> {
    fn block_time(&self) -> BlockTime {
        self.get()
    }

    fn local_time(&self) -> LocalTime {
        self.local_time()
    }

    fn from_block_time(t: BlockTime) -> Self {
        AdjustedTime::new(LocalTime::from_block_time(t))
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
            time - adjustment.unsigned_abs() as BlockTime
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

impl<T> std::ops::Deref for AdjustedTime<T> {
    type Target = LocalTime;

    fn deref(&self) -> &Self::Target {
        &self.local_time
    }
}

impl<T> std::ops::DerefMut for AdjustedTime<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.local_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    #[test]
    fn test_local_duration_display() {
        assert_eq!(LocalDuration::from_mins(90).to_string(), "1.50 hour(s)");
        assert_eq!(LocalDuration::from_mins(60).to_string(), "1 hour(s)");
        assert_eq!(
            LocalDuration::from_millis(1280).to_string(),
            "1.280 second(s)"
        );
        assert_eq!(
            LocalDuration::from_millis(980).to_string(),
            "980 millisecond(s)"
        );
    }

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
