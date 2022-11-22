//! Time-related functionality useful for reactors.
pub use nakamoto_net::time::{LocalDuration, LocalTime};

/// Manages timers and triggers timeouts.
pub struct TimeoutManager<K> {
    timeouts: Vec<(K, LocalTime)>,
    threshold: LocalDuration,
}

impl<K> TimeoutManager<K> {
    /// Create a new timeout manager.
    ///
    /// Takes a threshold below which two timeouts cannot overlap.
    pub fn new(threshold: LocalDuration) -> Self {
        Self {
            timeouts: vec![],
            threshold,
        }
    }

    /// Return the number of timeouts being tracked.
    pub fn len(&self) -> usize {
        self.timeouts.len()
    }

    /// Check whether there are timeouts being tracked.
    pub fn is_empty(&self) -> bool {
        self.timeouts.is_empty()
    }

    /// Register a new timeout with an associated key and wake-up time.
    ///
    /// ```
    /// use nakamoto_net_polling::time::{LocalTime, LocalDuration, TimeoutManager};
    ///
    /// let mut tm = TimeoutManager::new(LocalDuration::from_secs(1));
    /// let now = LocalTime::now();
    ///
    /// let registered = tm.register(0xA, now + LocalDuration::from_secs(8));
    /// assert!(registered);
    ///
    /// let registered = tm.register(0xB, now + LocalDuration::from_secs(9));
    /// assert!(registered);
    /// assert_eq!(tm.len(), 2);
    ///
    /// let registered = tm.register(0xC, now + LocalDuration::from_millis(9541));
    /// assert!(!registered);
    ///
    /// let registered = tm.register(0xC, now + LocalDuration::from_millis(9999));
    /// assert!(!registered);
    /// assert_eq!(tm.len(), 2);
    /// ```
    pub fn register(&mut self, key: K, time: LocalTime) -> bool {
        // If this timeout is too close to a pre-existing timeout,
        // don't register it.
        if self
            .timeouts
            .iter()
            .any(|(_, t)| t.diff(time) < self.threshold)
        {
            return false;
        }

        self.timeouts.push((key, time));
        self.timeouts.sort_unstable_by(|(_, a), (_, b)| b.cmp(a));

        true
    }

    /// Get the minimum time duration we should wait for at least one timeout
    /// to be reached.  Returns `None` if there are no timeouts.
    ///
    /// ```
    /// use nakamoto_net_polling::time::{LocalTime, LocalDuration, TimeoutManager};
    ///
    /// let mut tm = TimeoutManager::new(LocalDuration::from_secs(0));
    /// let mut now = LocalTime::now();
    ///
    /// tm.register(0xA, now + LocalDuration::from_millis(16));
    /// tm.register(0xB, now + LocalDuration::from_millis(8));
    /// tm.register(0xC, now + LocalDuration::from_millis(64));
    ///
    /// // We need to wait 8 millis to trigger the next timeout (1).
    /// assert!(tm.next(now) <= Some(LocalDuration::from_millis(8)));
    ///
    /// // ... sleep for a millisecond ...
    /// now.elapse(LocalDuration::from_millis(1));
    ///
    /// // Now we don't need to wait as long!
    /// assert!(tm.next(now).unwrap() <= LocalDuration::from_millis(7));
    /// ```
    pub fn next(&self, now: impl Into<LocalTime>) -> Option<LocalDuration> {
        let now = now.into();

        self.timeouts.last().map(|(_, t)| {
            if *t >= now {
                *t - now
            } else {
                LocalDuration::from_secs(0)
            }
        })
    }

    /// Given the current time, populate the input vector with the keys that
    /// have timed out. Returns the number of keys that timed out.
    ///
    /// ```
    /// use nakamoto_net_polling::time::{LocalTime, LocalDuration, TimeoutManager};
    ///
    /// let mut tm = TimeoutManager::new(LocalDuration::from_secs(0));
    /// let now = LocalTime::now();
    ///
    /// tm.register(0xA, now + LocalDuration::from_millis(8));
    /// tm.register(0xB, now + LocalDuration::from_millis(16));
    /// tm.register(0xC, now + LocalDuration::from_millis(64));
    /// tm.register(0xD, now + LocalDuration::from_millis(72));
    ///
    /// let mut timeouts = Vec::new();
    ///
    /// assert_eq!(tm.wake(now + LocalDuration::from_millis(21), &mut timeouts), 2);
    /// assert_eq!(timeouts, vec![0xA, 0xB]);
    /// assert_eq!(tm.len(), 2);
    /// ```
    pub fn wake(&mut self, now: LocalTime, woken: &mut Vec<K>) -> usize {
        let before = woken.len();

        while let Some((k, t)) = self.timeouts.pop() {
            if now >= t {
                woken.push(k);
            } else {
                self.timeouts.push((k, t));
                break;
            }
        }
        woken.len() - before
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck_macros::quickcheck;

    #[quickcheck]
    fn properties(timeouts: Vec<u64>, threshold: u64) -> bool {
        let threshold = LocalDuration::from_secs(threshold);
        let mut tm = TimeoutManager::new(threshold);
        let mut now = LocalTime::now();

        for t in timeouts {
            tm.register(t, now + LocalDuration::from_secs(t));
        }

        let mut woken = Vec::new();
        while let Some(delta) = tm.next(now) {
            now.elapse(delta);
            assert!(tm.wake(now, &mut woken) > 0);
        }

        let sorted = woken.windows(2).all(|w| w[0] <= w[1]);
        let granular = woken.windows(2).all(|w| w[1] - w[0] >= threshold.as_secs());

        sorted && granular
    }

    #[test]
    fn test_wake() {
        let mut tm = TimeoutManager::new(LocalDuration::from_secs(0));
        let now = LocalTime::now();

        tm.register(0xA, now + LocalDuration::from_millis(8));
        tm.register(0xB, now + LocalDuration::from_millis(16));
        tm.register(0xC, now + LocalDuration::from_millis(64));
        tm.register(0xD, now + LocalDuration::from_millis(72));

        let mut timeouts = Vec::new();

        assert_eq!(tm.wake(now, &mut timeouts), 0);
        assert_eq!(timeouts, vec![]);
        assert_eq!(tm.len(), 4);
        assert_eq!(
            tm.wake(now + LocalDuration::from_millis(9), &mut timeouts),
            1
        );
        assert_eq!(timeouts, vec![0xA]);
        assert_eq!(tm.len(), 3, "one timeout has expired");

        timeouts.clear();

        assert_eq!(
            tm.wake(now + LocalDuration::from_millis(66), &mut timeouts),
            2
        );
        assert_eq!(timeouts, vec![0xB, 0xC]);
        assert_eq!(tm.len(), 1, "another two timeouts have expired");

        timeouts.clear();

        assert_eq!(
            tm.wake(now + LocalDuration::from_millis(96), &mut timeouts),
            1
        );
        assert_eq!(timeouts, vec![0xD]);
        assert!(tm.is_empty(), "all timeouts have expired");
    }
}
