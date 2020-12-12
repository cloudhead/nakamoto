//! Time-related functionality useful for reactors.
use std::time::SystemTime;

pub use nakamoto_common::block::time::{LocalDuration, LocalTime};

/// Manages timers and triggers timeouts.
pub struct TimeoutManager<K> {
    timeouts: Vec<(K, LocalTime)>,
}

impl<K> TimeoutManager<K> {
    /// Create a new timeout manager.
    pub fn new() -> Self {
        Self { timeouts: vec![] }
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
    pub fn register(&mut self, key: K, time: LocalTime) {
        self.timeouts.push((key, time));
        self.timeouts.sort_unstable_by(|(_, a), (_, b)| b.cmp(a));
    }

    /// Get the minimum time duration we should wait for at least one timeout
    /// to be reached.  Returns `None` if there are no timeouts.
    ///
    /// ```
    /// use nakamoto_net_poll::time::{LocalTime, LocalDuration, TimeoutManager};
    ///
    /// let mut tm = TimeoutManager::new();
    /// let now = LocalTime::now();
    ///
    /// tm.register(0xA, now + LocalDuration::from_millis(16));
    /// tm.register(0xB, now + LocalDuration::from_millis(8));
    /// tm.register(0xC, now + LocalDuration::from_millis(64));
    ///
    /// // We need to wait 8 millis to trigger the next timeout (1).
    /// assert!(tm.next() <= Some(LocalDuration::from_millis(8)));
    ///
    /// // Sleep for a millisecond.
    /// std::thread::sleep(std::time::Duration::from_millis(1));
    ///
    /// // Now we don't need to wait as long!
    /// assert!(tm.next().unwrap() <= LocalDuration::from_millis(7));
    /// ```
    pub fn next(&self) -> Option<LocalDuration> {
        let now: LocalTime = SystemTime::now().into();

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
    /// use nakamoto_net_poll::time::{LocalTime, LocalDuration, TimeoutManager};
    ///
    /// let mut tm = TimeoutManager::new();
    /// let now = LocalTime::now();
    ///
    /// tm.register(0xA, now + LocalDuration::from_millis(8));
    /// tm.register(0xB, now + LocalDuration::from_millis(16));
    /// tm.register(0xC, now + LocalDuration::from_millis(64));
    /// tm.register(0xD, now + LocalDuration::from_millis(72));
    ///
    /// let mut timeouts = Vec::new();
    ///
    /// tm.wake(now, &mut timeouts);
    /// assert_eq!(timeouts, vec![]);
    /// assert_eq!(tm.len(), 4);
    ///
    /// tm.wake(now + LocalDuration::from_millis(9), &mut timeouts);
    /// assert_eq!(timeouts, vec![0xA]);
    /// assert_eq!(tm.len(), 3, "one timeout has expired");
    ///
    /// tm.wake(now + LocalDuration::from_millis(66), &mut timeouts);
    /// assert_eq!(timeouts, vec![0xB, 0xC]);
    /// assert_eq!(tm.len(), 1, "another two timeouts have expired");
    ///
    /// tm.wake(now + LocalDuration::from_millis(96), &mut timeouts);
    /// assert_eq!(timeouts, vec![0xD]);
    /// assert!(tm.is_empty(), "all timeouts have expired");
    ///
    /// ```
    pub fn wake(&mut self, now: LocalTime, woken: &mut Vec<K>) {
        woken.clear();

        while let Some((k, t)) = self.timeouts.pop() {
            if now >= t {
                woken.push(k);
            } else {
                self.timeouts.push((k, t));
                break;
            }
        }
    }
}
