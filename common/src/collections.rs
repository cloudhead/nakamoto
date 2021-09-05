//! Collections used in `nakamoto`.
use bitcoin_hashes::siphash24::Hash;
use std::ops::{Deref, DerefMut};

use crate::nonempty::NonEmpty;

/// A `HashMap` which uses `fastrand::Rng` for its random state.
pub type HashMap<K, V> = std::collections::HashMap<K, V, RandomState>;

/// A `HashSet` which uses `fastrand::Rng` for its random state.
pub type HashSet<K> = std::collections::HashSet<K, RandomState>;

/// Hasher using `siphash24`.
#[derive(Default)]
pub struct Hasher {
    data: Vec<u8>,
    key1: u64,
    key2: u64,
}

impl Hasher {
    fn new(key1: u64, key2: u64) -> Self {
        Self {
            data: vec![],
            key1,
            key2,
        }
    }
}

impl std::hash::Hasher for Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes)
    }

    fn finish(&self) -> u64 {
        Hash::hash_with_keys(self.key1, self.key2, &self.data).as_u64()
    }
}

/// Random hasher state.
#[derive(Default, Clone)]
pub struct RandomState {
    key1: u64,
    key2: u64,
}

impl RandomState {
    fn new(rng: fastrand::Rng) -> Self {
        Self {
            key1: rng.u64(..),
            key2: rng.u64(..),
        }
    }
}

impl std::hash::BuildHasher for RandomState {
    type Hasher = Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        Hasher::new(self.key1, self.key2)
    }
}

impl From<fastrand::Rng> for RandomState {
    fn from(rng: fastrand::Rng) -> Self {
        Self::new(rng)
    }
}

/// A map with the ability to randomly select values.
#[derive(Debug)]
pub struct AddressBook<K, V> {
    inner: HashMap<K, V>,
    rng: fastrand::Rng,
}

impl<K, V> AddressBook<K, V> {
    /// Create a new address book.
    pub fn new(rng: fastrand::Rng) -> Self {
        Self {
            inner: HashMap::with_hasher(rng.clone().into()),
            rng,
        }
    }

    /// Pick a random value in the book.
    pub fn sample(&self) -> Option<(&K, &V)> {
        self.sample_with(|_, _| true)
    }

    /// Pick a random value in the book matching a predicate.
    pub fn sample_with(&self, mut predicate: impl FnMut(&K, &V) -> bool) -> Option<(&K, &V)> {
        if let Some(pairs) = NonEmpty::from_vec(
            self.inner
                .iter()
                .filter(|(k, v)| predicate(*k, *v))
                .collect(),
        ) {
            let ix = self.rng.usize(..pairs.len());
            let pair = pairs[ix]; // Can't fail.

            Some(pair)
        } else {
            None
        }
    }

    /// Cycle through the keys at random. The random cycle repeats ad-infintum.
    pub fn cycle(&self) -> impl Iterator<Item = &K> {
        let mut keys = self.inner.keys().collect::<Vec<_>>();
        self.rng.shuffle(&mut keys);

        keys.into_iter().cycle()
    }
}

impl<K, V> Deref for AddressBook<K, V> {
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V> DerefMut for AddressBook<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
