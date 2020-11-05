//! Collections used in `nakamoto`.
use bitcoin_hashes::siphash24::Hash;

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
