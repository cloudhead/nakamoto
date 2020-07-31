#![allow(dead_code)]
use std::collections::{HashMap, HashSet};
use std::net;

use nakamoto_chain::block::time::LocalTime;

/// Maximum number of addresses we store for a given address range.
const MAX_RANGE_SIZE: usize = 256;

#[derive(Debug)]
struct KnownAddress {
    ip: net::Ipv4Addr,
    port: u16,

    last_success: Option<LocalTime>,
    last_attempt: Option<LocalTime>,
}

impl KnownAddress {
    fn new(ip: net::Ipv4Addr, port: u16) -> Self {
        Self {
            ip,
            port,
            last_success: None,
            last_attempt: None,
        }
    }

    fn to_socket_addr(&self) -> net::SocketAddr {
        net::SocketAddr::new(net::IpAddr::V4(self.ip), self.port)
    }
}

#[derive(Debug)]
pub struct AddressManager {
    // FIXME: HashMap and HashSet are non-deterministic.
    addresses: HashMap<net::Ipv4Addr, KnownAddress>,
    address_ranges: HashMap<u8, HashSet<net::Ipv4Addr>>,
}

impl AddressManager {
    /// Create a new, empty address manager.
    pub fn new() -> Self {
        Self {
            addresses: HashMap::new(),
            address_ranges: HashMap::new(),
        }
    }

    /// The number of addresses known.
    pub fn len(&self) -> usize {
        self.addresses.len()
    }

    /// Whether there are any addresses known to the address manager.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear the address manager of all addresses.
    pub fn clear(&mut self) {
        self.addresses.clear();
        self.address_ranges.clear();
    }

    /// Add addresses to the address manager.
    ///
    /// ```
    /// use nakamoto_p2p::protocol::bitcoin::address_manager::AddressManager;
    ///
    /// let mut addrmgr = AddressManager::new();
    ///
    /// addrmgr.insert(&[
    ///     ([183, 8, 55, 2], 8333).into(),
    ///     ([211, 48, 99, 4], 8333).into(),
    ///     ([241, 44, 12, 5], 8333).into(),
    /// ]);
    /// assert_eq!(addrmgr.len(), 3);
    ///
    /// addrmgr.insert(&[([183, 8, 55, 2], 8333).into()]);
    /// assert_eq!(addrmgr.len(), 3, "already known addresses are ignored");
    ///
    /// addrmgr.clear();
    /// addrmgr.insert(&[
    ///     ([127, 0, 0, 1], 8333).into(),
    ///     ([192, 168, 1, 1], 8333).into(),
    ///     ([255, 255, 255, 255], 8333).into(),
    /// ]);
    /// assert!(addrmgr.is_empty(), "non-routable addresses are ignored");
    /// ```
    pub fn insert(&mut self, addrs: &[net::SocketAddr]) {
        for addr in addrs {
            match addr.ip() {
                net::IpAddr::V4(ip) => {
                    if self::is_routable(&ip) {
                        if self.addresses.contains_key(&ip) {
                            return;
                        }
                        self.addresses
                            .insert(ip, KnownAddress::new(ip, addr.port()));

                        // Use the *second* component of the IP address to key into the
                        // range buckets.
                        //
                        // Eg. 124.99.123.1 and 124.54.123.1 would be placed in
                        // different buckets, but 100.99.43.12 and 100.99.12.8
                        // would be placed in the same bucket.
                        //
                        let range = ip.octets()[1];
                        let range = self.address_ranges.entry(range).or_default();

                        // If the address range is already full, remove a random address
                        // before inserting this new one.
                        if range.len() == MAX_RANGE_SIZE {
                            let ix = fastrand::usize(..range.len());
                            let addr = range
                                .iter()
                                .cloned()
                                .nth(ix)
                                .expect("the range is not empty");

                            range.remove(&addr);
                            self.addresses.remove(&addr);
                        }
                        range.insert(ip);
                    }
                }
                net::IpAddr::V6(_) => {}
            }
        }
    }

    /// Pick an address at random from the set of known addresses.
    ///
    /// This function tries to ensure a good geo-diversity of addresses, such that an adversary
    /// controlling a disproportionately large number of addresses in the same address range does
    /// not have an advantage over other peers.
    ///
    /// This works under the assumption that adversaries are *localized*.
    ///
    /// ```
    /// use nakamoto_p2p::protocol::bitcoin::address_manager::AddressManager;
    ///
    /// let mut addrmgr = AddressManager::new();
    ///
    /// // Addresses controlled by an adversary.
    /// let adversary_addrs = &[
    ///     ([111, 8, 55, 2], 8333).into(),
    ///     ([111, 8, 43, 11], 8333).into(),
    ///     ([111, 8, 89, 6], 8333).into(),
    ///     ([111, 8, 124, 41], 8333).into(),
    ///     ([111, 8, 65, 4], 8333).into(),
    ///     ([111, 8, 161, 73], 8333).into(),
    /// ];
    /// addrmgr.insert(adversary_addrs);
    ///
    /// // Safe addresses, controlled by non-adversarial peers.
    /// let safe_addrs = &[
    ///     ([183, 8, 55, 2], 8333).into(),
    ///     ([211, 48, 99, 4], 8333).into(),
    ///     ([241, 44, 12, 5], 8333).into(),
    /// ];
    /// addrmgr.insert(safe_addrs);
    ///
    /// // Keep track of how many times we pick a safe vs. an adversary-controlled address.
    /// let mut adversary = 0;
    /// let mut safe = 0;
    ///
    /// for _ in 0..99 {
    ///     let addr = addrmgr.sample().unwrap();
    ///
    ///     if adversary_addrs.contains(&addr) {
    ///         adversary += 1;
    ///     } else if safe_addrs.contains(&addr) {
    ///         safe += 1;
    ///     }
    /// }
    ///
    /// // Despite there being more adversary-controlled addresses, our safe addresses
    /// // are picked much more often.
    /// assert!(safe > adversary * 2, "safe addresses are picked twice more often");
    ///
    /// ```
    pub fn sample(&self) -> Option<net::SocketAddr> {
        if self.is_empty() {
            return None;
        }

        // First select a random address range.
        let ix = fastrand::usize(..self.address_ranges.len());
        let range = self.address_ranges.values().nth(ix)?;

        // Then select a random address in that range.
        let ix = fastrand::usize(..range.len());
        let addr = range.iter().nth(ix)?;

        self.addresses.get(addr).map(KnownAddress::to_socket_addr)
    }
}

/// Check whether an address is globally routable.
///
/// This code is adapted from the Rust standard library's `net::Ipv4Addr::is_global`. It can be
/// replaced once that function is stabilized.
fn is_routable(addr: &net::Ipv4Addr) -> bool {
    // Check if this address is 192.0.0.9 or 192.0.0.10. These addresses are the only two
    // globally routable addresses in the 192.0.0.0/24 range.
    if u32::from(*addr) == 0xc0000009 || u32::from(*addr) == 0xc000000a {
        return true;
    }
    !addr.is_private()
        && !addr.is_loopback()
        && !addr.is_link_local()
        && !addr.is_broadcast()
        && !addr.is_documentation()
        // Make sure the address is not in 0.0.0.0/8.
        && addr.octets()[0] != 0
}

#[cfg(test)]
mod tests {
    use super::AddressManager;
    use super::MAX_RANGE_SIZE;

    #[test]
    fn test_sample_empty() {
        let addrmgr = AddressManager::new();

        assert!(addrmgr.sample().is_none());
    }

    #[test]
    fn test_max_range_size() {
        let mut addrmgr = AddressManager::new();

        for i in 0..MAX_RANGE_SIZE + 1 {
            addrmgr.insert(&[([111, 111, (i / u8::MAX as usize) as u8, i as u8], 8333).into()]);
        }
        assert_eq!(
            addrmgr.len(),
            MAX_RANGE_SIZE,
            "we can't insert more than a certain amount of addresses in the same range"
        );

        addrmgr.insert(&[([129, 44, 12, 2], 8333).into()]);
        assert_eq!(
            addrmgr.len(),
            MAX_RANGE_SIZE + 1,
            "inserting in another range is perfectly fine"
        );
    }
}
