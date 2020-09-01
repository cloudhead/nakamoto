//!
//! The peer-to-peer address manager.
//!
#![warn(missing_docs)]
use std::net;

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;

use nakamoto_common::block::time::LocalTime;
use nakamoto_common::block::Time;
use nakamoto_common::collections::{HashMap, HashSet};

use crate::address_book::AddressBook;

/// Maximum number of addresses we store for a given address range.
const MAX_RANGE_SIZE: usize = 256;

/// Address source. Specifies where an address originated from.
#[derive(Debug, Clone)]
pub enum Source {
    /// An address that was shared by another peer.
    Peer(net::SocketAddr),
    /// An address that was discovered by connecting to a peer.
    Connected,
    /// An address from an unspecified source.
    Other,
}

impl Default for Source {
    fn default() -> Self {
        Self::Other
    }
}

/// A known address.
#[derive(Debug)]
struct KnownAddress {
    /// Network address.
    addr: Address,
    /// Address of the peer who sent us this address.
    source: Source,
    /// Last time this address was used to successfully connect to a peer.
    last_success: Option<LocalTime>,
    /// Last time this address was tried.
    last_attempt: Option<LocalTime>,
}

impl KnownAddress {
    fn new(addr: Address, source: Source) -> Self {
        Self {
            addr,
            source,
            last_success: None,
            last_attempt: None,
        }
    }
}

/// Manages peer network addresses.
#[derive(Debug)]
pub struct AddressManager {
    addresses: HashMap<net::IpAddr, KnownAddress>,
    address_ranges: HashMap<u8, HashSet<net::IpAddr>>,
    connected: HashSet<net::IpAddr>,
    local_addrs: HashSet<net::SocketAddr>,
    rng: fastrand::Rng,
}

impl AddressManager {
    /// Create a new, empty address manager.
    pub fn new(rng: fastrand::Rng) -> Self {
        Self {
            addresses: HashMap::with_hasher(rng.clone().into()),
            address_ranges: HashMap::with_hasher(rng.clone().into()),
            connected: HashSet::with_hasher(rng.clone().into()),
            local_addrs: HashSet::with_hasher(rng.clone().into()),
            rng,
        }
    }

    /// Create a new address manager from an address book.
    pub fn from(book: AddressBook, rng: fastrand::Rng) -> Self {
        let mut addrmgr = Self::new(rng);

        for addr in book.iter() {
            addrmgr.insert_sockaddr(std::iter::once(*addr), Source::Other);
        }
        addrmgr
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

    /// Record an address of ours as seen by a remote peer.
    /// This helps avoid self-connections.
    pub fn record_local_addr(&mut self, addr: net::SocketAddr) {
        self.local_addrs.insert(addr);
    }

    /// Same as `insert`, but supports adding bare socket addresses to the address manager.
    /// This is useful when adding addresses manually, or from sources other than the network.
    pub fn insert_sockaddr(
        &mut self,
        addrs: impl Iterator<Item = net::SocketAddr>,
        source: Source,
    ) -> bool {
        self.insert(
            addrs.map(|a| (Time::default(), Address::new(&a, ServiceFlags::NONE))),
            source,
        )
    }

    /// Called when a peer connection timed out while attempting to connect.
    pub fn peer_attempted(&mut self, addr: &net::SocketAddr, time: LocalTime) {
        self.addresses
            .get_mut(&addr.ip())
            .expect("the address must exist")
            .last_attempt = Some(time);
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, addr: &net::SocketAddr) {
        // TODO: For now, it's enough to remove the address, since we shouldn't
        // be trying to reconnect to a peer that disconnected. However in the future
        // we should probably keep it around, or decide based on the reason for
        // disconnection.
        self.addresses.remove(&addr.ip());
        self.connected.remove(&addr.ip());

        let key = self::addr_key(addr.ip());

        if let Some(range) = self.address_ranges.get_mut(&key) {
            range.remove(&addr.ip());

            if range.is_empty() {
                self.address_ranges.remove(&key);
            }
        }
    }

    /// Called when a peer has handshaked.
    pub fn peer_negotiated(&mut self, addr: &net::SocketAddr, services: ServiceFlags) {
        self.insert(
            std::iter::once((Time::default(), Address::new(addr, services))),
            Source::Connected,
        );
    }

    /// Called when a peer has connected.
    pub fn peer_connected(&mut self, addr: &net::SocketAddr) {
        self.connected.insert(addr.ip());
    }

    /// Add addresses to the address manager. The input matches that of the `addr` message
    /// sent by peers on the network.
    ///
    /// ```
    /// use bitcoin::network::address::Address;
    /// use bitcoin::network::constants::ServiceFlags;
    ///
    /// use nakamoto_p2p::protocol::bitcoin::addrmgr::{AddressManager, Source};
    /// use nakamoto_common::block::Time;
    ///
    /// let mut addrmgr = AddressManager::new(fastrand::Rng::new());
    ///
    /// addrmgr.insert(vec![
    ///     Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
    /// ].into_iter().map(|a| (Time::default(), a)), Source::default());
    ///
    /// assert_eq!(addrmgr.len(), 3);
    ///
    /// addrmgr.insert(std::iter::once(
    ///     (Time::default(), Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE))
    /// ), Source::default());
    ///
    /// assert_eq!(addrmgr.len(), 3, "already known addresses are ignored");
    ///
    /// addrmgr.clear();
    /// addrmgr.insert(vec![
    ///     Address::new(&([255, 255, 255, 255], 8333).into(), ServiceFlags::NONE),
    /// ].into_iter().map(|a| (Time::default(), a)), Source::default());
    ///
    /// assert!(addrmgr.is_empty(), "non-routable/non-local addresses are ignored");
    /// ```
    pub fn insert(&mut self, addrs: impl Iterator<Item = (Time, Address)>, source: Source) -> bool {
        // True if at least one address was added into the map.
        let mut okay = false;

        // TODO: Store timestamp.
        for (_, addr) in addrs {
            let net_addr = match addr.socket_addr() {
                Ok(a) => a,
                Err(_) => continue,
            };
            let ip = net_addr.ip();

            // Ensure no self-connections.
            if self.local_addrs.contains(&net_addr) {
                continue;
            }

            // Ignore non-routable addresses if they come from a peer.
            // Allow local addresses when the source is `Other`.
            //
            // Nb. This is perhaps still not quite correct. A local peer may want to share
            // another local address.
            match source {
                Source::Peer(_) if !self::is_routable(&ip) => continue,
                Source::Other if !self::is_routable(&ip) && !self::is_local(&ip) => continue,
                _ => {}
            }

            if self
                .addresses
                .insert(ip, KnownAddress::new(addr, source.clone()))
                .is_some()
            {
                // Ignore addresses we already know.
                continue;
            }
            okay = true; // As soon as one addresse was inserted, consider it a success.

            let key = self::addr_key(net_addr.ip());
            let range = self
                .address_ranges
                .entry(key)
                .or_insert(HashSet::with_hasher(self.rng.clone().into()));

            // If the address range is already full, remove a random address
            // before inserting this new one.
            if range.len() == MAX_RANGE_SIZE {
                let ix = self.rng.usize(..range.len());
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
        okay
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
    /// use bitcoin::network::address::Address;
    /// use bitcoin::network::constants::ServiceFlags;
    ///
    /// use nakamoto_p2p::protocol::bitcoin::addrmgr::{Source, AddressManager};
    /// use nakamoto_common::block::Time;
    ///
    /// let mut addrmgr = AddressManager::new(fastrand::Rng::new());
    ///
    /// // Addresses controlled by an adversary.
    /// let adversary_addrs = vec![
    ///     Address::new(&([111, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([111, 8, 43, 11], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([111, 8, 89, 6], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([111, 8, 124, 41], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([111, 8, 65, 4], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([111, 8, 161, 73], 8333).into(), ServiceFlags::NONE),
    /// ];
    /// addrmgr.insert(
    ///     adversary_addrs.iter().cloned().map(|a| (Time::default(), a)), Source::default());
    ///
    /// // Safe addresses, controlled by non-adversarial peers.
    /// let safe_addrs = vec![
    ///     Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([99, 129, 2, 15], 8333).into(), ServiceFlags::NONE),
    /// ];
    /// addrmgr.insert(
    ///     safe_addrs.iter().cloned().map(|a| (Time::default(), a)), Source::default());
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
    /// TODO: Should return an iterator.
    ///
    pub fn sample(&self) -> Option<&Address> {
        assert!(self.connected.len() <= self.len());

        if self.is_empty() {
            return None;
        }
        if self.connected.len() == self.len() {
            return None;
        }

        let ip = loop {
            // First select a random address range.
            let ix = self.rng.usize(..self.address_ranges.len());
            let range = self.address_ranges.values().nth(ix)?;

            assert!(!range.is_empty());

            // Then select a random address in that range.
            let ix = self.rng.usize(..range.len());
            let ip = range.iter().nth(ix)?;

            if !self.connected.contains(&ip) {
                break ip;
            }
        };

        self.addresses.get(&ip).map(|ka| &ka.addr)
    }

    /// Iterate over all addresses.
    pub fn iter(&self) -> impl Iterator<Item = &Address> {
        self.addresses.iter().map(|(_, ka)| &ka.addr)
    }
}

/// Check whether an IP address is globally routable.
pub fn is_routable(addr: &net::IpAddr) -> bool {
    match addr {
        net::IpAddr::V4(addr) => ipv4_is_routable(addr),
        net::IpAddr::V6(addr) => ipv6_is_routable(addr),
    }
}

/// Check whether an IP address is locally routable.
pub fn is_local(addr: &net::IpAddr) -> bool {
    match addr {
        net::IpAddr::V4(addr) => {
            addr.is_private() || addr.is_loopback() || addr.is_link_local() || addr.is_unspecified()
        }
        net::IpAddr::V6(_) => false,
    }
}

/// Get the 8-bit key of an IP address. This key is based on the IP address's
/// range, and is used as a key to group IP addresses by range.
fn addr_key(ip: net::IpAddr) -> u8 {
    match ip {
        net::IpAddr::V4(ip) => {
            // Use the /16 range (first two components) of the IP address to key into the
            // range buckets.
            //
            // Eg. 124.99.123.1 and 124.54.123.1 would be placed in
            // different buckets, but 100.99.43.12 and 100.99.12.8
            // would be placed in the same bucket.
            let octets: [u8; 4] = ip.octets();
            let bits: u16 = (octets[0] as u16) << 8 | octets[1] as u16;

            (bits % u8::MAX as u16) as u8
        }
        net::IpAddr::V6(ip) => {
            // Use the first 32 bits of an IPv6 address to as a key.
            let segments: [u16; 8] = ip.segments();
            let bits: u32 = (segments[0] as u32) << 16 | segments[1] as u32;

            (bits % u8::MAX as u32) as u8
        }
    }
}

/// Check whether an IPv4 address is globally routable.
///
/// This code is adapted from the Rust standard library's `net::Ipv4Addr::is_global`. It can be
/// replaced once that function is stabilized.
fn ipv4_is_routable(addr: &net::Ipv4Addr) -> bool {
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

/// Check whether an IPv6 address is globally routable.
///
/// For now, this always returns `true`, as IPv6 addresses
/// are not fully supported.
fn ipv6_is_routable(_addr: &net::Ipv6Addr) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;

    #[test]
    fn test_sample_empty() {
        let addrmgr = AddressManager::new(fastrand::Rng::new());

        assert!(addrmgr.sample().is_none());
    }

    #[test]
    fn test_max_range_size() {
        let mut addrmgr = AddressManager::new(fastrand::Rng::new());

        for i in 0..MAX_RANGE_SIZE + 1 {
            addrmgr.insert_sockaddr(
                iter::once(([111, 111, (i / u8::MAX as usize) as u8, i as u8], 8333).into()),
                Source::default(),
            );
        }
        assert_eq!(
            addrmgr.len(),
            MAX_RANGE_SIZE,
            "we can't insert more than a certain amount of addresses in the same range"
        );

        addrmgr.insert_sockaddr(
            iter::once(([129, 44, 12, 2], 8333).into()),
            Source::default(),
        );
        assert_eq!(
            addrmgr.len(),
            MAX_RANGE_SIZE + 1,
            "inserting in another range is perfectly fine"
        );
    }

    #[test]
    fn test_addr_key() {
        assert_eq!(
            addr_key(net::IpAddr::V4(net::Ipv4Addr::new(255, 0, 3, 4))),
            0
        );
        assert_eq!(
            addr_key(net::IpAddr::V4(net::Ipv4Addr::new(255, 1, 3, 4))),
            1
        );
        assert_eq!(
            addr_key(net::IpAddr::V4(net::Ipv4Addr::new(1, 255, 3, 4))),
            1
        );
    }
}
