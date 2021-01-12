//!
//! The peer-to-peer address manager.
//!
#![warn(missing_docs)]
use std::net;

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;

use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::block::BlockTime;
use nakamoto_common::collections::{HashMap, HashSet};
use nakamoto_common::p2p::peer::{AddressSource, KnownAddress, Source, Store};

use super::channel::SetTimeout;
use super::{Link, PeerId};

/// Time to wait until a request times out.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);

/// Idle timeout. Used to run periodic functions.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_mins(30);

/// Maximum number of addresses to return when receiving a `getaddr` message.
const MAX_GETADDR_ADDRESSES: usize = 8;
/// Maximum number of addresses we store for a given address range.
const MAX_RANGE_SIZE: usize = 256;

/// Address manager event emission.
pub trait Events {
    /// Emit an event.
    fn event(&self, event: Event);
}

/// Capability of getting new addresses.
pub trait SyncAddresses {
    /// Get new addresses from a peer.
    fn get_addresses(&self, addr: PeerId);
    /// Send addresses to a peer.
    fn send_addresses(&self, addr: PeerId, addrs: Vec<(BlockTime, Address)>);
}

impl Events for () {
    fn event(&self, _event: Event) {}
}

/// An event emitted by the address manager.
#[derive(Debug, Clone)]
pub enum Event {
    /// A new peer address was discovered.
    AddressDiscovered(Address, Source),
    /// An error was encountered.
    Error(String),
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::AddressDiscovered(addr, source) => {
                write!(fmt, "{:?} discovered from source `{}`", addr, source)
            }
            Event::Error(msg) => {
                write!(fmt, "error: {}", msg)
            }
        }
    }
}

/// Address manager configuration.
#[derive(Debug)]
pub struct Config {
    /// Services required from peers.
    pub required_services: ServiceFlags,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            required_services: ServiceFlags::NONE,
        }
    }
}

/// Manages peer network addresses.
#[derive(Debug)]
pub struct AddressManager<P, U> {
    /// Peer address store.
    peers: P,
    address_ranges: HashMap<u8, HashSet<net::IpAddr>>,
    connected: HashSet<net::IpAddr>,
    sources: HashSet<net::SocketAddr>,
    local_addrs: HashSet<net::SocketAddr>,
    /// The last time we asked our peers for new addresses.
    last_request: Option<LocalTime>,
    /// The last time we idled.
    last_idle: Option<LocalTime>,
    cfg: Config,
    upstream: U,
    rng: fastrand::Rng,
}

impl<P: Store, U> AddressManager<P, U> {
    /// Iterate over all addresses.
    pub fn iter(&self) -> impl Iterator<Item = &Address> {
        self.peers.iter().map(|(_, ka)| &ka.addr)
    }

    /// Check whether we have unused addresses.
    pub fn is_exhausted(&self) -> bool {
        for (addr, _) in self.peers.iter() {
            if !self.connected.contains(addr) {
                return false;
            }
        }
        true
    }
}

impl<P: Store, U: SyncAddresses + SetTimeout + Events> AddressManager<P, U> {
    /// Get addresses from peers.
    pub fn get_addresses(&self) {
        for peer in &self.sources {
            self.upstream.get_addresses(*peer);
        }
    }

    /// Called when we receive a `getaddr` message.
    pub fn received_getaddr(&mut self, from: &net::SocketAddr) {
        // TODO: Use `sample` here when it returns an iterator.
        let addrs = self
            .iter()
            // Don't send the peer their own address, nor non-TCP addresses.
            .filter(|a| a.socket_addr().map_or(false, |s| &s != from))
            .take(MAX_GETADDR_ADDRESSES)
            // TODO: Return a non-zero time value.
            .map(|a| (0, a.clone()))
            .collect();

        self.upstream.send_addresses(*from, addrs);
    }

    /// Called when a timeout is received.
    pub fn received_timeout(&mut self, local_time: LocalTime) {
        // If we're already using all the addresses we have available, we should fetch more.
        if local_time - self.last_request.unwrap_or_default() >= REQUEST_TIMEOUT
            && self.is_exhausted()
        {
            self.get_addresses();
            self.last_request = Some(local_time);
            self.upstream.set_timeout(REQUEST_TIMEOUT);
        }

        // If it's been a while, save addresses to store.
        if local_time - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            if let Err(err) = self.peers.flush() {
                self.upstream
                    .event(Event::Error(format!("flush to disk failed: {}", err)));
            }
            self.upstream.set_timeout(IDLE_TIMEOUT);
        }
    }

    /// Called when a peer connection is attempted.
    pub fn peer_attempted(&mut self, addr: &net::SocketAddr, time: LocalTime) {
        // We're only interested in connection attempts for addresses we keep track of.
        if let Some(ka) = self.peers.get_mut(&addr.ip()) {
            ka.last_attempt = Some(time);
        }
    }

    /// Called when a peer has connected.
    pub fn peer_connected(&mut self, addr: &net::SocketAddr, _local_time: LocalTime) {
        if !self::is_routable(&addr.ip()) || self::is_local(&addr.ip()) {
            return;
        }
        self.connected.insert(addr.ip());
    }

    /// Called when a peer has handshaked.
    pub fn peer_negotiated(
        &mut self,
        addr: &net::SocketAddr,
        services: ServiceFlags,
        link: Link,
        time: LocalTime,
    ) {
        if !self.connected.contains(&addr.ip()) {
            return;
        }
        let address = Address::new(addr, services);

        self.insert(
            std::iter::once((time.block_time(), address)),
            Source::Connected(time),
        );

        if link.is_outbound() {
            self.sources.insert(*addr);
            self.upstream.get_addresses(*addr);
        }
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, addr: &net::SocketAddr) {
        if self.connected.contains(&addr.ip()) {
            // TODO: We shouldn't remove. We should keep for later.
            self.remove_address(&addr.ip());
            self.sources.remove(&addr);
        }
    }
}

impl<P, U> AddressManager<P, U> {
    /// Record an address of ours as seen by a remote peer.
    /// This helps avoid self-connections.
    pub fn record_local_addr(&mut self, addr: net::SocketAddr) {
        self.local_addrs.insert(addr);
    }
}

impl<P: Store, U: Events> AddressManager<P, U> {
    /// Create a new, empty address manager.
    pub fn new(cfg: Config, rng: fastrand::Rng, peers: P, upstream: U) -> Self {
        let ips = peers.iter().map(|(ip, _)| *ip).collect::<Vec<_>>();
        let mut addrmgr = Self {
            cfg,
            peers,
            address_ranges: HashMap::with_hasher(rng.clone().into()),
            connected: HashSet::with_hasher(rng.clone().into()),
            sources: HashSet::with_hasher(rng.clone().into()),
            local_addrs: HashSet::with_hasher(rng.clone().into()),
            last_request: None,
            last_idle: None,
            upstream,
            rng,
        };

        for ip in ips.iter() {
            addrmgr.populate_address_ranges(ip);
        }
        addrmgr
    }

    /// The number of peers known.
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Whether there are any peers known to the address manager.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear the address manager of all peers.
    pub fn clear(&mut self) {
        self.peers.clear();
        self.address_ranges.clear();
    }

    /// Add addresses to the address manager. The input matches that of the `addr` message
    /// sent by peers on the network.
    ///
    /// ```
    /// use std::collections::HashMap;
    ///
    /// use bitcoin::network::address::Address;
    /// use bitcoin::network::constants::ServiceFlags;
    ///
    /// use nakamoto_p2p::protocol::addrmgr::{AddressManager, Config};
    /// use nakamoto_common::p2p::peer::Source;
    /// use nakamoto_common::block::BlockTime;
    ///
    /// let cfg = Config::default();
    /// let mut addrmgr = AddressManager::new(cfg, fastrand::Rng::new(), HashMap::new(), ());
    ///
    /// addrmgr.insert(vec![
    ///     Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
    /// ].into_iter().map(|a| (BlockTime::default(), a)), Source::default());
    ///
    /// assert_eq!(addrmgr.len(), 3);
    ///
    /// addrmgr.insert(std::iter::once(
    ///     (BlockTime::default(), Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE))
    /// ), Source::default());
    ///
    /// assert_eq!(addrmgr.len(), 3, "already known addresses are ignored");
    ///
    /// addrmgr.clear();
    /// addrmgr.insert(vec![
    ///     Address::new(&([255, 255, 255, 255], 8333).into(), ServiceFlags::NONE),
    /// ].into_iter().map(|a| (BlockTime::default(), a)), Source::default());
    ///
    /// assert!(addrmgr.is_empty(), "non-routable/non-local addresses are ignored");
    /// ```
    pub fn insert(
        &mut self,
        addrs: impl Iterator<Item = (BlockTime, Address)>,
        source: Source,
    ) -> bool {
        // True if at least one address was added into the map.
        let mut okay = false;

        // TODO: Store timestamp.
        for (_, addr) in addrs {
            if !addr.services.has(self.cfg.required_services) {
                continue;
            }

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
            //
            // FIXME: Local addresses don't really work because they have the same IP, so they
            // overwrite each other.
            match source {
                Source::Peer(_) if !self::is_routable(&ip) => continue,
                Source::Other if !self::is_routable(&ip) && !self::is_local(&ip) => continue,
                _ => {}
            }

            if self
                .peers
                .insert(ip, KnownAddress::new(addr.clone(), source.clone()))
                .is_some()
            {
                // Ignore addresses we already know.
                continue;
            }
            okay = true; // As soon as one address was inserted, consider it a success.

            self.populate_address_ranges(&net_addr.ip());
            self.upstream
                .event(Event::AddressDiscovered(addr, source.clone()));
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
    /// use std::collections::HashMap;
    ///
    /// use bitcoin::network::address::Address;
    /// use bitcoin::network::constants::ServiceFlags;
    ///
    /// use nakamoto_p2p::protocol::addrmgr::{AddressManager, Config};
    /// use nakamoto_common::p2p::peer::Source;
    /// use nakamoto_common::block::BlockTime;
    ///
    /// let cfg = Config::default();
    /// let mut addrmgr = AddressManager::new(cfg, fastrand::Rng::new(), HashMap::new(), ());
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
    ///     adversary_addrs.iter().cloned().map(|a| (BlockTime::default(), a)), Source::default());
    ///
    /// // Safe addresses, controlled by non-adversarial peers.
    /// let safe_addrs = vec![
    ///     Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([99, 129, 2, 15], 8333).into(), ServiceFlags::NONE),
    /// ];
    /// addrmgr.insert(
    ///     safe_addrs.iter().cloned().map(|a| (BlockTime::default(), a)), Source::default());
    ///
    /// // Keep track of how many times we pick a safe vs. an adversary-controlled address.
    /// let mut adversary = 0;
    /// let mut safe = 0;
    ///
    /// for _ in 0..99 {
    ///     let (addr, _) = addrmgr.sample().unwrap();
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
    pub fn sample(&self) -> Option<(&Address, Source)> {
        if self.is_empty() {
            return None;
        }
        assert!(!self.address_ranges.is_empty());

        // Keep track of the addresses we've visited, to make sure we don't
        // loop forever.
        let mut visited = HashSet::with_hasher(self.rng.clone().into());

        while visited.len() < self.peers.len() {
            // First select a random address range.
            let ix = self.rng.usize(..self.address_ranges.len());
            let range = self.address_ranges.values().nth(ix)?;

            assert!(!range.is_empty());

            // Then select a random address in that range.
            let ix = self.rng.usize(..range.len());
            let ip = range.iter().nth(ix)?;
            let ka = self.peers.get(&ip).expect("address must exist");

            visited.insert(ip);

            // FIXME
            if ka.last_attempt.is_some() {
                continue;
            }

            if !self.connected.contains(&ip) {
                return Some((&ka.addr, ka.source));
            }
        }

        None
    }

    ////////////////////////////////////////////////////////////////////////////

    /// Populate address ranges with an IP. This may remove an existing IP if
    /// its range is full. Returns the range key that was used.
    fn populate_address_ranges(&mut self, ip: &net::IpAddr) -> u8 {
        let key = self::addr_key(ip);
        let range = self.address_ranges.entry(key).or_insert_with({
            let rng = self.rng.clone();

            || HashSet::with_hasher(rng.into())
        });

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
            self.peers.remove(&addr);
        }
        range.insert(*ip);

        key
    }

    /// Remove an address permanently.
    fn remove_address(&mut self, addr: &net::IpAddr) -> (Option<KnownAddress>, bool) {
        // TODO: For now, it's enough to remove the address, since we shouldn't
        // be trying to reconnect to a peer that disconnected. However in the future
        // we should probably keep it around, or decide based on the reason for
        // disconnection.
        let ka = self.peers.remove(addr);
        let co = self.connected.remove(addr);

        let key = self::addr_key(addr);

        if let Some(range) = self.address_ranges.get_mut(&key) {
            range.remove(&addr);

            if range.is_empty() {
                self.address_ranges.remove(&key);
            }
        }

        (ka, co)
    }
}

impl<P: Store, U: Events + SyncAddresses> AddressSource for AddressManager<P, U> {
    fn sample(&self) -> Option<(&Address, Source)> {
        AddressManager::sample(&self)
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
fn addr_key(ip: &net::IpAddr) -> u8 {
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
    use std::collections::HashMap;
    use std::iter;

    #[test]
    fn test_sample_empty() {
        let addrmgr =
            AddressManager::new(Config::default(), fastrand::Rng::new(), HashMap::new(), ());

        assert!(addrmgr.sample().is_none());
    }

    #[test]
    fn test_max_range_size() {
        let services = ServiceFlags::NONE;
        let time = BlockTime::default();

        let mut addrmgr =
            AddressManager::new(Config::default(), fastrand::Rng::new(), HashMap::new(), ());

        for i in 0..MAX_RANGE_SIZE + 1 {
            addrmgr.insert(
                iter::once((
                    time,
                    Address::new(
                        &([111, 111, (i / u8::MAX as usize) as u8, i as u8], 8333).into(),
                        services,
                    ),
                )),
                Source::default(),
            );
        }
        assert_eq!(
            addrmgr.len(),
            MAX_RANGE_SIZE,
            "we can't insert more than a certain amount of addresses in the same range"
        );

        addrmgr.insert(
            iter::once((
                time,
                Address::new(&([129, 44, 12, 2], 8333).into(), services),
            )),
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
            addr_key(&net::IpAddr::V4(net::Ipv4Addr::new(255, 0, 3, 4))),
            0
        );
        assert_eq!(
            addr_key(&net::IpAddr::V4(net::Ipv4Addr::new(255, 1, 3, 4))),
            1
        );
        assert_eq!(
            addr_key(&net::IpAddr::V4(net::Ipv4Addr::new(1, 255, 3, 4))),
            1
        );
    }
}
