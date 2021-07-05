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
use super::{DisconnectReason, Link, PeerId};

/// Time to wait until a request times out.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);

/// Idle timeout. Used to run periodic functions.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_mins(30);

/// Maximum number of addresses expected in a `addr` message.
const MAX_ADDR_ADDRESSES: usize = 1000;
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
    /// Peer addresses have been received.
    AddressesReceived {
        /// Number of addresses received.
        count: usize,
        /// Source of addresses received.
        source: Source,
    },
    /// A new peer address was discovered.
    AddressDiscovered(Address, Source),
    /// Address book exhausted.
    AddressBookExhausted,
    /// An error was encountered.
    Error(String),
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::AddressesReceived { count, source } => {
                write!(
                    fmt,
                    "received {} addresse(s) from source `{}`",
                    count, source
                )
            }
            Event::AddressDiscovered(addr, source) => {
                write!(fmt, "{:?} discovered from source `{}`", addr, source)
            }
            Event::AddressBookExhausted => {
                write!(
                    fmt,
                    "Address book exhausted.. fetching new addresses from peers"
                )
            }
            Event::Error(msg) => {
                write!(fmt, "error: {}", msg)
            }
        }
    }
}

/// Iterator over addresses.
pub struct Iter<F>(F);

impl<F> Iterator for Iter<F>
where
    F: FnMut() -> Option<(Address, Source)>,
{
    type Item = (Address, Source);

    fn next(&mut self) -> Option<Self::Item> {
        (self.0)()
    }
}

impl<P: Store, U> AddressManager<P, U> {
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

impl<P: Store, U: SyncAddresses + SetTimeout + Events> AddressManager<P, U> {
    /// Initialize the address manager.
    pub fn initialize(&mut self, local_time: LocalTime) {
        self.idle(local_time);
    }

    /// Return an iterator over randomly sampled addresses.
    ///
    /// *May return duplicates.*
    pub fn iter(&mut self, services: ServiceFlags) -> impl Iterator<Item = (Address, Source)> + '_ {
        Iter(move || self.sample(services))
    }

    /// Get addresses from peers.
    pub fn get_addresses(&self) {
        for peer in &self.sources {
            self.upstream.get_addresses(*peer);
        }
    }

    /// Called when we receive a `getaddr` message.
    pub fn received_getaddr(&mut self, from: &net::SocketAddr) {
        // TODO: We should only respond with peers who were last active within
        // the last 3 hours.
        let mut addrs = Vec::new();

        // Include one random address per address range.
        for range in self.address_ranges.values() {
            let ix = self.rng.usize(..range.len());
            let ip = range.iter().nth(ix).expect("index must be present");
            let ka = self.peers.get(&ip).expect("address must exist");

            // TODO: Use time last active instead of `0`.
            addrs.push((0, ka.addr.clone()));
        }
        self.upstream.send_addresses(*from, addrs);
    }

    /// Called when a tick is received.
    pub fn received_tick(&mut self, local_time: LocalTime) {
        // If we're already using all the addresses we have available, we should fetch more.
        if local_time - self.last_request.unwrap_or_default() >= REQUEST_TIMEOUT
            && self.is_exhausted()
        {
            Events::event(&self.upstream, Event::AddressBookExhausted);

            self.get_addresses();
            self.last_request = Some(local_time);
            self.upstream.set_timeout(REQUEST_TIMEOUT);
        }

        if local_time - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            self.idle(local_time);
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
        if link.is_outbound() {
            self.sources.insert(*addr);
        }

        // We're only interested in peers we already know, eg. from DNS or peer
        // exchange. Peers should only be added to our address book if they are DNS seeds
        // or are discovered via a DNS seed.
        if let Some(ka) = self.peers.get_mut(&addr.ip()) {
            // Only ask for addresses when connecting for the first time.
            if ka.last_success.is_none() {
                self.upstream.get_addresses(*addr);
            }
            // Keep track of when the last successful handshake was.
            ka.last_success = Some(time);
            ka.addr.services = services;
        }
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(&mut self, addr: &net::SocketAddr, reason: DisconnectReason) {
        if self.connected.contains(&addr.ip()) {
            // Disconnected peers cannot be used as a source for new addresses.
            self.sources.remove(&addr);

            // If the reason for disconnecting the peer suggests that we shouldn't try to
            // connect to this peer again, then remove the peer from the address book.
            if !reason.is_transient() {
                self.discard(&addr.ip());
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////

    fn idle(&mut self, local_time: LocalTime) {
        // If it's been a while, save addresses to store.
        if let Err(err) = self.peers.flush() {
            self.upstream
                .event(Event::Error(format!("flush to disk failed: {}", err)));
        }
        self.last_idle = Some(local_time);
        self.upstream.set_timeout(IDLE_TIMEOUT);
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

    /// Called when we received an `addr` message from a peer.
    pub fn received_addr(&mut self, peer: net::SocketAddr, addrs: Vec<(BlockTime, Address)>) {
        if addrs.is_empty() || addrs.len() > MAX_ADDR_ADDRESSES {
            // Peer misbehaving, got empty message or too many addresses.
            return;
        }
        let source = Source::Peer(peer);

        self.upstream.event(Event::AddressesReceived {
            count: addrs.len(),
            source,
        });
        self.insert(addrs.into_iter(), source);
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
    /// ].into_iter().map(|a| (BlockTime::default(), a)), Source::Dns);
    ///
    /// assert_eq!(addrmgr.len(), 3);
    ///
    /// addrmgr.insert(std::iter::once(
    ///     (BlockTime::default(), Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE))
    /// ), Source::Dns);
    ///
    /// assert_eq!(addrmgr.len(), 3, "already known addresses are ignored");
    ///
    /// addrmgr.clear();
    /// addrmgr.insert(vec![
    ///     Address::new(&([255, 255, 255, 255], 8333).into(), ServiceFlags::NONE),
    /// ].into_iter().map(|a| (BlockTime::default(), a)), Source::Dns);
    ///
    /// assert!(addrmgr.is_empty(), "non-routable/non-local addresses are ignored");
    /// ```
    pub fn insert(&mut self, addrs: impl Iterator<Item = (BlockTime, Address)>, source: Source) {
        // TODO: Store timestamp.
        for (_, addr) in addrs {
            // Ignore addresses that don't have the required services.
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
            if !self::is_routable(&ip) {
                continue;
            }

            // Ignore local addresses.
            if self::is_local(&ip) {
                continue;
            }

            if !self
                .peers
                .insert(ip, KnownAddress::new(addr.clone(), source))
            {
                // Ignore addresses we already know.
                continue;
            }

            self.populate_address_ranges(&net_addr.ip());
            self.upstream.event(Event::AddressDiscovered(addr, source));
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
    ///     adversary_addrs.iter().cloned().map(|a| (BlockTime::default(), a)), Source::Dns);
    ///
    /// // Safe addresses, controlled by non-adversarial peers.
    /// let safe_addrs = vec![
    ///     Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
    ///     Address::new(&([99, 129, 2, 15], 8333).into(), ServiceFlags::NONE),
    /// ];
    /// addrmgr.insert(
    ///     safe_addrs.iter().cloned().map(|a| (BlockTime::default(), a)), Source::Dns);
    ///
    /// // Keep track of how many times we pick a safe vs. an adversary-controlled address.
    /// let mut adversary = 0;
    /// let mut safe = 0;
    ///
    /// for _ in 0..99 {
    ///     let (addr, _) = addrmgr.sample(ServiceFlags::NONE).unwrap();
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
    pub fn sample(&mut self, services: ServiceFlags) -> Option<(Address, Source)> {
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
            let ka = self.peers.get_mut(&ip).expect("address must exist");

            visited.insert(ip);

            // FIXME
            if ka.last_attempt.is_some() {
                continue;
            }
            if !ka.addr.services.has(services) {
                match ka.source {
                    Source::Dns => {
                        // If we've negotiated with this peer and it hasn't signaled the
                        // required services, we know not to return it.
                        // The reason we check this is that DNS-sourced addresses don't include
                        // service information, so we can only know once negotiated.
                        if ka.last_success.is_some() {
                            continue;
                        }
                    }
                    Source::Imported => {
                        // We expect that imported addresses will always include the correct
                        // service information. Hence, if this one doesn't have the necessary
                        // services, it's safe to skip.
                        continue;
                    }
                    Source::Peer(_) => {
                        // Peer-sourced addresses come with service information. It's safe to
                        // skip this address if it doesn't have the required services.
                        continue;
                    }
                }
            }

            if !self.connected.contains(&ip) {
                debug_assert!(self.last_idle.is_some());
                ka.last_sampled = self.last_idle;

                return Some((ka.addr.clone(), ka.source));
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
    fn discard(&mut self, addr: &net::IpAddr) -> (Option<KnownAddress>, bool) {
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

impl<P: Store, U: Events + SyncAddresses + SetTimeout> AddressSource for AddressManager<P, U> {
    fn sample(&mut self, services: ServiceFlags) -> Option<(Address, Source)> {
        AddressManager::sample(self, services)
    }

    fn iter(&mut self, services: ServiceFlags) -> Box<dyn Iterator<Item = (Address, Source)> + '_> {
        Box::new(AddressManager::iter(self, services))
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
        let mut addrmgr =
            AddressManager::new(Config::default(), fastrand::Rng::new(), HashMap::new(), ());

        assert!(addrmgr.sample(ServiceFlags::NONE).is_none());
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
                Source::Dns,
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
            Source::Dns,
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
