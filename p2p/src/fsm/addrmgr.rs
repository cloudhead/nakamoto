//!
//! The peer-to-peer address manager.
//!
#![warn(missing_docs)]
use std::net;

use nakamoto_common::bitcoin::network::address::Address;
use nakamoto_common::bitcoin::network::constants::ServiceFlags;

use nakamoto_common::block::time::Clock;
use nakamoto_common::block::time::{LocalDuration, LocalTime};
use nakamoto_common::block::BlockTime;
use nakamoto_common::collections::{HashMap, HashSet};
use nakamoto_common::p2p::peer::{AddressSource, KnownAddress, Source, Store};
use nakamoto_common::p2p::Domain;
use nakamoto_net::DisconnectReason;

use super::output::{Wakeup, Wire};
use super::ConnDirection;

/// Time to wait until a request times out.
pub const REQUEST_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);

/// Idle timeout. Used to run periodic functions.
pub const IDLE_TIMEOUT: LocalDuration = LocalDuration::from_mins(1);

/// Sample timeout. How long before a sampled address can be returned again.
pub const SAMPLE_TIMEOUT: LocalDuration = LocalDuration::from_mins(3);

/// Maximum number of addresses expected in a `addr` message.
const MAX_ADDR_ADDRESSES: usize = 1000;
/// Maximum number of addresses we store for a given address range.
const MAX_RANGE_SIZE: usize = 256;

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

impl<P: Store, U, C> AddressManager<P, U, C> {
    /// Check whether we have unused addresses.
    pub fn is_exhausted(&self) -> bool {
        let time = self
            .last_idle
            .expect("AddressManager::is_exhausted: manager must be initialized");

        for (addr, ka) in self.peers.iter() {
            // Unsuccessful attempt to connect.
            if ka.last_attempt.is_some() && ka.last_success.is_none() {
                continue;
            }
            if time - ka.last_sampled.unwrap_or_default() < SAMPLE_TIMEOUT {
                continue;
            }
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
    /// Communication domains we're interested in.
    pub domains: Vec<Domain>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            required_services: ServiceFlags::NONE,
            domains: Domain::all(),
        }
    }
}

/// Manages peer network addresses.
#[derive(Debug)]
pub struct AddressManager<P, U, C> {
    /// Peer address store.
    peers: P,
    bans: HashSet<net::IpAddr>,
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
    clock: C,
}

impl<P: Store, U: Wire<Event> + Wakeup, C: Clock> AddressManager<P, U, C> {
    /// Initialize the address manager.
    pub fn initialize(&mut self) {
        self.idle();
    }

    /// Return an iterator over randomly sampled addresses.
    pub fn iter(&mut self, services: ServiceFlags) -> impl Iterator<Item = (Address, Source)> + '_ {
        Iter(move || self.sample(services))
    }

    /// Get addresses from peers.
    pub fn get_addresses(&mut self) {
        for peer in &self.sources {
            self.upstream.get_addr(*peer);
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
            let ka = self.peers.get(ip).expect("address must exist");

            addrs.push((
                ka.last_active.map(|t| t.block_time()).unwrap_or_default(),
                ka.addr.clone(),
            ));
        }
        self.upstream.addr(*from, addrs);
    }

    /// Called when a tick is received.
    pub fn received_wake(&mut self) {
        let local_time = self.clock.local_time();

        // If we're already using all the addresses we have available, we should fetch more.
        if local_time - self.last_request.unwrap_or_default() >= REQUEST_TIMEOUT
            && self.is_exhausted()
        {
            self.upstream.event(Event::AddressBookExhausted);

            self.get_addresses();
            self.last_request = Some(local_time);
            self.upstream.wakeup(REQUEST_TIMEOUT);
        }

        if local_time - self.last_idle.unwrap_or_default() >= IDLE_TIMEOUT {
            self.idle();
        }
    }

    /// Called when a peer signaled activity.
    pub fn peer_active(&mut self, addr: net::SocketAddr) {
        let time = self.clock.local_time();
        if let Some(ka) = self.peers.get_mut(&addr.ip()) {
            ka.last_active = Some(time);
        }
    }

    /// Called when a peer connection is attempted.
    pub fn peer_attempted(&mut self, addr: &net::SocketAddr) {
        let time = self.clock.local_time();
        // We're only interested in connection attempts for addresses we keep track of.
        if let Some(ka) = self.peers.get_mut(&addr.ip()) {
            ka.last_attempt = Some(time);
        }
    }

    /// Called when a peer has connected.
    pub fn peer_connected(&mut self, addr: &net::SocketAddr) {
        if !self::is_routable(&addr.ip()) || self::is_local(&addr.ip()) {
            return;
        }
        self.connected.insert(addr.ip());
    }

    /// Called when a peer has handshaked.
    pub fn peer_negotiated(&mut self, addr: &net::SocketAddr, services: ServiceFlags, link: ConnDirection) {
        let time = self.clock.local_time();

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
                self.upstream.get_addr(*addr);
            }
            // Keep track of when the last successful handshake was.
            ka.last_success = Some(time);
            ka.last_active = Some(time);
            ka.addr.services = services;
        }
    }

    /// Called when a peer disconnected.
    pub fn peer_disconnected(
        &mut self,
        addr: &net::SocketAddr,
        reason: DisconnectReason<super::DisconnectReason>,
    ) {
        if self.connected.remove(&addr.ip()) {
            // Disconnected peers cannot be used as a source for new addresses.
            self.sources.remove(addr);

            // If the reason for disconnecting the peer suggests that we shouldn't try to
            // connect to this peer again, then remove the peer from the address book.
            // Otherwise, we leave it in the address buckets so that it can be chosen
            // in the future.
            if let DisconnectReason::OnDemand(r) = reason {
                if !r.is_transient() {
                    self.ban(&addr.ip());
                }
            } else if reason.is_dial_err() {
                self.ban(&addr.ip());
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////

    fn idle(&mut self) {
        // If it's been a while, save addresses to store.
        if let Err(err) = self.peers.flush() {
            self.upstream
                .event(Event::Error(format!("flush to disk failed: {}", err)));
        }
        self.last_idle = Some(self.clock.local_time());
        self.upstream.wakeup(IDLE_TIMEOUT);
    }
}

impl<P: Store, U: Wire<Event>, C: Clock> AddressManager<P, U, C> {
    /// Create a new, empty address manager.
    pub fn new(cfg: Config, rng: fastrand::Rng, peers: P, upstream: U, clock: C) -> Self {
        let ips = peers.iter().map(|(ip, _)| *ip).collect::<Vec<_>>();
        let mut addrmgr = Self {
            cfg,
            peers,
            bans: HashSet::with_hasher(rng.clone().into()),
            address_ranges: HashMap::with_hasher(rng.clone().into()),
            connected: HashSet::with_hasher(rng.clone().into()),
            sources: HashSet::with_hasher(rng.clone().into()),
            local_addrs: HashSet::with_hasher(rng.clone().into()),
            last_request: None,
            last_idle: None,
            upstream,
            rng,
            clock,
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
        self.peers.is_empty() || self.address_ranges.is_empty()
    }

    #[cfg(test)]
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
    pub fn insert(
        &mut self,
        addrs: impl IntoIterator<Item = (BlockTime, Address)>,
        source: Source,
    ) {
        let time = self
            .last_idle
            .expect("AddressManager::insert: manager must be initialized before inserting");

        for (last_active, addr) in addrs {
            // Ignore addresses that don't have the required services.
            if !addr.services.has(self.cfg.required_services) {
                continue;
            }
            // Ignore peers with these antiquated services offered. They are very unlikely to
            // support compact filters, or have up-to-date clients.
            if addr.services.has(ServiceFlags::GETUTXO) || addr.services.has(ServiceFlags::BLOOM) {
                continue;
            }
            // Ignore addresses that don't have a "last active" time.
            if last_active == 0 {
                continue;
            }
            // Ignore addresses that are too far into the future.
            if LocalTime::from_block_time(last_active) > time + LocalDuration::from_mins(60) {
                continue;
            }
            // Ignore addresses from unsupported domains.
            let net_addr = match addr.socket_addr() {
                Ok(a) if self.cfg.domains.contains(&Domain::for_address(&a)) => a,
                _ => continue,
            };
            let ip = net_addr.ip();

            // Ensure no self-connections.
            if self.local_addrs.contains(&net_addr) {
                continue;
            }
            // No banned addresses.
            if self.bans.contains(&ip) {
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

            let last_active = if last_active == 0 {
                // TODO: Cannot be 'None' due to above condition.
                None
            } else {
                Some(LocalTime::from_block_time(last_active))
            };

            // Record the address, and ignore addresses we already know.
            // Note that this should never overwrite an existing address.
            if !self
                .peers
                .insert(ip, KnownAddress::new(addr.clone(), source, last_active))
            {
                continue;
            }

            self.populate_address_ranges(&net_addr.ip());
        }
    }

    /// Pick an address at random from the set of known addresses.
    ///
    /// This function tries to ensure a good geo-diversity of addresses, such that an adversary
    /// controlling a disproportionately large number of addresses in the same address range does
    /// not have an advantage over other peers.
    ///
    /// This works under the assumption that adversaries are *localized*.
    pub fn sample(&mut self, services: ServiceFlags) -> Option<(Address, Source)> {
        self.sample_with(|ka: &KnownAddress| {
            if !ka.addr.services.has(services) {
                match ka.source {
                    Source::Dns => {
                        // If we've negotiated with this peer and it hasn't signaled the
                        // required services, we know not to return it.
                        // DNS-sourced addresses don't include service information,
                        // so we won't be including these until we know the services.
                    }
                    Source::Imported => {
                        // We expect that imported addresses will always include the correct
                        // service information. Hence, if this one doesn't have the necessary
                        // services, it's safe to skip.
                    }
                    Source::Peer(_) => {
                        // Peer-sourced addresses come with service information. It's safe to
                        // skip this address if it doesn't have the required services.
                    }
                }
                return false;
            }
            true
        })
    }

    /// Sample an address using the provided predicate. Only returns addresses which are `true`
    /// according to the predicate.
    pub fn sample_with(
        &mut self,
        predicate: impl Fn(&KnownAddress) -> bool,
    ) -> Option<(Address, Source)> {
        if self.is_empty() {
            return None;
        }
        let time = self
            .last_idle
            .expect("AddressManager::sample: manager must be initialized before sampling");
        let domains = &self.cfg.domains;

        let mut ranges: Vec<_> = self.address_ranges.values().collect();
        self.rng.shuffle(&mut ranges);

        // First select a random address range.
        for range in ranges.drain(..) {
            assert!(!range.is_empty());

            let mut ips: Vec<_> = range.iter().collect();
            self.rng.shuffle(&mut ips);

            // Then select a random address in that range.
            for ip in ips.drain(..) {
                let ka = self.peers.get_mut(ip).expect("address must exist");

                // If the address domain is unsupported, skip it.
                // Nb. this currently skips Tor addresses too.
                if !ka
                    .addr
                    .socket_addr()
                    .map_or(false, |a| domains.contains(&Domain::for_address(&a)))
                {
                    continue;
                }

                // If the address was already attempted unsuccessfully, skip it.
                if ka.last_attempt.is_some() && ka.last_success.is_none() {
                    continue;
                }
                // If we recently sampled this address, don't return it again.
                if time - ka.last_sampled.unwrap_or_default() < SAMPLE_TIMEOUT {
                    continue;
                }
                // If we're already connected to this address, skip it.
                if self.connected.contains(ip) {
                    continue;
                }
                // If the provided filter doesn't pass, keep looking.
                if !predicate(ka) {
                    continue;
                }
                // Ok, we've found a worthy address!
                ka.last_sampled = Some(time);

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

    /// Remove an address from the address book and prevent it from being sampled again.
    fn ban(&mut self, addr: &net::IpAddr) -> bool {
        debug_assert!(!self.connected.contains(addr));

        let key = self::addr_key(addr);

        if let Some(range) = self.address_ranges.get_mut(&key) {
            range.remove(addr);

            // TODO: Persist bans.
            self.peers.remove(addr);
            self.bans.insert(*addr);

            if range.is_empty() {
                self.address_ranges.remove(&key);
            }
            return true;
        }
        false
    }
}

impl<P: Store, U: Wire<Event> + Wakeup, C: Clock> AddressSource for AddressManager<P, U, C> {
    fn sample(&mut self, services: ServiceFlags) -> Option<(Address, Source)> {
        AddressManager::sample(self, services)
    }

    fn record_local_address(&mut self, addr: net::SocketAddr) {
        self.local_addrs.insert(addr);
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
    use crate::fsm;
    use std::collections::HashMap;
    use std::iter;

    use nakamoto_common::block::time::RefClock;
    use nakamoto_common::network::Network;
    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    #[test]
    fn test_sample_empty() {
        let mut addrmgr = AddressManager::new(
            Config::default(),
            fastrand::Rng::new(),
            HashMap::new(),
            (),
            LocalTime::now(),
        );

        assert!(addrmgr.sample(ServiceFlags::NONE).is_none());
    }

    #[test]
    fn test_known_addresses() {
        let time = LocalTime::now();
        let mut addrmgr = AddressManager::new(
            Config::default(),
            fastrand::Rng::new(),
            HashMap::new(),
            (),
            time,
        );
        let source = Source::Dns;
        let services = ServiceFlags::NETWORK;

        addrmgr.initialize();
        addrmgr.insert(
            [
                (
                    time.block_time(),
                    Address::new(&([33, 33, 33, 33], 8333).into(), services),
                ),
                (
                    time.block_time(),
                    Address::new(&([44, 44, 44, 44], 8333).into(), services),
                ),
            ],
            source,
        );

        let addr: &net::SocketAddr = &([33, 33, 33, 33], 8333).into();

        let ka = addrmgr.peers.get(&addr.ip()).unwrap();
        assert!(ka.last_success.is_none());
        assert!(ka.last_attempt.is_none());
        assert!(ka.last_active.is_some());
        assert!(ka.last_sampled.is_none());
        assert_eq!(
            ka.last_active,
            Some(LocalTime::from_block_time(time.block_time()))
        );

        addrmgr.peer_attempted(addr);

        let ka = addrmgr.peers.get(&addr.ip()).unwrap();
        assert!(ka.last_success.is_none());
        assert!(ka.last_attempt.is_some());
        assert!(ka.last_active.is_some());
        assert!(ka.last_sampled.is_none());

        // When a peer is connected, it is not yet considered a "success".
        addrmgr.peer_connected(addr);

        let ka = addrmgr.peers.get(&addr.ip()).unwrap();
        assert!(ka.last_success.is_none());
        assert!(ka.last_attempt.is_some());
        assert!(ka.last_active.is_some());
        assert!(ka.last_sampled.is_none());

        // Only when it is negotiated is it a "success".
        addrmgr.peer_negotiated(addr, services, ConnDirection::Outbound);

        let ka = addrmgr.peers.get(&addr.ip()).unwrap();
        assert!(ka.last_success.is_some());
        assert!(ka.last_attempt.is_some());
        assert!(ka.last_active.is_some());
        assert!(ka.last_sampled.is_none());
        assert_eq!(ka.last_active, ka.last_success);

        let (sampled, _) = addrmgr.sample(services).unwrap();
        assert_eq!(
            sampled.socket_addr().ok(),
            Some(([44, 44, 44, 44], 8333).into())
        );
        let ka = addrmgr.peers.get(&[44, 44, 44, 44].into()).unwrap();
        assert!(ka.last_success.is_none());
        assert!(ka.last_attempt.is_none());
        assert!(ka.last_active.is_some());
        assert!(ka.last_sampled.is_some());
    }

    #[test]
    fn test_is_exhausted() {
        let time = LocalTime::now();
        let mut addrmgr = AddressManager::new(
            Config::default(),
            fastrand::Rng::new(),
            HashMap::new(),
            (),
            time,
        );
        let source = Source::Dns;
        let services = ServiceFlags::NETWORK;

        addrmgr.initialize();
        addrmgr.insert(
            [(
                time.block_time(),
                Address::new(&net::SocketAddr::from(([33, 33, 33, 33], 8333)), services),
            )],
            source,
        );
        assert!(!addrmgr.is_exhausted());

        // Once a peer connects, it's no longer available as an address.
        addrmgr.peer_connected(&([33, 33, 33, 33], 8333).into());
        assert!(addrmgr.is_exhausted());

        addrmgr.insert(
            [(
                time.block_time(),
                Address::new(&net::SocketAddr::from(([44, 44, 44, 44], 8333)), services),
            )],
            source,
        );
        assert!(!addrmgr.is_exhausted());

        // If a peer has been attempted with no success, it should not count towards the available
        // addresses.
        addrmgr.peer_attempted(&([44, 44, 44, 44], 8333).into());
        assert!(addrmgr.is_exhausted());

        // If a peer has been connected to successfully, and then disconnected for a transient
        // reason, its address should be once again available.
        addrmgr.peer_connected(&([44, 44, 44, 44], 8333).into());
        addrmgr.peer_negotiated(&([44, 44, 44, 44], 8333).into(), services, ConnDirection::Outbound);
        addrmgr.peer_disconnected(
            &([44, 44, 44, 44], 8333).into(),
            fsm::DisconnectReason::PeerTimeout("timeout").into(),
        );
        assert!(!addrmgr.is_exhausted());
        assert!(addrmgr.sample(services).is_some());
        assert!(addrmgr.sample(services).is_none());
        assert!(addrmgr.is_exhausted());

        // If a peer has been attempted and then disconnected, it is not available
        // for sampling, even when the disconnect reason is transient.
        addrmgr.insert(
            [(
                time.block_time(),
                Address::new(&net::SocketAddr::from(([55, 55, 55, 55], 8333)), services),
            )],
            source,
        );
        addrmgr.sample(services);
        addrmgr.peer_attempted(&([55, 55, 55, 55], 8333).into());
        addrmgr.peer_connected(&([55, 55, 55, 55], 8333).into());
        addrmgr.peer_disconnected(
            &([55, 55, 55, 55], 8333).into(),
            fsm::DisconnectReason::PeerTimeout("timeout").into(),
        );
        assert!(addrmgr.sample(services).is_none());
    }

    #[test]
    fn test_disconnect_rediscover() {
        // Check that if we re-discover an address after permanent disconnection, we still know
        // not to connect to it.
        let time = LocalTime::now();
        let mut addrmgr = AddressManager::new(
            Config::default(),
            fastrand::Rng::new(),
            HashMap::new(),
            (),
            time,
        );
        let source = Source::Dns;
        let services = ServiceFlags::NETWORK;
        let addr: &net::SocketAddr = &([33, 33, 33, 33], 8333).into();

        addrmgr.initialize();
        addrmgr.insert([(time.block_time(), Address::new(addr, services))], source);

        let (sampled, _) = addrmgr.sample(services).unwrap();
        assert_eq!(sampled.socket_addr().ok(), Some(*addr));
        assert!(addrmgr.sample(services).is_none());

        addrmgr.peer_attempted(addr);
        addrmgr.peer_connected(addr);
        addrmgr.peer_negotiated(addr, services, ConnDirection::Outbound);
        addrmgr.peer_disconnected(
            addr,
            fsm::DisconnectReason::PeerMisbehaving("misbehaving").into(),
        );

        // Peer is now disconnected for non-transient reasons.
        // Receive from a new peer the same address we just disconnected from.
        addrmgr.received_addr(
            ([99, 99, 99, 99], 8333).into(),
            vec![(time.block_time(), Address::new(addr, services))],
        );
        // It should not be returned from `sample`.
        assert!(addrmgr.sample(services).is_none());
    }

    #[quickcheck]
    fn prop_sample_no_duplicates(size: usize, seed: u64) -> TestResult {
        let clock = LocalTime::now();

        if size > 24 {
            return TestResult::discard();
        }

        let mut addrmgr = {
            let upstream = crate::fsm::output::Outbox::new(Network::Mainnet, 0);

            AddressManager::new(
                Config::default(),
                fastrand::Rng::with_seed(seed),
                HashMap::new(),
                upstream,
                clock,
            )
        };
        let time = LocalTime::now();
        let services = ServiceFlags::NETWORK;
        let mut addrs = vec![];

        for i in 0..size {
            addrs.push([96 + i as u8, 96 + i as u8, 96, 96]);
        }

        addrmgr.initialize();
        addrmgr.insert(
            addrs.iter().map(|a| {
                (
                    time.block_time(),
                    Address::new(&net::SocketAddr::from((*a, 8333)), services),
                )
            }),
            Source::Dns,
        );

        let mut sampled = HashSet::with_hasher(fastrand::Rng::with_seed(seed).into());
        for _ in 0..addrs.len() {
            let (addr, _) = addrmgr
                .sample(services)
                .expect("an address should be returned");
            sampled.insert(addr.socket_addr().unwrap().ip());
        }

        assert_eq!(
            sampled,
            addrs.iter().map(|a| (*a).into()).collect::<HashSet<_>>()
        );
        TestResult::passed()
    }

    #[test]
    fn test_max_range_size() {
        let services = ServiceFlags::NONE;
        let time = LocalTime::now();

        let mut addrmgr = AddressManager::new(
            Config::default(),
            fastrand::Rng::new(),
            HashMap::new(),
            (),
            time,
        );
        addrmgr.initialize();

        for i in 0..MAX_RANGE_SIZE + 1 {
            addrmgr.insert(
                iter::once((
                    time.block_time(),
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
                time.block_time(),
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

    #[test]
    fn test_insert() {
        use std::collections::HashMap;

        use nakamoto_common::bitcoin::network::address::Address;
        use nakamoto_common::bitcoin::network::constants::ServiceFlags;
        use nakamoto_common::block::time::LocalTime;
        use nakamoto_common::p2p::peer::Source;

        let cfg = Config::default();
        let time = LocalTime::now();
        let mut addrmgr = AddressManager::new(cfg, fastrand::Rng::new(), HashMap::new(), (), time);

        addrmgr.initialize();
        addrmgr.insert(
            vec![
                Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
                Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
                Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
            ]
            .into_iter()
            .map(|a| (time.block_time(), a)),
            Source::Dns,
        );

        assert_eq!(addrmgr.len(), 3);

        addrmgr.insert(
            std::iter::once((
                time.block_time(),
                Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
            )),
            Source::Dns,
        );

        assert_eq!(addrmgr.len(), 3, "already known addresses are ignored");

        addrmgr.clear();
        addrmgr.insert(
            vec![Address::new(
                &([255, 255, 255, 255], 8333).into(),
                ServiceFlags::NONE,
            )]
            .into_iter()
            .map(|a| (time.block_time(), a)),
            Source::Dns,
        );

        assert!(
            addrmgr.is_empty(),
            "non-routable/non-local addresses are ignored"
        );
    }

    #[test]
    fn test_sample() {
        use std::collections::HashMap;

        use nakamoto_common::bitcoin::network::address::Address;
        use nakamoto_common::bitcoin::network::constants::ServiceFlags;
        use nakamoto_common::block::time::{LocalDuration, LocalTime};
        use nakamoto_common::p2p::peer::Source;

        let cfg = Config::default();
        let clock = RefClock::from(LocalTime::now());
        let mut addrmgr =
            AddressManager::new(cfg, fastrand::Rng::new(), HashMap::new(), (), clock.clone());

        addrmgr.initialize();

        // Addresses controlled by an adversary.
        let adversary_addrs = vec![
            Address::new(&([111, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
            Address::new(&([111, 8, 43, 11], 8333).into(), ServiceFlags::NONE),
            Address::new(&([111, 8, 89, 6], 8333).into(), ServiceFlags::NONE),
            Address::new(&([111, 8, 124, 41], 8333).into(), ServiceFlags::NONE),
            Address::new(&([111, 8, 65, 4], 8333).into(), ServiceFlags::NONE),
            Address::new(&([111, 8, 161, 73], 8333).into(), ServiceFlags::NONE),
        ];
        addrmgr.insert(
            adversary_addrs
                .iter()
                .cloned()
                .map(|a| (clock.block_time(), a)),
            Source::Dns,
        );

        // Safe addresses, controlled by non-adversarial peers.
        let safe_addrs = vec![
            Address::new(&([183, 8, 55, 2], 8333).into(), ServiceFlags::NONE),
            Address::new(&([211, 48, 99, 4], 8333).into(), ServiceFlags::NONE),
            Address::new(&([241, 44, 12, 5], 8333).into(), ServiceFlags::NONE),
            Address::new(&([99, 129, 2, 15], 8333).into(), ServiceFlags::NONE),
        ];
        addrmgr.insert(
            safe_addrs.iter().cloned().map(|a| (clock.block_time(), a)),
            Source::Dns,
        );

        // Keep track of how many times we pick a safe vs. an adversary-controlled address.
        let mut adversary = 0;
        let mut safe = 0;

        for _ in 0..99 {
            let (addr, _) = addrmgr.sample(ServiceFlags::NONE).unwrap();

            // Make sure we can re-sample the same addresses for the purpose of this example.
            clock.elapse(LocalDuration::from_mins(60));
            addrmgr.received_wake();

            if adversary_addrs.contains(&addr) {
                adversary += 1;
            } else if safe_addrs.contains(&addr) {
                safe += 1;
            }
        }

        // Despite there being more adversary-controlled addresses, our safe addresses
        // are picked much more often.
        assert!(
            safe > adversary * 2,
            "safe addresses are picked twice more often"
        );
    }
}
