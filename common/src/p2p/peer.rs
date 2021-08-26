//! Shared peer types.

use std::io;
use std::net;

use microserde as serde;

use bitcoin::network::address::Address;
use bitcoin::network::constants::ServiceFlags;

use crate::block::time::LocalTime;

/// Peer store.
///
/// Used to store peer addresses and metadata.
pub trait Store {
    /// Get a known peer address.
    fn get(&self, ip: &net::IpAddr) -> Option<&KnownAddress>;

    /// Get a known peer address mutably.
    fn get_mut(&mut self, ip: &net::IpAddr) -> Option<&mut KnownAddress>;

    /// Insert a *new* address into the store. Returns `true` if the address was inserted,
    /// or `false` if it was already known.
    fn insert(&mut self, ip: net::IpAddr, ka: KnownAddress) -> bool;

    /// Remove an address from the store.
    fn remove(&mut self, ip: &net::IpAddr) -> Option<KnownAddress>;

    /// Return an iterator over the known addresses.
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&net::IpAddr, &KnownAddress)> + 'a>;

    /// Returns the number of addresses.
    fn len(&self) -> usize;

    /// Returns true if there are no addresses.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Seed the peer store with addresses.
    /// Fails if *none* of the seeds could be resolved to addresses.
    fn seed<S: net::ToSocketAddrs>(
        &mut self,
        seeds: impl Iterator<Item = S>,
        source: Source,
    ) -> io::Result<()> {
        let mut error = None;
        let mut success = false;

        for seed in seeds {
            match seed.to_socket_addrs() {
                Ok(addrs) => {
                    success = true;
                    for addr in addrs {
                        self.insert(
                            addr.ip(),
                            KnownAddress::new(
                                Address::new(&addr, ServiceFlags::NONE),
                                source,
                                None,
                            ),
                        );
                    }
                }
                Err(err) => error = Some(err),
            }
        }

        if success {
            return Ok(());
        }
        if let Some(err) = error {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("seeds failed to resolve: {}", err),
            ));
        }
        Ok(())
    }

    /// Clears the store of all addresses.
    fn clear(&mut self);

    /// Flush data to permanent storage.
    fn flush(&mut self) -> io::Result<()>;
}

/// Implementation of [`Store`] for [`std::collections::HashMap`].
impl Store for std::collections::HashMap<net::IpAddr, KnownAddress> {
    fn get_mut(&mut self, ip: &net::IpAddr) -> Option<&mut KnownAddress> {
        self.get_mut(ip)
    }

    fn get(&self, ip: &net::IpAddr) -> Option<&KnownAddress> {
        self.get(ip)
    }

    fn remove(&mut self, ip: &net::IpAddr) -> Option<KnownAddress> {
        self.remove(ip)
    }

    fn insert(&mut self, ip: net::IpAddr, ka: KnownAddress) -> bool {
        use ::std::collections::hash_map::Entry;

        match self.entry(ip) {
            Entry::Vacant(v) => {
                v.insert(ka);
            }
            Entry::Occupied(_) => return false,
        }
        true
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&net::IpAddr, &KnownAddress)> + 'a> {
        Box::new(self.iter())
    }

    fn clear(&mut self) {
        self.clear()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Implementation of [`Store`] for [`crate::collections::HashMap`].
impl Store for crate::collections::HashMap<net::IpAddr, KnownAddress> {
    fn get_mut(&mut self, ip: &net::IpAddr) -> Option<&mut KnownAddress> {
        self.get_mut(ip)
    }

    fn get(&self, ip: &net::IpAddr) -> Option<&KnownAddress> {
        self.get(ip)
    }

    fn remove(&mut self, ip: &net::IpAddr) -> Option<KnownAddress> {
        self.remove(ip)
    }

    fn insert(&mut self, ip: net::IpAddr, ka: KnownAddress) -> bool {
        use ::std::collections::hash_map::Entry;

        match self.entry(ip) {
            Entry::Vacant(v) => {
                v.insert(ka);
            }
            Entry::Occupied(_) => return false,
        }
        true
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&net::IpAddr, &KnownAddress)> + 'a> {
        Box::new(self.iter())
    }

    fn clear(&mut self) {
        self.clear()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Address source. Specifies where an address originated from.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Source {
    /// An address that was shared by another peer.
    Peer(net::SocketAddr),
    /// An address that came from a DNS seed.
    Dns,
    /// An address that came from some source external to the system, eg.
    /// specified by the user or added directly to the address manager.
    Imported,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Peer(addr) => write!(f, "{}", addr),
            Self::Dns => write!(f, "DNS"),
            Self::Imported => write!(f, "Imported"),
        }
    }
}

/// A known address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownAddress {
    /// Network address.
    pub addr: Address,
    /// Address of the peer who sent us this address.
    pub source: Source,
    /// Last time this address was used to successfully connect to a peer.
    pub last_success: Option<LocalTime>,
    /// Last time this address was sampled.
    pub last_sampled: Option<LocalTime>,
    /// Last time this address was tried.
    pub last_attempt: Option<LocalTime>,
    /// Last time this peer was seen alive.
    pub last_active: Option<LocalTime>,
}

impl KnownAddress {
    /// Create a new known address.
    pub fn new(addr: Address, source: Source, last_active: Option<LocalTime>) -> Self {
        Self {
            addr,
            source,
            last_success: None,
            last_attempt: None,
            last_sampled: None,
            last_active,
        }
    }

    /// Convert to a JSON value.
    pub fn to_json(&self) -> serde::json::Value {
        use serde::json::{Number, Object, Value};

        let ip = &self.addr.address;
        let port = &self.addr.port;
        let address = net::SocketAddr::from((*ip, *port)).to_string();
        let services = self.addr.services.as_u64();

        let mut obj = Object::new();

        obj.insert("address".to_owned(), Value::String(address));
        obj.insert("services".to_owned(), Value::Number(Number::U64(services)));
        obj.insert(
            "last_success".to_owned(),
            match self.last_success {
                Some(t) => Value::Number(Number::U64(t.block_time() as u64)),
                None => Value::Null,
            },
        );
        obj.insert(
            "last_attempt".to_owned(),
            match self.last_attempt {
                Some(t) => Value::Number(Number::U64(t.block_time() as u64)),
                None => Value::Null,
            },
        );
        obj.insert(
            "last_sampled".to_owned(),
            match self.last_sampled {
                Some(t) => Value::Number(Number::U64(t.block_time() as u64)),
                None => Value::Null,
            },
        );
        obj.insert(
            "last_active".to_owned(),
            match self.last_active {
                Some(t) => Value::Number(Number::U64(t.block_time() as u64)),
                None => Value::Null,
            },
        );
        obj.insert(
            "source".to_owned(),
            match self.source {
                Source::Dns => Value::String("dns".to_owned()),
                Source::Imported => Value::String("imported".to_owned()),
                Source::Peer(addr) => Value::String(addr.to_string()),
            },
        );

        Value::Object(obj)
    }

    /// Convert from a JSON value.
    pub fn from_json(v: serde::json::Value) -> Result<Self, serde::Error> {
        use serde::json::{Number, Value};

        let obj = match v {
            Value::Object(obj) => obj,
            _ => return Err(serde::Error),
        };

        let addr = match obj.get("address") {
            Some(Value::String(addr)) => addr.parse().unwrap(),
            _ => return Err(serde::Error),
        };
        let services = match obj.get("services") {
            Some(Value::Number(Number::U64(srv))) => ServiceFlags::from(*srv),
            _ => return Err(serde::Error),
        };
        let last_success = match obj.get("last_success") {
            Some(Value::Null) => None,
            Some(Value::Number(Number::U64(n))) => Some(LocalTime::from_block_time(*n as u32)),
            None => None,
            _ => return Err(serde::Error),
        };
        let last_attempt = match obj.get("last_attempt") {
            Some(Value::Null) => None,
            Some(Value::Number(Number::U64(n))) => Some(LocalTime::from_block_time(*n as u32)),
            None => None,
            _ => return Err(serde::Error),
        };
        let last_sampled = match obj.get("last_sampled") {
            Some(Value::Null) => None,
            Some(Value::Number(Number::U64(n))) => Some(LocalTime::from_block_time(*n as u32)),
            None => None,
            _ => return Err(serde::Error),
        };
        let last_active = match obj.get("last_active") {
            Some(Value::Null) => None,
            Some(Value::Number(Number::U64(n))) => Some(LocalTime::from_block_time(*n as u32)),
            None => None,
            _ => return Err(serde::Error),
        };
        let source = match obj.get("source") {
            Some(Value::String(s)) => {
                if s == "dns" {
                    Source::Dns
                } else if s == "imported" {
                    Source::Imported
                } else {
                    match s.parse() {
                        Ok(addr) => Source::Peer(addr),
                        Err(_) => return Err(serde::Error),
                    }
                }
            }
            _ => return Err(serde::Error),
        };

        Ok(Self {
            addr: Address::new(&addr, services),
            source,
            last_success,
            last_sampled,
            last_attempt,
            last_active,
        })
    }
}

/// Source of peer addresses.
pub trait AddressSource {
    /// Sample a random peer address. Returns `None` if there are no addresses left.
    fn sample(&mut self, services: ServiceFlags) -> Option<(Address, Source)>;
    /// Record an address of ours as seen by a remote peer.
    fn record_local_address(&mut self, addr: net::SocketAddr);
    /// Return an iterator over random peer addresses.
    fn iter(&mut self, services: ServiceFlags) -> Box<dyn Iterator<Item = (Address, Source)> + '_>;
}

/// Functions and traits useful for testing.
pub mod test {
    use super::*;

    impl AddressSource for std::collections::VecDeque<(Address, Source)> {
        fn sample(&mut self, _services: ServiceFlags) -> Option<(Address, Source)> {
            self.pop_front()
        }

        fn record_local_address(&mut self, _addr: net::SocketAddr) {
            // Do nothing.
        }

        fn iter(
            &mut self,
            _services: ServiceFlags,
        ) -> Box<dyn Iterator<Item = (Address, Source)> + '_> {
            Box::new(std::collections::VecDeque::drain(self, ..))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_address() {
        let sockaddr = net::SocketAddr::from(([1, 2, 3, 4], 8333));
        let services = ServiceFlags::NETWORK;
        let ka = KnownAddress {
            addr: Address::new(&sockaddr, services),
            source: Source::Peer(net::SocketAddr::from(([4, 5, 6, 7], 8333))),
            last_success: Some(LocalTime::from_secs(42)),
            last_sampled: Some(LocalTime::from_secs(144)),
            last_attempt: None,
            last_active: None,
        };

        let value = ka.to_json();
        let deserialized = KnownAddress::from_json(value).unwrap();

        assert_eq!(ka, deserialized);
    }
}
