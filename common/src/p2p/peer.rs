//! Shared peer types.

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

    /// Insert an address into the store.
    fn insert(&mut self, ip: net::IpAddr, ka: KnownAddress) -> Option<KnownAddress>;

    /// Remove an address from the store.
    fn remove(&mut self, ip: &net::IpAddr) -> Option<KnownAddress>;

    /// Return an iterator over the known addresses.
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&net::IpAddr, &KnownAddress)> + 'a>;

    /// Returns the number of addresses.
    fn len(&self) -> usize;

    /// Clears the store of all addresses.
    fn clear(&mut self);

    /// Flush data to permanent storage.
    fn flush(&mut self) -> std::io::Result<()>;
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

    fn insert(&mut self, ip: net::IpAddr, ka: KnownAddress) -> Option<KnownAddress> {
        self.insert(ip, ka)
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownAddress {
    /// Network address.
    pub addr: Address,
    /// Address of the peer who sent us this address.
    pub source: Source,
    /// Last time this address was used to successfully connect to a peer.
    pub last_success: Option<LocalTime>,
    /// Last time this address was tried.
    pub last_attempt: Option<LocalTime>,
}

impl KnownAddress {
    /// Create a new known address.
    pub fn new(addr: Address, source: Source) -> Self {
        Self {
            addr,
            source,
            last_success: None,
            last_attempt: None,
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
            _ => return Err(serde::Error),
        };
        let last_attempt = match obj.get("last_attempt") {
            Some(Value::Null) => None,
            Some(Value::Number(Number::U64(n))) => Some(LocalTime::from_block_time(*n as u32)),
            _ => return Err(serde::Error),
        };

        Ok(Self {
            addr: Address::new(&addr, services),
            source: Source::Other,
            last_success,
            last_attempt,
        })
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
            source: Source::default(),
            last_success: Some(LocalTime::from_secs(42)),
            last_attempt: None,
        };

        let value = ka.to_json();
        let deserialized = KnownAddress::from_json(value).unwrap();

        assert_eq!(ka, deserialized);
    }
}
