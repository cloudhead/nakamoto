//! P2P-related types
use std::net;

pub mod peer;

/// Communication domain of a network socket.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Domain {
    /// IPv4.
    IPV4,
    /// IPv6.
    IPV6,
}

impl Domain {
    /// All domains.
    pub fn all() -> Vec<Self> {
        vec![Self::IPV4, Self::IPV6]
    }

    /// Returns the domain for `address`.
    pub const fn for_address(address: &net::SocketAddr) -> Domain {
        match address {
            net::SocketAddr::V4(_) => Domain::IPV4,
            net::SocketAddr::V6(_) => Domain::IPV6,
        }
    }
}
