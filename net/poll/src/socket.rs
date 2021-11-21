//! Peer-to-peer socket abstraction.
use std::fmt::Debug;
use std::io::{self, Read, Write};
use std::net;

use nakamoto_p2p::protocol::Link;

use crate::fallible;

/// Peer-to-peer socket abstraction.
#[derive(Debug)]
pub struct Socket<R: Read + Write> {
    pub address: net::SocketAddr,
    pub link: Link,

    raw: R,
}

impl Socket<net::TcpStream> {
    /// Get socket local address.
    pub fn local_address(&self) -> io::Result<net::SocketAddr> {
        self.raw.local_addr()
    }

    /// Disconnect socket.
    pub fn disconnect(&self) -> io::Result<()> {
        self.raw.shutdown(net::Shutdown::Both)
    }
}

impl<R: Read + Write> Socket<R> {
    /// Create a new socket from a `io::Read` and an address pair.
    pub fn from(raw: R, address: net::SocketAddr, link: Link) -> Self {
        Self { raw, link, address }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.raw.read(buf)
    }
}

impl<R: Read + Write> io::Write for &mut Socket<R> {
    fn write(&mut self, bytes: &[u8]) -> Result<usize, io::Error> {
        fallible! { io::Error::from(io::ErrorKind::Other) };

        self.raw.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.raw.flush()
    }
}
