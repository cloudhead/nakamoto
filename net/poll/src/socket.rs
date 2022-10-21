//! Peer-to-peer socket abstraction.
use std::fmt::Debug;
use std::io::{self, Read, Write};
use std::net;

use nakamoto_net::ConnDirection;

use crate::fallible;

/// Peer-to-peer socket abstraction.
#[derive(Debug)]
pub struct Socket<R: Read + Write> {
    pub address: net::SocketAddr,
    pub link: ConnDirection,

    buffer: Vec<u8>,
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
    pub fn from(raw: R, address: net::SocketAddr, link: ConnDirection) -> Self {
        Self {
            raw,
            link,
            address,
            buffer: Vec::with_capacity(1024),
        }
    }

    pub fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.raw.read(buf)
    }

    pub fn push(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    pub fn flush(&mut self) -> io::Result<()> {
        fallible! { io::Error::from(io::ErrorKind::Other) };

        while !self.buffer.is_empty() {
            match self.raw.write(&self.buffer) {
                Err(e) => return Err(e),

                Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                Ok(n) => {
                    self.buffer.drain(..n);
                }
            }
        }
        self.raw.flush()
    }
}
