use std::{io, net, time};

use nakamoto_p2p::traits::Dialer;
use socket2::{Domain, Socket, Type};

use crate::fallible;

/// Maximum time to wait when reading from a socket.
const READ_TIMEOUT: time::Duration = time::Duration::from_secs(6);
/// Maximum time to wait when writing to a socket.
const WRITE_TIMEOUT: time::Duration = time::Duration::from_secs(3);

/// Basic non-blocking TCP dialer.
pub struct TcpDialer {
    pub read_timeout: Option<time::Duration>,
    pub write_timeout: Option<time::Duration>,
}

impl Default for TcpDialer {
    fn default() -> Self {
        Self {
            read_timeout: Some(READ_TIMEOUT),
            write_timeout: Some(WRITE_TIMEOUT),
        }
    }
}

impl Dialer for TcpDialer {
    fn dial(&mut self, addr: &net::SocketAddr) -> Result<net::TcpStream, io::Error> {
        fallible! { io::Error::from(io::ErrorKind::Other) };

        let domain = if addr.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };
        let sock = Socket::new(domain, Type::STREAM, None)?;

        sock.set_read_timeout(self.read_timeout)?;
        sock.set_write_timeout(self.write_timeout)?;
        sock.set_nonblocking(true)?;

        match sock.connect(&(*addr).into()) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
            Err(e) if e.raw_os_error() == Some(libc::EALREADY) => {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e),
        }
        Ok(sock.into())
    }
}
