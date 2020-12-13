//! Peer-to-peer socket abstraction.
use std::collections::VecDeque;
use std::fmt::Debug;
use std::io::{self, Read, Write};
use std::net;

use bitcoin::consensus::encode::Decodable;
use bitcoin::consensus::encode::{self, Encodable};
use bitcoin::network::stream_reader::StreamReader;

use log::*;

use nakamoto_p2p::protocol::Input;

use crate::fallible;

/// Maximum peer-to-peer message size.
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Peer-to-peer socket abstraction.
#[derive(Debug)]
pub struct Socket<R: Read + Write, M> {
    raw: StreamReader<R>,
    address: net::SocketAddr,
    local_address: net::SocketAddr,
    queue: VecDeque<M>,
}

impl<M> Socket<net::TcpStream, M> {
    pub fn queue(&mut self, msg: M) {
        self.queue.push_back(msg);
    }
}

impl<M: Encodable + Decodable + Debug> Socket<net::TcpStream, M> {
    pub fn disconnect(&self) -> io::Result<()> {
        self.raw.stream.shutdown(net::Shutdown::Both)
    }
}

impl<R: Read + Write, M: Encodable + Decodable + Debug> Socket<R, M> {
    /// Create a new socket from a `io::Read` and an address pair.
    pub fn from(r: R, local_address: net::SocketAddr, address: net::SocketAddr) -> Self {
        let raw = StreamReader::new(r, Some(MAX_MESSAGE_SIZE));
        let queue = VecDeque::new();

        Self {
            raw,
            local_address,
            address,
            queue,
        }
    }

    pub fn read(&mut self) -> Result<M, encode::Error> {
        fallible! { encode::Error::Io(io::ErrorKind::Other.into()) };

        match self.raw.read_next::<M>() {
            Ok(msg) => {
                trace!("{}: (read) {:#?}", self.address, msg);

                Ok(msg)
            }
            Err(err) => Err(err),
        }
    }

    pub fn write(&mut self, msg: &M) -> Result<usize, encode::Error> {
        fallible! { encode::Error::Io(io::ErrorKind::Other.into()) };

        let mut buf = [0u8; MAX_MESSAGE_SIZE];

        match msg.consensus_encode(&mut buf[..]) {
            Ok(len) => {
                trace!("{}: (write) {:#?}", self.address, msg);

                // TODO: Is it possible to get a `WriteZero` here, given
                // the non-blocking socket?
                self.raw.stream.write_all(&buf[..len])?;
                self.raw.stream.flush()?;

                Ok(len)
            }
            Err(encode::Error::Io(err)) if err.kind() == io::ErrorKind::WriteZero => {
                unreachable!();
            }
            Err(err) => Err(err),
        }
    }

    pub fn drain(
        &mut self,
        inputs: &mut VecDeque<Input>,
        source: &mut popol::Source,
    ) -> Result<(), encode::Error> {
        while let Some(msg) = self.queue.pop_front() {
            match self.write(&msg) {
                Ok(n) => {
                    inputs.push_back(Input::Sent(self.address, n));
                }
                Err(encode::Error::Io(err)) if err.kind() == io::ErrorKind::WouldBlock => {
                    source.set(popol::interest::WRITE);
                    self.queue.push_front(msg);

                    return Ok(());
                }
                Err(err) => {
                    // An unexpected error occured. Push the message back to the front of the
                    // queue in case we're able to recover from it.
                    self.queue.push_front(msg);

                    return Err(err);
                }
            }
        }
        source.unset(popol::interest::WRITE);

        Ok(())
    }
}
