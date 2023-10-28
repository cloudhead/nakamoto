//! Protocol output capabilities.
//!
//! See [`Outbox`] type.
use log::*;
use std::collections::VecDeque;
use std::net;
use std::sync::Arc;

pub use crossbeam_channel as chan;

use nakamoto_common::bitcoin::network::address::Address;
use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use nakamoto_common::bitcoin::network::message_filter::{
    CFHeaders, CFilter, GetCFHeaders, GetCFilters,
};
use nakamoto_common::bitcoin::network::message_network::VersionMessage;
use nakamoto_common::bitcoin::Transaction;
use nakamoto_common::block::time::LocalDuration;
use nakamoto_common::block::{BlockHash, BlockHeader, BlockTime, Height};

use crate::fsm::{Event, PeerId};

use super::Locators;

/// Output of a state transition of the `Protocol` state machine.
pub type Io = nakamoto_net::Io<NetworkMessage, Event, super::DisconnectReason>;

impl From<Event> for Io {
    fn from(event: Event) -> Self {
        Io::Event(event)
    }
}

/// Holds protocol outputs and pending I/O.
#[derive(Debug)]
pub struct Outbox {
    /// Protocol version.
    version: u32,
    /// Output queue.
    outbound: VecDeque<Io>,
}

impl Default for Outbox {
    fn default() -> Self {
        Self::new(super::PROTOCOL_VERSION)
    }
}

impl Iterator for Outbox {
    type Item = Io;

    /// Get the next item in the outbound queue.
    fn next(&mut self) -> Option<Io> {
        self.outbound.pop_front()
    }
}

impl Outbox {
    /// Create a new outbox.
    pub fn new(version: u32) -> Self {
        Self {
            version,
            outbound: VecDeque::new(),
        }
    }

    /// Push an output to the channel.
    pub fn push(&mut self, output: Io) {
        self.outbound.push_back(output);
    }

    /// Drain the outbound queue.
    pub fn drain(&mut self) -> impl Iterator<Item = Io> + '_ {
        self.outbound.drain(..)
    }

    /// Get the outbound i/o queue.
    pub fn outbound(&mut self) -> &VecDeque<Io> {
        &self.outbound
    }

    /// Push a message to the channel.
    pub fn message(&mut self, addr: PeerId, payload: NetworkMessage) -> &Self {
        debug!(target: "p2p", "Sending {:?} to {}", payload.cmd(), addr);

        self.push(Io::Write(addr, payload));
        self
    }

    /// Push an event to the channel.
    pub fn event<E: std::fmt::Display + Into<Event>>(&mut self, event: E) {
        info!(target: "p2p", "{event}");

        self.push(Io::Event(event.into()));
    }

    /// Disconnect from a peer.
    pub fn disconnect(&mut self, addr: net::SocketAddr, reason: super::DisconnectReason) {
        debug!(target: "p2p", "Disconnecting from {addr}: {reason}");

        self.push(Io::Disconnect(addr, reason));
    }

    /// Set a timer expiring after the given duration.
    pub fn set_timer(&mut self, duration: LocalDuration) -> &mut Self {
        self.push(Io::SetTimer(duration));
        self
    }

    /// Connect to a peer.
    pub fn connect(&mut self, addr: net::SocketAddr, timeout: LocalDuration) {
        self.push(Io::Connect(addr));
        self.push(Io::SetTimer(timeout));
    }

    /// Send a `version` message.
    pub fn version(&mut self, addr: PeerId, msg: VersionMessage) -> &mut Self {
        self.message(addr, NetworkMessage::Version(msg));
        self
    }

    /// Send a `verack` message.
    pub fn verack(&mut self, addr: PeerId) -> &mut Self {
        self.message(addr, NetworkMessage::Verack);
        self
    }

    /// Send a BIP-339 `wtxidrelay` message.
    pub fn wtxid_relay(&mut self, addr: PeerId) -> &mut Self {
        self.message(addr, NetworkMessage::WtxidRelay);
        self
    }

    /// Send a `sendheaders` message.
    pub fn send_headers(&mut self, addr: PeerId) -> &mut Self {
        self.message(addr, NetworkMessage::SendHeaders);
        self
    }

    /// Send a `ping` message.
    pub fn ping(&mut self, addr: net::SocketAddr, nonce: u64) -> &mut Self {
        self.message(addr, NetworkMessage::Ping(nonce));
        self
    }

    /// Send a `pong` message.
    pub fn pong(&mut self, addr: net::SocketAddr, nonce: u64) -> &mut Self {
        self.message(addr, NetworkMessage::Pong(nonce));
        self
    }

    /// Send a `getaddr` message.
    pub fn get_addr(&mut self, addr: PeerId) {
        self.message(addr, NetworkMessage::GetAddr);
    }

    /// Send an `addr` message.
    pub fn addr(&mut self, addr: PeerId, addrs: Vec<(BlockTime, Address)>) {
        self.message(addr, NetworkMessage::Addr(addrs));
    }

    /// Get headers from a peer.
    pub fn get_headers(&mut self, addr: PeerId, (locator_hashes, stop_hash): Locators) {
        let msg = NetworkMessage::GetHeaders(GetHeadersMessage {
            version: self.version,
            // Starting hashes, highest heights first.
            locator_hashes,
            // Using the zero hash means *fetch as many blocks as possible*.
            stop_hash,
        });

        self.message(addr, msg);
    }

    /// Send headers to a peer.
    pub fn headers(&mut self, addr: PeerId, headers: Vec<BlockHeader>) {
        self.message(addr, NetworkMessage::Headers(headers));
    }

    /// Get compact filter headers from peer, starting at the start height,
    /// and ending at the stop hash.
    pub fn get_cfheaders(
        &mut self,
        addr: PeerId,
        start_height: Height,
        stop_hash: BlockHash,
        timeout: LocalDuration,
    ) {
        self.message(
            addr,
            NetworkMessage::GetCFHeaders(GetCFHeaders {
                filter_type: 0x0,
                start_height: start_height as u32,
                stop_hash,
            }),
        );
        self.set_timer(timeout);
    }

    /// Send compact filter headers to a peer.
    pub fn cfheaders(&mut self, addr: PeerId, headers: CFHeaders) {
        self.message(addr, NetworkMessage::CFHeaders(headers));
    }

    /// Get compact filters from a peer.
    pub fn get_cfilters(
        &mut self,
        addr: PeerId,
        start_height: Height,
        stop_hash: BlockHash,
        timeout: LocalDuration,
    ) {
        self.message(
            addr,
            NetworkMessage::GetCFilters(GetCFilters {
                filter_type: 0x0,
                start_height: start_height as u32,
                stop_hash,
            }),
        );
        self.set_timer(timeout);
    }

    /// Send a compact filter to a peer.
    pub fn cfilter(&mut self, addr: PeerId, cfilter: CFilter) {
        self.message(addr, NetworkMessage::CFilter(cfilter));
    }

    /// Sends an `inv` message to a peer.
    pub fn inv(&mut self, addr: PeerId, inventories: Vec<Inventory>) {
        self.message(addr, NetworkMessage::Inv(inventories));
    }

    /// Sends a `getdata` message to a peer.
    pub fn get_data(&mut self, addr: PeerId, inventories: Vec<Inventory>) {
        self.message(addr, NetworkMessage::GetData(inventories));
    }

    /// Sends a `tx` message to a peer.
    pub fn tx(&mut self, addr: PeerId, tx: Transaction) {
        self.message(addr, NetworkMessage::Tx(tx));
    }

    /// Output an error.
    pub fn error(
        &mut self,
        message: impl ToString,
        source: impl std::error::Error + Send + Sync + 'static,
    ) {
        self.event(Event::Error {
            message: message.to_string(),
            source: Arc::new(source),
        })
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::fsm;
    use nakamoto_common::bitcoin::network::message::NetworkMessage;

    pub mod raw {
        use super::*;

        pub fn messages_from(
            outbox: impl Iterator<Item = fsm::Io>,
            addr: &net::SocketAddr,
        ) -> impl Iterator<Item = NetworkMessage> {
            let addr = *addr;

            outbox.filter_map(move |o| match o {
                fsm::Io::Write(a, msg) if a == addr => Some(msg.payload),
                _ => None,
            })
        }

        pub fn messages(
            outbox: impl Iterator<Item = fsm::Io>,
        ) -> impl Iterator<Item = (net::SocketAddr, NetworkMessage)> {
            outbox.filter_map(move |o| match o {
                fsm::Io::Write(a, msg) => Some((a, msg.payload)),
                _ => None,
            })
        }
    }

    pub fn messages_from(
        outbox: impl Iterator<Item = Io>,
        addr: &net::SocketAddr,
    ) -> impl Iterator<Item = NetworkMessage> {
        let addr = *addr;

        outbox.filter_map(move |o| match o {
            Io::Write(a, msg) if a == addr => Some(msg),
            _ => None,
        })
    }

    pub fn messages(
        outbox: impl Iterator<Item = Io>,
    ) -> impl Iterator<Item = (net::SocketAddr, NetworkMessage)> {
        outbox.filter_map(move |o| match o {
            Io::Write(a, msg) => Some((a, msg)),
            _ => None,
        })
    }

    pub fn events(outbox: impl Iterator<Item = Io>) -> impl Iterator<Item = Event> {
        outbox.filter_map(move |o| match o {
            Io::Event(e) => Some(e),
            _ => None,
        })
    }
}
