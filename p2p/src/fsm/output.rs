//! Protocol output capabilities.
//!
//! See [`Outbox`] type.
//!
//! Each sub-protocol, eg. the "ping" or "handshake" protocols are given a copy of this outbox
//! with specific capabilities, eg. peer disconnection, message sending etc. to
//! communicate with the network.
use log::*;
use std::cell::{Ref, RefCell};
use std::collections::VecDeque;
use std::net;
use std::rc::Rc;

pub use crossbeam_channel as chan;

use nakamoto_common::bitcoin::network::address::Address;
use nakamoto_common::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use nakamoto_common::bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use nakamoto_common::bitcoin::network::message_filter::{
    CFHeaders, CFilter, GetCFHeaders, GetCFilters,
};
use nakamoto_common::bitcoin::network::message_network::VersionMessage;
use nakamoto_common::bitcoin::Transaction;
use nakamoto_common::block::time::LocalDuration;
use nakamoto_common::block::{BlockHash, BlockHeader, BlockTime, Height};

use crate::fsm::{Event, PeerId};

use super::network::Network;
use super::Locators;

/// Output of a state transition of the `Protocol` state machine.
pub type Io = nakamoto_net::ReactorDispatch<RawNetworkMessage, Event, super::DisconnectReason>;

impl From<Event> for Io {
    fn from(event: Event) -> Self {
        Io::NotifySubscribers(event)
    }
}

/// Ability to connect to peers.
pub trait Connect {
    /// Connect to peer.
    fn connect(&self, addr: net::SocketAddr, timeout: LocalDuration);
}

/// Ability to disconnect from peers.
pub trait Disconnect {
    /// Disconnect from peer.
    fn disconnect(&self, addr: net::SocketAddr, reason: super::DisconnectReason);
}

/// The ability to be woken up in the future.
pub trait Wakeup {
    /// Ask to be woken up in a predefined amount of time.
    fn wakeup(&self, duration: LocalDuration) -> &Self;
}

/// Bitcoin wire protocol.
pub trait Wire<E> {
    /// Emit an event.
    fn event(&self, event: E);

    // Handshake messages //////////////////////////////////////////////////////

    /// Send a `version` message.
    fn version(&mut self, addr: PeerId, msg: VersionMessage) -> &mut Self;

    /// Send a `verack` message.
    fn verack(&mut self, addr: PeerId) -> &mut Self;

    /// Send a BIP-339 `wtxidrelay` message.
    fn wtxid_relay(&mut self, addr: PeerId) -> &mut Self;

    /// Send a `sendheaders` message.
    fn send_headers(&mut self, addr: PeerId) -> &mut Self;

    // Ping/pong ///////////////////////////////////////////////////////////////

    /// Send a `ping` message.
    fn ping(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self;

    /// Send a `pong` message.
    fn pong(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self;

    // Addresses //////////////////////////////////////////////////////////////

    /// Send a `getaddr` message.
    fn get_addr(&mut self, addr: PeerId);

    /// Send an `addr` message.
    fn addr(&mut self, addr: PeerId, addrs: Vec<(BlockTime, Address)>);

    // Compact block filters ///////////////////////////////////////////////////

    /// Get compact filter headers from peer, starting at the start height,
    /// and ending at the stop hash.
    fn get_cfheaders(
        &mut self,
        addr: PeerId,
        start_height: Height,
        stop_hash: BlockHash,
        timeout: LocalDuration,
    );

    /// Get compact filters from a peer.
    fn get_cfilters(
        &mut self,
        addr: PeerId,
        start_height: Height,
        stop_hash: BlockHash,
        timeout: LocalDuration,
    );

    /// Send compact filter headers to a peer.
    fn cfheaders(&mut self, addr: PeerId, headers: CFHeaders);

    /// Send a compact filter to a peer.
    fn cfilter(&mut self, addr: PeerId, filter: CFilter);

    // Header sync /////////////////////////////////////////////////////////////

    /// Get headers from a peer.
    fn get_headers(&mut self, addr: PeerId, locators: Locators);

    /// Send headers to a peer.
    fn headers(&mut self, addr: PeerId, headers: Vec<BlockHeader>);

    // Inventory ///////////////////////////////////////////////////////////////

    /// Sends an `inv` message to a peer.
    fn inv(&mut self, addr: PeerId, inventories: Vec<Inventory>);

    /// Sends a `getdata` message to a peer.
    fn get_data(&mut self, addr: PeerId, inventories: Vec<Inventory>);

    /// Sends a `tx` message to a peer.
    fn tx(&mut self, addr: PeerId, tx: Transaction);
}

/// Holds protocol outputs and pending I/O.
#[derive(Debug, Clone)]
pub struct Outbox {
    /// Protocol version.
    version: u32,
    /// Bitcoin network.
    network: Network,
    /// Output queue.
    outbound: Rc<RefCell<VecDeque<Io>>>,
}

impl Iterator for Outbox {
    type Item = Io;

    /// Get the next item in the outbound queue.
    fn next(&mut self) -> Option<Io> {
        self.outbound.borrow_mut().pop_front()
    }
}

impl Outbox {
    /// Create a new channel.
    pub fn new(network: Network, version: u32) -> Self {
        Self {
            version,
            network,
            outbound: Rc::new(RefCell::new(VecDeque::new())),
        }
    }

    /// Push an output to the channel.
    pub fn push(&self, output: Io) {
        self.outbound.borrow_mut().push_back(output);
    }

    /// Drain the outbound queue.
    pub fn drain(&mut self) -> Drain {
        Drain {
            items: self.outbound.clone(),
        }
    }

    /// Get the outbound i/o queue.
    pub fn outbound(&mut self) -> Ref<VecDeque<Io>> {
        self.outbound.borrow()
    }

    /// Push a message to the channel.
    pub fn message(&mut self, addr: PeerId, payload: NetworkMessage) -> &Self {
        debug!(target: "p2p", "Sending {:?} to {}", payload.cmd(), addr);

        self.push(Io::SendPeer(
            addr,
            RawNetworkMessage {
                magic: self.network.magic(),
                payload,
            },
        ));
        self
    }

    /// Push an event to the channel.
    pub fn event(&self, event: Event) {
        self.push(Io::NotifySubscribers(event));
    }
}

/// Draining iterator over outbound channel queue.
pub struct Drain {
    items: Rc<RefCell<VecDeque<Io>>>,
}

impl Iterator for Drain {
    type Item = Io;

    fn next(&mut self) -> Option<Self::Item> {
        self.items.borrow_mut().pop_front()
    }
}

impl Disconnect for Outbox {
    fn disconnect(&self, addr: net::SocketAddr, reason: super::DisconnectReason) {
        debug!(target: "p2p", "Disconnecting from {}: {}", addr, reason);

        self.push(Io::DisconnectPeer(addr, reason));
    }
}

impl Wakeup for Outbox {
    fn wakeup(&self, duration: LocalDuration) -> &Self {
        self.push(Io::SetTimer(duration));
        self
    }
}

impl Connect for Outbox {
    fn connect(&self, addr: net::SocketAddr, timeout: LocalDuration) {
        self.push(Io::ConnectPeer(addr));
        self.push(Io::SetTimer(timeout));
    }
}

impl<E: Into<Event> + std::fmt::Display> Wire<E> for Outbox {
    fn event(&self, event: E) {
        info!(target: "p2p", "{}", &event);

        self.event(event.into());
    }

    fn version(&mut self, addr: PeerId, msg: VersionMessage) -> &mut Self {
        self.message(addr, NetworkMessage::Version(msg));
        self
    }

    fn verack(&mut self, addr: PeerId) -> &mut Self {
        self.message(addr, NetworkMessage::Verack);
        self
    }

    fn wtxid_relay(&mut self, addr: PeerId) -> &mut Self {
        self.message(addr, NetworkMessage::WtxidRelay);
        self
    }

    fn send_headers(&mut self, addr: PeerId) -> &mut Self {
        self.message(addr, NetworkMessage::SendHeaders);
        self
    }

    fn ping(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Ping(nonce));
        self
    }

    fn pong(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Pong(nonce));
        self
    }

    fn get_addr(&mut self, addr: PeerId) {
        self.message(addr, NetworkMessage::GetAddr);
    }

    fn addr(&mut self, addr: PeerId, addrs: Vec<(BlockTime, Address)>) {
        self.message(addr, NetworkMessage::Addr(addrs));
    }

    fn get_headers(&mut self, addr: PeerId, (locator_hashes, stop_hash): Locators) {
        let msg = NetworkMessage::GetHeaders(GetHeadersMessage {
            version: self.version,
            // Starting hashes, highest heights first.
            locator_hashes,
            // Using the zero hash means *fetch as many blocks as possible*.
            stop_hash,
        });

        self.message(addr, msg);
    }

    fn headers(&mut self, addr: PeerId, headers: Vec<BlockHeader>) {
        self.message(addr, NetworkMessage::Headers(headers));
    }

    fn get_cfheaders(
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
        self.wakeup(timeout);
    }

    fn cfheaders(&mut self, addr: PeerId, headers: CFHeaders) {
        self.message(addr, NetworkMessage::CFHeaders(headers));
    }

    fn get_cfilters(
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
        self.wakeup(timeout);
    }

    fn cfilter(&mut self, addr: PeerId, cfilter: CFilter) {
        self.message(addr, NetworkMessage::CFilter(cfilter));
    }

    fn inv(&mut self, addr: PeerId, inventories: Vec<Inventory>) {
        self.message(addr, NetworkMessage::Inv(inventories));
    }

    fn get_data(&mut self, addr: PeerId, inventories: Vec<Inventory>) {
        self.message(addr, NetworkMessage::GetData(inventories));
    }

    fn tx(&mut self, addr: PeerId, tx: Transaction) {
        self.message(addr, NetworkMessage::Tx(tx));
    }
}

#[cfg(test)]
#[allow(unused_variables)]
impl<E> Wire<E> for () {
    fn event(&self, event: E) {}
    fn tx(&mut self, addr: PeerId, tx: Transaction) {}
    fn inv(&mut self, addr: PeerId, inventories: Vec<Inventory>) {}
    fn get_data(&mut self, addr: PeerId, inventories: Vec<Inventory>) {}
    fn get_headers(&mut self, addr: PeerId, locators: Locators) {}
    fn get_addr(&mut self, addr: PeerId) {}
    fn cfilter(&mut self, addr: PeerId, filter: CFilter) {}
    fn headers(&mut self, addr: PeerId, headers: Vec<BlockHeader>) {}
    fn addr(&mut self, addr: PeerId, addrs: Vec<(BlockTime, Address)>) {}
    fn cfheaders(&mut self, addr: PeerId, headers: CFHeaders) {}
    fn ping(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self
    }
    fn pong(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self
    }
    fn verack(&mut self, addr: PeerId) -> &mut Self {
        self
    }
    fn version(&mut self, addr: PeerId, msg: VersionMessage) -> &mut Self {
        self
    }
    fn wtxid_relay(&mut self, addr: PeerId) -> &mut Self {
        self
    }
    fn send_headers(&mut self, addr: PeerId) -> &mut Self {
        self
    }
    fn get_cfilters(
        &mut self,
        addr: PeerId,
        start_height: Height,
        stop_hash: BlockHash,
        timeout: LocalDuration,
    ) {
    }
    fn get_cfheaders(
        &mut self,
        addr: PeerId,
        start_height: Height,
        stop_hash: BlockHash,
        timeout: LocalDuration,
    ) {
    }
}

#[cfg(test)]
#[allow(unused_variables)]
impl Connect for () {
    fn connect(&self, addr: net::SocketAddr, timeout: LocalDuration) {}
}

#[cfg(test)]
#[allow(unused_variables)]
impl Disconnect for () {
    fn disconnect(&self, addr: net::SocketAddr, reason: super::DisconnectReason) {}
}

#[cfg(test)]
#[allow(unused_variables)]
impl Wakeup for () {
    fn wakeup(&self, duration: LocalDuration) -> &Self {
        &()
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use nakamoto_common::bitcoin::network::message::NetworkMessage;

    pub fn messages_from(
        outbox: &mut Outbox,
        addr: &net::SocketAddr,
    ) -> impl Iterator<Item = NetworkMessage> {
        let mut msgs = Vec::new();

        outbox.outbound.borrow_mut().retain(|o| match o {
            Io::SendPeer(a, msg) if a == addr => {
                msgs.push(msg.payload.clone());
                false
            }
            _ => true,
        });

        msgs.into_iter()
    }

    pub fn messages(
        outbox: &mut Outbox,
    ) -> impl Iterator<Item = (net::SocketAddr, NetworkMessage)> {
        let mut msgs = Vec::new();

        outbox.outbound.borrow_mut().retain(|o| match o {
            Io::SendPeer(addr, msg) => {
                msgs.push((*addr, msg.payload.clone()));
                false
            }
            _ => true,
        });
        msgs.into_iter()
    }

    pub fn events(outbox: &mut Outbox) -> impl Iterator<Item = Event> {
        let mut events = Vec::new();

        outbox.outbound.borrow_mut().retain(|o| match o {
            Io::NotifySubscribers(e) => {
                events.push(e.clone());
                false
            }
            _ => true,
        });
        events.into_iter()
    }
}
