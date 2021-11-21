//! A channel for communicating between the main protocol and the sub-protocols.
//!
//! Each sub-protocol, eg. the "ping" or "handshake" protocols are given a channel
//! with specific capabilities, eg. peer disconnection, message sending etc. to
//! communicate with the main protocol and network.
use log::*;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::{io, net};

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

use crate::protocol::{DisconnectReason, Event, Io, PeerId};

use super::network::Network;
use super::{addrmgr, cbfmgr, invmgr, message, peermgr, pingmgr, syncmgr, Locators};

/// Used to construct a protocol output.
#[derive(Debug, Clone)]
pub struct Channel {
    /// Protocol version.
    version: u32,
    /// Output queue.
    outbound: Rc<RefCell<VecDeque<Io>>>,
    /// Message outbox.
    outbox: Rc<RefCell<HashMap<PeerId, Vec<u8>>>>,
    /// Network message builder.
    builder: message::Builder,
    /// Log target.
    target: &'static str,
}

impl Channel {
    /// Create a new channel.
    pub fn new(network: Network, version: u32, target: &'static str) -> Self {
        Self {
            version,
            outbound: Rc::new(RefCell::new(VecDeque::new())),
            outbox: Rc::new(RefCell::new(HashMap::new())),
            builder: message::Builder::new(network),
            target,
        }
    }

    /// Push an output to the channel.
    pub fn push(&self, output: Io) {
        self.outbound.borrow_mut().push_back(output);
    }

    /// Unregister peer. Clears the outbox.
    pub fn unregister(&mut self, peer: &PeerId) {
        if let Some(outbox) = self.outbox.borrow_mut().remove(peer) {
            if !outbox.is_empty() {
                debug!(target: self.target, "{}: Dropping outbox with {} bytes", peer, outbox.len());
            }
        }
    }

    /// Drain the outbound queue.
    pub fn drain(&mut self) -> Drain {
        Drain {
            items: self.outbound.clone(),
        }
    }

    /// Write the peer's output buffer to the given writer.
    pub fn write<W: io::Write>(&mut self, peer: &PeerId, mut writer: W) -> io::Result<()> {
        if let Some(buf) = self.outbox.borrow_mut().get_mut(peer) {
            while !buf.is_empty() {
                match writer.write(buf) {
                    Err(e) => return Err(e),

                    Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
                    Ok(n) => {
                        buf.drain(..n);
                    }
                }
            }
        }
        Ok(())
    }

    /// Push a message to the channel.
    pub fn message(&mut self, addr: PeerId, message: NetworkMessage) -> &Self {
        debug!(target: self.target, "{}: Sending {:?}", addr, message.cmd());

        let mut outbox = self.outbox.borrow_mut();
        let buffer = outbox.entry(addr).or_insert_with(Vec::new);

        // Nb. writing to a vector cannot result in an error.
        self.builder.write(message, buffer).ok();
        self.push(Io::Write(addr));
        self
    }

    /// Push an event to the channel.
    pub fn event(&self, event: Event) {
        self.push(Io::Event(event));
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

/// Ability to disconnect from peers.
pub trait Disconnect {
    /// Disconnect from peer.
    fn disconnect(&self, addr: net::SocketAddr, reason: DisconnectReason);
}

impl Disconnect for Channel {
    fn disconnect(&self, addr: net::SocketAddr, reason: DisconnectReason) {
        debug!(target: self.target, "{}: Disconnecting: {}", addr, reason);
        self.push(Io::Disconnect(addr, reason));
    }
}

impl Disconnect for () {
    fn disconnect(&self, _addr: net::SocketAddr, _reason: DisconnectReason) {}
}

/// The ability to be woken up in the future.
pub trait Wakeup {
    /// Ask to be woken up in a predefined amount of time.
    fn wakeup(&self, duration: LocalDuration) -> &Self;
}

impl Wakeup for Channel {
    fn wakeup(&self, duration: LocalDuration) -> &Self {
        self.push(Io::Wakeup(duration));
        self
    }
}

impl Wakeup for () {
    fn wakeup(&self, _timeout: LocalDuration) -> &Self {
        self
    }
}

impl addrmgr::SyncAddresses for Channel {
    fn get_addresses(&mut self, addr: PeerId) {
        self.message(addr, NetworkMessage::GetAddr);
    }

    fn send_addresses(&mut self, addr: PeerId, addrs: Vec<(BlockTime, Address)>) {
        self.message(addr, NetworkMessage::Addr(addrs));
    }
}

impl peermgr::Connect for Channel {
    fn connect(&self, addr: net::SocketAddr, timeout: LocalDuration) {
        info!(target: self.target, "[conn] {}: Connecting..", addr);
        self.push(Io::Connect(addr));
        self.push(Io::Wakeup(timeout));
    }
}

impl peermgr::Connect for () {
    fn connect(&self, _addr: net::SocketAddr, _timeout: LocalDuration) {}
}

impl peermgr::Events for () {
    fn event(&self, _event: peermgr::Event) {}
}

impl addrmgr::Events for Channel {
    fn event(&self, event: addrmgr::Event) {
        match &event {
            addrmgr::Event::Error(msg) => error!(target: self.target, "[addr] {}", msg),
            event @ addrmgr::Event::AddressDiscovered(_, _) => {
                trace!(target: self.target, "[addr] {}", &event);
            }
            event => {
                debug!(target: self.target, "[addr] {}", &event);
            }
        }
        self.event(Event::AddrManager(event));
    }
}

impl peermgr::Events for Channel {
    fn event(&self, event: peermgr::Event) {
        info!(target: self.target, "[peer] {}", &event);
        self.event(Event::PeerManager(event));
    }
}

impl pingmgr::Ping for Channel {
    fn ping(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Ping(nonce));
        self
    }

    fn pong(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Pong(nonce));
        self
    }
}

impl syncmgr::SyncHeaders for Channel {
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

    fn send_headers(&mut self, addr: PeerId, headers: Vec<BlockHeader>) {
        self.message(addr, NetworkMessage::Headers(headers));
    }

    fn negotiate(&mut self, addr: PeerId) {
        self.message(addr, NetworkMessage::SendHeaders);
    }

    fn event(&self, event: syncmgr::Event) {
        debug!(target: self.target, "[sync] {}", &event);

        match &event {
            syncmgr::Event::Synced(tip, height) => {
                info!(target: self.target, "Block height = {}, tip = {}", height, tip);
            }
            _ => {}
        }
        self.event(Event::SyncManager(event));
    }
}

impl peermgr::Handshake for Channel {
    fn version(&mut self, addr: PeerId, msg: VersionMessage) -> &mut Self {
        self.message(addr, NetworkMessage::Version(msg));
        self
    }

    fn verack(&mut self, addr: PeerId) -> &mut Self {
        self.message(addr, NetworkMessage::Verack);
        self
    }

    fn wtxidrelay(&mut self, addr: PeerId) -> &mut Self {
        self.message(addr, NetworkMessage::WtxidRelay);
        self
    }
}

impl peermgr::Handshake for () {
    fn version(&mut self, _addr: PeerId, _msg: VersionMessage) -> &mut Self {
        self
    }

    fn verack(&mut self, _addr: PeerId) -> &mut Self {
        self
    }

    fn wtxidrelay(&mut self, _addr: PeerId) -> &mut Self {
        self
    }
}

impl cbfmgr::SyncFilters for Channel {
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

    fn send_cfheaders(&mut self, addr: PeerId, headers: CFHeaders) {
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

    fn send_cfilter(&mut self, addr: PeerId, cfilter: CFilter) {
        self.message(addr, NetworkMessage::CFilter(cfilter));
    }
}

impl invmgr::Inventories for Channel {
    fn inv(&mut self, addr: PeerId, inventories: Vec<Inventory>) {
        self.message(addr, NetworkMessage::Inv(inventories));
    }

    fn getdata(&mut self, addr: PeerId, inventories: Vec<Inventory>) {
        self.message(addr, NetworkMessage::GetData(inventories));
    }

    fn tx(&mut self, addr: PeerId, tx: Transaction) {
        self.message(addr, NetworkMessage::Tx(tx));
    }

    fn event(&self, event: invmgr::Event) {
        debug!(target: self.target, "[invmgr] {}", &event);
        self.event(Event::InventoryManager(event));
    }
}

impl cbfmgr::Events for Channel {
    fn event(&self, event: cbfmgr::Event) {
        debug!(target: self.target, "[spv] {}", &event);

        match event {
            cbfmgr::Event::FilterHeadersImported { height, .. } => {
                info!(target: self.target, "Filter height = {}", height);
            }
            _ => {}
        }

        self.event(Event::FilterManager(event));
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use nakamoto_common::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};

    pub fn messages(
        channel: &mut Channel,
        addr: &net::SocketAddr,
    ) -> impl Iterator<Item = NetworkMessage> {
        let mut bytes = Vec::new();
        let mut stream = crate::stream::Decoder::new(2048);
        let mut msgs = Vec::new();

        channel.write(addr, &mut bytes).unwrap();
        stream.input(&bytes);

        while let Some(msg) = stream.decode_next::<RawNetworkMessage>().unwrap() {
            msgs.push(msg.payload);
        }
        msgs.into_iter()
    }
}
