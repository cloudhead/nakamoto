//! A channel for communicating between the main protocol and the sub-protocols.
//!
//! Each sub-protocol, eg. the "ping" or "handshake" protocols are given a channel
//! with specific capabilities, eg. peer disconnection, message sending etc. to
//! communicate with the main protocol and network.
use log::*;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::net;
use std::rc::Rc;

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

use crate::protocol::{DisconnectReason, Event, Out, PeerId};

use super::network::Network;
use super::{addrmgr, cbfmgr, invmgr, message, peermgr, pingmgr, syncmgr, Locators};

/// Used to construct a protocol output.
#[derive(Debug, Clone)]
pub struct Channel {
    /// Protocol version.
    version: u32,
    /// Output channel.
    outbound: Rc<RefCell<VecDeque<Out>>>,
    /// Network magic number.
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
            builder: message::Builder::new(network),
            target,
        }
    }

    /// Push an output to the channel.
    pub fn push(&self, output: Out) {
        self.outbound.borrow_mut().push_back(output);
    }

    /// Drain the outbound queue.
    pub fn drain(&mut self) -> Iter {
        Iter {
            items: self.outbound.clone(),
        }
    }

    /// Push a message to the channel.
    pub fn message(&self, addr: PeerId, message: NetworkMessage) -> &Self {
        debug!(target: self.target, "{}: Sending {:?}", addr, message.cmd());

        self.push(self.builder.message(addr, message));
        self
    }

    /// Push an event to the channel.
    pub fn event(&self, event: Event) {
        self.push(Out::Event(event));
    }
}

/// Iterator over outbound channel queue.
pub struct Iter {
    items: Rc<RefCell<VecDeque<Out>>>,
}

impl Iterator for Iter {
    type Item = Out;

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
        self.push(Out::Disconnect(addr, reason));
    }
}

impl Disconnect for () {
    fn disconnect(&self, _addr: net::SocketAddr, _reason: DisconnectReason) {}
}

/// The ability to set timeouts.
pub trait SetTimeout {
    /// Set a timeout. Returns the unique timeout identifier.
    fn set_timeout(&self, timeout: LocalDuration) -> &Self;
}

impl SetTimeout for Channel {
    fn set_timeout(&self, timeout: LocalDuration) -> &Self {
        self.push(Out::SetTimeout(timeout));
        self
    }
}

impl SetTimeout for () {
    fn set_timeout(&self, _timeout: LocalDuration) -> &Self {
        self
    }
}

impl addrmgr::SyncAddresses for Channel {
    fn get_addresses(&self, addr: PeerId) {
        self.message(addr, NetworkMessage::GetAddr);
    }

    fn send_addresses(&self, addr: PeerId, addrs: Vec<(BlockTime, Address)>) {
        self.message(addr, NetworkMessage::Addr(addrs));
    }
}

impl peermgr::Connect for Channel {
    fn connect(&self, addr: net::SocketAddr, timeout: LocalDuration) {
        info!(target: self.target, "[conn] {}: Connecting..", addr);
        self.push(Out::Connect(addr, timeout));
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
    fn ping(&self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Ping(nonce));
        self
    }

    fn pong(&self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Pong(nonce));
        self
    }
}

impl syncmgr::SyncHeaders for Channel {
    fn get_headers(&self, addr: PeerId, (locator_hashes, stop_hash): Locators) {
        let msg = NetworkMessage::GetHeaders(GetHeadersMessage {
            version: self.version,
            // Starting hashes, highest heights first.
            locator_hashes,
            // Using the zero hash means *fetch as many blocks as possible*.
            stop_hash,
        });

        self.message(addr, msg);
    }

    fn send_headers(&self, addr: PeerId, headers: Vec<BlockHeader>) {
        let msg = self.builder.message(addr, NetworkMessage::Headers(headers));

        self.push(msg);
    }

    fn negotiate(&self, addr: PeerId) {
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
    fn version(&self, addr: PeerId, msg: VersionMessage) -> &Self {
        self.message(addr, NetworkMessage::Version(msg));
        self
    }

    fn verack(&self, addr: PeerId) -> &Self {
        self.message(addr, NetworkMessage::Verack);
        self
    }

    fn wtxidrelay(&self, addr: PeerId) -> &Self {
        self.message(addr, NetworkMessage::WtxidRelay);
        self
    }
}

impl peermgr::Handshake for () {
    fn version(&self, _addr: PeerId, _msg: VersionMessage) -> &Self {
        self
    }

    fn verack(&self, _addr: PeerId) -> &Self {
        self
    }

    fn wtxidrelay(&self, _addr: PeerId) -> &Self {
        self
    }
}

impl cbfmgr::SyncFilters for Channel {
    fn get_cfheaders(
        &self,
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
        self.set_timeout(timeout);
    }

    fn send_cfheaders(&self, addr: PeerId, headers: CFHeaders) {
        self.message(addr, NetworkMessage::CFHeaders(headers));
    }

    fn get_cfilters(
        &self,
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
        self.set_timeout(timeout);
    }

    fn send_cfilter(&self, addr: PeerId, cfilter: CFilter) {
        self.message(addr, NetworkMessage::CFilter(cfilter));
    }
}

impl invmgr::Inventories for Channel {
    fn inv(&self, addr: PeerId, inventories: Vec<Inventory>) {
        self.message(addr, NetworkMessage::Inv(inventories));
    }

    fn getdata(&self, addr: PeerId, inventories: Vec<Inventory>) {
        self.message(addr, NetworkMessage::GetData(inventories));
    }

    fn tx(&self, addr: PeerId, tx: Transaction) {
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
