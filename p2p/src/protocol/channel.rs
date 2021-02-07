//! A channel for communicating between the main protocol and the sub-protocols.
//!
//! Each sub-protocol, eg. the "ping" or "handshake" protocols are given a channel
//! with specific capabilities, eg. peer disconnection, message sending etc. to
//! communicate with the main protocol and network.
use log::*;
use std::net;

use crossbeam_channel as chan;

use bitcoin::network::address::Address;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::GetHeadersMessage;
use bitcoin::network::message_filter::{CFHeaders, CFilter, GetCFHeaders, GetCFilters};
use bitcoin::network::message_network::VersionMessage;

use nakamoto_common::block::time::LocalDuration;
use nakamoto_common::block::tree::ImportResult;
use nakamoto_common::block::{BlockHash, BlockHeader, BlockTime, Height};

use crate::protocol::{DisconnectReason, Event, Out, PeerId};

use super::network::Network;
use super::{addrmgr, connmgr, message, peermgr, pingmgr, spvmgr, syncmgr, Link, Locators};

/// Used to construct a protocol output.
#[derive(Debug, Clone)]
pub struct Channel {
    /// Protocol version.
    version: u32,
    /// Output channel.
    outbound: chan::Sender<Out>,
    /// Network magic number.
    builder: message::Builder,
    /// Log target.
    target: &'static str,
}

impl Channel {
    /// Create a new channel.
    pub fn new(
        network: Network,
        version: u32,
        target: &'static str,
        outbound: chan::Sender<Out>,
    ) -> Self {
        Self {
            version,
            outbound,
            builder: message::Builder::new(network),
            target,
        }
    }

    /// Push an output to the channel.
    pub fn push(&self, output: Out) {
        self.outbound.send(output).unwrap();
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

/// Ability to disconnect from peers.
pub trait Disconnect {
    /// Disconnect from peer.
    fn disconnect(&self, addr: net::SocketAddr, reason: DisconnectReason);
}

impl Disconnect for Channel {
    fn disconnect(&self, addr: net::SocketAddr, reason: DisconnectReason) {
        info!(target: self.target, "{}: Disconnecting: {}", addr, reason);
        self.push(Out::Disconnect(addr, reason));
    }
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

impl addrmgr::SyncAddresses for Channel {
    fn get_addresses(&self, addr: PeerId) {
        self.message(addr, NetworkMessage::GetAddr);
    }

    fn send_addresses(&self, addr: PeerId, addrs: Vec<(BlockTime, Address)>) {
        self.message(addr, NetworkMessage::Addr(addrs));
    }
}

impl connmgr::Connect for Channel {
    fn connect(&self, addr: net::SocketAddr, timeout: LocalDuration) {
        debug!(target: self.target, "[conn] {}: Connecting..", addr);
        self.push(Out::Connect(addr, timeout));
    }
}

impl connmgr::Events for Channel {
    fn event(&self, event: connmgr::Event) {
        match event {
            connmgr::Event::Connected(_, Link::Outbound) => {
                info!(target: self.target, "{}", &event)
            }
            _ => {
                debug!(target: self.target, "[conn] {}", &event);
            }
        }
        self.event(Event::ConnManager(event));
    }
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
            syncmgr::Event::HeadersImported(import_result) => {
                if let ImportResult::TipChanged(tip, height, _) = import_result {
                    info!(target: self.target, "Block height = {}, tip = {}", height, tip);
                }
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
}

#[allow(unused_variables)]
impl spvmgr::SyncFilters for Channel {
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
    }

    fn send_cfilter(&self, addr: PeerId, cfilter: CFilter) {
        todo!()
    }
}

impl spvmgr::Events for Channel {
    fn event(&self, event: spvmgr::Event) {
        debug!(target: self.target, "[spv] {}", &event);

        match event {
            spvmgr::Event::FilterHeadersImported { height, .. } => {
                info!(target: self.target, "Filter height = {}", height);
            }
            _ => {}
        }

        self.event(Event::SpvManager(event));
    }
}
