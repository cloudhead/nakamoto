// TODO
#![allow(missing_docs)]
use log::*;
use std::net;

use crossbeam_channel as chan;

use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use bitcoin::network::message_blockdata::GetHeadersMessage;
use bitcoin::network::message_filter::{CFHeaders, CFilter};

use nakamoto_common::block::time::LocalDuration;
use nakamoto_common::block::tree::ImportResult;
use nakamoto_common::block::{BlockHash, BlockHeader, Height};

use crate::protocol::{Event, Out, PeerId};
use crate::protocol::{Message, TimeoutSource};

use super::network::Network;
use super::{addrmgr, connmgr, message, pingmgr, spvmgr, syncmgr, Locators};

pub use connmgr::Disconnect;

/// Used to construct a protocol output.
#[derive(Debug, Clone)]
pub struct Channel<M: Message> {
    /// Protocol version.
    version: u32,
    /// Output channel.
    outbound: chan::Sender<Out<M>>,
    /// Network magic number.
    builder: message::Builder,
    /// Log target.
    target: &'static str,
}

impl<M: Message> Channel<M> {
    /// Create a new output builder.
    pub fn new(
        network: Network,
        version: u32,
        target: &'static str,
        outbound: chan::Sender<Out<M>>,
    ) -> Self {
        Self {
            version,
            outbound,
            builder: message::Builder::new(network),
            target,
        }
    }

    /// Push an output to the queue.
    pub fn push(&self, output: Out<M>) {
        self.outbound.send(output).unwrap();
    }

    /// Push a message to the queue.
    pub fn message(&self, addr: PeerId, message: M::Payload) -> &Self {
        self.push(self.builder.message(addr, message));
        self
    }

    /// Set a timeout.
    pub fn set_timeout(&self, source: TimeoutSource, timeout: LocalDuration) -> &Self {
        self.push(Out::SetTimeout(source, timeout));
        self
    }

    /// Push an event to the queue.
    pub fn event(&self, event: Event<M::Payload>) {
        self.push(Out::Event(event));
    }
}

impl addrmgr::GetAddresses for Channel<RawNetworkMessage> {
    fn get_addresses(&self, addr: PeerId) {
        self.message(addr, NetworkMessage::GetAddr);
    }
}

impl connmgr::Connect for Channel<RawNetworkMessage> {
    fn connect(&self, addr: net::SocketAddr, timeout: LocalDuration) {
        debug!(target: self.target, "[conn] Connecting to {}..", addr);
        self.push(Out::Connect(addr, timeout));
    }
}

impl connmgr::Disconnect for Channel<RawNetworkMessage> {
    fn disconnect(&self, addr: net::SocketAddr) {
        debug!(target: self.target, "[conn] Disconnecting from {}..", addr);
        self.push(Out::Disconnect(addr));
    }
}

impl connmgr::Idle for Channel<RawNetworkMessage> {
    fn idle(&self, timeout: LocalDuration) {
        self.set_timeout(TimeoutSource::Connect, timeout);
    }
}

impl connmgr::Events for Channel<RawNetworkMessage> {
    fn event(&self, event: connmgr::Event) {
        debug!(target: self.target, "[conn] {}", &event);
        self.event(Event::ConnManager(event));
    }
}

impl addrmgr::Events for Channel<RawNetworkMessage> {
    fn event(&self, event: addrmgr::Event) {
        debug!(target: self.target, "[addr] {}", &event);
        self.event(Event::AddrManager(event));
    }
}

impl pingmgr::Ping for Channel<RawNetworkMessage> {
    fn ping(&self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Ping(nonce));
        self
    }

    fn pong(&self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Pong(nonce));
        self
    }

    fn set_timeout(&self, addr: net::SocketAddr, timeout: LocalDuration) -> &Self {
        self.set_timeout(TimeoutSource::Ping(addr), timeout);
        self
    }
}

impl syncmgr::Idle for Channel<RawNetworkMessage> {
    fn idle(&self, timeout: LocalDuration) {
        self.set_timeout(TimeoutSource::Synch(None), timeout);
    }
}

impl syncmgr::SyncHeaders for Channel<RawNetworkMessage> {
    fn get_headers(
        &self,
        addr: PeerId,
        (locator_hashes, stop_hash): Locators,
        timeout: LocalDuration,
    ) {
        let msg = NetworkMessage::GetHeaders(GetHeadersMessage {
            version: self.version,
            // Starting hashes, highest heights first.
            locator_hashes,
            // Using the zero hash means *fetch as many blocks as possible*.
            stop_hash,
        });

        self.message(addr, msg)
            .set_timeout(TimeoutSource::Synch(Some(addr)), timeout);
    }

    fn send_headers(&self, addr: PeerId, headers: Vec<BlockHeader>) {
        let msg = self.builder.message(addr, NetworkMessage::Headers(headers));

        self.push(msg);
    }

    fn event(&self, event: syncmgr::Event) {
        debug!(target: self.target, "[sync] {}", &event);

        match &event {
            syncmgr::Event::HeadersImported(import_result) => {
                debug!(target: self.target, "Import result: {:?}", &import_result);

                if let ImportResult::TipChanged(tip, height, _) = import_result {
                    info!(target: self.target, "Chain height = {}, tip = {}", height, tip);
                }
            }
            _ => {}
        }
        self.event(Event::SyncManager(event));
    }
}

#[allow(unused_variables)]
impl spvmgr::SyncFilters for Channel<RawNetworkMessage> {
    fn get_cfheaders(&self, start_height: Height, stop_hash: BlockHash, timeout: LocalDuration) {
        todo!()
    }

    fn send_cfheaders(&self, addr: PeerId, headers: CFHeaders) {
        todo!()
    }

    fn get_cfilters(&self, start_height: Height, stop_hash: BlockHash, timeout: LocalDuration) {
        todo!()
    }

    fn send_cfilter(&self, addr: PeerId, cfilter: CFilter) {
        todo!()
    }
}

impl spvmgr::Events for Channel<RawNetworkMessage> {
    fn event(&self, event: spvmgr::Event) {
        debug!(target: self.target, "[cflr] {}", &event);

        self.event(Event::SpvManager(event));
    }
}
