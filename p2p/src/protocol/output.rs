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
use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use nakamoto_common::bitcoin::network::message_filter::{
    CFHeaders, CFilter, GetCFHeaders, GetCFilters,
};
use nakamoto_common::bitcoin::network::message_network::VersionMessage;
use nakamoto_common::bitcoin::Transaction;
use nakamoto_common::block::time::LocalDuration;
use nakamoto_common::block::{BlockHash, BlockHeader, BlockTime, Height};

use crate::protocol::{Event, PeerId};

use super::network::Network;
use super::{addrmgr, cbfmgr, invmgr, peermgr, pingmgr, syncmgr, Locators};

/// Output of a state transition of the `Protocol` state machine.
pub type Io = nakamoto_net::Io<Event, super::DisconnectReason>;

impl From<Event> for Io {
    fn from(event: Event) -> Self {
        Io::Event(event)
    }
}

pub(crate) mod message {
    use nakamoto_common::bitcoin::consensus::Encodable;
    use nakamoto_common::bitcoin::network::message::RawNetworkMessage;

    use super::*;
    use std::io;

    #[derive(Debug, Clone)]
    pub struct Builder {
        magic: u32,
    }

    impl Builder {
        pub fn new(network: Network) -> Self {
            Builder {
                magic: network.magic(),
            }
        }

        pub fn write<W: io::Write>(
            &self,
            payload: NetworkMessage,
            writer: W,
        ) -> Result<usize, io::Error> {
            RawNetworkMessage {
                payload,
                magic: self.magic,
            }
            .consensus_encode(writer)
        }
    }
}

/// Holds protocol outputs and pending I/O.
#[derive(Debug, Clone)]
pub struct Outbox {
    /// Protocol version.
    version: u32,
    /// Output queue.
    outbound: Rc<RefCell<VecDeque<Io>>>,
    /// Network message builder.
    builder: message::Builder,
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
            outbound: Rc::new(RefCell::new(VecDeque::new())),
            builder: message::Builder::new(network),
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
    pub fn message(&mut self, addr: PeerId, message: NetworkMessage) -> &Self {
        debug!("Sending {:?} to {}", message.cmd(), addr);

        let mut buffer = Vec::new();
        // Nb. writing to a vector cannot result in an error.
        self.builder.write(message, &mut buffer).ok();
        self.push(Io::Write(addr, buffer));
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
    fn disconnect(&self, addr: net::SocketAddr, reason: super::DisconnectReason);
}

impl Disconnect for Outbox {
    fn disconnect(&self, addr: net::SocketAddr, reason: super::DisconnectReason) {
        debug!("{}: Disconnecting: {}", addr, reason);
        self.push(Io::Disconnect(addr, reason));
    }
}

impl Disconnect for () {
    fn disconnect(&self, _addr: net::SocketAddr, _reason: super::DisconnectReason) {}
}

/// The ability to be woken up in the future.
pub trait Wakeup {
    /// Ask to be woken up in a predefined amount of time.
    fn wakeup(&self, duration: LocalDuration) -> &Self;
}

impl Wakeup for Outbox {
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

impl addrmgr::SyncAddresses for Outbox {
    fn get_addresses(&mut self, addr: PeerId) {
        self.message(addr, NetworkMessage::GetAddr);
    }

    fn send_addresses(&mut self, addr: PeerId, addrs: Vec<(BlockTime, Address)>) {
        self.message(addr, NetworkMessage::Addr(addrs));
    }
}

impl peermgr::Connect for Outbox {
    fn connect(&self, addr: net::SocketAddr, timeout: LocalDuration) {
        info!("[conn] Connecting to {}..", addr);

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

impl addrmgr::Events for Outbox {
    fn event(&self, event: addrmgr::Event) {
        match &event {
            addrmgr::Event::Error(msg) => error!("[addr] {}", msg),
            event @ addrmgr::Event::AddressDiscovered(_, _) => {
                trace!("[addr] {}", &event);
            }
            event => {
                debug!("[addr] {}", &event);
            }
        }
        self.event(Event::Address(event));
    }
}

impl peermgr::Events for Outbox {
    fn event(&self, event: peermgr::Event) {
        info!("[peer] {}", &event);
        self.event(Event::Peer(event));
    }
}

impl pingmgr::Ping for Outbox {
    fn ping(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Ping(nonce));
        self
    }

    fn pong(&mut self, addr: net::SocketAddr, nonce: u64) -> &Self {
        self.message(addr, NetworkMessage::Pong(nonce));
        self
    }
}

impl syncmgr::SyncHeaders for Outbox {
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
        match &event {
            syncmgr::Event::Synced { .. } => {
                info!("[sync] {}", event);
            }
            _ => {
                debug!("[sync] {}", &event);
            }
        }
        self.event(Event::Chain(event));
    }
}

impl peermgr::Handshake for Outbox {
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

impl cbfmgr::SyncFilters for Outbox {
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

impl invmgr::Inventories for Outbox {
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
        debug!("[invmgr] {}", &event);
        self.event(Event::Inventory(event));
    }
}

impl cbfmgr::Events for Outbox {
    fn event(&self, event: cbfmgr::Event) {
        match event {
            cbfmgr::Event::FilterHeadersImported { .. } => {
                info!("[spv] {}", &event);
            }
            _ => {
                debug!("[spv] {}", &event);
            }
        }

        self.event(Event::Filter(event));
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use nakamoto_common::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};

    pub fn messages_from(
        outbox: &mut Outbox,
        addr: &net::SocketAddr,
    ) -> impl Iterator<Item = NetworkMessage> {
        let mut stream = crate::stream::Decoder::new(2048);
        let mut msgs = Vec::new();

        outbox.outbound.borrow_mut().retain(|o| match o {
            Io::Write(a, bytes) if a == addr => {
                stream.input(bytes);
                false
            }
            _ => true,
        });

        while let Some(msg) = stream.decode_next::<RawNetworkMessage>().unwrap() {
            msgs.push(msg.payload);
        }
        msgs.into_iter()
    }

    pub fn messages(
        outbox: &mut Outbox,
    ) -> impl Iterator<Item = (net::SocketAddr, NetworkMessage)> {
        let mut stream = crate::stream::Decoder::new(2048);
        let mut msgs = Vec::new();

        outbox.outbound.borrow_mut().retain(|o| match o {
            Io::Write(addr, bytes) => {
                stream.input(bytes);

                while let Some(msg) = stream.decode_next::<RawNetworkMessage>().unwrap() {
                    msgs.push((*addr, msg.payload));
                }
                false
            }
            _ => true,
        });
        msgs.into_iter()
    }

    pub fn events(outbox: &mut Outbox) -> impl Iterator<Item = Event> {
        let mut events = Vec::new();

        outbox.outbound.borrow_mut().retain(|o| match o {
            Io::Event(e) => {
                events.push(e.clone());
                false
            }
            _ => true,
        });
        events.into_iter()
    }
}
