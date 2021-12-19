//! Protocol output capabilities.
//!
//! See [`Outbox`] type.
//!
//! Each sub-protocol, eg. the "ping" or "handshake" protocols are given a copy of this outbox
//! with specific capabilities, eg. peer disconnection, message sending etc. to
//! communicate with the network.
use log::*;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::{fmt, io, net};

pub use crossbeam_channel as chan;

use nakamoto_common::bitcoin::consensus::encode;
use nakamoto_common::bitcoin::network::address::Address;
use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::bitcoin::network::message_blockdata::{GetHeadersMessage, Inventory};
use nakamoto_common::bitcoin::network::message_filter::{
    CFHeaders, CFilter, GetCFHeaders, GetCFilters,
};
use nakamoto_common::bitcoin::network::message_network::VersionMessage;
use nakamoto_common::bitcoin::{Block, Transaction};

use nakamoto_common::block::time::LocalDuration;
use nakamoto_common::block::{BlockHash, BlockHeader, BlockTime, Height};

use crate::protocol::{Event, PeerId};

use super::executor::Executor;
use super::executor::Request;
use super::invmgr::Inventories;
use super::network::Network;
use super::{addrmgr, cbfmgr, invmgr, peermgr, pingmgr, syncmgr, Locators};

/// Output of a state transition of the `Protocol` state machine.
#[derive(Debug)]
pub enum Io {
    /// There are some bytes ready to be sent to a peer.
    Write(PeerId),
    /// Connect to a peer.
    Connect(PeerId),
    /// Disconnect from a peer.
    Disconnect(PeerId, DisconnectReason),
    /// Ask for a wakeup in a specified amount of time.
    Wakeup(LocalDuration),
    /// Emit an event.
    Event(Event),
}

impl From<Event> for Io {
    fn from(event: Event) -> Self {
        Io::Event(event)
    }
}

/// Disconnect reason.
#[derive(Debug, Clone)]
pub enum DisconnectReason {
    /// Peer is misbehaving.
    PeerMisbehaving(&'static str),
    /// Peer protocol version is too old or too recent.
    PeerProtocolVersion(u32),
    /// Peer doesn't have the required services.
    PeerServices(ServiceFlags),
    /// Peer chain is too far behind.
    PeerHeight(Height),
    /// Peer magic is invalid.
    PeerMagic(u32),
    /// Peer timed out.
    PeerTimeout(&'static str),
    /// Peer was dropped by all sub-protocols.
    PeerDropped,
    /// Connection to self was detected.
    SelfConnection,
    /// Inbound connection limit reached.
    ConnectionLimit,
    /// Error with the underlying connection.
    ConnectionError(Arc<std::io::Error>),
    /// Error trying to decode incoming message.
    DecodeError(Arc<encode::Error>),
    /// Peer was forced to disconnect by external command.
    Command,
    /// Peer was disconnected for another reason.
    Other(&'static str),
}

impl DisconnectReason {
    /// Check whether the disconnect reason is transient, ie. may no longer be applicable
    /// after some time.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::ConnectionLimit
                | Self::PeerTimeout(_)
                | Self::PeerHeight(_)
                | Self::ConnectionError(_)
        )
    }
}

impl fmt::Display for DisconnectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerMisbehaving(reason) => write!(f, "peer misbehaving: {}", reason),
            Self::PeerProtocolVersion(_) => write!(f, "peer protocol version mismatch"),
            Self::PeerServices(_) => write!(f, "peer doesn't have the required services"),
            Self::PeerHeight(_) => write!(f, "peer is too far behind"),
            Self::PeerMagic(magic) => write!(f, "received message with invalid magic: {}", magic),
            Self::PeerTimeout(s) => write!(f, "peer timed out: {:?}", s),
            Self::PeerDropped => write!(f, "peer dropped"),
            Self::SelfConnection => write!(f, "detected self-connection"),
            Self::ConnectionLimit => write!(f, "inbound connection limit reached"),
            Self::ConnectionError(err) => write!(f, "connection error: {}", err),
            Self::DecodeError(err) => write!(f, "message decode error: {}", err),
            Self::Command => write!(f, "received external command"),
            Self::Other(reason) => write!(f, "{}", reason),
        }
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
    /// Futures executor.
    executor: Executor,
    /// Output queue.
    outbound: Rc<RefCell<VecDeque<Io>>>,
    /// Message outbox.
    outbox: Rc<RefCell<HashMap<PeerId, Vec<u8>>>>,
    /// Block requests to the network.
    block_requests: Rc<RefCell<HashMap<BlockHash, Request<Block>>>>,
    /// Network message builder.
    builder: message::Builder,
    /// Log target.
    target: &'static str,
}

impl Outbox {
    /// Create a new channel.
    pub fn new(network: Network, version: u32, target: &'static str) -> Self {
        Self {
            version,
            executor: Executor::new(),
            outbound: Rc::new(RefCell::new(VecDeque::new())),
            outbox: Rc::new(RefCell::new(HashMap::new())),
            block_requests: Rc::new(RefCell::new(HashMap::new())),
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

impl Outbox {
    /// A block was received from the network.
    pub fn block_received(&mut self, blk: Block) {
        let block_hash = blk.block_hash();

        if let Some(mut req) = self.block_requests.borrow_mut().remove(&block_hash) {
            req.complete(Ok(blk.clone()));
        }
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

impl Disconnect for Outbox {
    fn disconnect(&self, addr: net::SocketAddr, reason: DisconnectReason) {
        debug!(target: self.target, "{}: Disconnecting: {}", addr, reason);
        self.push(Io::Disconnect(addr, reason));
    }
}

impl Disconnect for () {
    fn disconnect(&self, _addr: net::SocketAddr, _reason: DisconnectReason) {}
}

pub trait Blocks {
    fn get_block(&mut self, hash: BlockHash, peer: &PeerId) -> Request<Block>;
}

impl Blocks for Outbox {
    /// Fetch a block from a peer.
    fn get_block(&mut self, hash: BlockHash, addr: &PeerId) -> Request<Block> {
        use std::collections::hash_map::Entry;

        let request = Request::new();

        match self.block_requests.borrow_mut().entry(hash) {
            Entry::Vacant(e) => {
                e.insert(request.clone());
            }
            Entry::Occupied(e) => {
                return e.get().clone();
            }
        }
        self.getdata(*addr, vec![Inventory::Block(hash)]);

        request
    }
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

impl addrmgr::Events for Outbox {
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
        self.event(Event::Address(event));
    }
}

impl peermgr::Events for Outbox {
    fn event(&self, event: peermgr::Event) {
        info!(target: self.target, "[peer] {}", &event);
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
        debug!(target: self.target, "[sync] {}", &event);

        match &event {
            syncmgr::Event::Synced(tip, height) => {
                info!(target: self.target, "Block height = {}, tip = {}", height, tip);
            }
            _ => {}
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
        debug!(target: self.target, "[invmgr] {}", &event);
        self.event(Event::Inventory(event));
    }
}

impl cbfmgr::Events for Outbox {
    fn event(&self, event: cbfmgr::Event) {
        debug!(target: self.target, "[spv] {}", &event);

        match event {
            cbfmgr::Event::FilterHeadersImported { height, .. } => {
                info!(target: self.target, "Filter height = {}", height);
            }
            _ => {}
        }

        self.event(Event::Filter(event));
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use nakamoto_common::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
    use nakamoto_test::assert_matches;

    pub fn messages(
        channel: &mut Outbox,
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

    #[test]
    fn test_get_block() {
        let network = Network::Mainnet;
        let remote: net::SocketAddr = ([88, 88, 88, 88], 8333).into();
        let mut outbox = Outbox::new(network, crate::protocol::PROTOCOL_VERSION, "test");

        outbox.executor.spawn({
            let mut outbox = outbox.clone();

            async move {
                outbox
                    .get_block(network.genesis_hash(), &remote)
                    .await
                    .unwrap();
            }
        });
        outbox.executor.spawn({
            let mut outbox = outbox.clone();

            async move {
                outbox
                    .get_block(network.genesis_hash(), &remote)
                    .await
                    .unwrap();
            }
        });
        assert!(outbox.executor.poll().is_pending());

        assert_matches!(
            messages(&mut outbox, &remote).next(),
            Some(NetworkMessage::GetData(_))
        );

        outbox.block_received(network.genesis_block());
        assert!(outbox.executor.poll().is_ready());
    }
}
