#![allow(dead_code)]
use std::net;
use std::ops::RangeInclusive;

use nakamoto_chain::block::Block;
use nakamoto_chain::filter::BlockFilter;

use nakamoto_common::block::tree::{self, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::network::Network;
use nakamoto_common::nonempty::NonEmpty;

use nakamoto_p2p::bitcoin::network::constants::ServiceFlags;
use nakamoto_p2p::bitcoin::network::message::NetworkMessage;
use nakamoto_p2p::bitcoin::network::Address;
use nakamoto_p2p::event::Event;
use nakamoto_p2p::protocol::Command;
use nakamoto_p2p::protocol::Link;
use nakamoto_p2p::protocol::Peer;

use crate::client::chan;
use crate::handle::{self, Handle};

pub struct Client {
    // Used by tests.
    pub network: Network,
    pub events: chan::Sender<Event>,
    pub blocks: chan::Sender<(Block, Height)>,
    pub filters: chan::Sender<(BlockFilter, BlockHash, Height)>,
    pub commands: chan::Receiver<Command>,

    // Used in handle.
    events_: chan::Receiver<Event>,
    blocks_: chan::Receiver<(Block, Height)>,
    filters_: chan::Receiver<(BlockFilter, BlockHash, Height)>,
    commands_: chan::Sender<Command>,
}

impl Client {
    pub fn new(network: Network) -> Self {
        Self {
            network,
            ..Self::default()
        }
    }

    pub fn handle(&self) -> TestHandle {
        TestHandle {
            tip: (0, self.network.genesis()),
            network: self.network,
            events: self.events_.clone(),
            blocks: self.blocks_.clone(),
            filters: self.filters_.clone(),
            commands: self.commands_.clone(),
        }
    }
}

impl Default for Client {
    fn default() -> Self {
        let (events, events_) = chan::unbounded();
        let (blocks, blocks_) = chan::unbounded();
        let (filters, filters_) = chan::unbounded();
        let (commands_, commands) = chan::unbounded();

        Self {
            network: Network::default(),
            events,
            events_,
            blocks,
            blocks_,
            filters,
            filters_,
            commands,
            commands_,
        }
    }
}

#[derive(Clone)]
pub struct TestHandle {
    pub tip: (Height, BlockHeader),

    network: Network,
    events: chan::Receiver<Event>,
    blocks: chan::Receiver<(Block, Height)>,
    filters: chan::Receiver<(BlockFilter, BlockHash, Height)>,
    commands: chan::Sender<Command>,
}

impl Handle for TestHandle {
    fn network(&self) -> Network {
        self.network
    }

    fn get_tip(&self) -> Result<(Height, BlockHeader), handle::Error> {
        Ok(self.tip)
    }

    fn get_block(&self, hash: &BlockHash) -> Result<(), handle::Error> {
        self.command(Command::GetBlock(*hash))?;

        Ok(())
    }

    fn get_filters(&self, range: RangeInclusive<Height>) -> Result<(), handle::Error> {
        let (transmit, receive) = chan::bounded(1);
        self.command(Command::GetFilters(range, transmit))?;

        receive.recv()?.map_err(handle::Error::GetFilters)
    }

    fn blocks(&self) -> chan::Receiver<(Block, Height)> {
        self.blocks.clone()
    }

    fn filters(&self) -> chan::Receiver<(BlockFilter, BlockHash, Height)> {
        self.filters.clone()
    }

    fn command(&self, cmd: Command) -> Result<(), handle::Error> {
        log::debug!("Sending {:?}", cmd);
        self.commands.send(cmd).map_err(handle::Error::from)
    }

    fn broadcast(
        &self,
        _msg: NetworkMessage,
        _predicate: fn(Peer) -> bool,
    ) -> Result<Vec<net::SocketAddr>, handle::Error> {
        unimplemented!()
    }

    fn query(&self, _msg: NetworkMessage) -> Result<Option<net::SocketAddr>, handle::Error> {
        unimplemented!()
    }

    fn connect(&self, _addr: net::SocketAddr) -> Result<Link, handle::Error> {
        unimplemented!()
    }

    fn disconnect(&self, _addr: net::SocketAddr) -> Result<(), handle::Error> {
        unimplemented!()
    }

    fn import_headers(
        &self,
        _headers: Vec<BlockHeader>,
    ) -> Result<Result<ImportResult, tree::Error>, handle::Error> {
        unimplemented!()
    }

    fn import_addresses(&self, _addrs: Vec<Address>) -> Result<(), handle::Error> {
        unimplemented!()
    }

    fn submit_transactions(
        &self,
        _txs: Vec<Transaction>,
    ) -> Result<NonEmpty<net::SocketAddr>, handle::Error> {
        unimplemented!()
    }

    fn wait<F, T>(&self, _f: F) -> Result<T, handle::Error>
    where
        F: FnMut(Event) -> Option<T>,
    {
        unimplemented!()
    }

    fn wait_for_peers(
        &self,
        _count: usize,
        _required_services: impl Into<ServiceFlags>,
    ) -> Result<(), handle::Error> {
        unimplemented!()
    }

    fn wait_for_ready(&self) -> Result<(), handle::Error> {
        unimplemented!()
    }

    fn wait_for_height(&self, _h: Height) -> Result<BlockHash, handle::Error> {
        unimplemented!()
    }

    fn events(&self) -> chan::Receiver<Event> {
        self.events.clone()
    }

    fn shutdown(self) -> Result<(), handle::Error> {
        self.command(Command::Shutdown)
    }
}
