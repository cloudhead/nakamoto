use std::collections::HashMap;
use std::net;
use std::ops::RangeInclusive;

use nakamoto_chain::block::Block;
use nakamoto_chain::filter::BlockFilter;

use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::network::message::NetworkMessage;
use nakamoto_common::bitcoin::network::Address;
use nakamoto_common::block::filter::FilterHeader;
use nakamoto_common::block::store::Genesis as _;
use nakamoto_common::block::time::{AdjustedTime, LocalTime};
use nakamoto_common::block::tree::{self, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::network::Network;
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_common::p2p::peer::KnownAddress;
use nakamoto_p2p::event;
use nakamoto_test::block::cache::model;

use nakamoto_p2p::protocol;
use nakamoto_p2p::protocol::Command;
use nakamoto_p2p::protocol::Link;
use nakamoto_p2p::protocol::Peer;
use nakamoto_p2p::protocol::Protocol;

use crate::client::{chan, Event};
use crate::handle::{self, Handle};
use crate::spv;

pub struct Client {
    // Used by tests.
    pub network: Network,
    pub events: chan::Sender<protocol::Event>,
    pub blocks: chan::Sender<(Block, Height)>,
    pub filters: chan::Sender<(BlockFilter, BlockHash, Height)>,
    pub subscriber: event::Broadcast<protocol::Event, Event>,
    pub commands: chan::Receiver<Command>,
    pub protocol: Protocol<model::Cache, model::FilterCache, HashMap<net::IpAddr, KnownAddress>>,
    pub outputs: chan::Receiver<protocol::Out>,

    // Used in handle.
    events_: chan::Receiver<protocol::Event>,
    blocks_: chan::Receiver<(Block, Height)>,
    filters_: chan::Receiver<(BlockFilter, BlockHash, Height)>,
    subscriber_: event::Subscriber<Event>,
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
            subscriber: self.subscriber_.clone(),
            commands: self.commands_.clone(),
        }
    }

    pub fn step(&mut self) -> Vec<protocol::Out> {
        let mut outputs = Vec::new();

        for out in self.outputs.try_iter() {
            match out {
                protocol::Out::Event(event) => {
                    self.subscriber.broadcast(event.clone());
                    self.events.send(event).ok();
                }
                _ => outputs.push(out),
            }
        }
        outputs
    }
}

impl Default for Client {
    fn default() -> Self {
        let (events, events_) = chan::unbounded();
        let (blocks, blocks_) = chan::unbounded();
        let (filters, filters_) = chan::unbounded();
        let (commands_, commands) = chan::unbounded();
        let (outputs_, outputs) = chan::unbounded();
        let mut mapper = spv::Mapper::new();
        let (subscriber, subscriber_) = event::broadcast(move |e, p| mapper.process(e, p));
        let network = Network::default();
        let protocol = {
            let tree = model::Cache::new(network.genesis());
            let cfilters = model::FilterCache::new(FilterHeader::genesis(network));
            let peers = HashMap::new();
            let time = LocalTime::now();
            let clock = AdjustedTime::new(time);
            let rng = fastrand::Rng::new();
            let cfg = protocol::Config::default();

            Protocol::new(tree, cfilters, peers, clock, rng, cfg, outputs_)
        };

        Self {
            network,
            protocol,
            outputs,
            events,
            events_,
            blocks,
            blocks_,
            filters,
            filters_,
            subscriber,
            subscriber_,
            commands,
            commands_,
        }
    }
}

#[derive(Clone)]
pub struct TestHandle {
    pub tip: (Height, BlockHeader),

    network: Network,
    events: chan::Receiver<protocol::Event>,
    blocks: chan::Receiver<(Block, Height)>,
    filters: chan::Receiver<(BlockFilter, BlockHash, Height)>,
    subscriber: event::Subscriber<Event>,
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

    fn find_branch(
        &self,
        _to: &BlockHash,
    ) -> Result<Option<(Height, NonEmpty<BlockHeader>)>, handle::Error> {
        unimplemented!()
    }

    fn blocks(&self) -> chan::Receiver<(Block, Height)> {
        self.blocks.clone()
    }

    fn filters(&self) -> chan::Receiver<(BlockFilter, BlockHash, Height)> {
        self.filters.clone()
    }

    fn subscribe(&self) -> chan::Receiver<Event> {
        self.subscriber.subscribe()
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

    fn query_tree(
        &self,
        _query: impl Fn(&dyn nakamoto_chain::BlockReader) + Send + Sync + 'static,
    ) -> Result<(), handle::Error> {
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

    fn submit_transaction(
        &self,
        _tx: Transaction,
    ) -> Result<NonEmpty<net::SocketAddr>, handle::Error> {
        unimplemented!()
    }

    fn wait<F, T>(&self, _f: F) -> Result<T, handle::Error>
    where
        F: FnMut(protocol::Event) -> Option<T>,
    {
        unimplemented!()
    }

    fn wait_for_peers(
        &self,
        _count: usize,
        _required_services: impl Into<ServiceFlags>,
    ) -> Result<Vec<(net::SocketAddr, Height, ServiceFlags)>, handle::Error> {
        unimplemented!()
    }

    fn wait_for_height(&self, _h: Height) -> Result<BlockHash, handle::Error> {
        unimplemented!()
    }

    fn events(&self) -> chan::Receiver<protocol::Event> {
        self.events.clone()
    }

    fn shutdown(self) -> Result<(), handle::Error> {
        Ok(())
    }
}
