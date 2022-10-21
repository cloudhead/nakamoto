use std::borrow::Cow;
use std::collections::HashMap;
use std::net;
use std::ops::RangeInclusive;

use nakamoto_chain::block::Block;
use nakamoto_chain::filter::BlockFilter;

use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
use nakamoto_common::bitcoin::network::Address;
use nakamoto_common::block::filter::FilterHeader;
use nakamoto_common::block::store::Genesis as _;
use nakamoto_common::block::time::{AdjustedTime, LocalTime};
use nakamoto_common::block::tree::{self, ImportResult};
use nakamoto_common::block::{BlockHash, BlockHeader, Height, Transaction};
use nakamoto_common::network::Network;
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_common::p2p::peer::KnownAddress;
use nakamoto_test::block::cache::model;

use nakamoto_net::event;
use nakamoto_net::PeerProtocol as _;
use nakamoto_p2p::fsm;
use nakamoto_p2p::fsm::Command;
use nakamoto_p2p::fsm::ConnDirection;
use nakamoto_p2p::fsm::Peer;
use nakamoto_p2p::fsm::StateMachine;

use crate::client::{chan, Event, Loading};
use crate::handle::{self, Handle};
use crate::spv;

pub struct Client {
    // Used by tests.
    pub network: Network,
    pub events: chan::Sender<fsm::Event>,
    pub blocks: chan::Sender<(Block, Height)>,
    pub filters: chan::Sender<(BlockFilter, BlockHash, Height)>,
    pub subscriber: event::Broadcast<fsm::Event, Event>,
    pub commands: chan::Receiver<Command>,
    pub loading: event::Emitter<Loading>,
    pub protocol: StateMachine<
        model::Cache,
        model::FilterCache,
        HashMap<net::IpAddr, KnownAddress>,
        AdjustedTime<net::SocketAddr>,
    >,

    // Used in handle.
    events_: chan::Receiver<fsm::Event>,
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
            loading: self.loading.subscriber(),
            events: self.events_.clone(),
            blocks: self.blocks_.clone(),
            filters: self.filters_.clone(),
            subscriber: self.subscriber_.clone(),
            commands: self.commands_.clone(),
        }
    }

    pub fn received(&mut self, remote: &net::SocketAddr, payload: NetworkMessage) {
        let msg = RawNetworkMessage {
            magic: self.network.magic(),
            payload,
        };

        self.protocol.received(remote, Cow::Owned(msg));
    }

    pub fn step(&mut self) -> Vec<fsm::Io> {
        let mut outputs = Vec::new();

        for out in self.protocol.drain() {
            match out {
                fsm::Io::NotifySubscribers(event) => {
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
        let mut mapper = spv::Mapper::new();
        let (subscriber, subscriber_) = event::broadcast(move |e, p| mapper.process(e, p));
        let loading = event::Emitter::default();
        let network = Network::default();
        let protocol = {
            let tree = model::Cache::new(network.genesis());
            let cfilters = model::FilterCache::new(FilterHeader::genesis(network));
            let peers = HashMap::new();
            let time = LocalTime::now();
            let clock = AdjustedTime::new(time);
            let rng = fastrand::Rng::new();
            let cfg = fsm::Config::default();

            StateMachine::new(tree, cfilters, peers, clock, rng, cfg)
        };

        Self {
            network,
            protocol,
            loading,
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

    #[allow(dead_code)]
    network: Network,
    events: chan::Receiver<fsm::Event>,
    blocks: chan::Receiver<(Block, Height)>,
    filters: chan::Receiver<(BlockFilter, BlockHash, Height)>,
    loading: event::Subscriber<Loading>,
    subscriber: event::Subscriber<Event>,
    commands: chan::Sender<Command>,
}

impl Handle for TestHandle {
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

    fn loading(&self) -> chan::Receiver<Loading> {
        self.loading.subscribe()
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

    fn connect(&self, _addr: net::SocketAddr) -> Result<ConnDirection, handle::Error> {
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
        F: FnMut(fsm::Event) -> Option<T>,
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

    fn events(&self) -> chan::Receiver<fsm::Event> {
        self.events.clone()
    }

    fn shutdown(self) -> Result<(), handle::Error> {
        Ok(())
    }
}
