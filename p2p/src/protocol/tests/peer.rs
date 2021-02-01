use super::*;

use bitcoin::network::message_network::VersionMessage;
use bitcoin::network::Address;

use std::collections::HashMap;

use nonempty::NonEmpty;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_common::block::filter::{self, FilterHash, FilterHeader};
use nakamoto_common::block::store::Genesis;
use nakamoto_common::block::BlockHeader;
use nakamoto_common::p2p::peer::KnownAddress;

use nakamoto_test::block::cache::model;

use crate::protocol::{Builder, Protocol};

pub struct Peer {
    pub protocol: Protocol<
        BlockCache<store::Memory<BlockHeader>>,
        model::FilterCache,
        HashMap<net::IpAddr, KnownAddress>,
    >,
    pub upstream: chan::Receiver<Out>,
    pub time: LocalTime,
    pub addr: PeerId,

    initialized: bool,
}

impl Peer {
    pub fn new(
        name: &'static str,
        ip: impl Into<net::IpAddr>,
        network: Network,
        headers: Vec<BlockHeader>,
        cfheaders: Vec<(FilterHash, FilterHeader)>,
        rng: fastrand::Rng,
    ) -> Self {
        let cfg = Config {
            network,
            target: name,
            // We don't actually have the required services, but we pretend to
            // for testing purposes.
            services: syncmgr::REQUIRED_SERVICES | spvmgr::REQUIRED_SERVICES,
            ..Config::default()
        };
        Self::config(ip, headers, cfheaders, cfg, rng)
    }

    pub fn genesis(
        name: &'static str,
        ip: impl Into<net::IpAddr>,
        network: Network,
        rng: fastrand::Rng,
    ) -> Self {
        Self::new(name, ip, network, vec![], vec![], rng)
    }

    pub fn config(
        ip: impl Into<net::IpAddr>,
        headers: Vec<BlockHeader>,
        cfheaders: Vec<(FilterHash, FilterHeader)>,
        cfg: Config,
        rng: fastrand::Rng,
    ) -> Self {
        let network = cfg.network;
        let genesis = network.genesis();
        let time = LocalTime::from_secs(genesis.time as u64);
        let clock = AdjustedTime::new(time);
        let peers = HashMap::new();
        let headers = NonEmpty::from((network.genesis(), headers));
        let cfheaders = NonEmpty::from((
            (
                filter::genesis_hash(network),
                FilterHeader::genesis(network),
            ),
            cfheaders,
        ));

        let store = store::Memory::new(headers);
        let tree = BlockCache::from(store, cfg.params.clone(), &[]).unwrap();
        let filters = model::FilterCache::from(cfheaders);

        let peer = Builder {
            cache: tree,
            clock,
            filters,
            peers,
            rng,
            cfg,
        };
        let (tx, rx) = chan::unbounded();
        let addr = (ip.into(), network.port()).into();

        Self {
            protocol: peer.build(tx),
            upstream: rx,
            time,
            addr,
            initialized: false,
        }
    }

    pub fn step(&mut self, input: Input) {
        self.initialize();
        self.protocol.step(input, self.time)
    }

    pub fn initialize(&mut self) {
        if !self.initialized {
            self.initialized = true;
            self.protocol.initialize(self.time);
        }
    }

    pub fn version(&self, local: PeerId, remote: PeerId, nonce: u64) -> VersionMessage {
        VersionMessage {
            version: self.protocol.protocol_version,
            services: self.protocol.peermgr.config.required_services,
            timestamp: self.time.block_time() as i64,
            receiver: Address::new(&remote, ServiceFlags::NONE),
            sender: Address::new(&local, ServiceFlags::NONE),
            nonce,
            user_agent: USER_AGENT.to_owned(),
            start_height: 144,
            relay: false,
        }
    }

    pub fn connect(&mut self, remote: &PeerId, link: Link) {
        self.initialize();

        let local = self.addr;
        let msg = message::Builder::new(self.protocol.network);
        let rng = self.protocol.rng.clone();
        let time = self.time;

        // Initiate connection.
        self.protocol.step(
            Input::Connected {
                addr: *remote,
                local_addr: local,
                link,
            },
            time,
        );

        // Send `version`.
        self.protocol.step(
            Input::Received(
                *remote,
                msg.raw(NetworkMessage::Version(self.version(
                    local,
                    *remote,
                    rng.u64(..),
                ))),
            ),
            time,
        );

        // Expect `version`.
        self.upstream
            .try_iter()
            .find(|o| {
                matches!(
                    o,
                    Out::Message(
                        addr,
                        RawNetworkMessage {
                            payload: NetworkMessage::Version(_),
                            ..
                        },
                    ) if addr == remote
                )
            })
            .expect("`version` should be sent");

        // Expect `verack`.
        self.upstream
            .try_iter()
            .find(|o| {
                matches!(
                    o,
                    Out::Message(
                        addr,
                        RawNetworkMessage {
                            payload: NetworkMessage::Verack,
                            ..
                        },
                    ) if addr == remote
                )
            })
            .expect("`verack` should be sent");

        // Send `verack`.
        self.protocol.step(
            Input::Received(*remote, msg.raw(NetworkMessage::Verack)),
            time,
        );

        // Expect hanshake event.
        self.upstream
            .try_iter()
            .find(|o| {
                matches!(
                    o,
                    Out::Event(
                        Event::PeerManager(peermgr::Event::PeerNegotiated { addr })
                    ) if addr == remote
                )
            })
            .expect("peer handshake is successful");
    }
}
