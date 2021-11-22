use std::ops::{Deref, DerefMut};

use super::*;

use nakamoto_common::bitcoin::consensus::Params;
use nakamoto_common::bitcoin::network::message_network::VersionMessage;
use nakamoto_common::bitcoin::network::Address;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_common::block::filter::{FilterHash, FilterHeader};
use nakamoto_common::block::store::Genesis;
use nakamoto_common::block::BlockHeader;
use nakamoto_common::collections::{HashMap, HashSet};
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_common::p2p::peer::KnownAddress;

use nakamoto_test::block::cache::model;

pub struct PeerDummy {
    pub addr: PeerId,
    pub height: Height,
    pub services: ServiceFlags,
    pub protocol_version: u32,
    pub relay: bool,
    pub time: LocalTime,
}

impl PeerDummy {
    pub fn new(
        ip: impl Into<net::IpAddr>,
        network: Network,
        height: Height,
        services: ServiceFlags,
    ) -> Self {
        let addr = (ip.into(), network.port()).into();
        let time = LocalTime::from_secs(network.genesis().time as u64)
            + LocalDuration::BLOCK_INTERVAL * height;

        Self {
            addr,
            height,
            services,
            relay: false,
            protocol_version: PROTOCOL_VERSION,
            time,
        }
    }

    pub fn version(&self, remote: PeerId, nonce: u64) -> VersionMessage {
        VersionMessage {
            version: self.protocol_version,
            services: self.services,
            timestamp: self.time.block_time() as i64,
            receiver: Address::new(&remote, ServiceFlags::NONE),
            sender: Address::new(&self.addr, ServiceFlags::NONE),
            nonce,
            user_agent: USER_AGENT.to_owned(),
            start_height: self.height as i32,
            relay: self.relay,
        }
    }
}

#[derive(Debug)]
pub struct Peer<P> {
    pub protocol: P,
    pub addr: PeerId,
    pub cfg: Config,

    time: LocalTime,
    initialized: bool,
}

impl<P> Deref for Peer<P> {
    type Target = P;

    fn deref(&self) -> &P {
        &self.protocol
    }
}

impl<P> DerefMut for Peer<P> {
    fn deref_mut(&mut self) -> &mut P {
        &mut self.protocol
    }
}

impl Peer<Protocol> {
    pub fn new(
        name: &'static str,
        ip: impl Into<net::IpAddr>,
        network: Network,
        headers: Vec<BlockHeader>,
        cfheaders: Vec<(FilterHash, FilterHeader)>,
        peers: Vec<(net::SocketAddr, Source, ServiceFlags)>,
        rng: fastrand::Rng,
    ) -> Self {
        let cfg = Config {
            network,
            params: Params::new(network.into()),
            target: name,
            // We don't actually have the required services, but we pretend to
            // for testing purposes.
            services: syncmgr::REQUIRED_SERVICES | cbfmgr::REQUIRED_SERVICES,
            ..Config::default()
        };
        Self::config(ip, headers, cfheaders, peers, cfg, rng)
    }

    pub fn genesis(
        name: &'static str,
        ip: impl Into<net::IpAddr>,
        network: Network,
        peers: Vec<(net::SocketAddr, Source, ServiceFlags)>,
        rng: fastrand::Rng,
    ) -> Self {
        Self::new(name, ip, network, vec![], vec![], peers, rng)
    }

    pub fn config(
        ip: impl Into<net::IpAddr>,
        headers: Vec<BlockHeader>,
        cfheaders: Vec<(FilterHash, FilterHeader)>,
        peers: Vec<(net::SocketAddr, Source, ServiceFlags)>,
        cfg: Config,
        rng: fastrand::Rng,
    ) -> Self {
        let network = cfg.network;
        let genesis = network.genesis();
        let time = LocalTime::from_secs(genesis.time as u64);
        let clock = AdjustedTime::new(time);
        let headers = NonEmpty::from((network.genesis(), headers));
        let cfheaders = NonEmpty::from((
            (FilterHash::genesis(network), FilterHeader::genesis(network)),
            cfheaders,
        ));
        let peers = peers
            .into_iter()
            .map(|(addr, src, srvs)| {
                (
                    addr.ip(),
                    KnownAddress::new(Address::new(&addr, srvs), src, None),
                )
            })
            .collect();

        let store = store::Memory::new(headers);
        let tree = BlockCache::from(store, cfg.params.clone(), &[]).unwrap();
        let filters = model::FilterCache::from(cfheaders);

        let addr = (ip.into(), network.port()).into();
        let protocol = Protocol::new(tree, filters, peers, clock, rng, cfg.clone());

        Self {
            protocol,
            time,
            addr,
            initialized: false,
            cfg,
        }
    }

    pub fn initialize(&mut self) {
        if !self.initialized {
            info!(target: self.cfg.target, "Initializing: address = {}", self.addr);

            self.initialized = true;
            self.protocol.initialize(self.time);
        }
    }

    pub fn tick(&mut self, local_time: LocalTime) {
        self.time = local_time;
        self.protocol.tick(local_time);
    }

    pub fn local_time(&self) -> LocalTime {
        self.protocol.clock.local_time()
    }

    pub fn connect_addr(&mut self, addr: &PeerId, link: Link) {
        self.connect(
            &PeerDummy {
                addr: *addr,
                height: 144,
                protocol_version: self.protocol.protocol_version,
                services: cbfmgr::REQUIRED_SERVICES | syncmgr::REQUIRED_SERVICES,
                relay: true,
                time: self.local_time(),
            },
            link,
        );
    }

    pub fn elapse(&mut self, duration: LocalDuration) {
        self.time.elapse(duration);
        self.protocol.tock(self.time);
    }

    pub fn tock(&mut self) {
        self.protocol.tock(self.time);
    }

    pub fn outputs(&mut self) -> impl Iterator<Item = Io> + '_ {
        self.protocol.drain()
    }

    pub fn messages(
        &mut self,
        addr: &net::SocketAddr,
    ) -> impl Iterator<Item = NetworkMessage> + '_ {
        crate::protocol::output::test::messages(&mut self.protocol.outbox, addr)
    }

    pub fn events(&mut self) -> impl Iterator<Item = Event> + '_ {
        self.protocol.drain().filter_map(|o| match o {
            Io::Event(e) => Some(e),
            _ => None,
        })
    }

    pub fn received(&mut self, remote: net::SocketAddr, payload: NetworkMessage) {
        let msg = message::Builder::new(self.protocol.network);

        let mut buf = Vec::new();
        msg.write(payload, &mut buf).unwrap();

        self.protocol.received_bytes(&remote, &buf);
    }

    pub fn drain(&mut self) {
        self.protocol.drain().for_each(drop);
    }

    pub fn connect(&mut self, remote: &PeerDummy, link: Link) {
        self.initialize();

        let local = self.addr;
        let rng = self.protocol.rng.clone();
        let time = self.local_time();

        if link.is_outbound() {
            self.protocol.peermgr.connect(&remote.addr, time);
        }

        // Initiate connection.
        self.protocol.connected(remote.addr, &local, link);

        // Receive `version`.
        self.received(
            remote.addr,
            NetworkMessage::Version(remote.version(local, rng.u64(..))),
        );

        {
            let mut messages = self.messages(&remote.addr);

            // Expect `version` to be sent in response.
            messages
                .find(|m| matches!(m, NetworkMessage::Version(_)))
                .expect("`version` should be sent");

            // Expect `verack`.
            messages
                .find(|m| matches!(m, NetworkMessage::Verack))
                .expect("`verack` should be sent");
        }

        // Receive `verack`.
        self.received(remote.addr, NetworkMessage::Verack);

        // Expect hanshake event.
        self.protocol
            .drain()
            .find(|o| {
                matches!(
                    o,
                    Io::Event(
                        Event::PeerManager(peermgr::Event::Negotiated { addr, services, .. })
                    ) if addr == &remote.addr && services.has(ServiceFlags::NETWORK)
                )
            })
            .expect("peer handshake is successful");
    }
}

/// Create a network of nodes of the given size.
/// Populates their respective address books so that they can connect with each other on startup.
pub fn network(network: Network, size: usize, rng: fastrand::Rng) -> Vec<Peer<Protocol>> {
    assert!(size <= 16);

    let mut addrs = HashSet::with_hasher(rng.clone().into());
    let names = [
        "peer#0", "peer#1", "peer#2", "peer#3", "peer#4", "peer#5", "peer#6", "peer#7", "peer#8",
        "peer#9", "peer#A", "peer#B", "peer#C", "peer#D", "peer#E", "peer#F",
    ];
    let reserved = [[88, 88, 88, 88], [44, 44, 44, 44], [48, 48, 48, 48]];

    while addrs.len() < size {
        let ip = [rng.u8(..), rng.u8(..), rng.u8(..), rng.u8(..)];

        if reserved.contains(&ip) {
            continue;
        }
        let addr: net::SocketAddr = (ip, network.port()).into();

        if !addrmgr::is_routable(&addr.ip()) {
            continue;
        }
        addrs.insert(addr);
    }

    let addresses = addrs
        .into_iter()
        .map(|a| {
            (
                a,
                Source::Dns,
                cbfmgr::REQUIRED_SERVICES | syncmgr::REQUIRED_SERVICES,
            )
        })
        .collect::<Vec<_>>();

    // Populate address books.
    let mut address_books = HashMap::with_hasher(rng.clone().into());
    for (i, (local, _, _)) in addresses.iter().enumerate() {
        for remote in addresses.iter().skip(i + 1) {
            address_books
                .entry(*local)
                .and_modify(|addrs: &mut Vec<_>| addrs.push(*remote))
                .or_insert_with(|| vec![*remote]);
        }
    }

    addresses
        .iter()
        .enumerate()
        .map(|(i, (addr, _, _))| {
            let peers = address_books.get(addr).unwrap_or(&Vec::new()).clone();
            let cfg = Config {
                network,
                target: names[i],
                // These nodes don't need to try connecting to other nodes.
                target_outbound_peers: 0,
                // These are full nodes.
                services: syncmgr::REQUIRED_SERVICES | cbfmgr::REQUIRED_SERVICES,
                ..Config::default()
            };
            Peer::config(addr.ip(), vec![], vec![], peers, cfg, rng.clone())
        })
        .collect::<Vec<_>>()
}
