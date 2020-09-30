#![cfg(test)]
pub mod simulator;

use super::*;

use simulator::PeerConfig;

use bitcoin::consensus::params::Params;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin_hashes::hex::FromHex;

use std::collections::VecDeque;
use std::time::SystemTime;

use nonempty::NonEmpty;
use quickcheck_macros::quickcheck;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_common::block::store::Store;
use nakamoto_common::block::BlockHeader;

use nakamoto_test::block::cache::model;
use nakamoto_test::logger;
use nakamoto_test::BITCOIN_HEADERS;

use crate::protocol::bitcoin::{connmgr, pingmgr, Bitcoin, Builder};
use crate::protocol::ProtocolBuilder;

fn payload<M: Message>(o: &Out<M>) -> Option<(net::SocketAddr, &M::Payload)> {
    match o {
        Out::Message(a, m) => Some((*a, m.payload())),
        _ => None,
    }
}

mod setup {
    use super::*;

    lazy_static! {
        /// Test protocol config.
        pub static ref CONFIG: Config = Config {
            network: network::Network::Mainnet,
            params: Params::new(network::Network::Mainnet.into()),
            address_book: AddressBook::new(),
            // Pretend that we're a full-node, to fool connections
            // between instances of this protocol in tests.
            services: ServiceFlags::NETWORK,
            protocol_version: PROTOCOL_VERSION,
            target_outbound_peers: 8,
            max_inbound_peers: 8,
            user_agent: USER_AGENT,
            whitelist: Whitelist {
                addr: HashSet::new(),
                user_agent: vec![USER_AGENT.to_owned()].into_iter().collect(),
            },
            target: "self",
        };
    }

    pub fn singleton(
        network: Network,
    ) -> (
        Bitcoin<model::Cache>,
        chan::Receiver<Out<RawNetworkMessage>>,
        LocalTime,
    ) {
        use bitcoin::blockdata::constants;

        let genesis = constants::genesis_block(network.into()).header;
        let cache = model::Cache::new(genesis);
        let time = LocalTime::from_secs(genesis.time as u64);
        let clock = AdjustedTime::new(time);
        let (tx, rx) = chan::unbounded();

        (
            Builder {
                cache,
                clock,
                rng: fastrand::Rng::new(),
                cfg: CONFIG.clone(),
            }
            .build(tx),
            rx,
            time,
        )
    }

    pub fn pair(
        network: Network,
    ) -> (
        (
            Bitcoin<model::Cache>,
            PeerId,
            chan::Receiver<Out<RawNetworkMessage>>,
        ),
        (
            Bitcoin<model::Cache>,
            PeerId,
            chan::Receiver<Out<RawNetworkMessage>>,
        ),
        LocalTime,
    ) {
        use bitcoin::blockdata::constants;

        let genesis = constants::genesis_block(network.into()).header;
        let cache = model::Cache::new(genesis);
        let time = LocalTime::from_secs(genesis.time as u64);
        let clock = AdjustedTime::new(time);

        let builder = Builder {
            cache,
            clock,
            rng: fastrand::Rng::new(),
            cfg: CONFIG.clone(),
        };

        let (alice_tx, alice_rx) = chan::unbounded();
        let (bob_tx, bob_rx) = chan::unbounded();

        let mut alice = builder.clone().build(alice_tx);
        let mut bob = builder.build(bob_tx);

        let alice_addr = ([152, 168, 3, 33], 3333).into();
        let bob_addr = ([152, 168, 7, 77], 7777).into();

        simulator::handshake(
            &mut alice,
            alice_addr,
            alice_rx.clone(),
            &mut bob,
            bob_addr,
            bob_rx.clone(),
            time,
        );

        ((alice, alice_addr, alice_rx), (bob, bob_addr, bob_rx), time)
    }

    pub fn network(
        network: Network,
        rng: fastrand::Rng,
        mut cfgs: Vec<PeerConfig>,
        configure: fn(&mut Config),
    ) -> (Vec<(PeerId, Builder<model::Cache>)>, LocalTime) {
        use bitcoin::blockdata::constants;

        let genesis = constants::genesis_block(network.into()).header;
        let time = LocalTime::from_secs(genesis.time as u64);
        let clock = AdjustedTime::new(time);
        let size = cfgs.len();

        assert!(size > 0);

        let mut addrs = Vec::with_capacity(size);
        while addrs.len() < size {
            let addr: net::SocketAddr = (
                [rng.u8(..), rng.u8(..), rng.u8(..), rng.u8(..)],
                rng.u16(1024..),
            )
                .into();

            if !addrmgr::is_routable(&addr.ip()) {
                continue;
            }
            if addrs.iter().any(|a| addr == *a) {
                continue;
            }
            addrs.push(addr);
        }

        let mut peers = Vec::with_capacity(size);
        for ((i, addr), peer_cfg) in addrs.iter().enumerate().zip(cfgs.drain(..)) {
            let mut address_book = AddressBook::new();

            for other in addrs.iter().skip(i + 1) {
                address_book.push(*other);
            }

            let mut cfg = Config {
                network,
                address_book,
                // Pretend that we're a full-node, to fool connections
                // between instances of this protocol in tests.
                services: ServiceFlags::NETWORK,
                target: peer_cfg.name,
                ..Config::default()
            };
            configure(&mut cfg);

            let chain = peer_cfg.chain;
            let tree = model::Cache::from(chain);
            let peer = Builder {
                cache: tree.clone(),
                clock: clock.clone(),
                rng: rng.clone(),
                cfg,
            };
            info!("(sim) {} = {}", peer_cfg.name, addr);

            peers.push((*addr, peer));
        }
        (peers, time)
    }
}

#[test]
fn test_handshake() {
    let genesis = BlockHeader {
        version: 1,
        prev_blockhash: Default::default(),
        merkle_root: Default::default(),
        nonce: 0,
        time: 0,
        bits: 0,
    };
    let tree = model::Cache::new(genesis);
    let clock = AdjustedTime::default();
    let local_time = LocalTime::from(SystemTime::now());

    let alice_addr = ([127, 0, 0, 1], 8333).into();
    let bob_addr = ([127, 0, 0, 2], 8333).into();

    let builder = Builder {
        cache: tree,
        clock,
        rng: fastrand::Rng::new(),
        cfg: setup::CONFIG.clone(),
    };

    let (alice_tx, alice_rx) = chan::unbounded();
    let (bob_tx, bob_rx) = chan::unbounded();

    let mut alice = builder.clone().build(alice_tx);
    let mut bob = builder.build(bob_tx);

    simulator::run(
        vec![
            (alice_addr, &mut alice, alice_rx),
            (bob_addr, &mut bob, bob_rx),
        ],
        vec![
            vec![Input::Connected {
                addr: bob_addr,
                local_addr: alice_addr,
                link: Link::Outbound,
            }],
            vec![Input::Connected {
                addr: alice_addr,
                local_addr: bob_addr,
                link: Link::Inbound,
            }],
        ],
        local_time,
    );

    assert!(
        alice.peers.values().all(|p| p.is_ready()),
        "alice: {:#?}",
        alice.peers
    );

    assert!(
        bob.peers.values().all(|p| p.is_ready()),
        "bob: {:#?}",
        bob.peers
    );
}

#[test]
#[allow(clippy::redundant_clone)]
fn test_initial_sync() {
    use fastrand::Rng;

    let clock = AdjustedTime::default();
    let local_time = LocalTime::from(SystemTime::now());
    let config = setup::CONFIG.clone();
    let store = store::Memory::new(BITCOIN_HEADERS.clone());
    let network = bitcoin::Network::Bitcoin;
    let params = Params::new(network);

    let alice_addr: PeerId = ([127, 0, 0, 1], 8333).into();
    let bob_addr: PeerId = ([127, 0, 0, 2], 8333).into();
    let (alice_tx, alice_rx) = chan::unbounded();
    let (bob_tx, bob_rx) = chan::unbounded();

    // Blockchain height we're going to be working with. Making it larger
    // than the threshold ensures a sync happens.
    let height = 144;

    // Truncate Alice's chain to test height.
    let mut alice_store = store;
    alice_store.rollback(height).unwrap();

    // Let's test Bob trying to sync with Alice from genesis.
    let alice_tree = BlockCache::from(alice_store, params.clone(), &[]).unwrap();
    let bob_tree = BlockCache::from(
        store::Memory::new(NonEmpty::new(*alice_tree.genesis())),
        params,
        &[],
    )
    .unwrap();

    let mut alice = Bitcoin::new(alice_tree, clock.clone(), Rng::new(), config, alice_tx);

    // Bob connects to Alice.
    {
        let mut bob = Bitcoin::new(bob_tree, clock, Rng::new(), setup::CONFIG.clone(), bob_tx);

        simulator::handshake(
            &mut bob,
            bob_addr,
            bob_rx.clone(),
            &mut alice,
            alice_addr,
            alice_rx.clone(),
            local_time,
        );

        assert_eq!(alice.syncmgr.tree.height(), height);
        assert_eq!(bob.syncmgr.tree.height(), height);
    }
    alice.step(Input::Disconnected(bob_addr), local_time);
}

/// Test what happens when a peer is idle for too long.
#[test]
fn test_idle() {
    let network = Network::Mainnet;
    let chain = NonEmpty::new(network.genesis());
    let mut sim = simulator::Net {
        network,
        rng: fastrand::Rng::new(),
        peers: vec![
            PeerConfig {
                name: "alice",
                chain: chain.clone(),
            },
            PeerConfig { name: "bob", chain },
        ],
        ..simulator::Net::default()
    }
    .into();

    // Connect all peers.
    sim.step();

    // Let a certain amount of time pass.
    sim.elapse(pingmgr::PING_INTERVAL);

    let bob = sim.get("bob");
    let alice = sim.get("alice");

    sim.input(&alice, Input::Timeout(TimeoutSource::Ping(bob)))
        .any(|o| {
            matches!(o, Out::Message(
                addr,
                RawNetworkMessage {
                    payload: NetworkMessage::Ping(_), ..
                },
            ) if addr == &bob)
        })
        .expect("Alice pings Bob");

    // More time passes, and Bob doesn't `pong` back.
    sim.elapse(pingmgr::PING_TIMEOUT);

    // Alice now decides to disconnect Bob.
    sim.input(&alice, Input::Timeout(TimeoutSource::Ping(bob)))
        .any(|o| matches!(o, Out::Disconnect(addr) if addr == &bob))
        .expect("Alice disconnects Bob");
}

#[test]
fn test_getheaders_timeout() {
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);
    // TODO: Protocol should try different peers if it can't get the headers from the first
    // peer. It should keep trying until it succeeds.
    let ((mut local, _, rx), (_, remote_addr, _), local_time) = setup::pair(network);
    // Some hash for a nonexistent block.
    let hash =
        BlockHash::from_hex("0000000000b7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();

    local.step(
        Input::Received(
            remote_addr,
            msg.raw(NetworkMessage::Inv(vec![Inventory::Block(hash)])),
        ),
        local_time,
    );

    let out = rx.try_iter().collect::<Vec<_>>();

    out.iter()
        .find(|o| matches!(payload(o), Some((_, NetworkMessage::GetHeaders(_)))))
        .expect("a `getheaders` message should be returned");
    out.iter()
        .find(
            |o| matches!(o, Out::SetTimeout(TimeoutSource::Synch(Some(addr)), _) if addr == &remote_addr),
        )
        .expect("a timer should be returned");
}

#[quickcheck]
fn test_maintain_connections(seed: u64) {
    const TARGET_PEERS: usize = 2;

    let rng = fastrand::Rng::with_seed(seed);
    let network = Network::Mainnet;
    let chain = NonEmpty::new(network.genesis());
    let mut sim = simulator::Net {
        network,
        peers: vec![
            PeerConfig::new("alice", chain.clone()),
            PeerConfig::new("bob", chain.clone()),
            PeerConfig::new("olive", chain.clone()),
            PeerConfig::new("john", chain.clone()),
            PeerConfig::new("misha", chain),
        ],
        configure: |cfg| {
            cfg.target_outbound_peers = TARGET_PEERS;
        },
        rng,
        ..Default::default()
    }
    .into();

    // The first peer always has the outbound connections.
    let alice = sim.get("alice");

    // Run the simulation until no messages are exchanged.
    sim.step();

    // Keep track of who Alice is connected to.
    let mut connected = Vec::new();

    for e in sim.events(&alice) {
        match e {
            Event::ConnManager(connmgr::Event::Connected(addr, link)) if link == Link::Outbound => {
                connected.push(addr)
            }
            _ => {}
        }
    }
    assert_eq!(connected.len(), TARGET_PEERS);

    let other = connected.pop().unwrap();
    let result = sim.input(&alice, Input::Disconnected(other));

    let addr = result
        .find(|o| match o {
            Out::Connect(addr, _) => Some(*addr),
            _ => None,
        })
        .expect("Alice connects to a peer");

    assert!(addr != other);
    assert!(!connected.contains(&addr));
}

#[quickcheck]
fn test_getheaders_retry(seed: u64) {
    logger::init(log::Level::Info);

    let rng = fastrand::Rng::with_seed(seed);
    // Some hash for a nonexistent block.
    let hash =
        BlockHash::from_hex("0000000000b7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);

    let shortest = NonEmpty::new(network.genesis());
    let longest = NonEmpty::from_vec(BITCOIN_HEADERS.iter().take(8).cloned().collect()).unwrap();

    let mut sim = simulator::Net {
        network,
        peers: vec![
            PeerConfig::new("alice", shortest),
            PeerConfig::new("bob", longest.clone()),
            PeerConfig::new("olive", longest.clone()),
            PeerConfig::new("fred", longest),
        ],
        rng,
        initialize: false,
        ..Default::default()
    }
    .into();

    let bob = sim.get("bob");
    let alice = sim.get("alice");
    let olive = sim.get("olive");
    let fred = sim.get("fred");

    sim.connect(&alice, &[bob, olive, fred]);
    sim.set_filter(|_, _, msg| matches!(msg, NetworkMessage::GetHeaders(_)));
    sim.step(); // Run the simulation until no messages are exchanged.
    sim.clear_filter();

    let ask = sim.peers.len() - 1;

    // Peers that have been asked.
    let mut asked = HashSet::new();

    // Trigger a `getheaders` by sending an inventory message to Alice.
    let result = sim.input(
        &alice,
        Input::Received(
            bob,
            msg.raw(NetworkMessage::Inv(vec![Inventory::Block(hash)])),
        ),
    );

    // The first time we ask for headers, we ask the peer who sent us the `inv` message.
    let (addr, _) = result.message(|_, m| matches!(m, NetworkMessage::GetHeaders(_)));
    assert_eq!(addr, bob);

    asked.insert(addr);
    result.schedule(&mut sim);

    // Keep track of who we asked last.
    let mut last_asked = addr;
    // While there's still peers to ask...
    while asked.len() < ask {
        sim.elapse(syncmgr::REQUEST_TIMEOUT);

        let result = sim.input(
            &alice,
            Input::Timeout(TimeoutSource::Synch(Some(last_asked))),
        );
        let (addr, _) = result.message(|_, m| matches!(m, NetworkMessage::GetHeaders(_)));

        assert!(
            !asked.contains(&addr),
            "Alice shouldn't ask the same peer twice"
        );

        asked.insert(addr);
        result.schedule(&mut sim);

        last_asked = addr;
    }
}

#[test]
fn test_handshake_version_timeout() {
    let network = Network::Mainnet;
    let (mut instance, rx, time) = setup::singleton(network);

    let remote = ([131, 31, 11, 33], 11111).into();
    let local = ([0, 0, 0, 0], 0).into();

    for link in &[Link::Outbound, Link::Inbound] {
        instance.step(
            Input::Connected {
                addr: remote,
                local_addr: local,
                link: *link,
            },
            time,
        );
        rx.try_iter()
            .find(|o| matches!(o, Out::SetTimeout(TimeoutSource::Handshake(addr), _) if addr == &remote))
            .expect("a timer should be returned");

        instance.step(Input::Timeout(TimeoutSource::Handshake(remote)), time);
        assert!(rx
            .iter()
            .any(|o| matches!(o, Out::Disconnect(a) if a == remote)));

        instance.step(Input::Disconnected(remote), time);
    }
}

#[test]
fn test_handshake_verack_timeout() {
    let network = Network::Mainnet;
    let (mut instance, rx, mut time) = setup::singleton(network);

    let remote = ([131, 31, 11, 33], 11111).into();
    let local = ([0, 0, 0, 0], 0).into();

    for link in &[Link::Outbound, Link::Inbound] {
        instance.step(
            Input::Connected {
                addr: remote,
                local_addr: local,
                link: *link,
            },
            time,
        );

        instance.step(
            Input::Received(
                remote,
                RawNetworkMessage {
                    magic: network.magic(),
                    payload: instance.version(local, remote, 0, 0),
                },
            ),
            time,
        );
        rx.try_iter().find(|o| matches!(o, Out::SetTimeout(TimeoutSource::Handshake(addr), _) if *addr == remote))
            .expect("a timer should be returned");

        time = time + LocalDuration::from_secs(60);

        instance.step(Input::Timeout(TimeoutSource::Handshake(remote)), time);
        assert!(rx
            .iter()
            .any(|o| matches!(o, Out::Disconnect(a) if a == remote)));

        instance.step(Input::Disconnected(remote), time);
    }
}

#[test]
fn test_getaddr() {
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);
    let chain = NonEmpty::new(network.genesis());
    let mut sim = simulator::Net {
        network,
        peers: vec![
            PeerConfig::new("alice", chain.clone()),
            PeerConfig::new("bob", chain.clone()),
            PeerConfig::new("olive", chain.clone()),
            PeerConfig::new("john", chain),
        ],
        configure: |cfg| {
            // Each peer only needs to connect to three other peers.
            cfg.target_outbound_peers = 3;
        },
        ..Default::default()
    }
    .into();

    // Run the simulation until no messages are exchanged.
    sim.step();

    // Pick a peer to test.
    let alice = sim.get("alice");

    // Disconnect a peer.
    let peer = sim
        .peer("alice")
        .protocol
        .peers
        .values()
        .filter(|p| p.is_ready())
        .next()
        .unwrap()
        .address;
    let result = sim.input(&alice, Input::Disconnected(peer));

    // This should trigger a `getaddr` because Alice isn't connected to enough peers now.
    let (peer, _) = result.message(|_, msg| matches!(msg, NetworkMessage::GetAddr));

    // We respond to the `getaddr` with a new peer address, Toto.
    let toto: net::SocketAddr = ([14, 45, 16, 57], 8333).into();
    sim.input(
        &alice,
        Input::Received(
            peer,
            msg.raw(NetworkMessage::Addr(vec![(
                0,
                Address::new(&toto, ServiceFlags::NETWORK),
            )])),
        ),
    );

    // After some time, Alice tries to connect to the new address.
    sim.elapse(connmgr::IDLE_TIMEOUT);
    sim.input(&alice, Input::Timeout(TimeoutSource::Connect))
        .any(|o| matches!(o, Out::Connect(addr, _) if addr == &toto))
        .expect("Alice tries to connect to Toto");
}

#[test]
fn test_stale_tip() {
    logger::init(Level::Debug);

    let network = Network::Mainnet;
    let msg = message::Builder::new(network);
    let chain = NonEmpty::new(network.genesis());
    let mut sim = simulator::Net {
        network,
        peers: vec![
            PeerConfig::new("alice", chain.clone()),
            PeerConfig::new("bob", chain),
        ],
        configure: |cfg| {
            // Each peer only needs to connect to three other peers.
            cfg.target_outbound_peers = 1;
        },
        initialize: false,
        ..Default::default()
    }
    .into();

    let alice = sim.get("alice");
    let bob = sim.get("bob");

    // Pretend `Bob` has a chain of height 144.
    let version = sim.peer("bob").protocol.version(alice, bob, 1, 144);

    // Handshake.
    sim.input(
        &alice,
        Input::Connected {
            addr: bob,
            local_addr: alice,
            link: Link::Outbound,
        },
    );
    sim.input(&alice, Input::Received(bob, msg.raw(version)));
    sim.input(
        &alice,
        Input::Received(bob, msg.raw(NetworkMessage::Verack)),
    );
    sim.input(
        &alice,
        Input::Received(
            bob,
            msg.raw(NetworkMessage::Headers(vec![*BITCOIN_HEADERS
                .get(1)
                .unwrap()])),
        ),
    )
    .message(|_, msg| matches!(msg, NetworkMessage::GetHeaders(_)));

    // Timeout the request.
    sim.elapse(syncmgr::REQUEST_TIMEOUT);
    sim.input(&alice, Input::Timeout(TimeoutSource::Synch(Some(bob))));

    // Some time has passed. The tip timestamp should be considered stale now.
    sim.elapse(syncmgr::TIP_STALE_DURATION);
    sim.input(&alice, Input::Timeout(TimeoutSource::Synch(None)))
        .message(|_, msg| matches!(msg, NetworkMessage::GetHeaders(_)));

    // Timeout the request.
    sim.elapse(syncmgr::REQUEST_TIMEOUT);
    sim.input(&alice, Input::Timeout(TimeoutSource::Synch(Some(bob))));

    // Now send another header and wait until the chain update is stale.
    sim.input(
        &alice,
        Input::Received(
            bob,
            msg.raw(NetworkMessage::Headers(vec![*BITCOIN_HEADERS
                .get(2)
                .unwrap()])),
        ),
    );

    // Some more time has passed.
    // Chain update should be stale this time.
    sim.elapse(syncmgr::TIP_STALE_DURATION);
    sim.input(&alice, Input::Timeout(TimeoutSource::Synch(None)))
        .message(|_, msg| matches!(msg, NetworkMessage::GetHeaders(_)));
}

#[test]
fn test_addrs() {
    let network = Network::Mainnet;
    let chain = NonEmpty::new(network.genesis());
    let mut sim = simulator::Net {
        network,
        peers: vec![
            PeerConfig::new("alice", chain.clone()),
            PeerConfig::new("bob", chain),
        ],
        configure: |cfg| {
            // Each peer only needs to connect to three other peers.
            cfg.target_outbound_peers = 3;
        },
        ..Default::default()
    }
    .into();

    // Run handshake.
    sim.step();

    let alice = sim.get("alice");
    let bob = sim.get("bob");

    let jak: net::SocketAddr = ([88, 13, 16, 59], 8333).into();
    let jim: net::SocketAddr = ([99, 45, 180, 58], 8333).into();
    let jon: net::SocketAddr = ([14, 48, 141, 57], 8333).into();

    let msg = message::Builder::new(network);

    // Let alice know about these amazing peers.
    sim.input(
        &alice,
        Input::Received(
            bob,
            msg.raw(NetworkMessage::Addr(vec![
                (0, Address::new(&jak, ServiceFlags::NETWORK)),
                (0, Address::new(&jim, ServiceFlags::NETWORK)),
                (0, Address::new(&jon, ServiceFlags::NETWORK)),
            ])),
        ),
    );

    // Let's make sure Alice has these addresses.
    let result = sim.input(
        &alice,
        Input::Received(bob, msg.raw(NetworkMessage::GetAddr)),
    );
    let (_, msg) = result.message(|_, msg| matches!(msg, NetworkMessage::Addr(_)));

    match msg {
        NetworkMessage::Addr(addrs) => {
            let addrs: HashSet<net::SocketAddr> = addrs
                .iter()
                .map(|(_, a)| a.socket_addr().unwrap())
                .collect();

            assert!(addrs.contains(&jak));
            assert!(addrs.contains(&jim));
            assert!(addrs.contains(&jon));

            assert_eq!(addrs.len(), 3);
        }
        _ => unreachable!(),
    }
}

#[quickcheck]
fn prop_connect_timeout(seed: u64) {
    let rng = fastrand::Rng::with_seed(seed);
    let network = Network::Mainnet;
    let chain = NonEmpty::new(network.genesis());
    let mut sim = simulator::Net {
        network,
        peers: vec![PeerConfig::new("alice", chain)],
        configure: |cfg| {
            cfg.target_outbound_peers = 2;

            cfg.address_book.push(([88, 13, 16, 59], 8333).into());
            cfg.address_book.push(([99, 45, 180, 58], 8333).into());
            cfg.address_book.push(([14, 48, 141, 57], 8333).into());
        },
        rng: rng.clone(),
        initialize: false,
    }
    .into();

    let time = sim.time;
    let alice = sim.peer("alice");

    alice.initialize(time);

    let result = alice
        .outbound
        .try_iter()
        .filter(|o| matches!(o, Out::Connect(_, _)))
        .collect::<Vec<_>>();

    assert_eq!(
        result.len(),
        alice.protocol.connmgr.config.target_outbound_peers
    );

    let mut attempted: Vec<net::SocketAddr> = result
        .into_iter()
        .map(|r| match r {
            Out::Connect(addr, _) => addr,
            _ => panic!(),
        })
        .collect();

    rng.shuffle(&mut attempted);

    // ... after a while, the connections time out.

    let alice = alice.id;
    attempted.pop().unwrap();

    sim.elapse(connmgr::IDLE_TIMEOUT);
    let result = sim.input(&alice, Input::Timeout(TimeoutSource::Connect));

    result
        .all(|o| match o {
            Out::Connect(addr, _) => !attempted.contains(&addr),
            _ => true,
        })
        .expect("Alice tries to connect to another peer");
}
