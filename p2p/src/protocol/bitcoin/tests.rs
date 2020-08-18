#![cfg(test)]
pub mod simulator;

use super::*;
use bitcoin_hashes::hex::FromHex;
use std::collections::VecDeque;
use std::time::SystemTime;

use quickcheck_macros::quickcheck;

use nakamoto_common::block::BlockHeader;

use nakamoto_test::block::cache::model;
use nakamoto_test::logger;
use nakamoto_test::TREE;

fn payload<M: Message>(o: &Output<M>) -> Option<(net::SocketAddr, &M::Payload)> {
    match o {
        Output::Message(a, m) => Some((*a, m.payload())),
        _ => None,
    }
}

mod setup {
    use super::*;

    /// Test protocol config.
    pub const CONFIG: Config = Config {
        network: network::Network::Mainnet,
        address_book: AddressBook::new(),
        // Pretend that we're a full-node, to fool connections
        // between instances of this protocol in tests.
        services: ServiceFlags::NETWORK,
        protocol_version: PROTOCOL_VERSION,
        target_peers: 8,
        user_agent: USER_AGENT,
        relay: false,
        name: "self",
    };

    pub fn singleton(network: Network) -> (Bitcoin<model::Cache>, LocalTime) {
        use bitcoin::blockdata::constants;

        let genesis = constants::genesis_block(network.into()).header;
        let tree = model::Cache::new(genesis);
        let time = LocalTime::from_secs(genesis.time as u64);
        let clock = AdjustedTime::new(time);

        (
            Bitcoin::new(tree, clock, fastrand::Rng::new(), CONFIG),
            time,
        )
    }

    pub fn pair(
        network: Network,
    ) -> (
        (Bitcoin<model::Cache>, PeerId),
        (Bitcoin<model::Cache>, PeerId),
        LocalTime,
    ) {
        use bitcoin::blockdata::constants;

        let genesis = constants::genesis_block(network.into()).header;
        let tree = model::Cache::new(genesis);
        let time = LocalTime::from_secs(genesis.time as u64);
        let clock = AdjustedTime::new(time);

        let mut alice = Bitcoin::new(tree.clone(), clock.clone(), fastrand::Rng::new(), CONFIG);
        let mut bob = Bitcoin::new(tree, clock, fastrand::Rng::new(), CONFIG);

        let alice_addr = ([152, 168, 3, 33], 3333).into();
        let bob_addr = ([152, 168, 7, 77], 7777).into();

        simulator::handshake(&mut alice, alice_addr, &mut bob, bob_addr, time);

        ((alice, alice_addr), (bob, bob_addr), time)
    }

    pub fn network(
        network: Network,
        rng: fastrand::Rng,
        peers: &[&'static str],
        configure: fn(&mut Config),
    ) -> (Vec<(PeerId, Bitcoin<model::Cache>)>, LocalTime) {
        use bitcoin::blockdata::constants;

        let genesis = constants::genesis_block(network.into()).header;
        let tree = model::Cache::new(genesis);
        let time = LocalTime::from_secs(genesis.time as u64);
        let clock = AdjustedTime::new(time);
        let size = peers.len();
        let names = &peers;

        assert!(size > 1);
        assert!(size <= names.len());

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
        for (i, addr) in addrs.iter().enumerate() {
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
                name: names[i],
                ..Config::default()
            };
            configure(&mut cfg);

            let peer = Bitcoin::new(tree.clone(), clock.clone(), rng.clone(), cfg);
            info!("(sim) {} = {}", names[i], addr);

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

    let mut alice = Bitcoin::new(
        tree.clone(),
        clock.clone(),
        fastrand::Rng::new(),
        setup::CONFIG,
    );
    let mut bob = Bitcoin::new(tree, clock, fastrand::Rng::new(), setup::CONFIG);

    simulator::run(
        vec![(alice_addr, &mut alice), (bob_addr, &mut bob)],
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
fn test_initial_sync() {
    use fastrand::Rng;

    let clock = AdjustedTime::default();
    let local_time = LocalTime::from(SystemTime::now());

    let alice_addr: PeerId = ([127, 0, 0, 1], 8333).into();
    let bob_addr: PeerId = ([127, 0, 0, 2], 8333).into();

    // Blockchain height we're going to be working with. Making it larger
    // than the threshold ensures a sync happens.
    let height = 144;

    // Let's test Bob trying to sync with Alice from genesis.
    let mut alice_tree = TREE.clone();
    let bob_tree = model::Cache::new(*alice_tree.genesis());

    // Truncate chain to test height.
    alice_tree.rollback(height).unwrap();

    let mut alice = Bitcoin::new(alice_tree, clock.clone(), Rng::new(), setup::CONFIG);

    // Bob connects to Alice.
    {
        let mut bob = Bitcoin::new(bob_tree.clone(), clock.clone(), Rng::new(), setup::CONFIG);

        simulator::handshake(&mut bob, bob_addr, &mut alice, alice_addr, local_time);

        assert_eq!(alice.syncmgr.tree.height(), height);
        assert_eq!(bob.syncmgr.tree.height(), height);
    }
    // Alice connects to Bob.
    {
        let mut bob = Bitcoin::new(bob_tree, clock, Rng::new(), setup::CONFIG);

        simulator::handshake(&mut alice, alice_addr, &mut bob, bob_addr, local_time);

        assert_eq!(alice.syncmgr.tree.height(), height);
        assert_eq!(bob.syncmgr.tree.height(), height);
    }
}

/// Test what happens when a peer is idle for too long.
#[test]
fn test_idle() {
    logger::init(log::Level::Debug);

    let mut sim = simulator::Net {
        network: Network::Mainnet,
        rng: fastrand::Rng::new(),
        peers: &["alice", "bob"],
        ..simulator::Net::default()
    }
    .into();

    // Connect all peers.
    sim.step();

    // Let a certain amount of time pass.
    sim.elapse(PING_INTERVAL);

    let bob = sim.get("bob");
    let alice = sim.get("alice");

    sim.input(&alice, Input::Timeout(bob, Component::PingManager))
        .any(|o| {
            matches!(o, Output::Message(
                addr,
                RawNetworkMessage {
                    payload: NetworkMessage::Ping(_), ..
                },
            ) if addr == &bob)
        })
        .expect("Alice pings Bob");

    // More time passes, and Bob doesn't `pong` back.
    sim.elapse(PING_TIMEOUT);

    // Alice now decides to disconnect Bob.
    sim.input(&alice, Input::Timeout(bob, Component::PingManager))
        .any(|o| matches!(o, Output::Disconnect(addr) if addr == &bob))
        .expect("Alice disconnects Bob");
}

#[test]
fn test_getheaders_timeout() {
    let network = Network::Mainnet;
    // TODO: Protocol should try different peers if it can't get the headers from the first
    // peer. It should keep trying until it succeeds.
    let ((mut local, _), (_, remote_addr), _time) = setup::pair(network);
    // Some hash for a nonexistent block.
    let hash =
        BlockHash::from_hex("0000000000b7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();

    let out = local.step(Input::Received(
        remote_addr,
        message::raw(
            NetworkMessage::Inv(vec![Inventory::Block(hash)]),
            network.magic(),
        ),
    ));
    out.iter()
        .find(|o| matches!(payload(o), Some((_, NetworkMessage::GetHeaders(_)))))
        .expect("a `getheaders` message should be returned");
    out.iter()
        .find(|o| matches!(o, Output::SetTimeout(addr, _, _) if addr == &remote_addr))
        .expect("a timer should be returned");

    local
        .step(Input::Timeout(remote_addr, Component::SyncManager))
        .iter()
        .find(|o| matches!(o, Output::Disconnect(addr) if addr == &remote_addr))
        .expect("the unresponsive peer should be disconnected");
}

#[quickcheck]
fn test_maintain_connections(seed: u64) {
    logger::init(log::Level::Debug);

    const TARGET_PEERS: usize = 2;

    let rng = fastrand::Rng::new();
    rng.seed(seed);

    let network = Network::Mainnet;
    let mut sim = simulator::Net {
        network,
        peers: &["alice", "bob", "olive", "john", "misha"],
        configure: |cfg| {
            cfg.target_peers = TARGET_PEERS;
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
            Event::Connected(addr, link) if link == Link::Outbound => connected.push(addr),
            _ => {}
        }
    }
    assert_eq!(connected.len(), TARGET_PEERS);

    let other = connected.pop().unwrap();
    let result = sim.input(&alice, Input::Disconnected(other));

    let addr = result
        .find(|o| match o {
            Output::Connect(addr) => Some(*addr),
            _ => None,
        })
        .expect("Alice connects to a peer");

    assert!(addr != other);
    assert!(!connected.contains(&addr));
}

#[test]
fn test_getheaders_retry() {
    logger::init(log::Level::Debug);

    // Some hash for a nonexistent block.
    let hash =
        BlockHash::from_hex("0000000000b7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();
    let network = Network::Mainnet;
    let mut sim = simulator::Net {
        network,
        peers: &["alice", "bob", "olive"],
        ..Default::default()
    }
    .into();

    // Run the simulation until no messages are exchanged.
    sim.step();

    let ask = sim.peers.len() - 1;
    let sender = sim.get("bob");
    let alice = sim.get("alice");

    // Peers that have been asked.
    let mut asked = HashSet::new();

    // Trigger a `getheaders` by sending an inventory message to Alice.
    let result = sim.input(
        &alice,
        Input::Received(
            sender,
            message::raw(
                NetworkMessage::Inv(vec![Inventory::Block(hash)]),
                network.magic(),
            ),
        ),
    );

    // The first time we ask for headers, we ask the peer who sent us the `inv` message.
    let (addr, _) = result.message(|_, m| matches!(m, NetworkMessage::GetHeaders(_)));
    assert_eq!(addr, sender);

    asked.insert(addr);
    result.schedule(&mut sim);

    // Keep track of who we asked last.
    let mut last_asked = addr;
    // While there's still peers to ask...
    while asked.len() < ask {
        let result = sim.input(&alice, Input::Timeout(last_asked, Component::SyncManager));
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
    let (mut instance, _time) = setup::singleton(network);

    let remote = ([131, 31, 11, 33], 11111).into();
    let local = ([0, 0, 0, 0], 0).into();

    for link in &[Link::Outbound, Link::Inbound] {
        let out = instance.step(Input::Connected {
            addr: remote,
            local_addr: local,
            link: *link,
        });
        out.iter()
            .find(|o| matches!(o, Output::SetTimeout(addr, _, _) if addr == &remote))
            .expect("a timer should be returned");

        let out = instance.step(Input::Timeout(remote, Component::HandshakeManager));
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::Disconnect(a) if a == &remote)));
    }
}

#[test]
fn test_handshake_verack_timeout() {
    let network = Network::Mainnet;
    let (mut instance, _time) = setup::singleton(network);

    let remote = ([131, 31, 11, 33], 11111).into();
    let local = ([0, 0, 0, 0], 0).into();

    for link in &[Link::Outbound, Link::Inbound] {
        instance.step(Input::Connected {
            addr: remote,
            local_addr: local,
            link: *link,
        });

        let out = instance.step(Input::Received(
            remote,
            RawNetworkMessage {
                magic: network.magic(),
                payload: instance.version(local, remote, 0, 0),
            },
        ));
        out.iter()
            .find(|o| matches!(o, Output::SetTimeout(addr, _, _) if *addr == remote))
            .expect("a timer should be returned");

        let out = instance.step(Input::Timeout(remote, Component::HandshakeManager));
        assert!(out
            .iter()
            .any(|o| matches!(o, Output::Disconnect(a) if *a == remote)));
    }
}
