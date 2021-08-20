#![cfg(test)]
pub mod peer;
pub mod simulator;

use std::iter;
use std::net;
use std::ops::{Bound, Range};
use std::sync::Arc;

use log::*;

use super::{addrmgr, cbfmgr, connmgr, invmgr, peermgr, pingmgr, syncmgr};
use super::{
    chan, message, AdjustedTime, BlockHash, BlockHeader, BlockTree as _, Command, Config,
    DisconnectReason, Event, HashSet, Height, Input, Link, LocalDuration, LocalTime, Network,
    NetworkMessage, Out, PeerId, RawNetworkMessage, ServiceFlags, VersionMessage,
};
use super::{PROTOCOL_VERSION, USER_AGENT};

use peer::{Peer, PeerDummy};
use simulator::{Options, Simulation};

use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message_filter::CFHeaders;
use bitcoin::network::message_filter::CFilter;
use bitcoin::network::Address;
use bitcoin_hashes::hex::FromHex;

use quickcheck_macros::quickcheck;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_chain::store::Genesis;

use nakamoto_common::block::filter::FilterHeader;
use nakamoto_common::collections::HashMap;
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_common::p2p::peer::KnownAddress;
use nakamoto_common::p2p::peer::Source;

use nakamoto_test::assert_matches;
use nakamoto_test::block::cache::model;
use nakamoto_test::block::gen;
use nakamoto_test::BITCOIN_HEADERS;

#[allow(unused_imports)]
use nakamoto_test::logger;

pub type Protocol = super::Protocol<
    BlockCache<store::Memory<BlockHeader>>,
    model::FilterCache,
    HashMap<net::IpAddr, KnownAddress>,
>;

mod setup {
    use super::*;

    #[allow(dead_code)]
    pub fn addresses(count: usize, rng: fastrand::Rng) -> Vec<net::SocketAddr> {
        let mut addrs = Vec::with_capacity(count);
        while addrs.len() < count {
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
        addrs
    }
}

#[test]
fn test_handshake() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let outbound = ([241, 19, 44, 19], 8333).into();
    let inbound = ([241, 19, 44, 18], 8333).into();

    peer.connect_addr(&inbound, Link::Inbound);
    peer.connect_addr(&outbound, Link::Outbound);
}

// TODO: We need to test this with a chain that is longer than 2000.
#[test]
fn test_initial_sync() {
    let rng = fastrand::Rng::new();
    // Blockchain height we're going to be working with. Making it larger
    // than the threshold ensures a sync happens.
    let height = 144;
    let network = Network::Mainnet;
    let headers = BITCOIN_HEADERS.tail[0..height].to_vec();
    let time = LocalTime::from_block_time(headers.last().unwrap().time);

    assert!(headers.len() >= height);

    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng.clone());
    let mut bob = Peer::new(
        "bob",
        [97, 97, 97, 97],
        network,
        headers,
        vec![],
        vec![],
        rng.clone(),
    );
    assert_eq!(bob.protocol.tree.height(), height as Height);

    alice.command(Command::Connect(bob.addr));

    let mut simulation = Simulation::new(time, rng, Options::default());
    simulation.initialize([&mut alice, &mut bob]);

    while simulation.step([&mut alice, &mut bob]) {
        if alice.protocol.tree.height() == height as Height {
            break;
        }
    }
}

/// Test what happens when a peer is idle for too long.
#[test]
fn test_idle_disconnect() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote = ([241, 19, 44, 18], 8333).into();

    peer.connect_addr(&remote, Link::Outbound);

    // Let a certain amount of time pass.
    peer.time.elapse(pingmgr::PING_INTERVAL);

    peer.tick();
    peer.outputs()
        .find(|o| {
            matches!(o, Out::Message(
                addr,
                RawNetworkMessage {
                    payload: NetworkMessage::Ping(_), ..
                },
            ) if addr == &remote)
        })
        .expect("`ping` is sent");

    // More time passes, and the remote doesn't `pong` back.
    peer.time.elapse(pingmgr::PING_TIMEOUT);

    // Peer now decides to disconnect remote.
    peer.tick();
    peer.outputs()
        .find(|o| {
            matches!(o, Out::Disconnect(
                addr,
                DisconnectReason::PeerTimeout("ping")
            ) if addr == &remote)
        })
        .expect("peer disconnects remote");
}

#[test]
fn test_inv_getheaders() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote: PeerId = ([241, 19, 44, 18], 8333).into();

    // Some hash for a nonexistent block.
    let hash =
        BlockHash::from_hex("0000000000b7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();

    peer.connect_addr(&remote, Link::Outbound);
    peer.step(Input::Received(
        remote,
        msg.raw(NetworkMessage::Inv(vec![Inventory::Block(hash)])),
    ));

    peer.messages()
        .find(|o| matches!(o, (_, NetworkMessage::GetHeaders(_))))
        .expect("a `getheaders` message should be returned");
    peer.outputs()
        .find(|o| matches!(o, Out::SetTimeout(_)))
        .expect("a timer should be returned");
}

#[test]
fn test_bad_magic() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote: PeerId = ([241, 19, 44, 18], 8333).into();

    peer.connect_addr(&remote, Link::Outbound);
    peer.step(Input::Received(
        remote,
        RawNetworkMessage {
            magic: 999,
            payload: NetworkMessage::Ping(1),
        },
    ));

    peer.outputs()
        .find(|o| matches!(o, Out::Disconnect(addr, DisconnectReason::PeerMagic(_)) if addr == &remote))
        .expect("peer should be disconnected");
}

#[test]
fn test_maintain_connections() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let port = network.port();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let time = alice.time.block_time();

    let peers: Vec<PeerId> = vec![
        ([88, 88, 88, 1], 8333).into(),
        ([88, 88, 88, 2], 8333).into(),
        ([88, 88, 88, 3], 8333).into(),
    ];
    let mut addrs: HashSet<_> = vec![
        ([77, 77, 77, 77], port).into(),
        ([78, 78, 78, 78], port).into(),
        ([79, 79, 79, 79], port).into(),
    ]
    .into_iter()
    .collect();

    // Keep track of who Alice is connected to.
    for peer in peers.iter() {
        alice.connect_addr(peer, Link::Outbound);
        alice.protocol.connmgr.is_connected(peer);
    }

    // Give alice some addresses for her address book.
    for addr in addrs.iter() {
        let addr = Address::new(addr, alice.protocol.peermgr.config.required_services);
        alice
            .protocol
            .addrmgr
            .insert(vec![(time, addr)].into_iter(), Source::Dns);
    }

    // Disconnect peers and expect connections to peers from address book.
    for peer in peers.iter() {
        alice.step(Input::Disconnected(
            *peer,
            DisconnectReason::PeerTimeout("timeout"),
        ));

        let addr = alice
            .outputs()
            .find_map(|o| match o {
                Out::Connect(addr, _) => Some(addr),
                _ => None,
            })
            .expect("Alice connects to a new peer");

        assert!(addr != *peer);
        assert!(addrs.remove(&addr));
    }
    assert!(addrs.is_empty());
}

#[test]
fn test_getheaders_retry() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);

    // Some hash for a nonexistent block.
    let hash =
        BlockHash::from_hex("0000000000b7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();

    let mut alice = Peer::genesis("alice", [49, 40, 43, 40], network, vec![], rng);
    let peers = [
        ([55, 55, 55, 55], network.port()).into(),
        ([66, 66, 66, 66], network.port()).into(),
        ([77, 77, 77, 77], network.port()).into(),
    ];
    for peer in peers.iter() {
        alice.connect_addr(peer, Link::Outbound);
    }
    assert!(alice.protocol.syncmgr.best_height().unwrap() > alice.protocol.tree.height());

    // Peers that have been asked.
    let mut asked = HashSet::new();

    // Trigger a `getheaders` by sending an inventory message to Alice.
    alice.step(Input::Received(
        peers[0],
        msg.raw(NetworkMessage::Inv(vec![Inventory::Block(hash)])),
    ));

    // The first time we ask for headers, we ask the peer who sent us the `inv` message.
    let (addr, _) = alice
        .messages()
        .find(|(_, m)| matches!(m, NetworkMessage::GetHeaders(_)))
        .expect("Alice asks the first peer for headers");
    assert_eq!(addr, peers[0]);

    asked.insert(addr);

    // While there's still peers to ask...
    while asked.len() < peers.len() {
        alice.time.elapse(syncmgr::REQUEST_TIMEOUT);
        alice.tick();

        let (addr, _) = alice
            .messages()
            .find(|(_, m)| matches!(m, NetworkMessage::GetHeaders(_)))
            .expect("Alice asks the next peer for headers");

        assert!(
            !asked.contains(&addr),
            "Alice shouldn't ask the same peer twice"
        );

        asked.insert(addr);
    }
}

#[test]
fn test_handshake_version_timeout() {
    let network = Network::Mainnet;
    let rng = fastrand::Rng::new();
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote = ([131, 31, 11, 33], 11111).into();

    for link in &[Link::Outbound, Link::Inbound] {
        peer.step(Input::Connected {
            addr: remote,
            local_addr: peer.addr,
            link: *link,
        });
        peer.outputs()
            .find(|o| matches!(o, Out::SetTimeout(_)))
            .expect("a timer should be returned");

        peer.time.elapse(peermgr::HANDSHAKE_TIMEOUT);
        peer.tick();
        peer.outputs().find(
            |o| matches!(o, Out::Disconnect(a, DisconnectReason::PeerTimeout(_)) if *a == remote)
        ).expect("peer should disconnect when no `version` is received");

        peer.step(Input::Disconnected(
            remote,
            DisconnectReason::PeerTimeout("test"),
        ));
    }
}

#[test]
fn test_handshake_verack_timeout() {
    let network = Network::Mainnet;
    let rng = fastrand::Rng::new();
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote = PeerDummy::new([131, 31, 11, 33], network, 144, ServiceFlags::NETWORK);

    for link in &[Link::Outbound, Link::Inbound] {
        peer.step(Input::Connected {
            addr: remote.addr,
            local_addr: peer.addr,
            link: *link,
        });

        peer.step(Input::Received(
            remote.addr,
            RawNetworkMessage {
                magic: network.magic(),
                payload: NetworkMessage::Version(remote.version(peer.addr, 0)),
            },
        ));
        peer.outputs()
            .find(|o| matches!(o, Out::SetTimeout(_)))
            .expect("a timer should be returned");

        peer.time.elapse(LocalDuration::from_secs(60));
        peer.tick();
        peer.outputs().find(
            |o| matches!(o, Out::Disconnect(a, DisconnectReason::PeerTimeout(_)) if *a == remote.addr)
        ).expect("peer should disconnect if no `verack` is received");

        peer.step(Input::Disconnected(
            remote.addr,
            DisconnectReason::PeerTimeout("verack"),
        ));
    }
}

#[test]
fn test_handshake_version_hook() {
    let network = Network::Mainnet;
    let rng = fastrand::Rng::new();

    let mut cfg = Config::default();
    cfg.hooks.on_version = Arc::new(|_, version: VersionMessage| {
        if version.user_agent.contains("craig") {
            return Err("craig is not satoshi");
        }
        Ok(())
    });

    let mut peer = Peer::config([48, 48, 48, 48], vec![], vec![], vec![], cfg, rng);
    let craig = PeerDummy::new([131, 31, 11, 33], network, 144, ServiceFlags::NETWORK);
    let satoshi = PeerDummy::new([131, 31, 11, 66], network, 144, ServiceFlags::NETWORK);

    peer.step(Input::Connected {
        addr: craig.addr,
        local_addr: peer.addr,
        link: Link::Inbound,
    });
    peer.step(Input::Received(
        craig.addr,
        RawNetworkMessage {
            magic: network.magic(),
            payload: NetworkMessage::Version(VersionMessage {
                user_agent: "/craig:0.1.0/".to_owned(),
                ..craig.version(peer.addr, 0)
            }),
        },
    ));
    peer.outputs()
        .find(|o| matches!(o, Out::Disconnect(a, DisconnectReason::Other("craig is not satoshi")) if *a == craig.addr))
        .expect("peer should disconnect when the 'on_version' hook returns an error");

    peer.step(Input::Connected {
        addr: satoshi.addr,
        local_addr: peer.addr,
        link: Link::Inbound,
    });
    peer.step(Input::Received(
        satoshi.addr,
        RawNetworkMessage {
            magic: network.magic(),
            payload: NetworkMessage::Version(VersionMessage {
                user_agent: "satoshi".to_owned(),
                ..satoshi.version(peer.addr, 0)
            }),
        },
    ));
    peer.messages()
        .find(|m| matches!(m, (a, NetworkMessage::Verack) if *a == satoshi.addr))
        .expect("peer should send a 'verack' message back");
}

#[test]
fn test_handshake_initial_messages() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);

    let remote = PeerDummy::new([131, 31, 11, 33], network, 144, ServiceFlags::NETWORK);
    let local = ([0, 0, 0, 0], 0).into();

    // Make sure the address manager trusts this remote address.
    peer.initialize();
    peer.protocol.addrmgr.insert(
        std::iter::once((
            peer.time.block_time(),
            Address::new(&remote.addr, ServiceFlags::NETWORK),
        )),
        Source::Dns,
    );

    // Handshake
    peer.step(Input::Connected {
        addr: remote.addr,
        local_addr: local,
        link: Link::Outbound,
    });
    peer.step(Input::Received(
        remote.addr,
        RawNetworkMessage {
            magic: network.magic(),
            payload: NetworkMessage::Version(remote.version(local, 0)),
        },
    ));
    peer.step(Input::Received(
        remote.addr,
        RawNetworkMessage {
            magic: network.magic(),
            payload: NetworkMessage::Verack,
        },
    ));

    let msgs = peer.messages().collect::<Vec<_>>();

    assert!(msgs.contains(&(remote.addr, NetworkMessage::SendHeaders)));
    assert!(msgs.contains(&(remote.addr, NetworkMessage::GetAddr)));
    assert!(msgs
        .iter()
        .any(|msg| matches!(msg, (addr, NetworkMessage::Ping(_)) if addr == &remote.addr)));
}

#[test]
fn test_connection_error() {
    let network = Network::Mainnet;
    let rng = fastrand::Rng::new();
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote = PeerDummy::new([131, 31, 11, 33], network, 144, ServiceFlags::NETWORK);

    peer.step(Input::Command(Command::Connect(remote.addr)));
    peer.outputs()
        .find(|o| matches!(o, Out::Connect(addr, _) if *addr == remote.addr))
        .expect("Alice should try to connect to remote");
    peer.step(Input::Connecting { addr: remote.addr });
    // Make sure we can handle a disconnection before an established connection.
    peer.step(Input::Disconnected(
        remote.addr,
        DisconnectReason::ConnectionError(String::from("oops")),
    ));
}

#[test]
fn test_getaddr() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let bob: PeerId = ([241, 19, 44, 18], 8333).into();
    let eve: PeerId = ([241, 19, 44, 19], 8333).into();

    alice.connect_addr(&bob, Link::Outbound);
    alice.connect_addr(&eve, Link::Outbound);

    // Disconnect a peer.
    alice.step(Input::Disconnected(bob, DisconnectReason::Command));

    // Alice should now fetch new addresses, but she won't find any and the requests will time out.
    alice.time.elapse(addrmgr::REQUEST_TIMEOUT);
    alice.tick();

    // Alice is unable to connect to a new peer because our address book is exhausted.
    alice
        .events()
        .find(|e| matches!(e, Event::AddrManager(addrmgr::Event::AddressBookExhausted)))
        .expect("Alice should emit `AddressBookExhausted`");

    // When we receive a timeout, we fetch new addresses, since our addresses have been exhausted.
    alice.tick();
    alice
        .messages()
        .find(|msg| matches!(msg, (_, NetworkMessage::GetAddr)))
        .expect("Alice should send `getaddr`");

    // We respond to the `getaddr` with a new peer address, Toto.
    let toto: net::SocketAddr = ([14, 45, 16, 57], 8333).into();
    alice.step(Input::Received(
        eve,
        msg.raw(NetworkMessage::Addr(vec![(
            alice.time.block_time(),
            Address::new(&toto, ServiceFlags::NETWORK),
        )])),
    ));

    // After some time, Alice tries to connect to the new address.
    alice.time.elapse(connmgr::IDLE_TIMEOUT);
    alice.tick();

    alice
        .outputs()
        .find(|o| matches!(o, Out::Connect(addr, _) if addr == &toto))
        .expect("Alice tries to connect to Toto");
}

#[test]
fn test_stale_tip() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote: PeerId = ([33, 33, 33, 33], network.port()).into();
    let headers = &BITCOIN_HEADERS;

    alice.connect_addr(&remote, Link::Outbound);
    alice.step(Input::Received(
        remote,
        // Receive an unsolicited header announcement for the latest.
        msg.raw(NetworkMessage::Headers(vec![*headers
            .get(alice.protocol.tree.height() as usize + 1)
            .unwrap()])),
    ));
    alice
        .messages()
        .find(|(_, msg)| matches!(msg, NetworkMessage::GetHeaders(_)))
        .expect("Alice sends a `getheaders` message");

    // Timeout the request.
    alice.time.elapse(syncmgr::REQUEST_TIMEOUT);
    alice.tick();

    // Some time has passed. The tip timestamp should be considered stale now.
    alice.time.elapse(syncmgr::TIP_STALE_DURATION);
    alice.tick();
    alice
        .events()
        .find(|e| matches!(e, Event::SyncManager(syncmgr::Event::StaleTipDetected(_))))
        .expect("Alice emits a `StaleTipDetected` event");

    // Timeout the `getheaders` request.
    alice.time.elapse(syncmgr::REQUEST_TIMEOUT);
    alice.tick();

    // Now send another header and wait until the chain update is stale.
    alice.step(Input::Received(
        remote,
        msg.raw(NetworkMessage::Headers(vec![*headers
            .get(alice.protocol.tree.height() as usize + 1)
            .unwrap()])),
    ));

    // Some more time has passed.
    alice.time.elapse(syncmgr::TIP_STALE_DURATION);
    alice.tick();
    // Chain update should be stale this time.
    alice
        .events()
        .find(|e| matches!(e, Event::SyncManager(syncmgr::Event::StaleTipDetected(_))))
        .expect("Alice emits a `StaleTipDetected` event");
}

#[quickcheck]
fn prop_addrs(seed: u64) {
    let rng = fastrand::Rng::with_seed(seed);
    let network = Network::Mainnet;
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let bob: PeerId = ([241, 19, 44, 18], 8333).into();

    alice.connect_addr(&bob, Link::Outbound);

    let jak: PeerId = ([88, 13, 16, 59], 8333).into();
    let jim: PeerId = ([99, 45, 180, 58], 8333).into();
    let jon: PeerId = ([14, 48, 141, 57], 8333).into();

    let msg = message::Builder::new(network);
    let time = alice.time.block_time();

    // Let alice know about these amazing peers.
    alice.step(Input::Received(
        bob,
        msg.raw(NetworkMessage::Addr(vec![
            (time, Address::new(&jak, ServiceFlags::NETWORK)),
            (time, Address::new(&jim, ServiceFlags::NETWORK)),
            (time, Address::new(&jon, ServiceFlags::NETWORK)),
        ])),
    ));

    // Let's query Alice to see if she has these addresses.
    alice.step(Input::Received(bob, msg.raw(NetworkMessage::GetAddr)));
    let (_, msg) = alice
        .messages()
        .find(|o| matches!(o, (_, NetworkMessage::Addr(_))))
        .expect("peer should respond with `addr`");

    let addrs = match msg {
        NetworkMessage::Addr(addrs) => addrs,
        _ => unreachable!(),
    };
    assert_eq!(addrs.len(), 3);

    let addrs: HashSet<net::SocketAddr> = addrs
        .iter()
        .map(|(_, a)| a.socket_addr().unwrap())
        .collect();

    assert!(addrs.contains(&jak));
    assert!(addrs.contains(&jim));
    assert!(addrs.contains(&jon));

    assert_eq!(addrs.len(), 3);
}

#[quickcheck]
fn prop_connect_timeout(seed: u64) {
    let rng = fastrand::Rng::with_seed(seed);
    let network = Network::Mainnet;
    let config = Config {
        target: "alice",
        target_outbound_peers: 3,
        connect: vec![
            ([77, 77, 77, 77], network.port()).into(),
            ([88, 88, 88, 88], network.port()).into(),
            ([99, 99, 88, 99], network.port()).into(),
        ],
        network,
        ..Config::default()
    };
    let mut alice = Peer::config(
        [48, 48, 48, 48],
        vec![],
        vec![],
        vec![],
        config,
        rng.clone(),
    );

    alice.protocol.initialize(alice.time);

    let result = alice
        .outputs()
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

    attempted.pop().unwrap();

    alice.time.elapse(connmgr::IDLE_TIMEOUT);
    alice.tick();

    assert!(alice.outputs().all(|o| match o {
        Out::Connect(addr, _) => !attempted.contains(&addr),
        _ => true,
    }));
}

/// Test that we can find and connect to peers amidst network errors.
#[test]
fn sim_connect_to_peers() {
    logger::init(log::Level::Debug);

    let rng = fastrand::Rng::with_seed(1);
    let network = Network::Mainnet;
    let headers = BITCOIN_HEADERS.tail.to_vec();
    let time = LocalTime::from_block_time(headers.last().unwrap().time);

    // Alice will try to connect to enough outbound peers.
    let mut peers = peer::network(network, connmgr::TARGET_OUTBOUND_PEERS + 1, rng.clone());
    let addrs = peers
        .iter()
        .map(|p| (p.addr, Source::Dns, p.cfg.services))
        .collect::<Vec<_>>();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, addrs, rng.clone());

    let mut simulator = Simulation::new(
        time,
        rng,
        Options {
            latency: 1..5,      // 1 - 5 seconds
            failure_rate: 0.04, // 4%
        },
    );
    alice.initialize();
    simulator.initialize(&mut peers);

    // TODO: Node needs to try to reconnect after disconnect!

    while simulator.step(iter::once(&mut alice).chain(&mut peers)) {
        if alice.protocol.connmgr.outbound_peers().count() >= connmgr::TARGET_OUTBOUND_PEERS {
            break;
        }
    }
    assert!(
        simulator.elapsed() < LocalDuration::from_secs(20),
        "Desired state took too long to reach: {}",
        simulator.elapsed()
    );
}

#[test]
fn test_submit_transactions() {
    let network = Network::Mainnet;
    let time = LocalTime::now();

    let mut rng = fastrand::Rng::new();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng.clone());

    // Relay peer.
    let remote1 = PeerDummy {
        addr: ([88, 88, 88, 88], 8333).into(),
        height: 144,
        protocol_version: alice.protocol.protocol_version,
        services: ServiceFlags::NETWORK,
        relay: true,
        time,
    };
    // Non-relay peer.
    let remote2 = PeerDummy {
        addr: ([99, 99, 99, 99], 8333).into(),
        relay: false,
        ..remote1
    };

    alice.connect(&remote1, Link::Outbound);
    assert!(alice.protocol.peermgr.outbound().next().unwrap().relay);

    let (transmit, receive) = chan::bounded(1);
    let tx = gen::transaction(&mut rng);
    let txid = tx.txid();
    let inventory = vec![Inventory::Transaction(txid)];
    alice.connect(&remote2, Link::Outbound);
    alice.command(Command::SubmitTransactions(vec![tx], transmit));

    let remotes = receive.recv().unwrap().unwrap();
    assert_eq!(Vec::from(remotes), vec![remote1.addr]);
    assert!(alice.protocol.invmgr.contains(&txid));

    alice.tick();
    alice
        .messages()
        .find(|(peer, msg)| msg == &NetworkMessage::Inv(inventory.clone()) && peer == &remote1.addr)
        .expect("Alice sends an `inv` message");

    alice.time.elapse(LocalDuration::from_secs(30));
    alice.receive(remote1.addr, NetworkMessage::GetData(inventory));
    alice
        .messages()
        .find(|(_, msg)| matches!(msg, NetworkMessage::Tx(_)))
        .expect("Alice responds to `getdata` with a `tx` message");
}

/// Should rebroadcast `inv` when no `getdata` is received.
/// Should rebroadcast when a new peer connects.
#[test]
fn test_inv_rebroadcast() {
    let network = Network::Mainnet;

    let mut rng = fastrand::Rng::new();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng.clone());

    let remote1 = ([88, 88, 88, 88], 8333).into();
    let remote2 = ([99, 99, 99, 99], 8333).into();
    let tx1 = gen::transaction(&mut rng);
    let tx2 = gen::transaction(&mut rng);
    let (transmit, _) = chan::unbounded();

    alice.connect_addr(&remote1, Link::Outbound);
    alice.command(Command::SubmitTransactions(vec![tx1, tx2], transmit));
    alice.tick(); // Broadcasting doesn't happen immediately
    alice
        .messages()
        .find(|m| {
            matches! {
                m, (addr, NetworkMessage::Inv(inv))
                if inv.len() == 2 && addr == &remote1
            }
        })
        .expect("Alice sends an initial `inv`");

    alice.outputs().count(); // Drain outputs
    alice.connect_addr(&remote2, Link::Outbound); // A new peer connects
    alice.tick(); // No delay until invs are sent to it
    alice
        .messages()
        .find(|m| matches!(m, (a, NetworkMessage::Inv(_)) if a == &remote2))
        .expect("Alice sends a first `inv` message to the new peer");

    // Let some time pass.
    alice.drain();
    alice.time.elapse(LocalDuration::from_mins(5));
    alice.tick();

    let msgs = alice
        .messages()
        .filter(|m| matches!(m, (_, NetworkMessage::Inv(_))))
        .collect::<Vec<_>>();

    assert_eq!(
        msgs.len(),
        2,
        "Two `inv` messages are sent, one for each peer"
    );

    msgs.iter()
        .find(|(addr, _)| addr == &remote1)
        .expect("Alice sends a second `inv` message to the first peer");
    msgs.iter()
        .find(|(addr, _)| addr == &remote2)
        .expect("Alice sends a second `inv` message to the second peer");

    // Let some more time pass.
    alice.time.elapse(LocalDuration::from_mins(5));
    alice.tick();
    alice
        .messages()
        .find(|m| {
            matches! {
                m, (addr, NetworkMessage::Inv(_))
                if addr == &remote1
            }
        })
        .expect("Alice sends a third `inv` message to the first peer");
}

#[test]
fn test_inv_partial_broadcast() {
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);

    let mut rng = fastrand::Rng::new();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng.clone());

    let remote1 = ([88, 88, 88, 88], 8333).into();
    let remote2 = ([99, 99, 99, 99], 8333).into();
    let tx1 = gen::transaction(&mut rng);
    let tx2 = gen::transaction(&mut rng);
    let (transmit, _) = chan::unbounded();

    alice.connect_addr(&remote1, Link::Outbound);
    alice.connect_addr(&remote2, Link::Outbound);
    alice.command(Command::SubmitTransactions(
        vec![tx1.clone(), tx2.clone()],
        transmit,
    ));
    alice.tick();

    // The first peer asks only for the first inventory item.
    alice.time.elapse(LocalDuration::from_secs(3));
    alice.step(Input::Received(
        remote1,
        msg.raw(NetworkMessage::GetData(vec![Inventory::Transaction(
            tx1.txid(),
        )])),
    ));
    // The second peer asks only for the second inventory item.
    alice.step(Input::Received(
        remote2,
        msg.raw(NetworkMessage::GetData(vec![Inventory::Transaction(
            tx2.txid(),
        )])),
    ));

    let messages = alice.messages().collect::<Vec<_>>();
    messages
        .iter()
        .find(|(addr, msg)| {
            if let NetworkMessage::Tx(tx) = msg {
                return addr == &remote1 && tx.txid() == tx1.txid();
            }
            false
        })
        .expect("Alice responds with only the requested inventory to peer#1");
    messages
        .iter()
        .find(|(addr, msg)| {
            if let NetworkMessage::Tx(tx) = msg {
                return addr == &remote2 && tx.txid() == tx2.txid();
            }
            false
        })
        .expect("Alice responds with only the requested inventory to peer#2");

    // Time passes.
    alice.drain();
    alice.time.elapse(LocalDuration::from_mins(5));
    alice.tick();

    let messages = alice.messages().collect::<Vec<_>>();
    messages
        .iter()
        .find(|m| {
            matches! {
                m, (addr, NetworkMessage::Inv(inv))
                if inv.first() == Some(&Inventory::Transaction(tx2.txid())) && addr == &remote1
            }
        })
        .expect("Alice re-sends the missing inv to peer#1");
    messages
        .iter()
        .find(|m| {
            matches! {
                m, (addr, NetworkMessage::Inv(inv))
                if inv.first() == Some(&Inventory::Transaction(tx1.txid())) && addr == &remote2
            }
        })
        .expect("Alice re-sends the missing inv to peer#2");

    // Now the peers ask for the remaining inventories.
    alice.step(Input::Received(
        remote1,
        msg.raw(NetworkMessage::GetData(vec![Inventory::Transaction(
            tx2.txid(),
        )])),
    ));
    alice.step(Input::Received(
        remote2,
        msg.raw(NetworkMessage::GetData(vec![Inventory::Transaction(
            tx1.txid(),
        )])),
    ));

    // More time passes.
    alice.drain();
    alice.time.elapse(LocalDuration::from_mins(5));
    alice.tick();

    assert_eq!(
        alice
            .messages()
            .filter(|(_, m)| matches!(m, NetworkMessage::Inv(_)))
            .count(),
        0,
        "Alice has nothing more to send"
    )
}

#[test]
fn test_confirmed_transaction() {
    let mut rng = fastrand::Rng::new();

    let network = Network::Regtest;
    let remote: PeerId = ([88, 88, 88, 88], 8333).into();
    let (transmit, _) = chan::unbounded();
    let genesis = network.genesis_block();
    let chain = gen::blockchain(genesis, 16, &mut rng);
    let headers = NonEmpty::from_vec(chain.iter().map(|b| b.header).collect()).unwrap();
    let msg = message::Builder::new(network);
    let mut alice = Peer::new(
        "alice",
        [48, 48, 48, 48],
        network,
        headers.tail,
        vec![],
        vec![],
        rng.clone(),
    );

    let blk1 = &chain[rng.usize(1..chain.len() / 2)];
    let blk2 = &chain[rng.usize(chain.len() / 2..chain.len())];
    let tx1 = &blk1.txdata[rng.usize(0..blk1.txdata.len())];
    let tx2 = &blk2.txdata[rng.usize(0..blk2.txdata.len())];

    alice.connect_addr(&remote, Link::Outbound);
    alice.command(Command::SubmitTransactions(
        vec![tx1.clone(), tx2.clone()],
        transmit,
    ));
    alice.tick();

    assert!(alice.protocol.invmgr.contains(&tx1.txid()));
    assert!(alice.protocol.invmgr.contains(&tx2.txid()));

    alice.protocol.invmgr.get_block(blk1.block_hash());
    alice.protocol.invmgr.get_block(blk2.block_hash());

    alice.tick();
    alice.step(Input::Received(
        remote,
        msg.raw(NetworkMessage::Block(blk2.clone())),
    ));
    alice
        .events()
        .find(|e| {
            matches!(
                e,
                Event::InventoryManager(invmgr::Event::BlockReceived { .. })
            )
        })
        .expect("Alice receives the 2nd block");

    alice.time.elapse(LocalDuration::from_mins(1));
    alice.step(Input::Received(
        remote,
        msg.raw(NetworkMessage::Block(blk1.clone())),
    ));

    let mut events = alice.events().filter_map(|e| {
        if let Event::InventoryManager(event) = e {
            Some(event)
        } else {
            None
        }
    });

    assert!(
        matches! {
            events.next().unwrap(),
            invmgr::Event::BlockReceived { .. }
        },
        "Alice receives the 1st block"
    );

    // ... Now Alice has all the blocks and can start processing them ...

    assert!(
        matches! {
            events.next().unwrap(), invmgr::Event::Confirmed { block, transaction, .. }
            if block == blk1.block_hash() && transaction.txid() == tx1.txid()
        },
        "Alice emits the first 'Confirmed' event"
    );
    assert!(
        matches! {
            events.next().unwrap(), invmgr::Event::BlockProcessed { block, .. }
            if block.block_hash() == blk1.block_hash()
        },
        "Alice is done processing the first block"
    );

    assert!(
        matches! {
            events.next().unwrap(), invmgr::Event::Confirmed { block, transaction, .. }
            if block == blk2.block_hash() && transaction.txid() == tx2.txid()
        },
        "Alice emits the second 'Confirmed' event"
    );
    assert!(
        matches! {
            events.next().unwrap(), invmgr::Event::BlockProcessed { block, .. }
            if block.block_hash() == blk2.block_hash()
        },
        "Alice is done processing the second block"
    );

    assert_eq!(events.count(), 0);
    assert!(alice.protocol.invmgr.is_empty());
}

#[test]
fn test_submitted_transaction_filtering() {
    let height = 16;
    let mut rng = fastrand::Rng::new();

    logger::init(log::Level::Debug);

    let network = Network::Regtest;
    let remote: PeerId = ([88, 88, 88, 88], 8333).into();
    let (transmit, _) = chan::unbounded();
    let genesis = network.genesis_block();
    let chain = gen::blockchain(genesis, height, &mut rng);
    let headers = NonEmpty::from_vec(chain.iter().map(|b| b.header).collect()).unwrap();
    let cfheader_genesis = FilterHeader::genesis(network);
    let cfheaders = gen::cfheaders_from_blocks(cfheader_genesis, chain.iter())
        .into_iter()
        .skip(1) // Skip genesis
        .collect::<Vec<_>>();
    let filter_type = 0x0;
    let mut alice = Peer::new(
        "alice",
        [48, 48, 48, 48],
        network,
        headers.tail,
        cfheaders.clone(),
        vec![],
        rng.clone(),
    );
    let tx = gen::transaction(&mut rng);

    // Connect to a peer and submit the transaction.
    alice.connect(
        &PeerDummy {
            addr: remote,
            height,
            protocol_version: alice.protocol.protocol_version,
            services: cbfmgr::REQUIRED_SERVICES | syncmgr::REQUIRED_SERVICES,
            relay: true,
            time: alice.time,
        },
        Link::Outbound,
    );

    // Start a rescan, to make sure we catch the transaction when it's confirmed.
    alice.command(Command::Rescan {
        from: Bound::Unbounded, // Start scanning from the current height.
        to: Bound::Unbounded,   // Keep scanning forever.
        watch: vec![],          // Submitted transactions are tracked automatically.
    });
    alice.command(Command::SubmitTransactions(vec![tx.clone()], transmit));
    alice.tick();

    assert!(alice.protocol.invmgr.contains(&tx.txid()));

    // The next block will have the matching transaction.
    let matching = gen::block_with(&chain.last().header, vec![tx.clone()], &mut rng);
    let cfilter = gen::cfilter(&matching);
    let (_, parent) = cfheaders.last().unwrap();
    let (cfhash, _) = gen::cfheader(parent, &cfilter);

    alice.time = LocalTime::from_block_time(chain.last().header.time);

    // Alice receives a header announcement.
    alice.receive(remote, NetworkMessage::Headers(vec![matching.header]));

    alice
        .messages()
        .find(|(_, m)| matches!(m, NetworkMessage::GetCFHeaders(_)))
        .expect("Alice asks for the matching cfheaders");

    // Alice receives the cfheaders.
    alice.receive(
        remote,
        NetworkMessage::CFHeaders(CFHeaders {
            filter_type,
            stop_hash: matching.block_hash(),
            previous_filter_header: *parent,
            filter_hashes: vec![cfhash],
        }),
    );

    alice
        .messages()
        .find(|(_, m)| matches!(m, NetworkMessage::GetCFilters(_)))
        .expect("Alice asks for the cfilter");

    // Alice receives the cfilter, which we expect to match.
    alice.receive(
        remote,
        NetworkMessage::CFilter(CFilter {
            filter_type,
            block_hash: matching.block_hash(),
            filter: cfilter.content,
        }),
    );

    // Alice asks for the corresponding block.
    let expected = vec![Inventory::Block(matching.block_hash())];

    alice.tick();
    alice
        .messages()
        .find(|(_, m)| matches!(m, NetworkMessage::GetData(data) if data == &expected))
        .expect("Alice asks for the matching block");
    alice.receive(remote, NetworkMessage::Block(matching));

    assert!(alice.protocol.invmgr.is_empty(), "The mempool is empty");
    assert!(
        !alice.protocol.cbfmgr.unwatch_transaction(&tx.txid()),
        "The transaction is no longer watched"
    );
}

/// Test that blocks being imported and going stale generates the right events.
#[test]
fn test_block_events() {
    let mut rng = fastrand::Rng::new();
    let network = Network::Regtest;
    let genesis = network.genesis();
    let remote: PeerId = ([88, 88, 88, 88], 8333).into();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng.clone());
    let (transmit, import) = chan::unbounded();

    let best = 16;
    let headers = gen::headers(genesis, best, &mut rng);
    let extra = gen::block(headers.last(), &mut rng);

    let fork_height = 8;
    let fork_best = 20;
    let fork = gen::headers(
        headers[fork_height as usize],
        fork_best - fork_height,
        &mut rng,
    );
    logger::init(log::Level::Debug);

    fn filter(events: impl Iterator<Item = Event>) -> impl Iterator<Item = syncmgr::Event> {
        events.filter_map(|e| match e {
            Event::SyncManager(event @ syncmgr::Event::BlockConnected { .. }) => Some(event),
            Event::SyncManager(event @ syncmgr::Event::BlockDisconnected { .. }) => Some(event),
            Event::SyncManager(event @ syncmgr::Event::Synced { .. }) => Some(event),
            _ => None,
        })
    }

    alice.time = LocalTime::from_block_time(headers.last().time);
    alice.initialize();
    alice.command(Command::ImportHeaders(
        headers.tail.clone(),
        transmit.clone(),
    ));

    import.recv().unwrap().unwrap();
    let mut events = filter(alice.events());

    assert_matches!(
        events.next().unwrap(),
        syncmgr::Event::Synced(hash, height)
        if height == 0 && hash == genesis.block_hash()
    );

    for (height_, header) in headers.iter().enumerate().skip(1) {
        let hash_ = header.block_hash();

        assert_matches!(
            events.next().unwrap(),
            syncmgr::Event::BlockConnected { height, header }
            if height == height_ as Height && header.block_hash() == hash_
        );
    }
    assert_matches!(events.next().unwrap(), syncmgr::Event::Synced(_, height) if height == best);
    assert_eq!(events.count(), 0);

    // Receive "extra" block.
    alice.connect_addr(&remote, Link::Outbound);
    alice.receive(
        remote,
        NetworkMessage::Inv(vec![Inventory::Block(extra.block_hash())]),
    );
    alice.receive(remote, NetworkMessage::Headers(vec![extra.header]));

    let mut events = filter(alice.events());
    assert_matches!(
        events.next().unwrap(),
        syncmgr::Event::BlockConnected { height, header }
        if height == best + 1 && header.block_hash() == extra.block_hash()
    );
    assert_matches!(
        events.next().unwrap(),
        syncmgr::Event::Synced(_, height) if height == best + 1
    );
    assert_eq!(0, events.count());

    // Receive fork.
    alice.time = LocalTime::from_block_time(extra.header.time);
    alice.command(Command::ImportHeaders(fork.tail.clone(), transmit));
    import.recv().unwrap().unwrap();

    let mut events = filter(alice.events());

    // Disconnected events.
    for height_ in fork_height + 1..=best {
        let hash_ = headers[height_ as usize].block_hash();

        assert_matches!(
            events.next().unwrap(),
            syncmgr::Event::BlockDisconnected { height, hash }
            if height == height_ as Height && hash == hash_
        );
    }
    assert_matches!(
        events.next().unwrap(),
        syncmgr::Event::BlockDisconnected { height, hash }
        if height == best + 1 && hash == extra.block_hash()
    );

    // Connected events.
    for height_ in fork_height + 1..=fork_best {
        let hash_ = fork[height_ as usize - fork_height as usize].block_hash();

        assert_matches!(
            events.next().unwrap(),
            syncmgr::Event::BlockConnected { height, header }
            if height == height_ as Height && header.block_hash() == hash_
        );
    }

    assert_matches!(
        events.next().unwrap(),
        syncmgr::Event::Synced(_, height)
        if height == fork_best
    );
    assert!(events.next().is_none());
}

#[test]
fn test_transaction_mempool_rebroadcast() {
    // TODO: Should check mempool to rebroadcast.
}

#[test]
fn test_getdata_retry() {
    // TODO: Should retry getting blocks
}
