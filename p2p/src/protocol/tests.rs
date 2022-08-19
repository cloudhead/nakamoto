#![cfg(test)]
pub mod peer;

mod simulations;

use std::io;
use std::iter;
use std::net;
use std::ops::Bound;
use std::sync::Arc;

use log::*;
use nakamoto_common::bitcoin::network::message_blockdata::GetHeadersMessage;

use super::{addrmgr, cbfmgr, invmgr, peermgr, pingmgr, syncmgr};
use super::{
    chan, network::Network, output::message, BlockHash, BlockHeader, Command, Config,
    DisconnectReason, Event, HashSet, Height, Io, NetworkMessage, PeerId, RawNetworkMessage,
    ServiceFlags, VersionMessage,
};
use super::{PROTOCOL_VERSION, USER_AGENT};

use peer::{Peer, PeerDummy};

use nakamoto_common::bitcoin::network::message_blockdata::Inventory;
use nakamoto_common::bitcoin::network::message_filter::CFilter;
use nakamoto_common::bitcoin::network::message_filter::{CFHeaders, GetCFHeaders, GetCFilters};
use nakamoto_common::bitcoin::network::Address;
use nakamoto_common::bitcoin_hashes::hex::FromHex;
use nakamoto_common::block::time::Clock as _;
use nakamoto_net::simulator::{Options, Peer as _, Simulation};
use nakamoto_net::{Link, LocalDuration, LocalTime, Protocol as _};

use quickcheck_macros::quickcheck;

use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_chain::store::Genesis;

use nakamoto_common::block::filter::FilterHeader;
use nakamoto_common::block::time::{AdjustedTime, RefClock};
use nakamoto_common::block::tree::BlockReader as _;
use nakamoto_common::collections::HashMap;
use nakamoto_common::nonempty::NonEmpty;
use nakamoto_common::p2p::peer::KnownAddress;
use nakamoto_common::p2p::peer::Source;

use nakamoto_test::arbitrary;
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
    RefClock<AdjustedTime<PeerId>>,
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
    // Ensures alice has to request multiple batches of headers from bob.
    bob.protocol.syncmgr.config.max_message_headers = 10;

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
    peer.elapse(pingmgr::PING_INTERVAL);
    peer.messages(&remote)
        .find(|o| matches!(o, NetworkMessage::Ping(_)))
        .expect("`ping` is sent");

    // More time passes, and the remote doesn't `pong` back.
    peer.elapse(pingmgr::PING_TIMEOUT);
    // Peer now decides to disconnect remote.
    peer.outputs()
        .find(|o| matches!(o, Io::Disconnect(addr, DisconnectReason::PeerTimeout("ping")) if addr == &remote))
        .expect("peer disconnects remote");
}

#[test]
fn test_inv_getheaders() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote: PeerId = ([241, 19, 44, 18], 8333).into();

    // Some hash for a nonexistent block.
    let hash =
        BlockHash::from_hex("0000000000b7b2c71f2a345e3a4fc328bf5bbb436012afca590b1a11466e2206")
            .unwrap();

    peer.connect_addr(&remote, Link::Outbound);
    peer.received(remote, NetworkMessage::Inv(vec![Inventory::Block(hash)]));

    peer.messages(&remote)
        .find(|o| matches!(o, NetworkMessage::GetHeaders(_)))
        .expect("a `getheaders` message should be returned");
    peer.outputs()
        .find(|o| matches!(o, Io::Wakeup(_)))
        .expect("a timer should be returned");
}

#[test]
fn test_bad_magic() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote: PeerId = ([241, 19, 44, 18], 8333).into();

    peer.connect_addr(&remote, Link::Outbound);
    peer.protocol.received(
        &remote,
        RawNetworkMessage {
            magic: 999,
            payload: NetworkMessage::Ping(1),
        },
    );

    peer.outputs()
        .find(|o| matches!(o, Io::Disconnect(addr, DisconnectReason::PeerMagic(999)) if addr == &remote))
        .expect("peer should be disconnected");
}

#[test]
fn test_maintain_connections() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let port = network.port();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let time = alice.local_time().block_time();

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
        alice.protocol.peermgr.is_connected(peer);
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
        alice.disconnected(peer, DisconnectReason::PeerTimeout("timeout").into());

        let addr = alice
            .outputs()
            .find_map(|o| match o {
                Io::Connect(addr) => Some(addr),
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
        alice.connect(
            &PeerDummy {
                addr: *peer,
                height: 0, // Make sure not to trigger a sync.
                protocol_version: PROTOCOL_VERSION,
                services: syncmgr::REQUIRED_SERVICES,
                relay: true,
                time: alice.local_time(),
            },
            Link::Outbound,
        );
    }
    assert_eq!(
        alice.protocol.syncmgr.best_height().unwrap(),
        alice.protocol.tree.height()
    );

    // Peers that have been asked.
    let mut asked = HashSet::new();

    // Trigger a `getheaders` by sending an inventory message to Alice.
    alice.received(peers[0], NetworkMessage::Inv(vec![Inventory::Block(hash)]));

    // The first time we ask for headers, we ask the peer who sent us the `inv` message.
    alice
        .messages(&peers[0])
        .find(|m| {
            matches!(
                m,
                NetworkMessage::GetHeaders(GetHeadersMessage { stop_hash, .. })
                if stop_hash == &hash
            )
        })
        .expect("Alice asks the first peer for headers");

    asked.insert(peers[0]);

    // While there's still peers to ask...
    while asked.len() < peers.len() {
        alice.elapse(syncmgr::REQUEST_TIMEOUT);

        let addr = peers
            .iter()
            .find(|peer| {
                alice.messages(peer).any(|m| {
                    matches!(
                        m,
                        NetworkMessage::GetHeaders(GetHeadersMessage { stop_hash, .. })
                        if stop_hash == hash
                    )
                })
            })
            .expect("Alice asks the next peer for headers");

        assert!(
            !asked.contains(addr),
            "Alice shouldn't ask the same peer twice"
        );
        asked.insert(*addr);
    }
}

#[test]
fn test_handshake_version_timeout() {
    let network = Network::Mainnet;
    let remote = ([131, 31, 11, 33], 11111).into();
    let rng = fastrand::Rng::new();
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);

    logger::init(Level::Debug);

    peer.init();

    for link in &[Link::Outbound, Link::Inbound] {
        if link.is_outbound() {
            peer.protocol.peermgr.connect(&remote);
        }
        peer.protocol.connected(remote, &peer.addr, *link);
        peer.outputs()
            .find(|o| matches!(o, Io::Wakeup(_)))
            .expect("a timer should be returned");

        peer.elapse(peermgr::HANDSHAKE_TIMEOUT);
        peer.outputs()
            .find(|o| {
                matches!(o, Io::Disconnect(a, DisconnectReason::PeerTimeout("handshake")) if a == &remote)
            })
            .expect("peer should disconnect when no `version` is received");

        peer.disconnected(&remote, DisconnectReason::PeerTimeout("test").into());
    }
}

#[test]
fn test_handshake_verack_timeout() {
    let network = Network::Mainnet;
    let rng = fastrand::Rng::new();
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote = PeerDummy::new([131, 31, 11, 33], network, 144, ServiceFlags::NETWORK);

    peer.init();

    for link in &[Link::Outbound, Link::Inbound] {
        if link.is_outbound() {
            peer.protocol.peermgr.connect(&remote.addr);
        }
        peer.protocol.connected(remote.addr, &peer.addr, *link);
        peer.received(
            remote.addr,
            NetworkMessage::Version(remote.version(peer.addr, 0)),
        );
        peer.outputs()
            .find(|o| matches!(o, Io::Wakeup(_)))
            .expect("a timer should be returned");

        peer.elapse(LocalDuration::from_secs(60));
        peer.outputs()
            .find(|o| {
                matches!(o, Io::Disconnect(a, DisconnectReason::PeerTimeout("handshake")) if a == &remote.addr)
            })
            .expect("peer should disconnect if no `verack` is received");

        peer.disconnected(&remote.addr, DisconnectReason::PeerTimeout("verack").into());
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

    peer.protocol
        .connected(craig.addr, &peer.addr, Link::Inbound);
    peer.received(
        craig.addr,
        NetworkMessage::Version(VersionMessage {
            user_agent: "/craig:0.1.0/".to_owned(),
            ..craig.version(peer.addr, 0)
        }),
    );
    peer.outputs()
        .find(|o| matches!(o, Io::Disconnect(a, _) if a == &craig.addr))
        .expect("peer should disconnect when the 'on_version' hook returns an error");

    peer.protocol
        .connected(satoshi.addr, &peer.addr, Link::Inbound);
    peer.received(
        satoshi.addr,
        NetworkMessage::Version(VersionMessage {
            user_agent: "satoshi".to_owned(),
            ..satoshi.version(peer.addr, 0)
        }),
    );
    peer.messages(&satoshi.addr)
        .find(|m| matches!(m, NetworkMessage::Verack))
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
    peer.init();
    peer.protocol.addrmgr.insert(
        std::iter::once((
            peer.local_time().block_time(),
            Address::new(&remote.addr, ServiceFlags::NETWORK),
        )),
        Source::Dns,
    );

    // Handshake
    peer.protocol.peermgr.connect(&remote.addr);
    peer.connected(remote.addr, &local, Link::Outbound);
    peer.received(
        remote.addr,
        NetworkMessage::Version(remote.version(local, 0)),
    );
    peer.received(remote.addr, NetworkMessage::Verack);

    let msgs = peer.messages(&remote.addr).collect::<Vec<_>>();

    assert!(msgs.contains(&NetworkMessage::SendHeaders));
    assert!(msgs.contains(&NetworkMessage::GetAddr));
    assert!(msgs
        .iter()
        .any(|msg| matches!(msg, NetworkMessage::Ping(_))));
}

#[test]
fn test_connection_error() {
    let network = Network::Mainnet;
    let rng = fastrand::Rng::new();
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote = PeerDummy::new([131, 31, 11, 33], network, 144, ServiceFlags::NETWORK);

    peer.command(Command::Connect(remote.addr));
    peer.outputs()
        .find(|o| matches!(o, Io::Connect(addr) if addr == &remote.addr))
        .expect("Alice should try to connect to remote");
    peer.attempted(&remote.addr);
    // Make sure we can handle a disconnection before an established connection.
    peer.disconnected(
        &remote.addr,
        nakamoto_net::DisconnectReason::ConnectionError(
            io::Error::from(io::ErrorKind::UnexpectedEof).into(),
        ),
    );
}

#[test]
fn test_getaddr() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let bob: PeerId = ([241, 19, 44, 18], 8333).into();
    let eve: PeerId = ([241, 19, 44, 19], 8333).into();

    alice.connect_addr(&bob, Link::Outbound);
    alice.connect_addr(&eve, Link::Outbound);

    // Disconnect a peer.
    alice.disconnected(&bob, DisconnectReason::Command.into());

    // Alice should now fetch new addresses, but she won't find any and the requests will time out.
    alice.elapse(addrmgr::REQUEST_TIMEOUT);

    // Alice is unable to connect to a new peer because our address book is exhausted.
    alice
        .events()
        .find(|e| matches!(e, Event::Address(addrmgr::Event::AddressBookExhausted)))
        .expect("Alice should emit `AddressBookExhausted`");

    // When we receive a timeout, we fetch new addresses, since our addresses have been exhausted.
    alice.tock();
    alice
        .messages(&eve)
        .find(|msg| matches!(msg, NetworkMessage::GetAddr))
        .expect("Alice should send `getaddr`");

    // We respond to the `getaddr` with a new peer address, Toto.
    let toto: net::SocketAddr = ([14, 45, 16, 57], 8333).into();
    alice.received(
        eve,
        NetworkMessage::Addr(vec![(
            alice.local_time().block_time(),
            Address::new(&toto, ServiceFlags::NETWORK),
        )]),
    );

    // After some time, Alice tries to connect to the new address.
    alice.elapse(peermgr::IDLE_TIMEOUT);

    alice
        .outputs()
        .find(|o| matches!(o, Io::Connect(addr) if addr == &toto))
        .expect("Alice tries to connect to Toto");
}

#[test]
fn test_stale_tip() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng);
    let remote: PeerId = ([33, 33, 33, 33], network.port()).into();
    let headers = &BITCOIN_HEADERS;

    alice.connect_addr(&remote, Link::Outbound);
    alice.received(
        remote,
        // Receive an unsolicited header announcement for the latest.
        NetworkMessage::Headers(vec![*headers
            .get(alice.protocol.tree.height() as usize + 1)
            .unwrap()]),
    );
    alice
        .messages(&remote)
        .find(|msg| matches!(msg, NetworkMessage::GetHeaders(_)))
        .expect("Alice sends a `getheaders` message");

    // Timeout the request.
    alice.elapse(syncmgr::REQUEST_TIMEOUT);

    // Some time has passed. The tip timestamp should be considered stale now.
    alice.elapse(syncmgr::TIP_STALE_DURATION);
    alice
        .events()
        .find(|e| matches!(e, Event::Chain(syncmgr::Event::StaleTip(_))))
        .expect("Alice emits a `StaleTip` event");

    // Timeout the `getheaders` request.
    alice.elapse(syncmgr::REQUEST_TIMEOUT);

    // Now send another header and wait until the chain update is stale.
    alice.received(
        remote,
        NetworkMessage::Headers(vec![*headers
            .get(alice.protocol.tree.height() as usize + 1)
            .unwrap()]),
    );

    // Some more time has passed.
    alice.elapse(syncmgr::TIP_STALE_DURATION);
    // Chain update should be stale this time.
    alice
        .events()
        .find(|e| matches!(e, Event::Chain(syncmgr::Event::StaleTip(_))))
        .expect("Alice emits a `StaleTip` event");
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

    let time = alice.local_time().block_time();

    // Let alice know about these amazing peers.
    alice.received(
        bob,
        NetworkMessage::Addr(vec![
            (time, Address::new(&jak, ServiceFlags::NETWORK)),
            (time, Address::new(&jim, ServiceFlags::NETWORK)),
            (time, Address::new(&jon, ServiceFlags::NETWORK)),
        ]),
    );

    // Let's query Alice to see if she has these addresses.
    alice.received(bob, NetworkMessage::GetAddr);
    let msg = alice
        .messages(&bob)
        .find(|o| matches!(o, NetworkMessage::Addr(_)))
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

    alice.init();

    let result = alice
        .outputs()
        .filter(|o| matches!(o, Io::Connect(_)))
        .collect::<Vec<_>>();

    assert_eq!(
        result.len(),
        alice.protocol.peermgr.config.target_outbound_peers
    );

    let mut attempted: Vec<net::SocketAddr> = result
        .into_iter()
        .map(|r| match r {
            Io::Connect(addr) => addr,
            _ => panic!(),
        })
        .collect();

    rng.shuffle(&mut attempted);

    // ... after a while, the connections time out.

    attempted.pop().unwrap();

    alice.elapse(peermgr::IDLE_TIMEOUT);

    assert!(alice.outputs().all(|o| match o {
        Io::Connect(addr) => !attempted.contains(&addr),
        _ => true,
    }));
}

#[test]
fn test_connect_to_peers() {
    quickcheck::QuickCheck::new()
        .tests(100)
        .max_tests(1000)
        .min_tests_passed(95) // 95% success rate.
        .quickcheck(
            simulations::connect_to_peers as fn(Options, u64, arbitrary::InRange<1, 6>) -> bool,
        );
}

#[test]
fn test_connect_to_peers_1() {
    assert!(simulations::connect_to_peers(
        Options {
            latency: 0..3,
            failure_rate: 0.1294790448987514,
        },
        5190880195044658821,
        arbitrary::InRange(8)
    ));
}

#[test]
fn test_connect_to_peers_2() {
    assert!(simulations::connect_to_peers(
        Options {
            latency: 0..3,
            failure_rate: 0.1391942598336996,
        },
        4237581564267684273,
        arbitrary::InRange(8)
    ));
}

#[test]
fn test_connect_to_peers_3() {
    assert!(simulations::connect_to_peers(
        Options {
            latency: 0..3,
            failure_rate: 0.1070592131461427
        },
        18131621610609499524,
        arbitrary::InRange(4)
    ));
}

#[test]
#[ignore]
// TODO: This test fails.
fn test_connect_to_peers_4() {
    assert!(simulations::connect_to_peers(
        Options {
            latency: 1..3,
            failure_rate: 0.18729837247381553
        },
        714649005678913971,
        arbitrary::InRange(8)
    ));
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
        protocol_version: PROTOCOL_VERSION,
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
    assert!(
        alice
            .protocol
            .peermgr
            .negotiated(Link::Outbound)
            .map(|(p, _)| p)
            .next()
            .unwrap()
            .relay
    );

    let (transmit, receive) = chan::bounded(1);
    let tx = gen::transaction(&mut rng);
    let wtxid = tx.txid();
    let inventory = vec![Inventory::Transaction(wtxid)];
    alice.connect(&remote2, Link::Outbound);
    alice.command(Command::SubmitTransaction(tx.clone(), transmit));

    let remotes = receive.recv().unwrap().unwrap();
    assert_eq!(Vec::from(remotes), vec![remote1.addr]);
    assert!(alice.protocol.invmgr.contains(&tx.wtxid()));

    alice.tock();
    alice
        .messages(&remote1.addr)
        .find(|msg| msg == &NetworkMessage::Inv(inventory.clone()))
        .expect("Alice sends an `inv` message");

    alice.elapse(LocalDuration::from_secs(30));
    alice.received(remote1.addr, NetworkMessage::GetData(inventory));
    alice
        .messages(&remote1.addr)
        .find(|msg| matches!(msg, NetworkMessage::Tx(_)))
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
    alice.command(Command::SubmitTransaction(tx1, transmit.clone()));
    alice.command(Command::SubmitTransaction(tx2, transmit));
    alice.tock(); // Broadcasting doesn't happen immediately
    alice
        .messages(&remote1)
        .find(|m| {
            matches! {
                m, NetworkMessage::Inv(inv)
                if inv.len() == 2
            }
        })
        .expect("Alice sends an initial `inv`");

    alice.outputs().count(); // Drain outputs
    alice.connect_addr(&remote2, Link::Outbound); // A new peer connects
    alice.tock(); // No delay until invs are sent to it
    alice
        .messages(&remote2)
        .find(|m| matches!(m, NetworkMessage::Inv(_)))
        .expect("Alice sends a first `inv` message to the new peer");

    // Let some time pass.
    alice.drain();
    alice.elapse(LocalDuration::from_mins(5));

    alice
        .messages(&remote1)
        .find(|m| matches!(m, NetworkMessage::Inv(_)))
        .expect("Alice sends a second `inv` message to the first peer");

    alice
        .messages(&remote2)
        .find(|m| matches!(m, NetworkMessage::Inv(_)))
        .expect("Alice sends a second `inv` message to the second peer");

    // Let some more time pass.
    alice.elapse(LocalDuration::from_mins(5));
    alice
        .messages(&remote1)
        .find(|m| {
            matches! {
                m, NetworkMessage::Inv(_)
            }
        })
        .expect("Alice sends a third `inv` message to the first peer");
}

#[test]
fn test_inv_partial_broadcast() {
    let network = Network::Mainnet;

    let mut rng = fastrand::Rng::new();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, vec![], rng.clone());

    let remote1 = ([88, 88, 88, 88], 8333).into();
    let remote2 = ([99, 99, 99, 99], 8333).into();
    let tx1 = gen::transaction(&mut rng);
    let tx2 = gen::transaction(&mut rng);
    let (transmit, _) = chan::unbounded();

    alice.connect_addr(&remote1, Link::Outbound);
    alice.connect_addr(&remote2, Link::Outbound);
    alice.command(Command::SubmitTransaction(tx1.clone(), transmit.clone()));
    alice.command(Command::SubmitTransaction(tx2.clone(), transmit));
    alice.tock();

    // The first peer asks only for the first inventory item.
    alice.elapse(LocalDuration::from_secs(3));
    alice.received(
        remote1,
        NetworkMessage::GetData(vec![Inventory::Transaction(tx1.txid())]),
    );
    // The second peer asks only for the second inventory item.
    alice.received(
        remote2,
        NetworkMessage::GetData(vec![Inventory::Transaction(tx2.txid())]),
    );

    alice
        .messages(&remote1)
        .find(|msg| {
            if let NetworkMessage::Tx(tx) = msg {
                return tx.txid() == tx1.txid();
            }
            false
        })
        .expect("Alice responds with only the requested inventory to peer#1");
    alice
        .messages(&remote2)
        .find(|msg| {
            if let NetworkMessage::Tx(tx) = msg {
                return tx.txid() == tx2.txid();
            }
            false
        })
        .expect("Alice responds with only the requested inventory to peer#2");

    // Time passes.
    alice.drain();
    alice.elapse(LocalDuration::from_mins(5));

    alice
        .messages(&remote1)
        .find(|m| {
            matches! {
                m, NetworkMessage::Inv(inv)
                if inv.first() == Some(&Inventory::Transaction(tx2.txid()))
            }
        })
        .expect("Alice re-sends the missing inv to peer#1");
    alice
        .messages(&remote2)
        .find(|m| {
            matches! {
                m, NetworkMessage::Inv(inv)
                if inv.first() == Some(&Inventory::Transaction(tx1.txid()))
            }
        })
        .expect("Alice re-sends the missing inv to peer#2");

    // Now the peers ask for the remaining inventories.
    alice.received(
        remote1,
        NetworkMessage::GetData(vec![Inventory::Transaction(tx2.txid())]),
    );
    alice.received(
        remote2,
        NetworkMessage::GetData(vec![Inventory::Transaction(tx1.txid())]),
    );

    // More time passes.
    alice.drain();
    alice.elapse(LocalDuration::from_mins(5));

    assert_eq!(
        alice
            .messages(&remote1)
            .filter(|m| matches!(m, NetworkMessage::Inv(_)))
            .count(),
        0,
        "Alice has nothing more to send"
    );
    assert_eq!(
        alice
            .messages(&remote2)
            .filter(|m| matches!(m, NetworkMessage::Inv(_)))
            .count(),
        0,
        "Alice has nothing more to send"
    );
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
    alice.command(Command::SubmitTransaction(tx1.clone(), transmit.clone()));
    alice.command(Command::SubmitTransaction(tx2.clone(), transmit));
    alice.tock();

    assert!(alice.protocol.invmgr.contains(&tx1.wtxid()));
    assert!(alice.protocol.invmgr.contains(&tx2.wtxid()));

    alice.protocol.invmgr.get_block(blk1.block_hash());
    alice.protocol.invmgr.get_block(blk2.block_hash());

    alice.tock();
    alice.received(remote, NetworkMessage::Block(blk2.clone()));
    alice
        .events()
        .find(|e| matches!(e, Event::Inventory(invmgr::Event::BlockReceived { .. })))
        .expect("Alice receives the 2nd block");

    alice.elapse(LocalDuration::from_mins(1));
    alice.received(remote, NetworkMessage::Block(blk1.clone()));

    let mut events = alice.events().filter_map(|e| {
        if let Event::Inventory(event) = e {
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

    events
        .find(|e| {
            matches!(
                e, invmgr::Event::BlockProcessed { block, .. }
                if block.block_hash() == blk2.block_hash()
            )
        })
        .expect("Alice is done processing the second block");

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

    // Set Alice's clock to the last block.
    alice.tick(LocalTime::from_block_time(chain.last().header.time));

    // Connect to a peer and submit the transaction.
    alice.connect(
        &PeerDummy {
            addr: remote,
            height,
            protocol_version: PROTOCOL_VERSION,
            services: cbfmgr::REQUIRED_SERVICES | syncmgr::REQUIRED_SERVICES,
            relay: true,
            time: alice.local_time(),
        },
        Link::Outbound,
    );

    // Start a rescan, to make sure we catch the transaction when it's confirmed.
    alice.command(Command::Rescan {
        from: Bound::Unbounded, // Start scanning from the current height.
        to: Bound::Unbounded,   // Keep scanning forever.
        watch: vec![],          // Submitted transactions are tracked automatically.
    });
    alice.command(Command::SubmitTransaction(tx.clone(), transmit));
    alice.tock();

    assert!(alice.protocol.invmgr.contains(&tx.wtxid()));

    // The next block will have the matching transaction.
    let matching = gen::block_with(&chain.last().header, vec![tx.clone()], &mut rng);
    let cfilter = gen::cfilter(&matching);
    let (_, parent) = cfheaders.last().unwrap();
    let (cfhash, _) = gen::cfheader(parent, &cfilter);

    // Alice receives a header announcement.
    alice.received(remote, NetworkMessage::Headers(vec![matching.header]));

    alice
        .messages(&remote)
        .find(|m| matches!(m, NetworkMessage::GetCFHeaders(_)))
        .expect("Alice asks for the matching cfheaders");

    // Alice receives the cfheaders.
    alice.received(
        remote,
        NetworkMessage::CFHeaders(CFHeaders {
            filter_type,
            stop_hash: matching.block_hash(),
            previous_filter_header: *parent,
            filter_hashes: vec![cfhash],
        }),
    );

    alice
        .messages(&remote)
        .find(|m| matches!(m, NetworkMessage::GetCFilters(_)))
        .expect("Alice asks for the cfilter");

    // Alice receives the cfilter, which we expect to match.
    alice.received(
        remote,
        NetworkMessage::CFilter(CFilter {
            filter_type,
            block_hash: matching.block_hash(),
            filter: cfilter.content,
        }),
    );

    // Alice asks for the corresponding block.
    let expected = vec![Inventory::Block(matching.block_hash())];

    alice.tock();
    alice
        .messages(&remote)
        .find(|m| matches!(m, NetworkMessage::GetData(data) if data == &expected))
        .expect("Alice asks for the matching block");
    alice.received(remote, NetworkMessage::Block(matching));

    assert!(alice.protocol.invmgr.is_empty(), "The mempool is empty");
    assert!(
        !alice.protocol.cbfmgr.unwatch_transaction(&tx.txid()),
        "The transaction is no longer watched"
    );
}

#[test]
fn test_transaction_reverted_reconfirm() {
    let height = 16;
    let mut rng = fastrand::Rng::new();

    logger::init(log::Level::Debug);

    let network = Network::Regtest;
    let remote: PeerId = ([88, 88, 88, 88], 8333).into();
    let genesis = network.genesis_block();
    let chain = gen::blockchain(genesis, height, &mut rng);
    let chain_tip = chain.last();
    let headers = NonEmpty::from_vec(chain.iter().map(|b| b.header).collect()).unwrap();
    let cfheader_genesis = FilterHeader::genesis(network);
    let cfheaders = gen::cfheaders_from_blocks(cfheader_genesis, chain.iter())
        .into_iter()
        .skip(1) // Skip genesis
        .collect::<Vec<_>>();
    let (_, cfheaders_tip) = cfheaders.last().unwrap();
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
            protocol_version: PROTOCOL_VERSION,
            services: cbfmgr::REQUIRED_SERVICES | syncmgr::REQUIRED_SERVICES,
            relay: true,
            time: alice.local_time(),
        },
        Link::Outbound,
    );

    let (submit_reply, _) = chan::bounded(1);

    // Start a rescan, to make sure we catch the transaction when it's confirmed.
    alice.command(Command::Rescan {
        from: Bound::Unbounded, // Start scanning from the current height.
        to: Bound::Unbounded,   // Keep scanning forever.
        watch: vec![],          // Submitted transactions are tracked automatically.
    });
    alice.command(Command::SubmitTransaction(tx.clone(), submit_reply));
    alice.tock();

    // Alice receives the initial shorter chain.
    {
        // The next block will have the matching transaction.
        let matching = gen::block_with(&chain_tip.header, vec![tx.clone()], &mut rng);
        let cfilter = gen::cfilter(&matching);
        let (_, parent) = cfheaders.last().unwrap();
        let (cfhash, _) = gen::cfheader(parent, &cfilter);

        // Set Alice's clock to the last block time.
        alice.tick(LocalTime::from_block_time(chain_tip.header.time));

        // Alice receives a header announcement.
        alice.received(remote, NetworkMessage::Headers(vec![matching.header]));

        // Alice receives the cfheaders.
        alice.received(
            remote,
            NetworkMessage::CFHeaders(CFHeaders {
                filter_type,
                stop_hash: matching.block_hash(),
                previous_filter_header: *parent,
                filter_hashes: vec![cfhash],
            }),
        );
        // Alice receives the cfilter, which we expect to match.
        alice.received(
            remote,
            NetworkMessage::CFilter(CFilter {
                filter_type,
                block_hash: matching.block_hash(),
                filter: cfilter.content,
            }),
        );
        // Alice receives the matching block.
        alice.received(remote, NetworkMessage::Block(matching));
        alice
            .events()
            .find(|e| {
                matches!(
                    e,
                    Event::Inventory(invmgr::Event::Confirmed { transaction, .. })
                    if transaction.txid() == tx.txid()
                )
            })
            .expect("The transaction is confirmed");
    }

    // Alice receives the new, longer fork.
    {
        let fork_trunk = gen::fork(&chain_tip.header, 3, &mut rng);
        let fork_trunk_tip = fork_trunk.last().unwrap().header;
        let fork_matching = gen::block_with(&fork_trunk_tip, vec![tx.clone()], &mut rng);
        let fork = fork_trunk
            .iter()
            .chain(iter::once(&fork_matching))
            .collect::<Vec<_>>();
        let fork_tip = fork.last().unwrap();
        let fork_cfheaders = gen::cfheaders_from_blocks(*cfheaders_tip, fork.iter().cloned())
            .into_iter()
            .collect::<Vec<_>>();
        let fork_cfilters = gen::cfilters(fork.iter().cloned());

        // Alice receives a new header announcement for a longer chain.
        alice.received(
            remote,
            NetworkMessage::Headers(fork.iter().map(|b| b.header).collect()),
        );
        alice
            .events()
            .find(|e| {
                matches!(
                    e,
                    Event::Inventory(invmgr::Event::Reverted { transaction })
                    if transaction.txid() == tx.txid()
                )
            })
            .expect("The transaction is reverted");

        assert!(alice.protocol.invmgr.contains(&tx.wtxid()));

        alice.tock();
        alice
            .messages(&remote)
            .find(|m| {
                matches!(
                    m,
                    NetworkMessage::GetCFHeaders(GetCFHeaders { stop_hash, .. })
                    if stop_hash == &fork_tip.block_hash()
                )
            })
            .expect("Alice asks for the cfheaders");

        alice.received(
            remote,
            NetworkMessage::CFHeaders(CFHeaders {
                filter_type,
                stop_hash: fork_tip.block_hash(),
                previous_filter_header: *cfheaders_tip,
                filter_hashes: fork_cfheaders.iter().map(|(hash, _)| *hash).collect(),
            }),
        );
        alice
            .messages(&remote)
            .find(|m| {
                matches!(
                    m,
                    NetworkMessage::GetCFilters(GetCFilters { stop_hash, .. })
                    if stop_hash == &fork_tip.block_hash()
                )
            })
            .expect("Alice asks for the cfilters");

        for (cfilter, block) in fork_cfilters.into_iter().zip(fork.iter()) {
            alice.received(
                remote,
                NetworkMessage::CFilter(CFilter {
                    filter_type,
                    block_hash: block.block_hash(),
                    filter: cfilter.content,
                }),
            );
        }
        alice
            .events()
            .find(|e| {
                matches!(
                    e,
                    Event::Filter(cbfmgr::Event::FilterProcessed { block, matched: true, .. })
                    if block == &fork_matching.block_hash()
                )
            })
            .expect("The new block is matched");

        alice.received(remote, NetworkMessage::Block(fork_matching.clone()));
        alice
            .events()
            .find(|e| {
                matches!(
                    e,
                    Event::Inventory(invmgr::Event::Confirmed { transaction, block, .. })
                    if transaction.txid() == tx.txid() && block == &fork_matching.block_hash()
                )
            })
            .expect("The transaction is re-confirmed");
    }
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
            Event::Chain(event @ syncmgr::Event::BlockConnected { .. }) => Some(event),
            Event::Chain(event @ syncmgr::Event::BlockDisconnected { .. }) => Some(event),
            Event::Chain(event @ syncmgr::Event::Synced { .. }) => Some(event),
            _ => None,
        })
    }

    alice.tick(LocalTime::from_block_time(headers.last().time));
    alice.init();
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
    alice.received(
        remote,
        NetworkMessage::Inv(vec![Inventory::Block(extra.block_hash())]),
    );
    alice.received(remote, NetworkMessage::Headers(vec![extra.header]));

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
    alice.tick(LocalTime::from_block_time(extra.header.time));
    alice.command(Command::ImportHeaders(fork.tail.clone(), transmit));
    import.recv().unwrap().unwrap();

    let mut events = filter(alice.events());

    // Disconnected events.
    assert_matches!(
        events.next().unwrap(),
        syncmgr::Event::BlockDisconnected { height, header }
        if height == best + 1 && header.block_hash() == extra.block_hash()
    );
    for height_ in (fork_height + 1..=best).rev() {
        let hash_ = headers[height_ as usize].block_hash();

        assert_matches!(
            events.next().unwrap(),
            syncmgr::Event::BlockDisconnected { height, header }
            if height == height_ as Height && header.block_hash() == hash_
        );
    }

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
