#![cfg(test)]
pub mod peer;
pub mod simulator;

use super::*;
use peer::{Peer, PeerDummy};
use simulator::Simulation;

use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::Address;
use bitcoin_hashes::hex::FromHex;

use quickcheck_macros::quickcheck;

use nakamoto_common::p2p::peer::Source;

#[allow(unused_imports)]
use nakamoto_test::logger;
use nakamoto_test::BITCOIN_HEADERS;

use crate::protocol::{connmgr, pingmgr};

fn payload(o: Out) -> Option<(net::SocketAddr, NetworkMessage)> {
    match o {
        Out::Message(a, m) => Some((a, m.payload)),
        _ => None,
    }
}

fn event(o: Out) -> Option<Event> {
    match o {
        Out::Event(e) => Some(e),
        _ => None,
    }
}

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
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
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

    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, rng.clone());
    let mut bob = Peer::new(
        "bob",
        [97, 97, 97, 97],
        network,
        headers,
        vec![],
        rng.clone(),
    );
    assert_eq!(bob.protocol.tree.height(), height as Height);

    let mut simulator = Simulation::new(time, rng);

    simulator.connect(&mut alice, &mut bob);
    simulator.run([&mut alice, &mut bob]);

    assert_eq!(alice.protocol.tree.height(), height as Height);
}

/// Test what happens when a peer is idle for too long.
#[test]
fn test_idle_disconnect() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
    let remote = ([241, 19, 44, 18], 8333).into();

    peer.connect_addr(&remote, Link::Outbound);

    // Let a certain amount of time pass.
    peer.time.elapse(pingmgr::PING_INTERVAL);

    peer.tick();
    peer.upstream
        .try_iter()
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
    peer.upstream
        .try_iter()
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
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
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

    peer.upstream
        .try_iter()
        .filter_map(payload)
        .find(|o| matches!(o, (_, NetworkMessage::GetHeaders(_))))
        .expect("a `getheaders` message should be returned");
    peer.upstream
        .try_iter()
        .find(|o| matches!(o, Out::SetTimeout(_)))
        .expect("a timer should be returned");
}

#[test]
fn test_maintain_connections() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let port = network.port();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, rng);

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
            .insert(vec![(0, addr)].into_iter(), Source::Dns);
    }

    // Disconnect peers and expect connections to peers from address book.
    for peer in peers.iter() {
        alice.step(Input::Disconnected(
            *peer,
            DisconnectReason::PeerTimeout("timeout"),
        ));

        let addr = alice
            .upstream
            .try_iter()
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

    let mut alice = Peer::genesis("alice", [49, 40, 43, 40], network, rng);
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
        .upstream
        .try_iter()
        .filter_map(payload)
        .find(|(_, m)| matches!(m, NetworkMessage::GetHeaders(_)))
        .expect("Alice asks the first peer for headers");
    assert_eq!(addr, peers[0]);

    asked.insert(addr);

    // While there's still peers to ask...
    while asked.len() < peers.len() {
        alice.time.elapse(syncmgr::REQUEST_TIMEOUT);
        alice.tick();

        let (addr, _) = alice
            .upstream
            .try_iter()
            .filter_map(payload)
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
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
    let remote = ([131, 31, 11, 33], 11111).into();

    for link in &[Link::Outbound, Link::Inbound] {
        peer.step(Input::Connected {
            addr: remote,
            local_addr: peer.addr,
            link: *link,
        });
        peer.upstream
            .try_iter()
            .find(|o| matches!(o, Out::SetTimeout(_)))
            .expect("a timer should be returned");

        peer.time.elapse(peermgr::HANDSHAKE_TIMEOUT);
        peer.tick();
        peer.upstream.try_iter().find(
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
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
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
        peer.upstream
            .try_iter()
            .find(|o| matches!(o, Out::SetTimeout(_)))
            .expect("a timer should be returned");

        peer.time.elapse(LocalDuration::from_secs(60));
        peer.tick();
        peer.upstream.try_iter().find(
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

    let mut peer = Peer::config([48, 48, 48, 48], vec![], vec![], cfg, rng);
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
    peer.upstream
        .try_iter()
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
    peer.upstream
        .try_iter()
        .filter_map(payload)
        .find(|m| matches!(m, (a, NetworkMessage::Verack) if *a == satoshi.addr))
        .expect("peer should send a 'verack' message back");
}

#[test]
fn test_handshake_initial_messages() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, rng);

    let remote = PeerDummy::new([131, 31, 11, 33], network, 144, ServiceFlags::NETWORK);
    let local = ([0, 0, 0, 0], 0).into();

    // Make sure the address manager trusts this remote address.
    peer.protocol.addrmgr.insert(
        std::iter::once((
            Default::default(),
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

    let msgs = peer
        .upstream
        .try_iter()
        .filter_map(payload)
        .collect::<Vec<_>>();

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
    let mut peer = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
    let remote = PeerDummy::new([131, 31, 11, 33], network, 144, ServiceFlags::NETWORK);

    peer.step(Input::Command(Command::Connect(remote.addr)));
    peer.upstream
        .try_iter()
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
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
    let bob: PeerId = ([241, 19, 44, 18], 8333).into();
    let eve: PeerId = ([241, 19, 44, 19], 8333).into();

    alice.connect_addr(&bob, Link::Outbound);
    alice.connect_addr(&eve, Link::Outbound);

    // Disconnect a peer.
    alice.step(Input::Disconnected(bob, DisconnectReason::Command));

    // We are unable to connect to a new peer because our address book is exhausted.
    alice
        .upstream
        .try_iter()
        .filter_map(event)
        .find(|e| matches!(e, Event::ConnManager(connmgr::Event::AddressBookExhausted)))
        .expect("Alice should emit `AddressBookExhausted`");

    // When we receive a timeout, we fetch new addresses, since our addresses have been exhausted.
    alice.tick();
    alice
        .upstream
        .try_iter()
        .filter_map(payload)
        .find(|msg| matches!(msg, (_, NetworkMessage::GetAddr)))
        .expect("Alice should send `getaddr`");

    // We respond to the `getaddr` with a new peer address, Toto.
    let toto: net::SocketAddr = ([14, 45, 16, 57], 8333).into();
    alice.step(Input::Received(
        eve,
        msg.raw(NetworkMessage::Addr(vec![(
            0,
            Address::new(&toto, ServiceFlags::NETWORK),
        )])),
    ));

    // After some time, Alice tries to connect to the new address.
    alice.time.elapse(connmgr::IDLE_TIMEOUT);
    alice.tick();

    alice
        .upstream
        .try_iter()
        .find(|o| matches!(o, Out::Connect(addr, _) if addr == &toto))
        .expect("Alice tries to connect to Toto");
}

#[test]
fn test_stale_tip() {
    let rng = fastrand::Rng::new();
    let network = Network::Mainnet;
    let msg = message::Builder::new(network);
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
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
        .upstream
        .try_iter()
        .filter_map(payload)
        .find(|(_, msg)| matches!(msg, NetworkMessage::GetHeaders(_)))
        .expect("Alice sends a `getheaders` message");

    // Timeout the request.
    alice.time.elapse(syncmgr::REQUEST_TIMEOUT);
    alice.tick();

    // Some time has passed. The tip timestamp should be considered stale now.
    alice.time.elapse(syncmgr::TIP_STALE_DURATION);
    alice.tick();
    alice
        .upstream
        .try_iter()
        .filter_map(event)
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
        .upstream
        .try_iter()
        .filter_map(event)
        .find(|e| matches!(e, Event::SyncManager(syncmgr::Event::StaleTipDetected(_))))
        .expect("Alice emits a `StaleTipDetected` event");
}

#[quickcheck]
fn prop_addrs(seed: u64) {
    let rng = fastrand::Rng::with_seed(seed);
    let network = Network::Mainnet;
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, rng);
    let bob: PeerId = ([241, 19, 44, 18], 8333).into();

    alice.connect_addr(&bob, Link::Outbound);

    let jak: PeerId = ([88, 13, 16, 59], 8333).into();
    let jim: PeerId = ([99, 45, 180, 58], 8333).into();
    let jon: PeerId = ([14, 48, 141, 57], 8333).into();

    let msg = message::Builder::new(network);

    // Let alice know about these amazing peers.
    alice.step(Input::Received(
        bob,
        msg.raw(NetworkMessage::Addr(vec![
            (0, Address::new(&jak, ServiceFlags::NETWORK)),
            (0, Address::new(&jim, ServiceFlags::NETWORK)),
            (0, Address::new(&jon, ServiceFlags::NETWORK)),
        ])),
    ));

    // Let's query Alice to see if she has these addresses.
    alice.step(Input::Received(bob, msg.raw(NetworkMessage::GetAddr)));
    let (_, msg) = alice
        .upstream
        .try_iter()
        .filter_map(payload)
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
    let mut alice = Peer::config([48, 48, 48, 48], vec![], vec![], config, rng.clone());

    alice.protocol.initialize(alice.time);

    let result = alice
        .upstream
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

    attempted.pop().unwrap();

    alice.time.elapse(connmgr::IDLE_TIMEOUT);
    alice.tick();

    assert!(alice.upstream.try_iter().all(|o| match o {
        Out::Connect(addr, _) => !attempted.contains(&addr),
        _ => true,
    }));
}
