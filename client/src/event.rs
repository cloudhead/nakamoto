//! Client events.
#![allow(clippy::manual_range_contains)]
use std::fmt;

use nakamoto_common::block::Height;

/// Event emitted by the client during the "loading" phase.
#[derive(Clone, Debug)]
pub enum Loading {
    /// A block header was loaded from the store.
    /// This event only fires during startup.
    BlockHeaderLoaded {
        /// Height of loaded block.
        height: Height,
    },
    /// A filter header was loaded from the store.
    /// This event only fires during startup.
    FilterHeaderLoaded {
        /// Height of loaded filter header.
        height: Height,
    },
    /// A filter header was verified.
    /// This event only fires during startup.
    FilterHeaderVerified {
        /// Height of verified filter header.
        height: Height,
    },
}

impl fmt::Display for Loading {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockHeaderLoaded { height } => {
                write!(fmt, "Block header #{} loaded", height)
            }
            Self::FilterHeaderLoaded { height } => {
                write!(fmt, "Filter header #{} loaded", height)
            }
            Self::FilterHeaderVerified { height } => {
                write!(fmt, "Filter header #{} verified", height)
            }
        }
    }
}

#[cfg(test)]
mod test {
    //! Properties of the [`client::Client`] we'd like to test.
    //!
    //! 1. The final output is invariant to the order in which `block` and `cfilter` messages are
    //!    received.
    //!
    //!    Rationale: Blocks and compact filters are often fetched from multiple peers in parallel.
    //!    Hence, it's important that the system be able to handle out-of-order receipt of this data,
    //!    and that it not affect the final outcome, eg. the balance of the UTXOs.
    //!
    //! 2. The final output is invariant to the granularity of the the filter header chain updates.
    //!
    //!    Rationale: Filter header updates are received via the `cfheaders` message. These messages
    //!    can carry anywhere between 1 and [`nakamoto_p2p::protocol::cbfmgr::MAX_MESSAGE_CFHEADERS`]
    //!    headers. The system should handle many small messages the same way as it handles a few
    //!    large ones.
    //!
    //! 3. The final output is invariant to chain re-orgs.
    //!
    //!    Rationale: Chain re-organizations happen, and filters can be invalidated. The final output
    //!    of the system should always match the main chain at any given point in time.
    //!
    //! 4. The final output is always a function of the input.
    //!
    //!    Rationale: Irrespective to how the system converges towards its final state, the final output
    //!    should always match the given input.
    //!
    //! 5. The commands `watch_address`, `unwatch_address`, `watch_scripts`, `unwatch_scripts`,
    //!    `submit` are idempotent.
    //!
    //! 6. The `rescan` command is always a no-op if the start of the range is equal or greater
    //!    than the current synced height plus one.
    //!
    //!    Rationale: Any re-scans for future blocks are equivalent to the default behavior of
    //!    scanning incoming blocks as they come.
    //!
    //! 7. The system is *injective*, in the sense that for every input there is a unique, distinct
    //!    output.
    //!
    use std::io;

    use nakamoto_common::block::time::Clock as _;
    use nakamoto_common::network::Network;
    use nakamoto_net::{Disconnect, Link, LocalTime, StateMachine as _};
    use nakamoto_p2p::fsm;
    use nakamoto_p2p::Event;
    use nakamoto_test::assert_matches;

    use super::*;

    use crate::handle::Handle as _;
    use crate::tests::mock;
    use crate::Command;

    #[test]
    fn test_ready_event() {
        let network = Network::Regtest;
        let mut client = mock::Client::new(network);
        let handle = client.handle();
        let events = handle.events();
        let time = LocalTime::now();

        client.protocol.initialize(time);
        client.step();

        assert_matches!(events.try_recv(), Ok(Event::Initializing));
        assert_matches!(events.try_recv(), Ok(Event::Ready { .. }));
    }

    #[test]
    fn test_peer_connected_disconnected() {
        let network = Network::Regtest;
        let mut client = mock::Client::new(network);
        let handle = client.handle();
        let remote = ([44, 44, 44, 44], 8333).into();
        let local_addr = ([0, 0, 0, 0], 16333).into();
        let events = handle.events();

        client
            .protocol
            .connected(remote, &local_addr, Link::Inbound);
        client.step();

        assert_matches!(
            events.try_recv(),
            Ok(Event::PeerConnected { addr, link, .. })
            if addr == remote && link == Link::Inbound
        );

        client.protocol.disconnected(
            &remote,
            Disconnect::ConnectionError(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
        );
        client.step();

        assert_matches!(
            events.try_recv(),
            Ok(Event::PeerDisconnected { addr, reason: Disconnect::ConnectionError(_) })
            if addr == remote
        );
    }

    #[test]
    fn test_peer_connection_failed() {
        let network = Network::Regtest;
        let mut client = mock::Client::new(network);
        let handle = client.handle();
        let remote = ([44, 44, 44, 44], 8333).into();
        let events = handle.events();

        client.protocol.command(Command::Connect(remote));
        client.protocol.attempted(&remote);
        client.step();

        assert_matches!(events.try_recv(), Err(_));

        client.protocol.disconnected(
            &remote,
            Disconnect::ConnectionError(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
        );
        client.step();

        assert_matches!(
            events.try_recv(),
            Ok(Event::PeerConnectionFailed { addr, error })
            if addr == remote && error.kind() == io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn test_peer_height_updated() {
        use nakamoto_common::bitcoin::network::address::Address;
        use nakamoto_common::bitcoin::network::constants::ServiceFlags;
        use nakamoto_common::bitcoin::network::message::NetworkMessage;
        use nakamoto_common::bitcoin::network::message_network::VersionMessage;

        let network = Network::default();
        let mut client = mock::Client::new(network);
        let handle = client.handle();
        let remote = ([44, 44, 44, 44], 8333).into();
        let local_time = LocalTime::now();
        let local_addr = ([0, 0, 0, 0], 16333).into();
        let events = handle.events();

        let version = |height: Height| -> NetworkMessage {
            NetworkMessage::Version(VersionMessage {
                version: fsm::MIN_PROTOCOL_VERSION,
                services: ServiceFlags::NETWORK,
                timestamp: local_time.block_time() as i64,
                receiver: Address::new(&remote, ServiceFlags::NONE),
                sender: Address::new(&local_addr, ServiceFlags::NONE),
                nonce: 42,
                user_agent: "?".to_owned(),
                start_height: height as i32,
                relay: false,
            })
        };

        client
            .protocol
            .connected(remote, &local_addr, Link::Inbound);
        client.step();
        client.received(&remote, version(42));
        client.received(&remote, NetworkMessage::Verack);
        client.step();

        events
            .try_iter()
            .find(|e| matches!(e, Event::PeerHeightUpdated { height } if *height == 42))
            .expect("We receive an event for the updated peer height");

        let remote = ([45, 45, 45, 45], 8333).into();

        client
            .protocol
            .connected(remote, &local_addr, Link::Inbound);
        client.step();
        client.received(&remote, version(43));
        client.received(&remote, NetworkMessage::Verack);
        client.step();

        events
            .try_iter()
            .find(|e| matches!(e, Event::PeerHeightUpdated { height } if *height == 43))
            .expect("We receive an event for the updated peer height");
    }

    #[test]
    fn test_peer_negotiated() {
        use nakamoto_common::bitcoin::network::address::Address;
        use nakamoto_common::bitcoin::network::constants::ServiceFlags;
        use nakamoto_common::bitcoin::network::message::NetworkMessage;
        use nakamoto_common::bitcoin::network::message_network::VersionMessage;

        let network = Network::default();
        let mut client = mock::Client::new(network);
        let handle = client.handle();
        let remote = ([44, 44, 44, 44], 8333).into();
        let local_time = LocalTime::now();
        let local_addr = ([0, 0, 0, 0], 16333).into();
        let events = handle.events();

        client
            .protocol
            .connected(remote, &local_addr, Link::Inbound);
        client.step();

        let version = NetworkMessage::Version(VersionMessage {
            version: fsm::MIN_PROTOCOL_VERSION,
            services: ServiceFlags::NETWORK,
            timestamp: local_time.block_time() as i64,
            receiver: Address::new(&remote, ServiceFlags::NONE),
            sender: Address::new(&local_addr, ServiceFlags::NONE),
            nonce: 42,
            user_agent: "?".to_owned(),
            start_height: 42,
            relay: false,
        });

        client.received(&remote, version);
        client.received(&remote, NetworkMessage::Verack);
        client.step();

        assert_matches!(events.try_recv(), Ok(Event::PeerConnected { .. }));
        assert_matches!(
            events.try_recv(),
            Ok(Event::PeerNegotiated { addr, height, user_agent, .. })
            if addr == remote && height == 42 && user_agent == "?"
        );
    }
}
