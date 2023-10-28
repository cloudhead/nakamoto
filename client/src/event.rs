//! Client events.
#![allow(clippy::manual_range_contains)]
use std::collections::HashSet;
use std::fmt;

use nakamoto_common::block::{Block, BlockHash, Height};
use nakamoto_net::event::Emitter;
use nakamoto_p2p::fsm;
use nakamoto_p2p::fsm::Event;

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
                write!(fmt, "block header #{} loaded", height)
            }
            Self::FilterHeaderLoaded { height } => {
                write!(fmt, "filter header #{} loaded", height)
            }
            Self::FilterHeaderVerified { height } => {
                write!(fmt, "filter header #{} verified", height)
            }
        }
    }
}

/// Event mapper for client events.
/// Consumes raw state machine events and emits [`Event`].
pub(crate) struct Mapper {
    /// Best height known.
    tip: Height,
    /// The height up to which we've processed filters and matching blocks.
    sync_height: Height,
    /// The height up to which we've processed filters.
    /// This is usually going to be greater than `sync_height`.
    filter_height: Height,
    /// The height up to which we've processed matching blocks.
    /// This is always going to be lesser or equal to `filter_height`.
    block_height: Height,
    /// Filter heights that have been matched, and for which we are awaiting a block to process.
    pending: HashSet<Height>,
}

impl Default for Mapper {
    /// Create a new client event mapper.
    fn default() -> Self {
        let tip = 0;
        let sync_height = 0;
        let filter_height = 0;
        let block_height = 0;
        let pending = HashSet::new();

        Self {
            tip,
            sync_height,
            filter_height,
            block_height,
            pending,
        }
    }
}

impl Mapper {
    /// Process protocol event and map it to client event(s).
    pub fn process(&mut self, event: fsm::Event, emitter: &Emitter<Event>) {
        match event {
            Event::BlockHeadersSynced { height, .. } => {
                self.tip = height;
                emitter.emit(event);
            }
            Event::BlockProcessed {
                block,
                height,
                fees,
            } => {
                let hash = self.process_block(block, height);

                if let Some(fees) = fees {
                    emitter.emit(Event::FeeEstimated {
                        block: hash,
                        height,
                        fees,
                    });
                }
            }
            Event::FilterRescanStarted { start, .. } => {
                self.pending.clear();

                self.filter_height = start;
                self.sync_height = start;
                self.block_height = start;
            }
            Event::FilterProcessed {
                height,
                matched,
                valid: true,
                ..
            } => {
                debug_assert!(height >= self.filter_height);

                if matched {
                    log::debug!("Filter matched for block #{}", height);
                    self.pending.insert(height);
                }
                self.filter_height = height;
                emitter.emit(event);
            }
            other => emitter.emit(other),
        }
        assert!(
            self.block_height <= self.filter_height,
            "Filters are processed before blocks"
        );
        assert!(
            self.sync_height <= self.filter_height,
            "Filters are processed before we are done"
        );

        // If we have no blocks left to process, we are synced to the height of the last
        // processed filter. Otherwise, we're synced up to the last processed block.
        let height = if self.pending.is_empty() {
            self.filter_height
        } else {
            self.block_height
        };

        // Ensure we only broadcast sync events when the sync height has changed.
        if height > self.sync_height {
            self.sync_height = height;

            emitter.emit(Event::Synced {
                height,
                tip: self.tip,
            });
        }
    }

    // PRIVATE METHODS /////////////////////////////////////////////////////////

    // TODO: Instead of receiving the block, fetch it if matched.
    fn process_block(&mut self, block: Block, height: Height) -> BlockHash {
        let hash = block.block_hash();

        if !self.pending.remove(&height) {
            // Received unexpected block.
            return hash;
        }

        log::debug!("Received block {} at height {}", hash, height);
        debug_assert!(height >= self.block_height);

        self.block_height = height;

        hash
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

    use quickcheck::TestResult;
    use quickcheck_macros::quickcheck;

    use nakamoto_common::block::time::Clock as _;
    use nakamoto_common::network::Network;
    use nakamoto_net::{Disconnect, Link, LocalTime, StateMachine as _};
    use nakamoto_test::assert_matches;
    use nakamoto_test::block::gen;

    use super::Event;
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

    #[quickcheck]
    fn prop_client_side_filtering(birth: Height, height: Height, seed: u64) -> TestResult {
        if height < 1 || height > 24 || birth >= height {
            return TestResult::discard();
        }

        let mut rng = fastrand::Rng::with_seed(seed);
        let network = Network::Regtest;
        let genesis = network.genesis_block();
        let chain = gen::blockchain(genesis, height, &mut rng);
        let mut mock = mock::Client::new(network);
        let mut client = mock.handle();

        client.tip = (height, chain[height as usize].header, Default::default());

        let mut spent = 0;
        let (watch, heights, balance) = gen::watchlist_rng(birth, chain.iter(), &mut rng);

        log::debug!(
            "-- Test case with birth = {} and height = {}",
            birth,
            height
        );
        let subscriber = client.events();

        mock.subscriber.broadcast(fsm::Event::BlockHeadersSynced {
            hash: chain.last().block_hash(),
            height,
        });

        for h in birth..=height {
            let matched = heights.contains(&h);
            let block = chain[h as usize].clone();

            mock.subscriber.broadcast(fsm::Event::FilterProcessed {
                block: block.block_hash(),
                height: h,
                matched,
                cached: false,
                valid: true,
            });

            if matched {
                mock.subscriber
                    .broadcast(fsm::Event::BlockMatched { block, height: h });
            }
        }

        for event in subscriber.try_iter() {
            match event {
                Event::BlockMatched { block, .. } => {
                    for t in &block.txdata {
                        for output in &t.output {
                            if watch.contains(&output.script_pubkey) {
                                spent += output.value;
                            }
                        }
                    }
                }
                Event::Synced {
                    height: sync_height,
                    tip,
                } => {
                    assert_eq!(height, tip);

                    if sync_height == tip {
                        break;
                    }
                }
                _ => {}
            }
        }
        assert_eq!(balance, spent);
        client.shutdown().unwrap();

        TestResult::passed()
    }
}
