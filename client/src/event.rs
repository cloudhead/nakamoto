//! Client events.
#![allow(clippy::manual_range_contains)]
use std::collections::HashSet;
use std::sync::Arc;
use std::{fmt, io, net};

use nakamoto_common::bitcoin::network::constants::ServiceFlags;
use nakamoto_common::bitcoin::{Transaction, Txid};
use nakamoto_common::block::{Block, BlockHash, BlockHeader, Height};
use nakamoto_common::network::Network;
use nakamoto_net::event::Emitter;
use nakamoto_net::Disconnect;
use nakamoto_p2p::fsm;
use nakamoto_p2p::fsm::fees::FeeEstimate;
use nakamoto_p2p::fsm::{Link, PeerId};

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

/// Event emitted by the client, after the "loading" phase is over.
#[derive(Debug, Clone)]
pub enum Event {
    /// Ready to process peer events and start receiving commands.
    /// Note that this isn't necessarily the first event emitted.
    Ready {
        /// The tip of the block header chain.
        tip: Height,
        /// The hash of the tip.
        hash: BlockHash,
        /// The tip of the filter header chain.
        filter_tip: Height,
    },
    /// Peer connected. This is fired when the physical TCP/IP connection
    /// is established. Use [`Event::PeerNegotiated`] to know when the P2P handshake
    /// has completed.
    PeerConnected {
        /// Peer address.
        addr: PeerId,
        /// Connection link.
        link: Link,
    },
    /// Peer disconnected after successful connection.
    PeerDisconnected {
        /// Peer address.
        addr: PeerId,
        /// Reason for disconnection.
        reason: Disconnect<fsm::DisconnectReason>,
    },
    /// Connection was never established and timed out or failed.
    PeerConnectionFailed {
        /// Peer address.
        addr: PeerId,
        /// Connection error.
        error: Arc<io::Error>,
    },
    /// Peer handshake completed. The peer connection is fully functional from this point.
    PeerNegotiated {
        /// Peer address.
        addr: PeerId,
        /// Connection link.
        link: Link,
        /// Peer services.
        services: ServiceFlags,
        /// Peer height.
        height: Height,
        /// Peer user agent.
        user_agent: String,
        /// Negotiated protocol version.
        version: u32,
    },
    /// The best known height amongst connected peers has been updated.
    /// Note that there is no guarantee that this height really exists;
    /// peers don't have to follow the protocol and could send a bogus
    /// height.
    PeerHeightUpdated {
        /// Best block height known.
        height: Height,
    },
    /// A block was added to the main chain.
    BlockConnected {
        /// Block header.
        header: BlockHeader,
        /// Block hash.
        hash: BlockHash,
        /// Height of the block.
        height: Height,
    },
    /// One of the blocks of the main chain was reverted, due to a re-org.
    /// These events will fire from the latest block starting from the tip, to the earliest.
    /// Mark all transactions belonging to this block as *unconfirmed*.
    BlockDisconnected {
        /// Header of the block.
        header: BlockHeader,
        /// Block hash.
        hash: BlockHash,
        /// Height of the block when it was part of the main chain.
        height: Height,
    },
    /// A block has matched one of the filters and is ready to be processed.
    /// This event usually precedes [`Event::TxStatusChanged`] events.
    BlockMatched {
        /// Hash of the matching block.
        hash: BlockHash,
        /// Block header.
        header: BlockHeader,
        /// Block height.
        height: Height,
        /// Transactions in this block.
        transactions: Vec<Transaction>,
    },
    /// Transaction fee rate estimated for a block.
    FeeEstimated {
        /// Block hash of the estimate.
        block: BlockHash,
        /// Block height of the estimate.
        height: Height,
        /// Fee estimate.
        fees: FeeEstimate,
    },
    /// A filter was processed. If it matched any of the scripts in the watchlist,
    /// the corresponding block was scheduled for download, and a [`Event::BlockMatched`]
    /// event will eventually be fired.
    FilterProcessed {
        /// Corresponding block hash.
        block: BlockHash,
        /// Filter height (same as block).
        height: Height,
        /// Whether or not this filter matched any of the watched scripts.
        matched: bool,
        /// Whether or not this filter is valid.
        valid: bool,
    },
    /// The status of a transaction has changed.
    TxStatusChanged {
        /// The Transaction ID.
        txid: Txid,
        /// The new transaction status.
        status: TxStatus,
    },
    /// Compact filters have been synced and processed up to this point and matching blocks have
    /// been fetched.
    ///
    /// If filters have been processed up to the last block in the client's header chain, `height`
    /// and `tip` will be equal.
    Synced {
        /// Height up to which we are synced.
        height: Height,
        /// Block hash up to which we are synced.
        hash: BlockHash,
        /// Tip of our block header chain.
        tip: Height,
    },
}

impl fmt::Display for Event {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready { .. } => {
                write!(fmt, "ready to process events and commands")
            }
            Self::BlockConnected { hash, height, .. } => {
                write!(fmt, "block {} connected at height {}", hash, height)
            }
            Self::BlockDisconnected { hash, height, .. } => {
                write!(fmt, "block {} disconnected at height {}", hash, height)
            }
            Self::BlockMatched { hash, height, .. } => {
                write!(
                    fmt,
                    "block {} ready to be processed at height {}",
                    hash, height
                )
            }
            Self::FeeEstimated { fees, height, .. } => {
                write!(
                    fmt,
                    "transaction median fee rate for block #{} is {} sat/vB",
                    height, fees.median,
                )
            }
            Self::FilterProcessed {
                height, matched, ..
            } => {
                write!(
                    fmt,
                    "filter processed at height {} (match = {})",
                    height, matched
                )
            }
            Self::TxStatusChanged { txid, status } => {
                write!(fmt, "transaction {} status changed: {}", txid, status)
            }
            Self::Synced { height, .. } => write!(fmt, "filters synced up to height {}", height),
            Self::PeerConnected { addr, link } => {
                write!(fmt, "peer {} connected ({:?})", &addr, link)
            }
            Self::PeerConnectionFailed { addr, error } => {
                write!(
                    fmt,
                    "peer connection attempt to {} failed with {}",
                    &addr, error
                )
            }
            Self::PeerHeightUpdated { height } => {
                write!(fmt, "peer height updated to {}", height)
            }
            Self::PeerDisconnected { addr, reason } => {
                write!(fmt, "disconnected from {} ({})", &addr, reason)
            }
            Self::PeerNegotiated {
                addr,
                height,
                services,
                ..
            } => write!(
                fmt,
                "peer {} negotiated with services {} and height {}..",
                addr, services, height
            ),
        }
    }
}

/// Transaction status of a given transaction.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum TxStatus {
    /// This is the initial state of a transaction after it has been announced by the
    /// client.
    Unconfirmed,
    /// Transaction was acknowledged by a peer.
    ///
    /// This is the case when a peer requests the transaction data from us after an inventory
    /// announcement. It does not mean the transaction is considered valid by the peer.
    Acknowledged {
        /// Peer acknowledging the transaction.
        peer: net::SocketAddr,
    },
    /// Transaction was included in a block. This event is fired after
    /// a block from the main chain is scanned.
    Confirmed {
        /// Height at which it was included.
        height: Height,
        /// Hash of the block in which it was included.
        block: BlockHash,
    },
    /// A transaction that was previously confirmed, and is now reverted due to a
    /// re-org. Note that this event can only fire if the originally confirmed tx
    /// is still in memory.
    Reverted,
    /// Transaction was replaced by another transaction, and will probably never
    /// be included in a block. This can happen if an RBF transaction is replaced by one with
    /// a higher fee, or if a transaction is reverted and a conflicting transaction replaces
    /// it. In this case it would be preceded by a [`TxStatus::Reverted`] status.
    Stale {
        /// Transaction replacing the given transaction and causing it to be stale.
        replaced_by: Txid,
        /// Block of the included transaction.
        block: BlockHash,
    },
}

impl fmt::Display for TxStatus {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfirmed => write!(fmt, "transaction is unconfirmed"),
            Self::Acknowledged { peer } => {
                write!(fmt, "transaction was acknowledged by peer {}", peer)
            }
            Self::Confirmed { height, block } => write!(
                fmt,
                "transaction was included in block {} at height {}",
                block, height
            ),
            Self::Reverted => write!(fmt, "transaction has been reverted"),
            Self::Stale { replaced_by, block } => write!(
                fmt,
                "transaction was replaced by {} in block {}",
                replaced_by, block
            ),
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
    filter_tip: (Height, BlockHash),
    /// The height up to which we've processed matching blocks.
    /// This is always going to be lesser or equal to `filter_height`.
    block_tip: (Height, BlockHash),
    /// Filter heights that have been matched, and for which we are awaiting a block to process.
    pending: HashSet<Height>,
}

impl Mapper {
    /// Create a new client event mapper.
    pub fn new(network: Network) -> Self {
        let tip = 0;
        let sync_height = 0;
        let filter_tip = (0, network.genesis_hash());
        let block_tip = (0, network.genesis_hash());
        let pending = HashSet::new();

        Self {
            tip,
            sync_height,
            filter_tip,
            block_tip,
            pending,
        }
    }
}

impl Mapper {
    /// Process protocol event and map it to client event(s).
    pub fn process(&mut self, event: fsm::Event, emitter: &Emitter<Event>) {
        match event {
            fsm::Event::Ready {
                height,
                hash,
                filter_height,
                ..
            } => {
                emitter.emit(Event::Ready {
                    tip: height,
                    hash,
                    filter_tip: filter_height,
                });
            }
            fsm::Event::Peer(fsm::PeerEvent::Connected(addr, link)) => {
                emitter.emit(Event::PeerConnected { addr, link });
            }
            fsm::Event::Peer(fsm::PeerEvent::ConnectionFailed(addr, error)) => {
                emitter.emit(Event::PeerConnectionFailed { addr, error });
            }
            fsm::Event::Peer(fsm::PeerEvent::Negotiated {
                addr,
                link,
                services,
                user_agent,
                height,
                version,
            }) => {
                emitter.emit(Event::PeerNegotiated {
                    addr,
                    link,
                    services,
                    user_agent,
                    height,
                    version,
                });
            }
            fsm::Event::Peer(fsm::PeerEvent::Disconnected(addr, reason)) => {
                emitter.emit(Event::PeerDisconnected { addr, reason });
            }
            fsm::Event::Chain(fsm::ChainEvent::PeerHeightUpdated { height }) => {
                emitter.emit(Event::PeerHeightUpdated { height });
            }
            fsm::Event::Chain(fsm::ChainEvent::Synced(_, height)) => {
                self.tip = height;
            }
            fsm::Event::Chain(fsm::ChainEvent::BlockConnected { header, height }) => {
                emitter.emit(Event::BlockConnected {
                    header,
                    hash: header.block_hash(),
                    height,
                });
            }
            fsm::Event::Chain(fsm::ChainEvent::BlockDisconnected { header, height }) => {
                emitter.emit(Event::BlockDisconnected {
                    header,
                    hash: header.block_hash(),
                    height,
                });
            }
            fsm::Event::Inventory(fsm::InventoryEvent::BlockProcessed {
                block,
                height,
                fees,
            }) => {
                let hash = self.process_block(block, height, emitter);

                if let Some(fees) = fees {
                    emitter.emit(Event::FeeEstimated {
                        block: hash,
                        height,
                        fees,
                    });
                }
            }
            fsm::Event::Inventory(fsm::InventoryEvent::Confirmed {
                transaction,
                height,
                block,
            }) => {
                emitter.emit(Event::TxStatusChanged {
                    txid: transaction.txid(),
                    status: TxStatus::Confirmed { height, block },
                });
            }
            fsm::Event::Inventory(fsm::InventoryEvent::Acknowledged { txid, peer }) => {
                emitter.emit(Event::TxStatusChanged {
                    txid,
                    status: TxStatus::Acknowledged { peer },
                });
            }
            fsm::Event::Filter(fsm::FilterEvent::RescanStarted { .. }) => {
                self.pending.clear();
            }
            fsm::Event::Filter(fsm::FilterEvent::FilterProcessed {
                block,
                height,
                matched,
                valid,
                ..
            }) => {
                self.process_filter(block, height, matched, valid, emitter);
            }
            _ => {}
        }
        assert!(
            self.block_tip <= self.filter_tip,
            "Filters are processed before blocks"
        );
        assert!(
            self.sync_height <= self.filter_tip.0,
            "Filters are processed before we are done"
        );

        // If we have no blocks left to process, we are synced to the height of the last
        // processed filter. Otherwise, we're synced up to the last processed block.
        let (height, hash) = if self.pending.is_empty() {
            self.filter_tip.max(self.block_tip)
        } else {
            self.block_tip
        };

        // Ensure we only broadcast sync events when the sync height has changed.
        if height > self.sync_height {
            debug_assert!(height == self.sync_height + 1);

            self.sync_height = height;

            emitter.emit(Event::Synced {
                height,
                hash,
                tip: self.tip,
            });
        }
    }

    // PRIVATE METHODS /////////////////////////////////////////////////////////

    // TODO: Instead of receiving the block, fetch it if matched.
    fn process_block(
        &mut self,
        block: Block,
        height: Height,
        emitter: &Emitter<Event>,
    ) -> BlockHash {
        let hash = block.block_hash();

        if !self.pending.remove(&height) {
            // Received unexpected block.
            return hash;
        }

        log::debug!("Received block {} at height {}", hash, height);
        debug_assert!(height >= self.block_tip.0);

        self.block_tip = (height, block.block_hash());

        emitter.emit(Event::BlockMatched {
            height,
            hash,
            header: block.header,
            transactions: block.txdata,
        });

        hash
    }

    fn process_filter(
        &mut self,
        block: BlockHash,
        height: Height,
        matched: bool,
        valid: bool,
        emitter: &Emitter<Event>,
    ) {
        debug_assert!(height >= self.filter_tip.0);

        if matched {
            log::debug!("Filter matched for block #{}", height);
            self.pending.insert(height);
        }
        self.filter_tip = (height, block);

        emitter.emit(Event::FilterProcessed {
            height,
            matched,
            valid,
            block,
        });
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

    use nakamoto_common::bitcoin_hashes::Hash;
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

        mock.subscriber
            .broadcast(fsm::Event::Chain(fsm::ChainEvent::Synced(
                chain.last().block_hash(),
                height,
            )));

        for h in birth..=height {
            let matched = heights.contains(&h);
            let block = chain[h as usize].clone();

            mock.subscriber
                .broadcast(fsm::Event::Filter(fsm::FilterEvent::FilterProcessed {
                    block: block.block_hash(),
                    height: h,
                    matched,
                    cached: false,
                    valid: true,
                }));

            if matched {
                mock.subscriber.broadcast(fsm::Event::Inventory(
                    fsm::InventoryEvent::BlockProcessed {
                        block,
                        height: h,
                        fees: None,
                    },
                ));
            }
        }

        for event in subscriber.try_iter() {
            match event {
                Event::BlockMatched { transactions, .. } => {
                    for t in &transactions {
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
                    ..
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

    #[test]
    fn test_tx_status_ordering() {
        assert!(
            TxStatus::Unconfirmed
                < TxStatus::Acknowledged {
                    peer: ([0, 0, 0, 0], 0).into()
                }
        );
        assert!(
            TxStatus::Acknowledged {
                peer: ([0, 0, 0, 0], 0).into()
            } < TxStatus::Confirmed {
                height: 0,
                block: BlockHash::all_zeros(),
            }
        );
        assert!(
            TxStatus::Confirmed {
                height: 0,
                block: BlockHash::all_zeros(),
            } < TxStatus::Reverted
        );
        assert!(
            TxStatus::Reverted
                < TxStatus::Stale {
                    replaced_by: Txid::all_zeros(),
                    block: BlockHash::all_zeros()
                }
        );
    }
}
