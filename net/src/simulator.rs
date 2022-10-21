//! A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
#![allow(clippy::collapsible_if)]

use crate::{DisconnectReason, ReactorDispatch, ConnDirection, LocalDuration, LocalTime};
use log::*;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::{Deref, DerefMut, Range};
use std::{fmt, io, net};

use crate::PeerProtocol;

#[cfg(feature = "quickcheck")]
pub mod arbitrary;

/// Minimum latency between peers.
pub const MIN_LATENCY: LocalDuration = LocalDuration::from_millis(1);
/// Maximum number of events buffered per peer.
pub const MAX_EVENTS: usize = 2048;

/// Identifier for a simulated node/peer.
/// The simulator requires each peer to have a distinct IP address.
type NodeId = net::IpAddr;

/// A simulated peer. Protocol instances have to be wrapped in this type to be simulated.
pub trait Peer<P>: Deref<Target = P> + DerefMut<Target = P> + 'static
where
    P: PeerProtocol,
{
    /// Initialize the peer. This should at minimum initialize the protocol with the
    /// current time.
    fn init(&mut self);
    /// Get the peer address.
    fn addr(&self) -> net::SocketAddr;
}

/// Simulated protocol input.
#[derive(Debug, Clone)]
pub enum Input<M, D> {
    /// Connection attempt underway.
    Connecting {
        /// Remote peer address.
        addr: net::SocketAddr,
    },
    /// New connection with a peer.
    Connected {
        /// Remote peer id.
        addr: net::SocketAddr,
        /// Local peer id.
        local_addr: net::SocketAddr,
        /// Link direction.
        link: ConnDirection,
    },
    /// Disconnected from peer.
    Disconnected(net::SocketAddr, DisconnectReason<D>),
    /// Received a message from a remote peer.
    Received(net::SocketAddr, M),
    /// Used to advance the state machine after some wall time has passed.
    Wake,
}

/// A scheduled protocol input.
#[derive(Debug, Clone)]
pub struct Scheduled<M, D> {
    /// The node for which this input is scheduled.
    pub node: NodeId,
    /// The remote peer from which this input originates.
    /// If the input originates from the local node, this should be set to the zero address.
    pub remote: net::SocketAddr,
    /// The input being scheduled.
    pub input: Input<M, D>,
}

impl<M: fmt::Debug, D: fmt::Display> fmt::Display for Scheduled<M, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.input {
            Input::Received(from, msg) => {
                write!(f, "{} <- {} ({:?})", self.node, from, msg)
            }
            Input::Connected {
                addr,
                local_addr,
                link: ConnDirection::Inbound,
                ..
            } => write!(f, "{} <== {}: Connected", local_addr, addr),
            Input::Connected {
                local_addr,
                addr,
                link: ConnDirection::Outbound,
                ..
            } => write!(f, "{} ==> {}: Connected", local_addr, addr),
            Input::Connecting { addr } => {
                write!(f, "{} => {}: Connecting", self.node, addr)
            }
            Input::Disconnected(addr, reason) => {
                write!(f, "{} =/= {}: Disconnected: {}", self.node, addr, reason)
            }
            Input::Wake => {
                write!(f, "{}: Tock", self.node)
            }
        }
    }
}

/// Inbox of scheduled state machine inputs to be delivered to the simulated nodes.
#[derive(Debug)]
pub struct Inbox<M, D> {
    /// The set of scheduled inputs. We use a `BTreeMap` to ensure inputs are always
    /// ordered by scheduled delivery time.
    messages: BTreeMap<LocalTime, Scheduled<M, D>>,
}

impl<M: Clone, D: Clone> Inbox<M, D> {
    /// Add a scheduled input to the inbox.
    fn insert(&mut self, mut time: LocalTime, msg: Scheduled<M, D>) {
        // Make sure we don't overwrite an existing message by using the same time slot.
        while self.messages.contains_key(&time) {
            time = time + MIN_LATENCY;
        }
        self.messages.insert(time, msg);
    }

    /// Get the next scheduled input to be delivered.
    fn next(&mut self) -> Option<(LocalTime, Scheduled<M, D>)> {
        self.messages
            .iter()
            .next()
            .map(|(time, scheduled)| (*time, scheduled.clone()))
    }

    /// Get the last message sent between two peers. Only checks one direction.
    fn last(
        &self,
        node: &NodeId,
        remote: &net::SocketAddr,
    ) -> Option<(&LocalTime, &Scheduled<M, D>)> {
        self.messages
            .iter()
            .rev()
            .find(|(_, v)| &v.node == node && &v.remote == remote)
    }
}

/// Simulation options.
#[derive(Debug, Clone)]
pub struct Options {
    /// Minimum and maximum latency between nodes, in seconds.
    pub latency: Range<u64>,
    /// Probability that network I/O fails.
    /// A rate of `1.0` means 100% of I/O fails.
    pub failure_rate: f64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            latency: Range::default(),
            failure_rate: 0.,
        }
    }
}

/// A peer-to-peer node simulation.
pub struct Simulation<T>
where
    T: PeerProtocol,
{
    /// Inbox of inputs to be delivered by the simulation.
    inbox: Inbox<<T::PeerMessage as ToOwned>::Owned, T::DisconnectDemand>,
    /// Events emitted during simulation.
    events: BTreeMap<NodeId, VecDeque<T::Notification>>,
    /// Priority events that should happen immediately.
    priority: VecDeque<Scheduled<<T::PeerMessage as ToOwned>::Owned, T::DisconnectDemand>>,
    /// Simulated latencies between nodes.
    latencies: BTreeMap<(NodeId, NodeId), LocalDuration>,
    /// Network partitions between two nodes.
    partitions: BTreeSet<(NodeId, NodeId)>,
    /// Set of existing connections between nodes.
    connections: BTreeMap<(NodeId, NodeId), u16>,
    /// Set of connection attempts.
    attempts: BTreeSet<(NodeId, NodeId)>,
    /// Simulation options.
    opts: Options,
    /// Start time of simulation.
    start_time: LocalTime,
    /// Current simulation time. Updated when a scheduled message is processed.
    time: LocalTime,
    /// RNG.
    rng: fastrand::Rng,
}

impl<T> Simulation<T>
where
    T: PeerProtocol + 'static,
    T::DisconnectDemand: Clone + fmt::Debug + fmt::Display,

    <T::PeerMessage as ToOwned>::Owned: fmt::Debug + Clone,
{
    /// Create a new simulation.
    pub fn new(time: LocalTime, rng: fastrand::Rng, opts: Options) -> Self {
        Self {
            inbox: Inbox {
                messages: BTreeMap::new(),
            },
            events: BTreeMap::new(),
            priority: VecDeque::new(),
            partitions: BTreeSet::new(),
            latencies: BTreeMap::new(),
            connections: BTreeMap::new(),
            attempts: BTreeSet::new(),
            opts,
            start_time: time,
            time,
            rng,
        }
    }

    /// Check whether the simulation is done, ie. there are no more messages to process.
    pub fn is_done(&self) -> bool {
        self.inbox.messages.is_empty()
    }

    /// Total amount of simulated time elapsed.
    #[allow(dead_code)]
    pub fn elapsed(&self) -> LocalDuration {
        self.time - self.start_time
    }

    /// Check whether the simulation has settled, ie. the only messages left to process
    /// are (periodic) timeouts.
    pub fn is_settled(&self) -> bool {
        self.inbox
            .messages
            .iter()
            .all(|(_, s)| matches!(s.input, Input::Wake))
    }

    /// Get a node's emitted events.
    pub fn events(&mut self, node: &NodeId) -> impl Iterator<Item = T::Notification> + '_ {
        self.events.entry(*node).or_default().drain(..)
    }

    /// Get the latency between two nodes. The minimum latency between nodes is 1 millisecond.
    pub fn latency(&self, from: NodeId, to: NodeId) -> LocalDuration {
        self.latencies
            .get(&(from, to))
            .cloned()
            .map(|l| {
                if l <= MIN_LATENCY {
                    l
                } else {
                    // Create variance in the latency. The resulting latency
                    // will be between half, and two times the base latency.
                    let millis = l.as_millis();

                    if self.rng.bool() {
                        // More latency.
                        LocalDuration::from_millis(millis + self.rng.u128(0..millis))
                    } else {
                        // Less latency.
                        LocalDuration::from_millis(millis - self.rng.u128(0..millis / 2))
                    }
                }
            })
            .unwrap_or_else(|| MIN_LATENCY)
    }

    /// Initialize peers.
    pub fn initialize<'a, P: Peer<T>>(self, peers: impl IntoIterator<Item = &'a mut P>) -> Self {
        for peer in peers.into_iter() {
            peer.init();
        }
        self
    }

    /// Run the simulation while the given predicate holds.
    pub fn run_while<'a, P: Peer<T>>(
        &mut self,
        peers: impl IntoIterator<Item = &'a mut P>,
        pred: impl Fn(&Self) -> bool,
    ) {
        let mut nodes: BTreeMap<_, _> = peers.into_iter().map(|p| (p.addr().ip(), p)).collect();

        while self.step_(&mut nodes) {
            if !pred(self) {
                break;
            }
        }
    }

    /// Process one scheduled input from the inbox, using the provided peers.
    /// This function should be called until it returns `false`, or some desired state is reached.
    /// Returns `true` if there are more messages to process.
    pub fn step<'a, P: Peer<T>>(&mut self, peers: impl IntoIterator<Item = &'a mut P>) -> bool {
        let mut nodes: BTreeMap<_, _> = peers.into_iter().map(|p| (p.addr().ip(), p)).collect();
        self.step_(&mut nodes)
    }

    fn step_<P: Peer<T>>(&mut self, nodes: &mut BTreeMap<NodeId, &mut P>) -> bool {
        if !self.opts.latency.is_empty() {
            // Configure latencies.
            for (i, from) in nodes.keys().enumerate() {
                for to in nodes.keys().skip(i + 1) {
                    let range = self.opts.latency.clone();
                    let latency = LocalDuration::from_millis(
                        self.rng
                            .u128(range.start as u128 * 1_000..range.end as u128 * 1_000),
                    );

                    self.latencies.entry((*from, *to)).or_insert(latency);
                    self.latencies.entry((*to, *from)).or_insert(latency);
                }
            }
        }

        // Create and heal partitions.
        // TODO: These aren't really "network" partitions, as they are only
        // between individual nodes. We need to think about more realistic
        // scenarios. We should also think about creating various network
        // topologies.
        if self.time.as_secs() % 10 == 0 {
            for (i, x) in nodes.keys().enumerate() {
                for y in nodes.keys().skip(i + 1) {
                    if self.is_fallible() {
                        self.partitions.insert((*x, *y));
                    } else {
                        self.partitions.remove(&(*x, *y));
                    }
                }
            }
        }

        // Schedule any messages in the pipes.
        for peer in nodes.values_mut() {
            let ip = peer.addr().ip();

            for o in peer.by_ref() {
                self.schedule(&ip, o);
            }
        }
        // Next high-priority message.
        let priority = self.priority.pop_front().map(|s| (self.time, s));

        if let Some((time, next)) = priority.or_else(|| self.inbox.next()) {
            let elapsed = (time - self.start_time).as_millis();
            if matches!(next.input, Input::Wake) {
                trace!(target: "sim", "{:05} {}", elapsed, next);
            } else {
                // TODO: This can be confusing, since this event may not actually be passed to
                // the protocol. It would be best to only log the events that are being sent
                // to the protocol, or to log when an input is being dropped.
                info!(target: "sim", "{:05} {} ({})", elapsed, next, self.inbox.messages.len());
            }
            assert!(time >= self.time, "Time only moves forwards!");

            self.time = time;
            self.inbox.messages.remove(&time);

            let Scheduled { input, node, .. } = next;

            if let Some(ref mut p) = nodes.get_mut(&node) {
                p.tick(time);

                match input {
                    Input::Connecting { addr } => {
                        if self.attempts.insert((node, addr.ip())) {
                            p.attempted(&addr);
                        }
                    }
                    Input::Connected {
                        addr,
                        local_addr,
                        link,
                    } => {
                        let conn = (node, addr.ip());

                        let attempted = link.is_outbound() && self.attempts.remove(&conn);
                        if attempted || link.is_inbound() {
                            if self.connections.insert(conn, local_addr.port()).is_none() {
                                p.connected(addr, &local_addr, link);
                            }
                        }
                    }
                    Input::Disconnected(addr, reason) => {
                        let conn = (node, addr.ip());
                        let attempt = self.attempts.remove(&conn);
                        let connection = self.connections.remove(&conn).is_some();

                        // Can't be both attempting and connected.
                        assert!(!(attempt && connection));

                        if attempt || connection {
                            p.disconnected(&addr, reason);
                        }
                    }
                    Input::Wake => p.on_timer(),
                    Input::Received(addr, msg) => {
                        p.received(&addr, Cow::Owned(msg));
                    }
                }
                for o in p.by_ref() {
                    self.schedule(&node, o);
                }
            } else {
                panic!(
                    "Node {} not found when attempting to schedule {:?}",
                    node, input
                );
            }
        }
        !self.is_done()
    }

    /// Process a protocol output event from a node.
    pub fn schedule(
        &mut self,
        node: &NodeId,
        out: ReactorDispatch<<T::PeerMessage as ToOwned>::Owned, T::Notification, T::DisconnectDemand, net::SocketAddr>,
    ) {
        let node = *node;

        match out {
            ReactorDispatch::SendPeer(receiver, msg) => {
                // If the other end has disconnected the sender with some latency, there may not be
                // a connection remaining to use.
                let port = if let Some(port) = self.connections.get(&(node, receiver.ip())) {
                    *port
                } else {
                    return;
                };

                let sender: net::SocketAddr = (node, port).into();
                if self.is_partitioned(sender.ip(), receiver.ip()) {
                    // Drop message if nodes are partitioned.
                    info!(
                        target: "sim",
                        "{} -> {} (DROPPED)",
                         sender, receiver,
                    );
                    return;
                }

                // Schedule message in the future, ensuring messages don't arrive out-of-order
                // between two peers.
                let latency = self.latency(node, receiver.ip());
                let time = self
                    .inbox
                    .last(&receiver.ip(), &sender)
                    .map(|(k, _)| *k)
                    .unwrap_or_else(|| self.time);
                let time = time + latency;
                let elapsed = (time - self.start_time).as_millis();

                info!(
                    target: "sim",
                    "{:05} {} -> {}: ({:?}) ({})",
                    elapsed, sender, receiver, &msg, latency
                );

                self.inbox.insert(
                    time,
                    Scheduled {
                        remote: sender,
                        node: receiver.ip(),
                        input: Input::Received(sender, msg),
                    },
                );
            }
            ReactorDispatch::ConnectPeer(remote) => {
                assert!(remote.ip() != node, "self-connections are not allowed");

                // Create an ephemeral sockaddr for the connecting (local) node.
                let local_addr: net::SocketAddr = net::SocketAddr::new(node, self.rng.u16(8192..));
                let latency = self.latency(node, remote.ip());

                self.inbox.insert(
                    self.time + MIN_LATENCY,
                    Scheduled {
                        node,
                        remote,
                        input: Input::Connecting { addr: remote },
                    },
                );

                // Fail to connect if the nodes are partitioned.
                if self.is_partitioned(node, remote.ip()) {
                    log::info!(target: "sim", "{} -/-> {} (partitioned)", node, remote.ip());

                    // Sometimes, the protocol gets a failure input, other times it just hangs.
                    if self.rng.bool() {
                        self.inbox.insert(
                            self.time + MIN_LATENCY,
                            Scheduled {
                                node,
                                remote,
                                input: Input::Disconnected(
                                    remote,
                                    DisconnectReason::ConnectionError(
                                        io::Error::from(io::ErrorKind::UnexpectedEof).into(),
                                    ),
                                ),
                            },
                        );
                    }
                    return;
                }

                self.inbox.insert(
                    // The remote will get the connection attempt with some latency.
                    self.time + latency,
                    Scheduled {
                        node: remote.ip(),
                        remote: local_addr,
                        input: Input::Connected {
                            addr: local_addr,
                            local_addr: remote,
                            link: ConnDirection::Inbound,
                        },
                    },
                );
                self.inbox.insert(
                    // The local node will have established the connection after some latency.
                    self.time + latency,
                    Scheduled {
                        remote,
                        node,
                        input: Input::Connected {
                            addr: remote,
                            local_addr,
                            link: ConnDirection::Outbound,
                        },
                    },
                );
            }
            ReactorDispatch::DisconnectPeer(remote, reason) => {
                // The local node is immediately disconnected.
                self.priority.push_back(Scheduled {
                    remote,
                    node,
                    input: Input::Disconnected(remote, reason.into()),
                });

                // Nb. It's possible for disconnects to happen simultaneously from both ends, hence
                // it can be that a node will try to disconnect a remote that is already
                // disconnected from the other side.
                //
                // It's also possible that the connection was only attempted and never succeeded,
                // in which case we would return here.
                let port = if let Some(port) = self.connections.get(&(node, remote.ip())) {
                    *port
                } else {
                    debug!(target: "sim", "Ignoring disconnect of {remote} from {node}");
                    return;
                };
                let local_addr: net::SocketAddr = (node, port).into();
                let latency = self.latency(node, remote.ip());

                // The remote node receives the disconnection with some delay.
                self.inbox.insert(
                    self.time + latency,
                    Scheduled {
                        node: remote.ip(),
                        remote: local_addr,
                        input: Input::Disconnected(
                            local_addr,
                            DisconnectReason::ConnectionError(
                                io::Error::from(io::ErrorKind::ConnectionReset).into(),
                            ),
                        ),
                    },
                );
            }
            ReactorDispatch::SetTimer(duration) => {
                let time = self.time + duration;

                if !matches!(
                    self.inbox.messages.get(&time),
                    Some(Scheduled {
                        input: Input::Wake,
                        ..
                    })
                ) {
                    self.inbox.insert(
                        time,
                        Scheduled {
                            node,
                            // The remote is not applicable for this type of output.
                            remote: ([0, 0, 0, 0], 0).into(),
                            input: Input::Wake,
                        },
                    );
                }
            }
            ReactorDispatch::NotifySubscribers(event) => {
                let events = self.events.entry(node).or_insert_with(VecDeque::new);
                if events.len() >= MAX_EVENTS {
                    warn!(target: "sim", "Dropping event: buffer is full");
                } else {
                    events.push_back(event);
                }
            }
        }
    }

    /// Check whether we should fail the next operation.
    fn is_fallible(&self) -> bool {
        self.rng.f64() % 1.0 < self.opts.failure_rate
    }

    /// Check whether two nodes are partitioned.
    fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitions.contains(&(a, b)) || self.partitions.contains(&(b, a))
    }
}
