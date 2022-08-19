//! A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
#![allow(clippy::collapsible_if)]

use crate::{DisconnectReason, Io, Link, LocalDuration, LocalTime, Protocol};
use log::*;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::{Deref, DerefMut, Range};
use std::{fmt, io, net};

#[cfg(feature = "quickcheck")]
pub mod arbitrary;

/// Minimum latency between peers.
pub const MIN_LATENCY: LocalDuration = LocalDuration::from_millis(1);

/// Port used for all nodes.
const PORT: u16 = 8333;

/// Identifier for a simulated node/peer.
/// The simulator requires each peer to have a distinct IP address.
type NodeId = std::net::IpAddr;

/// A simulated peer. Protocol instances have to be wrapped in this type to be simulated.
pub trait Peer<P>: Deref<Target = P> + DerefMut<Target = P> + 'static
where
    P: Protocol,
{
    /// Initialize the peer. This should at minimum initialize the protocol with the
    /// current time.
    fn init(&mut self);
    /// Get the peer address.
    fn addr(&self) -> net::SocketAddr;
}

/// Simulated protocol input.
#[derive(Debug, Clone)]
pub enum Input<D> {
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
        link: Link,
    },
    /// Disconnected from peer.
    Disconnected(net::SocketAddr, DisconnectReason<D>),
    /// Received a message from a remote peer.
    Received(net::SocketAddr, Vec<u8>),
    /// Used to advance the state machine after some wall time has passed.
    Wake,
}

/// A scheduled protocol input.
#[derive(Debug, Clone)]
pub struct Scheduled<D> {
    /// The node for which this input is scheduled.
    pub node: NodeId,
    /// The remote peer from which this input originates.
    /// If the input originates from the local node, this should be set to the zero address.
    pub remote: net::SocketAddr,
    /// The input being scheduled.
    pub input: Input<D>,
}

impl<D: fmt::Display> fmt::Display for Scheduled<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.input {
            Input::Received(from, bytes) => {
                write!(f, "{} <- {} ({}B)", self.node, from, bytes.len())
            }
            Input::Connected {
                addr,
                local_addr,
                link: Link::Inbound,
                ..
            } => write!(f, "{} <== {}: Connected", local_addr, addr),
            Input::Connected {
                local_addr,
                addr,
                link: Link::Outbound,
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
pub struct Inbox<D> {
    /// The set of scheduled inputs. We use a `BTreeMap` to ensure inputs are always
    /// ordered by scheduled delivery time.
    messages: BTreeMap<LocalTime, Scheduled<D>>,
}

impl<D: Clone> Inbox<D> {
    /// Add a scheduled input to the inbox.
    fn insert(&mut self, mut time: LocalTime, msg: Scheduled<D>) {
        // Make sure we don't overwrite an existing message by using the same time slot.
        while self.messages.contains_key(&time) {
            time = time + MIN_LATENCY;
        }
        self.messages.insert(time, msg);
    }

    /// Get the next scheduled input to be delivered.
    fn next(&mut self) -> Option<(LocalTime, Scheduled<D>)> {
        self.messages
            .iter()
            .next()
            .map(|(time, scheduled)| (*time, scheduled.clone()))
    }

    /// Get the last message sent between two peers. Only checks one direction.
    fn last(&self, node: &NodeId, remote: &net::SocketAddr) -> Option<(&LocalTime, &Scheduled<D>)> {
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
    T: Protocol,
{
    /// Inbox of inputs to be delivered by the simulation.
    inbox: Inbox<T::DisconnectReason>,
    /// Priority events that should happen immediately.
    priority: VecDeque<Scheduled<T::DisconnectReason>>,
    /// Simulated latencies between nodes.
    latencies: BTreeMap<(NodeId, NodeId), LocalDuration>,
    /// Network partitions between two nodes.
    partitions: BTreeSet<(NodeId, NodeId)>,
    /// Set of existing connections between nodes.
    connections: BTreeSet<(NodeId, NodeId)>,
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
    T: Protocol + 'static,
    T::DisconnectReason: Clone + Into<DisconnectReason<T::DisconnectReason>>,
{
    /// Create a new simulation.
    pub fn new(time: LocalTime, rng: fastrand::Rng, opts: Options) -> Self {
        Self {
            inbox: Inbox {
                messages: BTreeMap::new(),
            },
            priority: VecDeque::new(),
            partitions: BTreeSet::new(),
            latencies: BTreeMap::new(),
            connections: BTreeSet::new(),
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
    #[allow(dead_code)]
    pub fn is_settled(&self) -> bool {
        self.inbox
            .messages
            .iter()
            .all(|(_, s)| matches!(s.input, Input::Wake))
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
                            if self.connections.insert(conn) {
                                p.connected(addr, &local_addr, link);
                            }
                        }
                    }
                    Input::Disconnected(addr, reason) => {
                        let conn = (node, addr.ip());
                        let attempt = self.attempts.remove(&conn);
                        let connection = self.connections.remove(&conn);

                        // Can't be both attempting and connected.
                        assert!(!(attempt && connection));

                        if attempt || connection {
                            p.disconnected(&addr, reason);
                        }
                    }
                    Input::Wake => p.wake(),
                    Input::Received(addr, msg) => {
                        p.received_bytes(&addr, &msg);
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
    pub fn schedule(&mut self, node: &NodeId, out: Io<T::Event, T::DisconnectReason>) {
        let node = *node;

        match out {
            Io::Write(receiver, msg) => {
                if msg.is_empty() {
                    return;
                }
                // If the other end has disconnected the sender with some latency, there may not be
                // a connection remaining to use.
                if !self.connections.contains(&(node, receiver.ip())) {
                    return;
                }

                let sender: net::SocketAddr = (node, PORT).into();
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
                    "{:05} {} -> {}: ({})",
                    elapsed, sender, receiver, latency
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
            Io::Connect(remote) => {
                assert!(remote.ip() != node, "self-connections are not allowed");

                // Create an ephemeral sockaddr for the connecting (local) node.
                let local_addr: net::SocketAddr = net::SocketAddr::new(node, PORT);
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
                            link: Link::Inbound,
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
                            link: Link::Outbound,
                        },
                    },
                );
            }
            Io::Disconnect(remote, reason) => {
                // Nb. It's possible for disconnects to happen simultaneously from both ends, hence
                // it can be that a node will try to disconnect a remote that is already
                // disconnected from the other side.
                let local_addr: net::SocketAddr = (node, PORT).into();
                let latency = self.latency(node, remote.ip());

                // The local node is immediately disconnected.
                self.priority.push_back(Scheduled {
                    remote,
                    node,
                    input: Input::Disconnected(remote, reason.into()),
                });
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
            Io::Wakeup(duration) => {
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
            Io::Event(_) => {
                // Ignored.
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
