//! A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
use super::*;

use nakamoto_common::collections::HashMap;

use std::collections::BTreeMap;
use std::fmt;

/// Minimum latency between peers.
pub const MIN_LATENCY: LocalDuration = LocalDuration::from_millis(1);

/// Identifier for a simulated node/peer.
/// The simulator requires each peer to have a distinct IP address.
type NodeId = std::net::IpAddr;

/// A scheduled protocol input.
#[derive(Debug, Clone)]
pub struct Scheduled {
    /// The node for which this input is scheduled.
    pub node: NodeId,
    /// The remote peer from which this input originates.
    /// If the input originates from the local node, this should be set to the zero address.
    pub remote: PeerId,
    /// The input being scheduled.
    pub input: Input,
}

impl fmt::Display for Scheduled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.input {
            Input::Sent(to, msg) => write!(f, "{} -> {}: {:?}", self.node, to, msg),
            Input::Received(from, msg) => {
                let s = match &msg.payload {
                    NetworkMessage::Headers(vec) => {
                        format!("Headers({})", vec.len())
                    }
                    NetworkMessage::Verack
                    | NetworkMessage::SendHeaders
                    | NetworkMessage::GetAddr => {
                        format!("")
                    }
                    msg => {
                        format!("{:?}", msg)
                    }
                };
                write!(f, "{} <- {}: `{}` {}", self.node, from, msg.cmd(), s)
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
            Input::Tick => {
                write!(f, "{}: Tick", self.node)
            }
            _ => {
                write!(f, "{:?}", self)
            }
        }
    }
}

/// Inbox of scheduled state machine inputs to be delivered to the simulated nodes.
#[derive(Debug)]
pub struct Inbox {
    /// The set of scheduled inputs. We use a `BTreeMap` to ensure inputs are always
    /// ordered by scheduled delivery time.
    messages: BTreeMap<LocalTime, Scheduled>,
}

impl Inbox {
    /// Iterate over all scheduled inputs.
    #[allow(dead_code)]
    pub fn iter(&self) -> impl Iterator<Item = &Scheduled> {
        self.messages.values()
    }

    /// Add a scheduled input to the inbox.
    fn insert(&mut self, mut time: LocalTime, msg: Scheduled) {
        // Make sure we don't overwrite an existing message by using the same time slot.
        while self.messages.contains_key(&time) {
            time = time + MIN_LATENCY;
        }
        self.messages.insert(time, msg);
    }

    /// Get the next scheduled input to be delivered.
    fn next(&mut self) -> Option<(LocalTime, Scheduled)> {
        self.messages
            .iter()
            .next()
            .map(|(time, scheduled)| (*time, scheduled.clone()))
    }
}

/// Simulation options.
pub struct Options {
    /// Minimum and maximum latency between nodes, in seconds.
    pub latency: Range<u64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            latency: Range::default(),
        }
    }
}

/// A peer-to-peer node simulation.
pub struct Simulation {
    /// Inbox of inputs to be delivered by the simulation.
    inbox: Inbox,
    /// Simulated latencies between nodes.
    latencies: HashMap<(NodeId, NodeId), LocalDuration>,
    /// Map of existing connections between nodes.
    /// We use this to keep track of the local port used when establishing connections,
    /// since each connection is established from a different local port.
    connections: HashMap<(NodeId, NodeId), (net::SocketAddr, net::SocketAddr)>,
    /// Simulation options.
    opts: Options,
    /// Current simulation time. Updated when a scheduled message is processed.
    time: LocalTime,
    /// RNG.
    rng: fastrand::Rng,
}

impl Simulation {
    /// Create a new simulation.
    pub fn new(time: LocalTime, rng: fastrand::Rng, opts: Options) -> Self {
        Self {
            inbox: Inbox {
                messages: BTreeMap::new(),
            },
            latencies: HashMap::with_hasher(rng.clone().into()),
            connections: HashMap::with_hasher(rng.clone().into()),
            opts,
            time,
            rng,
        }
    }

    /// Check whether the simulation is done, ie. there are no more messages to process.
    pub fn is_done(&self) -> bool {
        self.inbox.messages.is_empty()
    }

    /// Check whether the simulation has settled, ie. the only messages left to process
    /// are (periodic) timeouts.
    #[allow(dead_code)]
    pub fn is_settled(&self) -> bool {
        self.inbox
            .messages
            .iter()
            .all(|(_, s)| matches!(s.input, Input::Tick))
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
    pub fn initialize<'a, M: 'a + Machine>(
        &self,
        peers: impl IntoIterator<Item = &'a mut super::Peer<M>>,
    ) {
        for peer in peers.into_iter() {
            peer.initialize();
        }
    }

    /// Process one scheduled input from the inbox, using the provided peers.
    /// This function should be called until it returns `false`, or some desired state is reached.
    /// Returns `true` if there are more messages to process.
    pub fn step<'a, M: 'a + Machine + Debug>(
        &mut self,
        peers: impl IntoIterator<Item = &'a mut super::Peer<M>>,
    ) -> bool {
        let mut nodes = HashMap::with_hasher(self.rng.clone().into());
        for peer in peers.into_iter() {
            nodes.insert(peer.addr.ip(), peer);
        }

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

        // Schedule any messages in the pipes.
        for peer in nodes.values() {
            for o in peer.upstream.try_iter() {
                self.schedule(&peer.addr.ip(), o);
            }
        }

        if let Some((time, next)) = self.inbox.next() {
            info!(target: "sim", "{} ({})", next, self.inbox.messages.len());

            self.time = time;
            self.inbox.messages.remove(&time);

            let Scheduled { input, node, .. } = next;

            if let Some(ref mut p) = nodes.get_mut(&node) {
                p.protocol.step(input, self.time);
                for o in p.upstream.try_iter() {
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
    pub fn schedule(&mut self, node: &NodeId, out: Out) {
        let node = *node;

        match out {
            Out::Message(receiver, msg) => {
                let latency = self.latency(node, receiver.ip());
                let time = self.time + latency;
                let (sender_addr, _) = self.connections.get(&(node, receiver.ip())).expect(
                    "Simulator::schedule: messages can only be sent between connected peers",
                );
                info!(target: "sim", "{} -> {}: `{}` ({})", sender_addr, receiver, msg.cmd(), latency);

                self.inbox.insert(
                    time,
                    Scheduled {
                        remote: *sender_addr,
                        node: receiver.ip(),
                        input: Input::Received(*sender_addr, msg),
                    },
                );
            }
            Out::Connect(remote, _timeout) => {
                assert!(remote.ip() != node, "self-connections are not allowed");

                // Create an ephemeral sockaddr for the connecting (local) node.
                let local_addr: net::SocketAddr = net::SocketAddr::new(node, self.rng.u16(8192..));
                let latency = self.latency(node, remote.ip());

                self.connections
                    .insert((node, remote.ip()), (local_addr, remote));
                self.connections
                    .insert((remote.ip(), node), (remote, local_addr));

                self.inbox.insert(
                    self.time + MIN_LATENCY,
                    Scheduled {
                        node,
                        remote,
                        input: Input::Connecting { addr: remote },
                    },
                );
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
            Out::Disconnect(remote, reason) => {
                // It's possible for disconnects to happen simultaneously from both ends, hence
                // it can be that a node will try to disconnect a remote that is already
                // disconnected from the other side.
                if let Some((local_addr, _)) = self.connections.remove(&(node, remote.ip())) {
                    self.connections.remove(&(remote.ip(), node));

                    let latency = self.latency(node, remote.ip());

                    // The local node is immediately disconnected.
                    self.inbox.insert(
                        self.time,
                        Scheduled {
                            remote,
                            node,
                            input: Input::Disconnected(remote, reason.clone()),
                        },
                    );
                    // The remote node receives the disconnection with some delay.
                    self.inbox.insert(
                        self.time + latency,
                        Scheduled {
                            node: remote.ip(),
                            remote: local_addr,
                            input: Input::Disconnected(local_addr, reason),
                        },
                    );
                }
            }
            Out::SetTimeout(duration) => {
                self.inbox.insert(
                    self.time + duration,
                    Scheduled {
                        node,
                        // The remote is not applicable for this type of output.
                        remote: ([0, 0, 0, 0], 0).into(),
                        input: Input::Tick,
                    },
                );
            }
            Out::Event(_) => {
                // Ignored.
            }
            Out::Shutdown => {
                unimplemented! {}
            }
        }
    }
}
