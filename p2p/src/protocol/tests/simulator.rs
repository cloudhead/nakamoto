//! A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
use super::*;

use nakamoto_common::collections::HashMap;

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;

/// A scheduled protocol input.
#[derive(Debug, Clone)]
pub struct Scheduled {
    remote: PeerId,
    peer: PeerId,
    input: Input,
    time: LocalTime,
}

impl fmt::Display for Scheduled {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.input {
            Input::Sent(to, msg) => write!(f, "{}: {} -> {}: {:?}", self.time, self.peer, to, msg),
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
                write!(
                    f,
                    "{}: {} <- {}: `{}` {}",
                    self.time,
                    self.peer,
                    from,
                    msg.cmd(),
                    s
                )
            }
            Input::Connected {
                addr,
                link: Link::Inbound,
                ..
            } => write!(f, "{}: {} <== {}: Connected", self.time, self.peer, addr),
            Input::Connected {
                addr,
                link: Link::Outbound,
                ..
            } => write!(f, "{}: {} ==> {}: Connected", self.time, self.peer, addr),
            Input::Disconnected(addr, reason) => {
                write!(
                    f,
                    "{}: {} =/= {}: Disconnected: {}",
                    self.time, self.peer, addr, reason
                )
            }
            _ => {
                write!(f, "")
            }
        }
    }
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}
impl Eq for Scheduled {}

impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.time.cmp(&other.time))
    }
}

pub struct Simulation {
    inbox: BTreeSet<Scheduled>,
    latencies: HashMap<(PeerId, PeerId), LocalDuration>,
    local_time: LocalTime,
    rng: fastrand::Rng,
}

impl Simulation {
    pub fn new(time: LocalTime, rng: fastrand::Rng) -> Self {
        Self {
            inbox: BTreeSet::new(),
            latencies: HashMap::with_hasher(rng.clone().into()),
            local_time: time,
            rng,
        }
    }

    pub fn run<'a, M: 'a + Machine>(&mut self, peers: impl Into<Vec<&'a mut super::Peer<M>>>) {
        let mut map = HashMap::with_hasher(self.rng.clone().into());
        for peer in peers.into().into_iter() {
            map.insert(peer.addr, peer);
        }
        // Configure latencies.
        for (i, from) in map.keys().enumerate() {
            for to in map.keys().skip(i + 1) {
                let latency = LocalDuration::from_secs(self.rng.u64(0..5));

                self.latencies.insert((*from, *to), latency);
                self.latencies.insert((*to, *from), latency);
            }
        }
        self.step(&mut map);
    }

    pub fn connect<M: Machine>(&mut self, peer: &mut super::Peer<M>, remote: &mut super::Peer<M>) {
        peer.step(Input::Connected {
            local_addr: peer.addr,
            addr: remote.addr,
            link: Link::Outbound,
        });
        remote.step(Input::Connected {
            local_addr: remote.addr,
            addr: peer.addr,
            link: Link::Inbound,
        });

        for o in peer.upstream.try_iter() {
            self.schedule(&peer.addr, o);
        }
        for o in remote.upstream.try_iter() {
            self.schedule(&remote.addr, o);
        }
    }

    /// Run the simulation until there are no events left to schedule.
    pub fn step<M: Machine>(&mut self, peers: &mut HashMap<PeerId, &mut super::Peer<M>>) {
        while let Some(next) = self.inbox.iter().cloned().next() {
            info!(target: "sim", "{}", next);

            self.inbox.remove(&next);

            let Scheduled {
                input, peer, time, ..
            } = next;

            self.local_time = time;

            if let Some(ref mut p) = peers.get_mut(&peer) {
                p.protocol.step(input, self.local_time);
                for o in p.upstream.try_iter() {
                    self.schedule(&peer, o);
                }
            }
            if self.inbox.iter().all(|s| matches!(s.input, Input::Tick)) {
                break;
            }
        }
    }

    /// Process a protocol output event.
    pub fn schedule(&mut self, peer: &PeerId, out: Out) {
        let peer = *peer;
        let Simulation {
            inbox,
            latencies,
            ref mut local_time,
            ..
        } = self;

        match out {
            Out::Message(receiver, msg) => {
                let latency = latencies
                    .get(&(peer, receiver))
                    .cloned()
                    .unwrap_or_else(|| LocalDuration::from_secs(0));

                info!(target: "sim", "{}: {} -> {}: `{}`", local_time, peer, receiver, msg.cmd());

                inbox.insert(Scheduled {
                    remote: peer,
                    peer: receiver,
                    input: Input::Received(peer, msg),
                    time: *local_time + latency,
                });
            }
            Out::Connect(remote, _timeout) => {
                assert!(remote != peer, "self-connections are not allowed");

                let latency = latencies
                    .get(&(peer, remote))
                    .cloned()
                    .unwrap_or_else(|| LocalDuration::from_secs(0));
                let time = *local_time + latency;

                inbox.insert(Scheduled {
                    remote,
                    peer,
                    input: Input::Connecting { addr: remote },
                    time,
                });
                inbox.insert(Scheduled {
                    remote: peer,
                    peer: remote,
                    input: Input::Connected {
                        addr: peer,
                        local_addr: remote,
                        link: Link::Inbound,
                    },
                    time,
                });
                inbox.insert(Scheduled {
                    remote,
                    peer,
                    input: Input::Connected {
                        addr: remote,
                        local_addr: peer,
                        link: Link::Outbound,
                    },
                    time,
                });
            }
            Out::Disconnect(remote, reason) => {
                let latency = latencies
                    .get(&(peer, remote))
                    .cloned()
                    .unwrap_or_else(|| LocalDuration::from_secs(0));

                inbox.insert(Scheduled {
                    remote: peer,
                    peer: remote,
                    input: Input::Disconnected(peer, reason.clone()),
                    time: *local_time + latency,
                });
                inbox.insert(Scheduled {
                    remote,
                    peer,
                    input: Input::Disconnected(remote, reason),
                    time: *local_time,
                });
            }
            Out::SetTimeout(duration) => {
                inbox.insert(Scheduled {
                    remote: peer,
                    peer,
                    input: Input::Tick,
                    time: *local_time + duration,
                });
            }
            _ => {}
        }
        local_time.elapse(LocalDuration::from_millis(1));
    }
}
