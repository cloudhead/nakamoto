//! A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
use super::*;

use nakamoto_common::block::filter::{FilterHash, FilterHeader};
use nakamoto_common::collections::HashMap;

pub struct PeerConfig {
    pub name: &'static str,
    pub chain: Option<NonEmpty<BlockHeader>>,
    pub cfheaders: Option<NonEmpty<(FilterHash, FilterHeader)>>,
}

impl PeerConfig {
    pub fn new(
        name: &'static str,
        chain: NonEmpty<BlockHeader>,
        cfheaders: NonEmpty<(FilterHash, FilterHeader)>,
    ) -> Self {
        Self {
            name,
            chain: Some(chain),
            cfheaders: Some(cfheaders),
        }
    }

    pub fn genesis(name: &'static str) -> Self {
        Self {
            name,
            chain: None,
            cfheaders: None,
        }
    }
}

pub struct Net {
    pub network: Network,
    pub rng: fastrand::Rng,
    pub peers: Vec<PeerConfig>,
    pub configure: fn(&mut Config),
    pub initialize: bool,
}

impl Default for Net {
    fn default() -> Self {
        Self {
            network: Network::default(),
            rng: fastrand::Rng::new(),
            peers: vec![],
            configure: |_| {},
            initialize: true,
        }
    }
}

impl Net {
    pub fn into(self) -> Sim {
        let (peers, time) =
            setup::network(self.network, self.rng.clone(), self.peers, self.configure);
        let mut sim = Sim::new(peers, time, self.rng);

        if self.initialize {
            sim.initialize();
        }
        sim
    }
}

#[derive(Debug)]
pub struct InputResult {
    peer: PeerId,
    outputs: Vec<Out>,
}

impl From<InputResult> for Vec<Out> {
    fn from(input: InputResult) -> Self {
        input.outputs.into_iter().collect()
    }
}

impl IntoIterator for InputResult {
    type Item = Out;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.outputs.into_iter()
    }
}

impl InputResult {
    pub fn find<F, T>(&self, f: F) -> Option<T>
    where
        F: Fn(&Out) -> Option<T>,
    {
        self.outputs.iter().find_map(|m| f(&m))
    }

    pub fn any<F>(&self, f: F) -> Option<&Out>
    where
        F: Fn(&Out) -> bool,
    {
        self.outputs.iter().find(|m| f(&m))
    }

    pub fn all<F>(&self, f: F) -> Option<()>
    where
        F: Fn(&Out) -> bool,
    {
        if self
            .outputs
            .iter()
            .filter(|o| !matches!(o, Out::Event(_)))
            .all(|m| f(&m))
        {
            Some(())
        } else {
            None
        }
    }

    #[track_caller]
    #[allow(clippy::expect_fun_call)]
    pub fn event<F>(&self, f: F)
    where
        F: Fn(&Event) -> bool,
    {
        self.outputs
            .iter()
            .filter_map(|out| match out {
                Out::Event(event) => Some(event),
                _ => None,
            })
            .find(|event| f(event))
            .expect(&format!(
                "expected event was not found in output: {:?}",
                self.outputs
            ));
    }

    #[track_caller]
    #[allow(clippy::expect_fun_call)]
    pub fn message<F>(&self, f: F) -> (PeerId, &NetworkMessage)
    where
        F: Fn(&PeerId, &NetworkMessage) -> bool,
    {
        self.outputs
            .iter()
            .filter_map(payload)
            .find(|(addr, msg)| f(addr, msg))
            .expect(&format!(
                "expected message was not found in output: {:?}",
                self.outputs
            ))
    }

    pub fn schedule(self, sim: &mut Sim) {
        let peer = sim.peers.get_mut(&self.peer).unwrap();

        for o in self.outputs.into_iter() {
            peer.schedule(&mut sim.inbox, o);
        }
    }
}

pub struct Peer {
    pub id: PeerId,
    pub name: &'static str,
    pub protocol: Protocol<
        model::Cache,
        model::FilterCache,
        std::collections::HashMap<net::IpAddr, KnownAddress>,
    >,
    pub outbound: chan::Receiver<Out>,

    events: Vec<Event>,
}

impl Peer {
    pub fn initialize(&mut self, time: LocalTime) {
        self.protocol.initialize(time)
    }

    pub fn schedule(&mut self, inbox: &mut VecDeque<(PeerId, Input)>, output: Out) {
        Sim::schedule(&mut self.events, inbox, &self.id, output)
    }
}

pub struct Sim {
    pub peers: HashMap<PeerId, Peer>,
    pub time: LocalTime,

    index: HashMap<&'static str, PeerId>,
    inbox: VecDeque<(PeerId, Input)>,

    filter: Box<dyn Fn(&PeerId, &PeerId, &NetworkMessage) -> bool>,

    #[allow(dead_code)]
    rng: fastrand::Rng,
}

impl Sim {
    fn new(
        peers: Vec<(
            PeerId,
            Builder<
                model::Cache,
                model::FilterCache,
                std::collections::HashMap<net::IpAddr, KnownAddress>,
            >,
        )>,
        time: LocalTime,
        rng: fastrand::Rng,
    ) -> Self {
        let peers = {
            let mut hm = HashMap::with_hasher(rng.clone().into());
            for (id, builder) in peers.into_iter() {
                let (transmit, outbound) = chan::unbounded();
                let protocol = builder.build(transmit);

                hm.insert(
                    id,
                    Peer {
                        id,
                        name: protocol.target,
                        protocol,
                        events: vec![],
                        outbound,
                    },
                );
            }
            hm
        };

        let mut index = HashMap::with_hasher(rng.clone().into());
        for (addr, peer) in &peers {
            index.insert(peer.protocol.target, *addr);
        }
        let inbox = VecDeque::new();
        let filter = Box::new(|_: &PeerId, _: &PeerId, _: &NetworkMessage| false);

        Self {
            peers,
            index,
            inbox,
            time,
            filter,
            rng,
        }
    }

    /// Get a peer by name.
    pub fn get(&mut self, name: &str) -> PeerId {
        *self
            .index
            .get(name)
            .unwrap_or_else(|| panic!("Sim::get: peer {:?} doesn't exist", name))
    }

    /// Get a peer instance by name.
    pub fn peer(&mut self, name: &str) -> &mut Peer {
        let id = self.get(name);

        self.peers
            .get_mut(&id)
            .unwrap_or_else(|| panic!("Sim::peer: peer {:?} doesn't exist", id))
    }

    /// Send an input directly to a peer and return the result.
    pub fn input(&mut self, addr: &PeerId, input: Input) -> InputResult {
        let peer = self.peers.get_mut(&addr).unwrap();
        peer.protocol.step(input, self.time);

        InputResult {
            peer: *addr,
            outputs: peer.outbound.try_iter().collect(),
        }
    }

    /// Create a connection between peers.
    pub fn connect(&mut self, addr: &PeerId, remotes: &[PeerId]) {
        let peer = self.peers.get_mut(addr).unwrap();

        for remote in remotes {
            peer.protocol
                .step(Input::Command(Command::Connect(*remote)), self.time);

            for o in peer.outbound.clone().try_iter() {
                peer.schedule(&mut self.inbox, o);
            }
        }
    }

    /// Drain the outgoing events queue for the given peer.
    pub fn events<'a>(&'a mut self, addr: &PeerId) -> impl Iterator<Item = Event> + 'a {
        self.peers.get_mut(addr).unwrap().events.drain(..)
    }

    /// Let some time pass.
    pub fn elapse(&mut self, duration: LocalDuration) {
        log::info!(target: "sim", "Elapsing {} seconds", duration.as_secs());

        self.time = self.time + duration;
    }

    /// Process a protocol output event.
    pub fn schedule(
        events: &mut Vec<Event>,
        inbox: &mut VecDeque<(PeerId, Input)>,
        peer: &PeerId,
        out: Out,
    ) {
        let peer = *peer;

        match out {
            Out::Message(receiver, msg) => {
                info!("(sim) {} -> {}: {:?}", peer, receiver, msg);
                inbox.push_back((receiver, Input::Received(peer, msg)))
            }
            Out::Connect(remote, _timeout) => {
                assert!(remote != peer, "self-connections are not allowed");
                info!("(sim) {} => {}", peer, remote);

                inbox.push_back((
                    remote,
                    Input::Connected {
                        addr: peer,
                        local_addr: remote,
                        link: Link::Inbound,
                    },
                ));
                inbox.push_back((
                    peer,
                    Input::Connected {
                        addr: remote,
                        local_addr: peer,
                        link: Link::Outbound,
                    },
                ));
            }
            Out::Disconnect(remote, reason) => {
                info!("(sim) {} =/= {} ({})", peer, remote, reason);

                inbox.push_back((remote, Input::Disconnected(peer, reason.clone())));
                inbox.push_back((peer, Input::Disconnected(remote, reason)));
            }
            Out::Event(event) => {
                events.push(event);
            }
            _ => {}
        }
    }

    /// Initialize peers, scheduling events returned by initialization.
    pub fn initialize(&mut self) {
        for peer in self.peers.values_mut() {
            log::debug!("(sim) Initializing {:?}", peer.name);

            peer.initialize(self.time);

            for o in peer.outbound.clone().try_iter() {
                peer.schedule(&mut self.inbox, o);
            }
        }
    }

    /// Run the simulation until there are no events left to schedule.
    pub fn step(&mut self) {
        while !self.inbox.is_empty() {
            let mut events: Vec<_> = self.inbox.drain(..).collect();

            for (addr, event) in events.drain(..) {
                if let Some(ref mut peer) = self.peers.get_mut(&addr) {
                    peer.protocol.step(event, self.time);

                    for o in peer.outbound.clone().try_iter() {
                        match &o {
                            Out::Message(addr, msg) => {
                                if !(self.filter)(&peer.id, &addr, &msg.payload) {
                                    peer.schedule(&mut self.inbox, o);
                                } else {
                                    log::info!("(sim) Filtered {:?}", msg);
                                }
                            }
                            _ => {
                                peer.schedule(&mut self.inbox, o);
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn set_filter<F>(&mut self, f: F)
    where
        F: 'static + Fn(&PeerId, &PeerId, &NetworkMessage) -> bool,
    {
        self.filter = Box::new(f);
    }

    pub fn clear_filter(&mut self) {
        self.filter = Box::new(|_, _, _| false);
    }
}

pub fn handshake<T: BlockTree, F: Filters, P: peer::Store>(
    alice: &mut Protocol<T, F, P>,
    alice_addr: net::SocketAddr,
    alice_rx: chan::Receiver<Out>,
    bob: &mut Protocol<T, F, P>,
    bob_addr: net::SocketAddr,
    bob_rx: chan::Receiver<Out>,
    local_time: LocalTime,
) {
    self::run(
        vec![(alice_addr, alice, alice_rx), (bob_addr, bob, bob_rx)],
        vec![
            vec![Input::Connected {
                addr: bob_addr,
                local_addr: alice_addr,
                link: Link::Outbound,
            }],
            vec![Input::Connected {
                addr: alice_addr,
                local_addr: bob_addr,
                link: Link::Inbound,
            }],
        ],
        local_time,
    );

    assert!(alice.peermgr.peers().all(|p| p.is_negotiated()));
    assert!(bob.peermgr.peers().all(|p| p.is_negotiated()));
}

pub fn run<T: BlockTree, F: Filters, P: peer::Store>(
    peers: Vec<(PeerId, &mut Protocol<T, F, P>, chan::Receiver<Out>)>,
    inputs: Vec<Vec<Input>>,
    local_time: LocalTime,
) {
    let mut sim: HashMap<PeerId, (&mut Protocol<T, F, P>, VecDeque<Input>, chan::Receiver<Out>)> =
        HashMap::with_hasher(fastrand::Rng::new().into());
    let mut events = VecDeque::new();
    let mut tmp = Vec::new();

    // Add peers to simulator.
    for ((addr, proto, rx), evs) in peers.into_iter().zip(inputs.into_iter()) {
        proto.initialize(local_time);

        for o in rx.try_iter() {
            Sim::schedule(&mut tmp, &mut events, &addr, o);
        }
        for e in evs.into_iter() {
            events.push_back((addr, e));
        }
        sim.insert(addr, (proto, VecDeque::new(), rx));
    }

    while !events.is_empty() || sim.values().any(|(_, q, _)| !q.is_empty()) {
        // Prepare event queues.
        for (receiver, event) in events.drain(..) {
            let (_, q, _) = sim.get_mut(&receiver).unwrap();
            q.push_back(event);
        }

        for (peer, (proto, queue, rx)) in sim.iter_mut() {
            if let Some(event) = queue.pop_front() {
                proto.step(event, local_time);
                for out in rx.try_iter() {
                    Sim::schedule(&mut tmp, &mut events, peer, out);
                }
            }
        }
    }
}
