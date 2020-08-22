//! A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
use super::*;

use nakamoto_common::collections::HashMap;

pub struct Net<'a> {
    pub network: Network,
    pub rng: fastrand::Rng,
    pub peers: &'a [&'static str],
    pub configure: fn(&mut Config),
    pub initialize: bool,
}

impl<'a> Default for Net<'a> {
    fn default() -> Self {
        Self {
            network: Network::default(),
            rng: fastrand::Rng::new(),
            peers: &[],
            configure: |_| {},
            initialize: true,
        }
    }
}

impl<'a> Net<'a> {
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
    outputs: Vec<Out<RawNetworkMessage>>,
}

impl From<InputResult> for Vec<Out<RawNetworkMessage>> {
    fn from(input: InputResult) -> Self {
        input.outputs
    }
}

impl IntoIterator for InputResult {
    type Item = Out<RawNetworkMessage>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.outputs.into_iter()
    }
}

impl InputResult {
    pub fn find<F, T>(&self, f: F) -> Option<T>
    where
        F: Fn(&Out<RawNetworkMessage>) -> Option<T>,
    {
        self.outputs.iter().find_map(|m| f(m))
    }

    pub fn any<F>(&self, f: F) -> Option<&Out<RawNetworkMessage>>
    where
        F: Fn(&Out<RawNetworkMessage>) -> bool,
    {
        self.outputs.iter().find(|m| f(m))
    }

    pub fn message<F>(&self, f: F) -> (PeerId, &NetworkMessage)
    where
        F: Fn(&PeerId, &NetworkMessage) -> bool,
    {
        self.outputs
            .iter()
            .filter_map(payload)
            .find(|(addr, msg)| f(addr, msg))
            .expect("expected message in output was not found")
    }

    pub fn schedule(self, sim: &mut Sim) {
        let peer = sim.peers.get_mut(&self.peer).unwrap();

        for o in self.outputs.into_iter() {
            peer.schedule(&mut sim.inbox, o);
        }
    }
}

pub struct Peer {
    id: PeerId,
    name: &'static str,
    protocol: Bitcoin<model::Cache>,
    events: Vec<Event<NetworkMessage>>,
}

impl Peer {
    pub fn schedule(
        &mut self,
        inbox: &mut VecDeque<(PeerId, Input)>,
        output: Out<RawNetworkMessage>,
    ) {
        Sim::schedule(&mut self.events, inbox, &self.id, output)
    }
}

pub struct Sim {
    pub peers: HashMap<PeerId, Peer>,
    pub time: LocalTime,

    index: HashMap<&'static str, PeerId>,
    inbox: VecDeque<(PeerId, Input)>,

    #[allow(dead_code)]
    rng: fastrand::Rng,
}

impl Sim {
    fn new(
        peers: Vec<(PeerId, Bitcoin<model::Cache>)>,
        time: LocalTime,
        rng: fastrand::Rng,
    ) -> Self {
        let peers = {
            let mut hm = HashMap::with_hasher(rng.clone().into());
            for (id, protocol) in peers.into_iter() {
                hm.insert(
                    id,
                    Peer {
                        id,
                        name: protocol.name,
                        protocol,
                        events: vec![],
                    },
                );
            }
            hm
        };

        let mut index = HashMap::with_hasher(rng.clone().into());
        for (addr, peer) in &peers {
            index.insert(peer.protocol.name, *addr);
        }
        let inbox = VecDeque::new();

        Self {
            peers,
            index,
            inbox,
            time,
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

    /// Send an input directly to a peer and return the result.
    pub fn input(&mut self, addr: &PeerId, input: Input) -> InputResult {
        let peer = self.peers.get_mut(&addr).unwrap();

        InputResult {
            peer: *addr,
            outputs: peer.protocol.step(input).collect(),
        }
    }

    /// Drain the outgoing events queue for the given peer.
    pub fn events<'a>(
        &'a mut self,
        addr: &PeerId,
    ) -> impl Iterator<Item = Event<NetworkMessage>> + 'a {
        self.peers.get_mut(addr).unwrap().events.drain(..)
    }

    /// Let some time pass.
    pub fn elapse(&mut self, duration: LocalDuration) {
        log::info!("(sim) Elasing {} seconds", duration.as_secs());

        self.time = self.time + duration;

        for peer in self.peers.values_mut() {
            for output in peer.protocol.step(Input::Tick(self.time)) {
                peer.schedule(&mut self.inbox, output);
            }
        }
    }

    /// Process a protocol output event.
    pub fn schedule<M, C>(
        events: &mut Vec<Event<<M as protocol::Message>::Payload>>,
        inbox: &mut VecDeque<(PeerId, protocol::Input<M, C>)>,
        peer: &PeerId,
        out: Out<M>,
    ) where
        M: Message + Debug,
        C: Debug,
    {
        let peer = *peer;

        match out {
            Out::Message(receiver, msg) => {
                info!("(sim) {} -> {}: {:#?}", peer, receiver, msg);
                inbox.push_back((receiver, protocol::Input::Received(peer, msg)))
            }
            Out::Connect(remote) => {
                info!("(sim) {} => {}", peer, remote);
                inbox.push_back((
                    remote,
                    protocol::Input::Connected {
                        addr: peer,
                        local_addr: remote,
                        link: Link::Inbound,
                    },
                ));
                inbox.push_back((
                    peer,
                    protocol::Input::Connected {
                        addr: remote,
                        local_addr: peer,
                        link: Link::Outbound,
                    },
                ));
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

            for o in peer.protocol.initialize(self.time).into_iter() {
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
                    for o in peer.protocol.step(event) {
                        peer.schedule(&mut self.inbox, o);
                    }
                }
            }
        }
    }
}

pub fn handshake<T: BlockTree>(
    alice: &mut Bitcoin<T>,
    alice_addr: net::SocketAddr,
    bob: &mut Bitcoin<T>,
    bob_addr: net::SocketAddr,
    local_time: LocalTime,
) {
    self::run(
        vec![(alice_addr, alice), (bob_addr, bob)],
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

    assert!(alice.peers.values().all(|p| p.is_ready()));
    assert!(bob.peers.values().all(|p| p.is_ready()));
}

pub fn run<P: Protocol<M, Command = C>, M: Message + Debug, C: Debug>(
    peers: Vec<(PeerId, &mut P)>,
    inputs: Vec<Vec<protocol::Input<M, C>>>,
    local_time: LocalTime,
) {
    let mut sim: HashMap<PeerId, (&mut P, VecDeque<protocol::Input<M, C>>)> =
        HashMap::with_hasher(fastrand::Rng::new().into());
    let mut events = VecDeque::new();
    let mut tmp = Vec::new();

    // Add peers to simulator.
    for ((addr, proto), evs) in peers.into_iter().zip(inputs.into_iter()) {
        for o in proto.initialize(local_time).into_iter() {
            Sim::schedule(&mut tmp, &mut events, &addr, o);
        }
        for e in evs.into_iter() {
            events.push_back((addr, e));
        }
        sim.insert(addr, (proto, VecDeque::new()));
    }

    while !events.is_empty() || sim.values().any(|(_, q)| !q.is_empty()) {
        // Prepare event queues.
        for (receiver, event) in events.drain(..) {
            let (_, q) = sim.get_mut(&receiver).unwrap();
            q.push_back(event);
        }

        for (peer, (proto, queue)) in sim.iter_mut() {
            if let Some(event) = queue.pop_front() {
                for out in proto.step(event) {
                    Sim::schedule(&mut tmp, &mut events, peer, out);
                }
            }
        }
    }
}
