//! A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
use super::*;

pub struct Net<'a> {
    pub network: Network,
    pub rng: fastrand::Rng,
    pub peers: &'a [&'static str],
    pub initialize: bool,
}

impl<'a> Net<'a> {
    pub fn into(self) -> Sim {
        let (peers, time) = setup::network(self.network, self.rng.clone(), self.peers);
        let mut sim = Sim::new(peers, time, self.rng);

        if self.initialize {
            sim.initialize();
        }
        sim
    }
}

pub struct InputResult {
    peer: PeerId,
    outputs: Vec<Output<RawNetworkMessage>>,
}

impl From<InputResult> for Vec<Output<RawNetworkMessage>> {
    fn from(input: InputResult) -> Self {
        input.outputs
    }
}

impl IntoIterator for InputResult {
    type Item = Output<RawNetworkMessage>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.outputs.into_iter()
    }
}

impl InputResult {
    #[allow(dead_code)]
    pub fn expect<F>(&self, f: F, msg: &str) -> &Output<RawNetworkMessage>
    where
        F: Fn(&Output<RawNetworkMessage>) -> bool,
    {
        self.outputs.iter().find(|m| f(m)).expect(msg)
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
        for o in self.outputs.into_iter() {
            Sim::schedule(&mut sim.inbox, &self.peer, o);
        }
    }
}

pub struct Sim {
    pub peers: HashMap<PeerId, Bitcoin<model::Cache>>,
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
        let peers = peers.into_iter().collect::<HashMap<_, _>>();
        let index = peers
            .iter()
            .map(|(addr, peer)| (peer.name, *addr))
            .collect();
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
            outputs: peer.step(input, self.time),
        }
    }

    /// Let some time pass.
    pub fn elapse(&mut self, duration: LocalDuration) {
        self.time = self.time + duration;
    }

    /// Process a protocol output event.
    pub fn schedule<M, C>(
        inbox: &mut VecDeque<(PeerId, protocol::Input<M, C>)>,
        peer: &PeerId,
        out: Output<M>,
    ) where
        M: Message + Debug,
        C: Debug,
    {
        let peer = *peer;

        match out {
            Output::Message(receiver, msg) => {
                info!("(sim) {} -> {}: {:#?}", peer, receiver, msg);
                inbox.push_back((receiver, protocol::Input::Received(peer, msg)))
            }
            Output::Connect(remote) => {
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
            _ => {}
        }
    }

    /// Initialize peers, scheduling events returned by initialization.
    pub fn initialize(&mut self) {
        for (addr, proto) in self.peers.iter_mut() {
            for o in proto.initialize(self.time).into_iter() {
                Sim::schedule(&mut self.inbox, &addr, o);
            }
        }
    }

    /// Run the simulation until there are no events left to schedule.
    pub fn step(&mut self) {
        while !self.inbox.is_empty() {
            let mut events: Vec<_> = self.inbox.drain(..).collect();

            for (addr, event) in events.drain(..) {
                // dbg!((addr, &event));
                if let Some(ref mut peer) = self.peers.get_mut(&addr) {
                    let outs = peer.step(event, self.time);

                    for o in outs.into_iter() {
                        Sim::schedule(&mut self.inbox, &addr, o);
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
    let mut sim: HashMap<PeerId, (&mut P, VecDeque<protocol::Input<M, C>>)> = HashMap::new();
    let mut events = VecDeque::new();

    // Add peers to simulator.
    for ((addr, proto), evs) in peers.into_iter().zip(inputs.into_iter()) {
        for o in proto.initialize(local_time).into_iter() {
            Sim::schedule(&mut events, &addr, o);
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
                let outs = proto.step(event, local_time);

                for out in outs.into_iter() {
                    Sim::schedule(&mut events, peer, out);
                }
            }
        }
    }
}
