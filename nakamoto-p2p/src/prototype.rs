pub mod protocol {
    use log::*;

    use crate::error::Error;
    use crate::peer::{Config, Link};
    use crate::PeerId;

    use std::collections::{HashMap, HashSet};
    use std::fmt::Debug;
    use std::net;
    use std::time::{self, SystemTime, UNIX_EPOCH};

    use bitcoin::network::address::Address;
    use bitcoin::network::constants::ServiceFlags;
    use bitcoin::network::message::{NetworkMessage, RawNetworkMessage};
    use bitcoin::network::message_blockdata::GetHeadersMessage;
    use bitcoin::network::message_network::VersionMessage;
    use bitcoin::util::hash::BitcoinHash;

    use nakamoto_chain::block::time::AdjustedTime;
    use nakamoto_chain::block::tree::BlockTree;
    use nakamoto_chain::block::{BlockHash, Height};

    /// User agent included in `version` messages.
    pub const USER_AGENT: &str = "/nakamoto:0.0.0/";
    /// Duration of inactivity before timing out a peer.
    pub const IDLE_TIMEOUT: time::Duration = time::Duration::from_secs(60 * 5);
    /// How long to wait between sending pings.
    pub const PING_INTERVAL: time::Duration = time::Duration::from_secs(60);
    /// Number of blocks out of sync we have to be to trigger an initial sync.
    pub const SYNC_THRESHOLD: Height = 144;
    /// Minimum number of peers to be connected to.
    pub const PEER_CONNECTION_THRESHOLD: usize = 1;
    /// Maximum time adjustment between network and local time (70 minutes).
    pub const MAX_TIME_ADJUSTMENT: TimeOffset = 70 * 60;

    /// A time offset, in seconds.
    pub type TimeOffset = i64;

    /// A finite-state machine that can advance one step at a time, given an input event.
    /// Parametrized over the message type.
    pub trait Protocol<M> {
        /// Process the next event and advance the state-machine by one step.
        /// Returns messages destined for peers.
        fn step(&mut self, event: Event<M>) -> Vec<(PeerId, M)>;
    }

    /// A protocol event, parametrized over the network message type.
    #[derive(Debug)]
    pub enum Event<M> {
        /// New connection with a peer.
        Connected {
            /// Remote peer id.
            addr: PeerId,
            /// Local peer id.
            local_addr: PeerId,
            /// Link direction.
            link: Link,
        },
        /// Received a message from a remote peer.
        Received(PeerId, M),
        /// Sent a message to a remote peer, of the given size.
        Sent(PeerId, usize),
        /// Got an error trying to send or receive from the remote peer.
        Error(PeerId, Error),
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// An instantiation of `Protocol`, for the Bitcoin P2P network. Parametrized over the
    /// block-tree.
    #[derive(Debug)]
    pub struct Rpc<T> {
        /// Peer states.
        pub peers: HashMap<PeerId, Peer>,
        /// Peer configuration.
        pub config: Config,
        /// Protocol state.
        pub state: State,
        /// Block tree.
        pub tree: T,

        /// Network-adjusted clock.
        clock: AdjustedTime<PeerId>,
        /// Set of connected peers that have completed the handshake.
        connected: HashSet<PeerId>,
        /// Set of disconnected peers.
        disconnected: HashSet<PeerId>,
    }

    /// Protocol state.
    #[derive(Debug, PartialEq, Eq)]
    pub enum State {
        /// Connecting to the network. Syncing hasn't started yet.
        Connecting,
        /// Initial syncing (IBD) has started with the designated peer.
        InitialSync(PeerId),
        /// We're in sync.
        Synced,
    }

    impl<T: BlockTree> Rpc<T> {
        pub fn new(tree: T, clock: AdjustedTime<PeerId>, config: Config) -> Self {
            Self {
                peers: HashMap::new(),
                config,
                state: State::Connecting,
                tree,
                clock,
                connected: HashSet::new(),
                disconnected: HashSet::new(),
            }
        }

        fn connect(&mut self, addr: PeerId, local_addr: net::SocketAddr, link: Link) -> bool {
            self.disconnected.remove(&addr);

            match self.peers.insert(
                addr,
                Peer::new(
                    addr,
                    local_addr,
                    PeerState::Handshake(Handshake::default()),
                    link,
                ),
            ) {
                Some(_) => false,
                None => true,
            }
        }

        /// Start initial block header sync.
        pub fn initial_sync(&mut self, addr: PeerId) -> Vec<(PeerId, NetworkMessage)> {
            self.transition(State::InitialSync(addr));

            let locator_hashes = self.locator_hashes();
            let peer = self.peers.get_mut(&addr).unwrap();

            peer.transition(PeerState::Syncing(Syncing::AwaitingHeaders(
                locator_hashes.clone(),
            )));

            vec![(addr, self.get_headers(locator_hashes))]
        }

        /// Check whether or not we are in sync with the network.
        /// TODO: Should return the minimum peer height, so that we can
        /// keep track of it in our state, while syncing to it.
        pub fn is_synced(&self) -> bool {
            let height = self.tree.height();

            // TODO: Check actual block hashes once we are caught up on height.
            if let Some(peer_height) = self
                .peers
                .values()
                .filter(|p| p.is_ready())
                .map(|p| p.height)
                .min()
            {
                height >= peer_height || peer_height - height <= SYNC_THRESHOLD
            } else {
                true
            }
        }
    }

    /// Handshake states.
    ///
    /// The steps for an *outbound* handshake are:
    ///
    ///   1. Send "version" message.
    ///   2. Expect "version" message from remote.
    ///   3. Expect "verack" message from remote.
    ///   4. Send "verack" message.
    ///
    /// The steps for an *inbound* handshake are:
    ///
    ///   1. Expect "version" message from remote.
    ///   2. Send "version" message.
    ///   3. Send "verack" message.
    ///   4. Expect "verack" message from remote.
    ///
    #[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
    pub enum Handshake {
        /// Waiting for "version" message from remote.
        AwaitingVersion,
        /// Waiting for "verack" message from remote.
        AwaitingVerack,
    }

    impl Default for Handshake {
        fn default() -> Self {
            Self::AwaitingVersion
        }
    }

    /// Synchronization states.
    #[derive(Clone, Debug)]
    pub enum Syncing {
        // TODO: This should keep track of what hash we requested.
        AwaitingHeaders(Vec<BlockHash>),
    }

    /// State of a remote peer.
    #[derive(Debug)]
    pub enum PeerState {
        Handshake(Handshake),
        Ready,
        Syncing(Syncing),
    }

    /// A remote peer.
    /// TODO: It should be possible to statically enforce the state-machine rules.
    /// Eg. `Peer<State>`.
    #[derive(Debug)]
    pub struct Peer {
        /// Remote peer address.
        pub address: net::SocketAddr,
        /// Local peer address.
        pub local_address: net::SocketAddr,
        /// The peer's best height.
        pub height: Height,
        /// An offset in seconds, between this peer's clock and ours.
        /// A positive offset means the peer's clock is ahead of ours.
        pub time_offset: TimeOffset,
        /// Whether this is an inbound or outbound peer connection.
        pub link: Link,
        /// Peer state.
        pub state: PeerState,
        /// Last time we heard from this peer.
        pub last_active: Option<time::Instant>,
    }

    impl Peer {
        pub fn new(
            address: net::SocketAddr,
            local_address: net::SocketAddr,
            state: PeerState,
            link: Link,
        ) -> Self {
            Self {
                address,
                local_address,
                height: 0,
                time_offset: 0,
                last_active: None,
                state,
                link,
            }
        }

        fn is_ready(&self) -> bool {
            matches!(self.state, PeerState::Ready)
        }

        fn receive_version(
            &mut self,
            VersionMessage {
                start_height,
                timestamp,
                ..
            }: VersionMessage,
        ) {
            let height = 0;
            // TODO: I'm not sure we should be getting the system time here.
            // It may be a better idea _not_ to store the time offset locally,
            // and instead send the remote time back to the network controller.
            let local_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            if height > start_height as Height + 1 {
                // We're ahead of this peer by more than one block. Don't use it
                // for IBD.
                todo!();
            }
            // TODO: Check version
            // TODO: Check services
            // TODO: Check start_height
            self.height = start_height as Height;
            self.time_offset = timestamp - local_time;

            self.transition(PeerState::Handshake(Handshake::AwaitingVerack));
        }

        fn receive_verack(&mut self) {
            self.transition(PeerState::Ready);
        }

        fn transition(&mut self, state: PeerState) {
            debug!("{}: {:?} -> {:?}", self.address, self.state, state);

            self.state = state;
        }
    }

    impl<T: BlockTree> Protocol<RawNetworkMessage> for Rpc<T> {
        fn step(&mut self, event: Event<RawNetworkMessage>) -> Vec<(PeerId, RawNetworkMessage)> {
            let mut outbound = match event {
                Event::Connected {
                    addr,
                    local_addr,
                    link,
                } => {
                    self.connect(addr, local_addr, link);

                    match link {
                        Link::Outbound => {
                            vec![(addr, self.version(addr, local_addr, self.tree.height()))]
                        }
                        Link::Inbound => vec![], // Wait to receive remote version.
                    }
                }
                Event::Received(addr, msg) => self.receive(addr, msg),
                Event::Sent(_addr, _msg) => vec![],
                Event::Error(addr, err) => {
                    debug!("Disconnected from {}", &addr);
                    debug!("error: {}: {}", addr, err);

                    self.connected.remove(&addr);
                    self.disconnected.insert(addr);
                    // TODO: Protocol shouldn't handle socket and io errors directly, because it
                    // needs to understand all kinds of socket errors then, even though it's agnostic
                    // to the transport. This doesn't make sense. What should happen is that
                    // transport errors should be handled at the transport (or reactor) layer. The "protocol"
                    // doesn't decide on what to do about transport errors. It _may_ receive a higher
                    // level event like `Disconnected`, or an opaque `Error`, just to keep track of
                    // peer errors, scores etc.
                    // TODO: If this is a disconnect, then we need to send Command::Quit to the
                    // connection somehow. Maybe not directly here, but perhaps this should return
                    // not just messages but also the ability to drop a peer?
                    // TODO: The other option is that error events (Event::Error) and disconnects
                    // are handled one layer above. But this means the protocol can't decide on
                    // these things, but instead it is the reactor that does.
                    vec![]
                }
            };

            // TODO: Should be triggered when idle, potentially by an `Idle` event.
            if self.config.sync && self.connected.len() >= PEER_CONNECTION_THRESHOLD {
                if self.is_synced() {
                    if matches!(self.state, State::Connecting) {
                        self.transition(State::Synced);
                    }
                } else {
                    // TODO: Pick a peer whos `height` is high enough.
                    let ix = fastrand::usize(..self.connected.len());
                    let p = *self.connected.iter().nth(ix).unwrap();

                    let sync_msgs = self.initial_sync(p);
                    outbound.extend_from_slice(&sync_msgs);
                }
            }

            outbound
                .into_iter()
                .map(|(addr, msg)| {
                    (
                        addr,
                        RawNetworkMessage {
                            magic: self.config.network.magic(),
                            payload: msg,
                        },
                    )
                })
                .collect()
        }
    }

    impl<T: BlockTree> Rpc<T> {
        pub fn transition(&mut self, state: State) {
            if state == self.state {
                return;
            }
            debug!("state: {:?} -> {:?}", self.state, state);

            self.state = state;
        }

        pub fn receive(
            &mut self,
            addr: PeerId,
            msg: RawNetworkMessage,
        ) -> Vec<(PeerId, NetworkMessage)> {
            debug!("{}: Received {:?}", addr, msg.cmd());

            if msg.magic != self.config.network.magic() {
                // TODO: Send rejection messsage to peer and close connection.
                todo!();
            }
            let peer = self
                .peers
                .get_mut(&addr)
                .unwrap_or_else(|| panic!("peer {} is not known", addr));
            let local_addr = peer.local_address;

            peer.last_active = Some(time::Instant::now());

            // TODO: Make sure we handle all messages at all times in some way.
            match &peer.state {
                PeerState::Handshake(Handshake::AwaitingVersion) => {
                    if let NetworkMessage::Version(version) = msg.payload {
                        peer.receive_version(version);

                        match peer.link {
                            Link::Outbound => {}
                            Link::Inbound => {
                                return vec![
                                    (addr, self.version(addr, local_addr, self.tree.height())),
                                    (addr, NetworkMessage::Verack),
                                ]
                            }
                        }
                    }
                }
                PeerState::Handshake(Handshake::AwaitingVerack) => {
                    if msg.payload == NetworkMessage::Verack {
                        peer.receive_verack();

                        self.connected.insert(addr);
                        self.clock.add_sample(addr, peer.time_offset);

                        if peer.link == Link::Outbound {
                            return vec![(addr, NetworkMessage::Verack)];
                        }
                    }
                }
                PeerState::Syncing(Syncing::AwaitingHeaders(_locators)) => {
                    assert_eq!(self.state, State::InitialSync(peer.address));

                    if let NetworkMessage::Headers(headers) = msg.payload {
                        let length = headers.len();

                        debug!("{}: Received {} header(s)", addr, length);

                        if let (Some(first), Some(last)) = (headers.first(), headers.last()) {
                            debug!(
                                "{}: Range = {}..{}",
                                addr,
                                first.bitcoin_hash(),
                                last.bitcoin_hash()
                            );
                        } else {
                            info!("{}: Finished synchronizing", addr);

                            peer.transition(PeerState::Ready);
                            self.transition(State::Synced);

                            return vec![];
                        }

                        match self.tree.import_blocks(headers.into_iter()) {
                            Ok((tip, height)) => {
                                info!("Imported {} headers from {}", length, addr);
                                info!("Chain height = {}, tip = {}", height, tip);

                                let locators = vec![tip];

                                peer.transition(PeerState::Syncing(Syncing::AwaitingHeaders(
                                    locators.clone(),
                                )));

                                // TODO: We can break here if we've received less than 2'000 headers.
                                return vec![(addr, self.get_headers(locators))];
                            }
                            Err(err) => {
                                // TODO: Bad blocks received!
                                error!("Error importing headers: {}", err);
                            }
                        }
                    }
                }
                PeerState::Ready if self.state == State::Synced => {
                    if let NetworkMessage::GetHeaders(GetHeadersMessage {
                        locator_hashes, ..
                    }) = msg.payload
                    {
                        assert!(locator_hashes.len() == 1);

                        let start_hash = locator_hashes[0];
                        let (start_height, _) = self.tree.get_block(&start_hash).unwrap();

                        // TODO: Set this to highest locator hash. We can assume that the peer
                        // is at this height if they know this hash.
                        peer.height = start_height;

                        let headers = self
                            .tree
                            .range(start_height + 1..self.tree.height() + 1)
                            .collect();

                        return vec![(addr, NetworkMessage::Headers(headers))];
                    }
                }
                PeerState::Ready => {
                    debug!("Ignoring {} from peer {}", msg.cmd(), peer.address);
                }
            }

            vec![]
        }

        fn version(
            &self,
            addr: net::SocketAddr,
            local_addr: net::SocketAddr,
            start_height: Height,
        ) -> NetworkMessage {
            let start_height = start_height as i32;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            NetworkMessage::Version(VersionMessage {
                version: self.config.protocol_version,
                services: self.config.services,
                timestamp,
                receiver: Address::new(
                    &addr,
                    ServiceFlags::NETWORK | ServiceFlags::COMPACT_FILTERS,
                ),
                sender: Address::new(&local_addr, ServiceFlags::NONE),
                nonce: 0,
                user_agent: USER_AGENT.to_owned(),
                start_height,
                relay: self.config.relay,
            })
        }

        fn get_headers(&self, locator_hashes: Vec<BlockHash>) -> NetworkMessage {
            NetworkMessage::GetHeaders(GetHeadersMessage {
                version: self.config.protocol_version,
                // Starting hashes, highest heights first.
                locator_hashes,
                // Using the zero hash means *fetch as many blocks as possible*.
                stop_hash: BlockHash::default(),
            })
        }

        fn locator_hashes(&self) -> Vec<BlockHash> {
            let (hash, _) = self.tree.tip();

            vec![hash]
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::collections::VecDeque;

        use nakamoto_chain::block::cache::model;
        use nakamoto_chain::block::BlockHeader;
        use nakamoto_test::TREE;

        /// A simple P2P network simulator. Acts as the _reactor_, but without doing any I/O.
        mod simulator {
            use super::*;

            pub fn run<P: Protocol<M>, M>(peers: Vec<(PeerId, &mut P, Vec<Event<M>>)>) {
                let mut sim: HashMap<PeerId, (&mut P, VecDeque<Event<M>>)> = HashMap::new();
                let mut events = Vec::new();

                // Add peers to simulator.
                for (addr, proto, evs) in peers.into_iter() {
                    sim.insert(addr, (proto, VecDeque::new()));

                    for e in evs.into_iter() {
                        events.push((addr, e));
                    }
                }

                while !events.is_empty() || sim.values().any(|(_, q)| !q.is_empty()) {
                    // Prepare event queues.
                    for (receiver, event) in events.drain(..) {
                        let (_, q) = sim.get_mut(&receiver).unwrap();
                        q.push_back(event);
                    }

                    for (peer, (proto, queue)) in sim.iter_mut() {
                        if let Some(event) = queue.pop_front() {
                            let out = proto.step(event);

                            for (receiver, msg) in out.into_iter() {
                                events.push((receiver, Event::Received(*peer, msg)));
                            }
                        }
                    }
                }
            }
        }

        #[test]
        fn test_handshake() {
            let genesis = BlockHeader {
                version: 1,
                prev_blockhash: Default::default(),
                merkle_root: Default::default(),
                nonce: 0,
                time: 0,
                bits: 0,
            };
            let tree = model::Cache::new(genesis);
            let config = Config {
                sync: false,
                ..Config::default()
            };
            let clock = AdjustedTime::new();

            let alice_addr = ([127, 0, 0, 1], 8333).into();
            let bob_addr = ([127, 0, 0, 2], 8333).into();

            let mut alice = Rpc::new(tree.clone(), clock.clone(), config);
            let mut bob = Rpc::new(tree, clock, config);

            simulator::run(vec![
                (
                    alice_addr,
                    &mut alice,
                    vec![Event::Connected {
                        addr: bob_addr,
                        local_addr: alice_addr,
                        link: Link::Outbound,
                    }],
                ),
                (
                    bob_addr,
                    &mut bob,
                    vec![Event::Connected {
                        addr: alice_addr,
                        local_addr: bob_addr,
                        link: Link::Inbound,
                    }],
                ),
            ]);

            assert!(
                alice
                    .peers
                    .values()
                    .all(|p| matches!(p.state, PeerState::Ready)),
                "alice: {:#?}",
                alice.peers
            );

            assert!(
                bob.peers
                    .values()
                    .all(|p| matches!(p.state, PeerState::Ready)),
                "bob: {:#?}",
                bob.peers
            );
        }

        #[test]
        fn test_initial_sync() {
            let config = Config::default();
            let clock = AdjustedTime::new();

            let alice_addr: PeerId = ([127, 0, 0, 1], 8333).into();
            let bob_addr: PeerId = ([127, 0, 0, 2], 8333).into();

            // Blockchain height we're going to be working with. Making it larger
            // than the threshold ensures a sync happens.
            let height = SYNC_THRESHOLD + 1;

            // Let's test Bob trying to sync with Alice from genesis.
            let mut alice_tree = TREE.clone();
            let bob_tree = model::Cache::new(*alice_tree.genesis());

            // Truncate chain to test height.
            alice_tree.rollback(height).unwrap();

            let mut alice = Rpc::new(alice_tree, clock.clone(), config);
            let mut bob = Rpc::new(bob_tree, clock, config);

            simulator::run(vec![
                (
                    alice_addr,
                    &mut alice,
                    vec![Event::Connected {
                        addr: bob_addr,
                        local_addr: alice_addr,
                        link: Link::Outbound,
                    }],
                ),
                (
                    bob_addr,
                    &mut bob,
                    vec![Event::Connected {
                        addr: alice_addr,
                        local_addr: bob_addr,
                        link: Link::Inbound,
                    }],
                ),
            ]);

            assert_eq!(alice.tree.height(), height);
            assert_eq!(bob.tree.height(), height);
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

pub mod reactor {
    use bitcoin::consensus::encode::Decodable;
    use bitcoin::consensus::encode::{self, Encodable};
    use bitcoin::network::stream_reader::StreamReader;

    use super::protocol::{Event, Protocol, IDLE_TIMEOUT, PING_INTERVAL};

    use crate::address_book::AddressBook;
    use crate::error::Error;
    use crate::peer::Link;

    use log::*;
    use std::collections::HashMap;
    use std::fmt::Debug;
    use std::io::prelude::*;
    use std::net;

    use crossbeam_channel as crossbeam;

    /// Stack size for spawned threads, in bytes.
    /// Since we're creating a thread per peer, we want to keep the stack size small.
    const THREAD_STACK_SIZE: usize = 1024 * 1024;

    /// Maximum peer-to-peer message size.
    pub const MAX_MESSAGE_SIZE: usize = 6 * 1024;

    /// Command sent to a writer thread.
    #[derive(Debug)]
    pub enum Command<T> {
        Write(net::SocketAddr, T),
        Disconnect(net::SocketAddr),
        Quit,
    }

    /// Reader socket.
    #[derive(Debug)]
    pub struct Reader<R: Read + Write, M> {
        events: crossbeam::Sender<Event<M>>,
        raw: StreamReader<R>,
        address: net::SocketAddr,
        local_address: net::SocketAddr,
    }

    impl<R: Read + Write, M: Decodable + Encodable + Debug + Send + Sync + 'static> Reader<R, M> {
        /// Create a new peer from a `io::Read` and an address pair.
        pub fn from(
            r: R,
            local_address: net::SocketAddr,
            address: net::SocketAddr,
            events: crossbeam::Sender<Event<M>>,
        ) -> Self {
            let raw = StreamReader::new(r, Some(MAX_MESSAGE_SIZE));

            Self {
                raw,
                local_address,
                address,
                events,
            }
        }

        pub fn run(&mut self, link: Link) -> Result<(), Error> {
            self.events.send(Event::Connected {
                addr: self.address,
                local_addr: self.local_address,
                link,
            })?;

            loop {
                match self.read() {
                    Ok(msg) => self.events.send(Event::Received(self.address, msg))?,
                    Err(err) => {
                        self.events.send(Event::Error(self.address, err.into()))?;
                        break;
                    }
                }
            }
            Ok(())
        }

        pub fn read(&mut self) -> Result<M, encode::Error> {
            match self.raw.read_next::<M>() {
                Ok(msg) => {
                    trace!("{}: {:#?}", self.address, msg);

                    Ok(msg)
                }
                Err(err) => Err(err),
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    /// Writer socket.
    pub struct Writer<T> {
        address: net::SocketAddr,
        raw: T,
    }

    impl<T: Write> Writer<T> {
        pub fn write<M: Encodable + Debug>(&mut self, msg: M) -> Result<usize, Error> {
            let mut buf = [0u8; MAX_MESSAGE_SIZE];

            match msg.consensus_encode(&mut buf[..]) {
                Ok(len) => {
                    trace!("{}: {:#?}", self.address, msg);

                    self.raw.write_all(&buf[..len])?;
                    self.raw.flush()?;

                    Ok(len)
                }
                Err(err) => panic!(err.to_string()),
            }
        }

        fn thread<M: Encodable + Send + Sync + Debug + 'static>(
            mut peers: HashMap<net::SocketAddr, Self>,
            cmds: crossbeam::Receiver<Command<M>>,
            events: crossbeam::Sender<Event<M>>,
        ) -> Result<(), Error> {
            loop {
                let cmd = cmds.recv().unwrap();

                match cmd {
                    Command::Write(addr, msg) => {
                        let peer = peers.get_mut(&addr).unwrap();

                        // TODO: Watch out for head-of-line blocking here!
                        // TODO: We can't just set this to non-blocking, because
                        // this will also affect the read-end.
                        match peer.write(msg) {
                            Ok(nbytes) => {
                                events.send(Event::Sent(addr, nbytes))?;
                            }
                            Err(err) => {
                                events.send(Event::Error(addr, err))?;
                            }
                        }
                    }
                    Command::Disconnect(addr) => {
                        peers.remove(&addr);
                    }
                    Command::Quit => break,
                }
            }
            Ok(())
        }
    }

    impl<T: Write> std::ops::Deref for Writer<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.raw
        }
    }

    impl<T: Write> std::ops::DerefMut for Writer<T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.raw
        }
    }

    /// Run the given protocol with the reactor.
    pub fn run<P: Protocol<M>, M: Decodable + Encodable + Send + Sync + Debug + 'static>(
        addrs: AddressBook,
        mut protocol: P,
    ) -> Result<Vec<()>, Error> {
        use std::thread;

        let (events_tx, events_rx): (crossbeam::Sender<Event<M>>, _) = crossbeam::bounded(1);
        let (cmds_tx, cmds_rx) = crossbeam::bounded(1);

        let mut spawned = Vec::with_capacity(addrs.len());
        let mut peers = HashMap::new();

        for addr in addrs.iter() {
            let (mut conn, writer) = self::dial(&addr, events_tx.clone())?;

            debug!("Connected to {}", &addr);
            trace!("{:#?}", conn);

            peers.insert(*addr, writer);

            let handle = thread::Builder::new()
                .name(addr.to_string())
                .stack_size(THREAD_STACK_SIZE)
                .spawn(move || conn.run(Link::Outbound))?;

            spawned.push(handle);
        }

        thread::Builder::new().spawn(move || Writer::thread(peers, cmds_rx, events_tx))?;

        loop {
            let result = events_rx.recv_timeout(PING_INTERVAL);

            match result {
                Ok(event) => {
                    let msgs = protocol.step(event);

                    for (addr, msg) in msgs.into_iter() {
                        cmds_tx.send(Command::Write(addr, msg)).unwrap();
                    }
                }
                Err(crossbeam::RecvTimeoutError::Disconnected) => {
                    // TODO: We need to connect to new peers.
                    // This always means that all senders have been dropped.
                    break;
                }
                Err(crossbeam::RecvTimeoutError::Timeout) => {
                    // TODO: Ping peers, nothing was received in a while. Find out
                    // who to ping.
                }
            }
        }

        spawned
            .into_iter()
            .flat_map(thread::JoinHandle::join)
            .collect()
    }

    /// Connect to a peer given a remote address.
    pub fn dial<M: Encodable + Decodable + Send + Sync + Debug + 'static>(
        addr: &net::SocketAddr,
        events_tx: crossbeam::Sender<Event<M>>,
    ) -> Result<(Reader<net::TcpStream, M>, Writer<net::TcpStream>), Error> {
        debug!("Connecting to {}...", &addr);

        let sock = net::TcpStream::connect(addr)?;

        // TODO: We probably don't want the same timeouts for read and write.
        // For _write_, we want something much shorter.
        sock.set_read_timeout(Some(IDLE_TIMEOUT))?;
        sock.set_write_timeout(Some(IDLE_TIMEOUT))?;

        let w = sock.try_clone()?;
        let address = sock.peer_addr()?;
        let local_address = sock.local_addr()?;

        Ok((
            Reader::from(sock, local_address, address, events_tx),
            Writer { raw: w, address },
        ))
    }
}
