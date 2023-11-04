//! TODO
use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::net;
use std::sync::Arc;

use nakamoto_chain::BlockTree;
use nakamoto_common::bitcoin::consensus::Encodable;
use nakamoto_common::block::time::{AdjustedClock, LocalTime};
use nakamoto_net::{Disconnect, Io, Link, StateMachine};
use nakamoto_p2p as p2p;

use crate::client::Config;
use crate::peer;
use nakamoto_common::block::filter;
use nakamoto_common::block::filter::Filters;

/// Client service. Wraps a state machine and handles decoding and encoding of network messages.
pub struct Service<T, F, P, C> {
    inboxes: HashMap<net::SocketAddr, p2p::stream::Decoder>,
    machine: p2p::StateMachine<T, F, P, C>,
}

impl<T: BlockTree, F: filter::Filters, P: peer::Store, C: AdjustedClock<net::SocketAddr>>
    Service<T, F, P, C>
{
    /// Create a new client service.
    pub fn new(
        tree: T,
        filters: F,
        peers: P,
        clock: C,
        rng: fastrand::Rng,
        config: Config,
    ) -> Self {
        Self {
            inboxes: HashMap::new(),
            machine: p2p::StateMachine::new(
                tree,
                filters,
                peers,
                clock,
                rng,
                p2p::Config {
                    network: config.network,
                    domains: config.domains,
                    connect: config.connect,
                    user_agent: config.user_agent,
                    hooks: config.hooks,
                    limits: config.limits,
                    services: config.services,

                    ..p2p::Config::default()
                },
            ),
        }
    }
}

impl<T, F, P, C> nakamoto_net::Service for Service<T, F, P, C>
where
    T: BlockTree,
    F: filter::Filters,
    P: peer::Store,
    C: AdjustedClock<net::SocketAddr>,
{
    type Command = p2p::Command;

    fn command_received(&mut self, cmd: Self::Command) {
        // TODO: Commands shouldn't be handled by the inner state machine.
        self.machine.command(cmd)
    }
}

impl<T, F, P, C> StateMachine for Service<T, F, P, C>
where
    T: BlockTree,
    F: filter::Filters,
    P: peer::Store,
    C: AdjustedClock<net::SocketAddr>,
{
    type Message = [u8];
    type Event = p2p::Event;
    type DisconnectReason = p2p::DisconnectReason;

    fn initialize(&mut self, time: LocalTime) {
        self.machine.initialize(time);
    }

    fn tick(&mut self, local_time: LocalTime) {
        self.machine.tick(local_time);
    }

    fn timer_expired(&mut self) {
        self.machine.timer_expired();
    }

    fn message_received(&mut self, addr: &net::SocketAddr, bytes: Cow<[u8]>) {
        if let Some(inbox) = self.inboxes.get_mut(addr) {
            inbox.input(bytes.borrow());

            loop {
                match inbox.decode_next() {
                    Ok(Some(msg)) => self.machine.message_received(addr, Cow::Owned(msg)),
                    Ok(None) => break,

                    Err(err) => {
                        log::error!("Invalid message received from {}: {}", addr, err);

                        self.machine
                            .disconnect(*addr, p2p::DisconnectReason::DecodeError(Arc::new(err)));

                        return;
                    }
                }
            }
        } else {
            log::debug!("Received message from unknown peer {}", addr);
        }
    }

    fn attempted(&mut self, addr: &net::SocketAddr) {
        self.machine.attempted(addr)
    }

    fn connected(&mut self, addr: net::SocketAddr, local_addr: &net::SocketAddr, link: Link) {
        self.inboxes.insert(addr, p2p::stream::Decoder::new(1024));
        self.machine.connected(addr, local_addr, link)
    }

    fn disconnected(&mut self, addr: &net::SocketAddr, reason: Disconnect<Self::DisconnectReason>) {
        self.inboxes.remove(addr);
        self.machine.disconnected(addr, reason)
    }
}

impl<T: BlockTree, F: Filters, P: peer::Store, C: AdjustedClock<p2p::PeerId>> Iterator
    for Service<T, F, P, C>
{
    type Item = Io<Vec<u8>, p2p::Event, p2p::DisconnectReason>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.machine.next() {
            Some(Io::Write(addr, msg)) => {
                log::debug!(target: "client", "Write {:?} to {}", &msg, addr.ip());
                let mut buf = Vec::new();

                msg.consensus_encode(&mut buf)
                    .expect("writing to an in-memory buffer doesn't fail");

                Some(Io::Write(addr, buf))
            }
            Some(Io::Event(e)) => Some(Io::Event(e)),
            Some(Io::Connect(a)) => Some(Io::Connect(a)),
            Some(Io::Disconnect(a, r)) => Some(Io::Disconnect(a, r)),
            Some(Io::SetTimer(d)) => Some(Io::SetTimer(d)),

            None => None,
        }
    }
}
