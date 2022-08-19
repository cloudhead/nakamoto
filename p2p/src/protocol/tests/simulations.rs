//! Simulations.

#![cfg(test)]
use super::*;

/// Test that we can find and connect to peers amidst network errors.
pub fn connect_to_peers(
    options: Options,
    seed: u64,
    arbitrary::InRange(target): arbitrary::InRange<1, 6>,
) -> bool {
    logger::init(log::Level::Debug);

    assert!(target > 0);

    let target = target as usize;
    let rng = fastrand::Rng::with_seed(seed);
    let network = Network::Mainnet;
    let headers = BITCOIN_HEADERS.tail.to_vec();
    let time = LocalTime::from_block_time(headers.last().unwrap().time);

    // Alice will try to connect to enough outbound peers.
    let mut peers = peer::network(network, target * 2, rng.clone());
    let addrs = peers
        .iter()
        .map(|p| (p.addr, Source::Dns, p.cfg.services))
        .collect::<Vec<_>>();
    let mut alice = Peer::genesis("alice", [48, 48, 48, 48], network, addrs, rng.clone());
    alice.protocol.peermgr.config.target_outbound_peers = target;
    alice.init();

    let mut simulator = Simulation::new(time, rng, options).initialize(&mut peers);
    let (mut prev_negotiated, mut prev_connecting, mut prev_connected) = (0, 0, 0);

    while simulator.step(iter::once(&mut alice).chain(&mut peers)) {
        let negotiated = alice.protocol.peermgr.negotiated(Link::Outbound).count();
        let connecting = alice.protocol.peermgr.connecting().count();
        let connected = alice.protocol.peermgr.connected().count();

        if (prev_negotiated, prev_connecting, prev_connected) != (negotiated, connecting, connected)
        {
            prev_negotiated = negotiated;
            prev_connected = connected;
            prev_connecting = connecting;

            info!(
                target: "test",
                "--- negotiated: {}, connecting: {}, connected: {} ---",
                negotiated, connecting, connected
            );
        }

        if negotiated >= target {
            break;
        }
        if simulator.elapsed() > LocalDuration::from_mins(4) {
            return false;
        }
    }
    true
}
