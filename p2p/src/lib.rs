//! Nakamoto's peer-to-peer library.
//!
//! The `p2p` crate implements the core protocol state-machine. It can be found under the
//! [protocol](crate::protocol) module, which has the following sub-protocol:
//!
//! * [`AddressManager`][addrmgr]: handles peer address exchange
//! * [`SyncManager`][syncmgr]: handles block header sync
//! * [`ConnectionManager`][connmgr]: handles peer connections
//! * [`PingManager`][pingmgr]: handles pings and pongs
//! * [`SpvManager`][spvmgr]: handles compact filter sync
//! * [`PeerManager`][peermgr]: handles peer handshake
//!
//! [addrmgr]: crate::protocol::addrmgr::AddressManager
//! [syncmgr]: crate::protocol::syncmgr::SyncManager
//! [connmgr]: crate::protocol::connmgr::ConnectionManager
//! [pingmgr]: crate::protocol::pingmgr::PingManager
//! [spvmgr]: crate::protocol::spvmgr::SpvManager
//! [peermgr]: crate::protocol::peermgr::PeerManager
//!
//! Nakamoto's implementation of the peer-to-peer protocol(s) is *I/O-free*. The
//! core logic is implemented as a state machine with *inputs* and *outputs* and a
//! *step* function that does not perform any network I/O.
//!
//! The reason for this is to keep the protocol code easy to read and simple to
//! test. Not having I/O minimizes the possible error states and error-handling
//! code in the protocol, and allows for a fully *deterministic* protocol. This
//! means failing tests can always be reproduced and 100% test coverage is within
//! reach.
//!
//! To achieve this, handling of network I/O is cleanly separated into the
//! [reactor](crate::reactor) module, which translates network events into
//! protocol events. This has the added benefit that it's trivial to swap
//! nakamoto's networking code with a different implementation, as the code is
//! fully self-contained.
//!
//! To illustrate the above, lets trace the behavior of the system when a `ping`
//! message is received via a peer connection to the client:
//!
//! 1. The `Reactor` reads from the socket and decodes a `NetworkMessage::Ping`
//!    message.
//! 2. The `Reactor` wraps this message into a protocol input `Input::Received(addr,
//!    NetworkMessage::Ping)`, where `addr` is the remote address of the socket on
//!    which it received this message.
//! 3. The `Reactor` calls `Protocol::step(input, time)`, where `input` is the above
//!    input, and `time` is the current local time.
//! 4. The `Protocol` forwards this message to the `PingManager`, which constructs
//!    a new output `Out::Message(addr, NetworkMessage::Pong)`, and forwards it
//!    upstream, to the reactor.
//! 5. The `Reactor` processes the output, encodes the raw message and writes it to
//!    the socket corresponding to the `addr` address, effectively sending a `pong`
//!    message back to the original sender.
//!
//! Though simplified, the above steps provide a good mental model of how the
//! reactor and protocol interplay to handle network events.
//!
#![allow(clippy::type_complexity)]
#![allow(clippy::new_without_default)]
#![allow(clippy::single_match)]
#![allow(clippy::comparison_chain)]
#![deny(missing_docs, unsafe_code)]
pub mod address_book;
pub mod error;
pub mod event;
pub mod protocol;
pub mod reactor;
pub use bitcoin;

pub use protocol::PeerId;

#[cfg(test)]
mod fallible;

#[cfg(test)]
#[macro_use]
extern crate lazy_static;

/// Makes a function randomly fail with the given error.
#[macro_export]
macro_rules! fallible {
    ($err:expr) => {
        #[cfg(test)]
        {
            let fallible = fallible::FALLIBLE.lock().unwrap();

            if let Some(p) = *fallible {
                let r = fastrand::f64();

                if r <= p {
                    return Err($err.into());
                }
            }
        }
    };
}
