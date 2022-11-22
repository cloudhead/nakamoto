//! I/O reactor that drives the protocol state machine.
//!
//! The reactor translates network events into protocol events. This has the
//! added benefit that it's trivial to swap nakamoto's networking code with a
//! different implementation, as the code is fully self-contained.
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
#![allow(clippy::new_without_default)]
#![allow(clippy::inconsistent_struct_constructor)]

#[cfg(unix)]
pub mod reactor;
pub mod socket;
pub mod time;

pub use reactor::{Reactor, Waker};

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
