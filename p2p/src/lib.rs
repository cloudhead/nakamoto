//! Nakamoto's peer-to-peer library.
//!
//! The `p2p` crate implements the core protocol state-machine. It can be found under the
//! [fsm](crate::fsm) module.
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
//! To achieve this, handling of network I/O is cleanly separated into a network
//! *reactor*. See the `nakamoto-net-poll` crate for an example of a reactor.
//!
#![allow(clippy::type_complexity)]
#![allow(clippy::new_without_default)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::single_match)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::inconsistent_struct_constructor)]
#![allow(clippy::too_many_arguments)]
#![deny(missing_docs, unsafe_code)]
pub mod fsm;
pub mod stream;

pub use fsm::{Command, Config, ConnDirection, DisconnectReason, Event, Io, PeerId, StateMachine};
pub use nakamoto_net as net;
