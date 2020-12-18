//! Nakamoto is a high-assurance Bitcoin light-client library.
//!
//! The project is broken down into the following crates:
//!
//! * [`client`]: the core light-client library
//! * [`p2p`]: the protocol implementation
//! * [`chain`]: the block store and fork selection logic
//! * [`common`]: common functionality used by all crates
//!
//! The [`client`] crate is intended to be the entry point for most users of the
//! library, and is a good place to start, to see how everything fits together.
//!
//! ```
//! use std::{net, thread};
//!
//! use nakamoto_client::client::{AddressBook, Client, ClientConfig, Network};
//! use nakamoto_client::error::Error;
//! use nakamoto_client::handle::Handle;
//!
//! /// The network reactor we're going to use.
//! type Reactor = nakamoto_net_poll::Reactor<net::TcpStream>;
//!
//! /// Run the light-client.
//! fn main() {
//!     let cfg = ClientConfig {
//!         network: Network::Testnet,
//!         address_book: AddressBook::new(),
//!         ..ClientConfig::default()
//!     };
//!     // Create a client using the above network reactor.
//!     let client = Client::<Reactor>::new(cfg).unwrap();
//!     let handle = client.handle();
//!
//!     // Run the client on a different thread, to not block the main thread.
//!     thread::spawn(|| client.run());
//!
//!     // Ask the client to terminate.
//!     handle.shutdown().unwrap()
//! }
//! ```

#[cfg(feature = "nakamoto-chain")]
pub use nakamoto_chain as chain;
#[cfg(feature = "nakamoto-client")]
pub use nakamoto_client as client;
#[cfg(feature = "nakamoto-common")]
pub use nakamoto_common as common;
#[cfg(feature = "nakamoto-node")]
pub use nakamoto_node as node;
#[cfg(feature = "nakamoto-p2p")]
pub use nakamoto_p2p as p2p;

#[cfg(test)]
#[cfg(feature = "nakamoto-test")]
pub use nakamoto_test as test;
