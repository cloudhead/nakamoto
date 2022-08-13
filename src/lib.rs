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
//! ```no_run
//! use std::{net, thread};
//!
//! use nakamoto::client::{Client, Config, Network, Services};
//! use nakamoto::client::error::Error;
//! use nakamoto::client::handle::Handle as _;
//!
//! /// The network reactor we're going to use.
//! type Reactor = nakamoto::net::poll::Reactor<net::TcpStream>;
//!
//! /// Run the light-client.
//! fn main() -> Result<(), Error> {
//!     let cfg = Config::new(Network::Testnet);
//!
//!     // Create a client using the above network reactor.
//!     let client = Client::<Reactor>::new()?;
//!     let handle = client.handle();
//!
//!     // Run the client on a different thread, to not block the main thread.
//!     thread::spawn(|| client.run(cfg).unwrap());
//!
//!     // Wait for the client to be connected to a peer.
//!     handle.wait_for_peers(1, Services::default())?;
//!
//!     // Ask the client to terminate.
//!     handle.shutdown()?;
//!
//!     Ok(())
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
#[cfg(feature = "nakamoto-wallet")]
pub use nakamoto_wallet as wallet;

#[cfg(test)]
#[cfg(feature = "nakamoto-test")]
pub use nakamoto_test as test;

pub mod net {
    #[cfg(feature = "nakamoto-net-poll")]
    pub use nakamoto_net_poll as poll;
}
