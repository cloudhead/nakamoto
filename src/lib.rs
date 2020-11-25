//! Nakamoto is a high-assurance Bitcoin light-client library and daemon.

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
