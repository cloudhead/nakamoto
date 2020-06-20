use bitcoin::consensus::encode;

use std::io;
use std::time;

use thiserror::Error;

use nakamoto_chain::block::tree;

/// An error occuring in peer-to-peer networking code.
#[derive(Error, Debug)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("timeout error: {0:?}")]
    Timeout(time::Duration),

    #[error("chain validation error: {0}")]
    BlockImport(#[from] tree::Error),

    #[error("encode/decode error: {0}")]
    Encode(#[from] encode::Error),

    #[error("not connected to the peer network")]
    NotConnected,
}
