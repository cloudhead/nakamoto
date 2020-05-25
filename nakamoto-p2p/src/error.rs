use std::io;

use thiserror::Error;

use nakamoto_chain::blocktree;

/// An error occuring in peer-to-peer networking code.
#[derive(Error, Debug)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),

    #[error("chain validation error: {0}")]
    ChainValidation(#[from] blocktree::Error),
}
