use nakamoto_common::block::BlockHeader;
pub use nakamoto_common::block::*;

pub mod cache {
    pub mod model;
}

/// Solve a block's proof of work puzzle.
pub fn solve(header: &mut BlockHeader) {
    let target = header.target();
    while header.validate_pow(&target).is_err() {
        header.nonce += 1;
    }
}
