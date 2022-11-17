//! Client API model
use nakamoto_chain::BlockHeader;
use nakamoto_common::bitcoin::util::uint::Uint256;
use nakamoto_common::block::Height;

/// Tip Network with the information regarding the chain.
#[allow(dead_code)]
pub struct Tip {
    /// current chain height
    pub height: Height,
    /// Block Header
    pub blk_header: BlockHeader,
    /// total accumulated work
    pub works: Uint256,
}

impl From<(Height, BlockHeader, Uint256)> for Tip {
    fn from(val: (Height, BlockHeader, Uint256)) -> Self {
        Tip {
            height: val.0,
            blk_header: val.1,
            works: val.2,
        }
    }
}
