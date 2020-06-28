use std::path::{Path, PathBuf};

use bitcoin::blockdata::constants;
use lazy_static::*;

use nakamoto_chain::block::cache::model;
use nakamoto_chain::block::store::{self, Store};

lazy_static! {
    pub static ref TREE: model::Cache = {
        let genesis = constants::genesis_block(bitcoin::Network::Bitcoin).header;
        let store = store::File::open(&*self::headers::PATH, genesis).unwrap();

        let headers = store
            .iter()
            .map(|r| r.map(|(_, h)| h))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        model::Cache::from(headers)
    };
}

pub mod headers {
    use super::*;

    lazy_static! {
        pub static ref PATH: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("data/headers.bin")
            .into();
    }
}
