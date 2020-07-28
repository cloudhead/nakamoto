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
        pub static ref PATH: PathBuf =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("data/headers.bin");
    }
}

pub mod logger {
    use chrono::prelude::*;
    use log::*;

    struct Logger {
        level: Level,
    }

    impl Log for Logger {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() <= self.level
        }

        fn log(&self, record: &Record) {
            if self.enabled(record.metadata()) {
                let target = if !record.target().is_empty() {
                    record.target()
                } else {
                    record.module_path().unwrap_or_default()
                };

                println!(
                    "{} {:<5} [{}] {}",
                    Local::now().to_rfc3339_opts(SecondsFormat::Millis, true),
                    record.level(),
                    target,
                    record.args()
                )
            }
        }

        fn flush(&self) {}
    }

    pub fn init(level: Level) {
        let logger = Logger { level };

        log::set_boxed_logger(Box::new(logger)).ok();
        log::set_max_level(level.to_level_filter());
    }
}
