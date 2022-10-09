pub mod assert;
pub mod block;

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use nakamoto_common::bitcoin;
use nakamoto_common::bitcoin::blockdata::constants;
use nakamoto_common::bitcoin::consensus::encode::Decodable;

use lazy_static::*;

use nakamoto_common::block::BlockHeader;
use nakamoto_common::nonempty::NonEmpty;

lazy_static! {
    pub static ref BITCOIN_HEADERS: NonEmpty<BlockHeader> = {
        let genesis = constants::genesis_block(bitcoin::Network::Bitcoin).header;
        let mut f = File::open(&*self::headers::PATH).unwrap();
        let mut buf = [0; 80];
        let mut headers = NonEmpty::new(genesis);

        while f.read_exact(&mut buf).is_ok() {
            let header = BlockHeader::consensus_decode(&buf[..]).unwrap();
            headers.push(header);
        }
        headers
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
    use log::*;

    struct Logger {
        level: Level,
    }

    impl Log for Logger {
        fn enabled(&self, metadata: &Metadata) -> bool {
            metadata.level() <= self.level
        }

        fn log(&self, record: &Record) {
            use colored::Colorize;

            match record.target() {
                "test" => {
                    println!(
                        "{} {}",
                        "test:".yellow(),
                        record.args().to_string().yellow()
                    )
                }
                "sim" => {
                    println!("{}  {}", "sim:".bold(), record.args().to_string().bold())
                }
                target => {
                    if self.enabled(record.metadata()) {
                        let s = format!("{:<5} {}", format!("{}:", target), record.args());
                        println!("{}", s.dimmed());
                    }
                }
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

pub mod arbitrary {
    /// Generator for numbers in a statically defined inclusive range.
    #[derive(Debug, Clone)]
    pub struct InRange<const N: u64, const M: u64>(pub u64);

    impl<const N: u64, const M: u64> quickcheck::Arbitrary for InRange<N, M> {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let rng = fastrand::Rng::with_seed(u64::arbitrary(g));

            Self(rng.u64(N..=M))
        }

        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            let InRange(n) = self;

            if *n > N {
                Box::new((N..*n).map(InRange))
            } else {
                Box::new(std::iter::empty())
            }
        }
    }
}
