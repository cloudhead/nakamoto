use nakamoto_chain::block::cache::BlockCache;
use nakamoto_chain::block::store;
use nakamoto_daemon::Options;
use nakamoto_p2p as p2p;

use std::io;
use std::path::Path;
use std::process;
use std::sync::{Arc, RwLock};

use log;

fn main() {
    let opts = Options::from_env();

    #[cfg(feature = "logging")]
    {
        use atty::Stream;
        use fern::colors::{Color, ColoredLevelConfig};

        let colors = ColoredLevelConfig::new().info(Color::Green);
        let stream = Stream::Stderr;
        let io = std::io::stderr();
        let isatty = atty::is(stream);

        fern::Dispatch::new()
            .format(move |out, message, record| {
                if isatty {
                    out.finish(format_args!(
                        "{:5} [{}] {}",
                        colors.color(record.level()),
                        record.target(),
                        message
                    ))
                } else {
                    out.finish(format_args!(
                        "{:5} [{}] {}",
                        record.level(),
                        record.target(),
                        message
                    ))
                }
            })
            .level(opts.log)
            .chain(io)
            .apply()
            .unwrap();
    }

    log::info!("Initializing daemon..");

    let cfg = p2p::peer::Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();

    log::info!("Genesis block hash is {}", cfg.network.genesis_hash());

    let path = Path::new("headers.db");
    let store = match store::File::create(path, genesis) {
        Err(store::Error::Io(e)) if e.kind() == io::ErrorKind::AlreadyExists => {
            log::info!("Found existing store {:?}", path);
            store::File::open(path).unwrap()
        }
        Err(err) => panic!(err.to_string()),
        Ok(store) => {
            log::info!("Initializing new block store {:?}", path);
            store
        }
    };
    log::info!("Loading blocks from store..");

    let cache = BlockCache::from(store, params).unwrap();
    let block_cache = Arc::new(RwLock::new(cache));
    let mut net = p2p::Network::new(cfg, block_cache);

    if opts.connect.is_empty() {
        log::error!("at least one peer must be supplied using `--connect`");
        process::exit(1);
    }

    net.connect(opts.connect.as_slice()).unwrap();
}
