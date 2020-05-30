use nakamoto_chain::blocktree::BlockCache;
use nakamoto_p2p as p2p;

use std::sync::{Arc, RwLock};

use log;

fn main() {
    #[cfg(feature = "fern")]
    {
        use fern::colors::{Color, ColoredLevelConfig};

        let colors = ColoredLevelConfig::new().info(Color::Green);
        fern::Dispatch::new()
            .format(move |out, message, record| {
                out.finish(format_args!(
                    "{:5} [{}] {}",
                    colors.color(record.level()),
                    record.target(),
                    message
                ))
            })
            .level(log::LevelFilter::Trace)
            .chain(std::io::stderr())
            .apply()
            .unwrap();
    }

    log::info!("Initializing daemon..");

    let cfg = p2p::peer::Config::default();
    let genesis = cfg.network.genesis();
    let params = cfg.network.params();

    log::info!("Genesis block hash is {}", cfg.network.genesis_hash());

    let block_cache = Arc::new(RwLock::new(BlockCache::new(genesis, params)));
    let mut net = p2p::Network::new(cfg, block_cache);

    net.connect("0.0.0.0").unwrap();
}
