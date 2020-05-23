use nakamoto_p2p as p2p;

use fern;
use fern::colors::{Color, ColoredLevelConfig};

use log;

fn main() {
    {
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

    let mut net = p2p::Network::new(p2p::peer::Config::default());

    net.connect("0.0.0.0").unwrap();
}
