use argh::FromArgs;
use std::net;

#[derive(FromArgs)]
/// A Bitcoin light client.
pub struct Options {
    #[argh(option)]
    /// connect to the specified peers only
    pub connect: Vec<net::SocketAddr>,

    #[argh(switch)]
    /// use the bitcoin test network (default: false)
    pub testnet: bool,

    #[argh(option, default = "log::LevelFilter::Info")]
    /// log level (default: info)
    pub log: log::LevelFilter,
}

impl Options {
    pub fn from_env() -> Self {
        argh::from_env()
    }
}

fn main() {
    let opts = Options::from_env();

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

    if let Err(err) = nakamoto_node::run(&opts.connect) {
        log::error!("{}", err);
        std::process::exit(1);
    }
}
