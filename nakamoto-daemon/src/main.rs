use nakamoto_daemon::Options;

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

    if let Err(err) = nakamoto_daemon::run(opts) {
        log::error!("{}", err);
        std::process::exit(1);
    }
}
