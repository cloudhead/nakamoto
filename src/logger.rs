use std::io;

use chrono::prelude::*;
use colored::*;
use log::{Level, Log, Metadata, Record, SetLoggerError};

struct Logger {
    level: Level,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let target = if record.target().len() > 0 {
                record.target()
            } else {
                record.module_path().unwrap_or_default()
            };

            if record.level() == Level::Error {
                write(record, target, io::stderr());
            } else {
                write(record, target, io::stdout());
            }

            fn write(record: &log::Record, target: &str, mut stream: impl io::Write) {
                let level_string = match record.level() {
                    Level::Error => record.level().to_string().red(),
                    Level::Warn => record.level().to_string().yellow(),
                    Level::Info => record.level().to_string().green(),
                    Level::Debug => record.level().to_string().white(),
                    Level::Trace => record.level().to_string().white().dimmed(),
                };

                write!(
                    stream,
                    "{} {:<5} {} {}\n",
                    Local::now()
                        .to_rfc3339_opts(SecondsFormat::Millis, true)
                        .white(),
                    level_string,
                    target.bold(),
                    record.args()
                )
                .expect("write shouldn't fail");
            };
        }
    }

    fn flush(&self) {}
}

pub fn init(level: Level) -> Result<(), SetLoggerError> {
    let logger = Logger { level };

    log::set_boxed_logger(Box::new(logger))?;
    log::set_max_level(level.to_level_filter());

    Ok(())
}
