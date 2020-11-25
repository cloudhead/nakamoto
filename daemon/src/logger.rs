//! Logging module.
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
            let module = record.module_path().unwrap_or_default();

            if record.level() == Level::Error {
                write(record, module, io::stderr());
            } else {
                write(record, module, io::stdout());
            }

            fn write(record: &log::Record, module: &str, mut stream: impl io::Write) {
                let message = format!(
                    "{} {} {}",
                    record.level().to_string(),
                    module.bold(),
                    record.args().to_string()
                );
                let message = match record.level() {
                    Level::Error => message.red(),
                    Level::Warn => message.yellow(),
                    Level::Info => message.normal(),
                    Level::Debug => message.dimmed(),
                    Level::Trace => message.white().dimmed(),
                };

                writeln!(
                    stream,
                    "{} {}",
                    Local::now()
                        .to_rfc3339_opts(SecondsFormat::Millis, true)
                        .white(),
                    message,
                )
                .expect("write shouldn't fail");
            };
        }
    }

    fn flush(&self) {}
}

/// Initialize a new logger.
pub fn init(level: Level) -> Result<(), SetLoggerError> {
    let logger = Logger { level };

    log::set_boxed_logger(Box::new(logger))?;
    log::set_max_level(level.to_level_filter());

    Ok(())
}
