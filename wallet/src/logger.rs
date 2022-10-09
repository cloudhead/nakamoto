//! Logging module.
use std::{io, time::SystemTime};

use chrono::prelude::*;
use log::{Level, Log, Metadata, Record, SetLoggerError};

struct Logger {
    level: Level,
    stream: io::Stderr,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            write(record, &self.stream);

            fn write(record: &log::Record, mut stream: impl io::Write) {
                let now =
                    DateTime::from(SystemTime::now()).to_rfc3339_opts(SecondsFormat::Millis, true);
                writeln!(stream, "{} {}", now, record.args()).expect("write shouldn't fail");
            }
        }
    }

    fn flush(&self) {}
}

/// Initialize a new logger.
pub fn init(level: Level) -> Result<(), SetLoggerError> {
    let logger = Logger {
        level,
        stream: io::stderr(),
    };

    log::set_boxed_logger(Box::new(logger))?;
    log::set_max_level(level.to_level_filter());

    Ok(())
}
