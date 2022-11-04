use std::io;

use crossbeam_channel as chan;
use termion::event::{Event, Key};
use termion::input::TermRead;
use thiserror::Error;

/// An input error.
#[derive(Error, Debug)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    #[error("channel error: {0}")]
    EventChannel(#[from] chan::SendError<Event>),
    #[error("channel error: {0}")]
    SignalChannel(#[from] chan::SendError<Signal>),
}

#[derive(Copy, Clone, Debug)]
pub enum Signal {
    WindowResized,
    Interrupted,
}

pub fn run(channel: chan::Sender<Event>, exit: chan::Receiver<()>) -> Result<(), Error> {
    let stdin = io::stdin().lock();

    for event in stdin.events() {
        let event = event?;

        if exit.try_recv().is_ok() {
            return Ok(());
        }
        if let Event::Key(Key::Char('q')) | Event::Key(Key::Esc) = event {
            return Ok(());
        }
        channel.send(event)?;
    }
    Ok(())
}

pub fn signals(channel: chan::Sender<Signal>) -> Result<(), Error> {
    use signal_hook::consts::signal::*;

    let mut signals = signal_hook::iterator::Signals::new([SIGWINCH, SIGINT])?;
    for signal in signals.forever() {
        match signal {
            SIGWINCH => channel.send(Signal::WindowResized)?,
            SIGINT => channel.send(Signal::Interrupted)?,
            _ => {}
        }
    }
    Ok(())
}
