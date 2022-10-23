mod table;

use std::ops::ControlFlow;
use std::{fmt, io, time};

use termion::event::Event;
use termion::event::{Key, MouseEvent};
use termion::{clear, color, cursor, style};

use nakamoto_client as client;
use nakamoto_common::bitcoin;
use nakamoto_common::bitcoin::Address;
use nakamoto_common::block::Height;

use crate::wallet::db;
use table::Table;

/// Redraw flags. Sets what needs redrawing.
type Redraw = u8;

/// Don't redraw anything.
const REDRAW_NONE: Redraw = 0b0000;
/// Redraw the header.
const REDRAW_HEADER: Redraw = 0b0001;
/// Redraw the main area.
const REDRAW_MAIN: Redraw = 0b0010;
/// Redraw the footer.
const REDRAW_FOOTER: Redraw = 0b0100;
/// Redraw everything.
const REDRAW_ALL: Redraw = REDRAW_MAIN | REDRAW_HEADER | REDRAW_FOOTER;
/// Row number at which header area starts (1-indexed).
const HEADER_ROW: u16 = 1;
/// Row number at which main area starts (1-indexed).
const MAIN_ROW: u16 = 3;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Db(#[from] db::Error),
}

#[derive(Debug)]
pub struct Balance(u64);

impl std::fmt::Display for Balance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let number = self.0 as f64 / 100000000.0;
        let formatted = format!("{:.4} BTC", number);

        f.pad(&formatted)
    }
}

#[derive(Debug, Copy, Clone, Default)]
struct Vec2D {
    x: u16,
    y: u16,
}

impl From<(u16, u16)> for Vec2D {
    fn from((x, y): (u16, u16)) -> Self {
        Self { x, y }
    }
}

#[derive(Debug)]
struct Aligned {
    text: String,
    position: Vec2D,
    constraint: Vec2D,
}

impl fmt::Display for Aligned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}",
            cursor::Goto(self.position.x, self.position.y),
            self.text
        )
    }
}

impl Aligned {
    fn new(text: impl ToString, constraint: Vec2D) -> Self {
        Self {
            text: text.to_string(),
            position: Vec2D { x: 1, y: 1 },
            constraint,
        }
    }

    fn right(mut self) -> Aligned {
        self.position.x = self.constraint.x - self.text.len() as u16 + 1;
        self
    }

    #[allow(dead_code)]
    fn left(mut self) -> Aligned {
        self.position.x = 1;
        self
    }

    fn center(mut self) -> Aligned {
        self.position.x = self.constraint.x / 2 - self.text.len() as u16 / 2;
        self
    }
}

#[derive(Debug)]
pub struct Ui {
    pub message: String,

    status: Status,
    balance: Balance,
    tab: Tab,
    tip: Height,
    size: Vec2D,

    last_redraw: Option<time::Instant>,
    redraw: Redraw,
}

impl Default for Ui {
    fn default() -> Self {
        Self {
            balance: Balance(0),
            tab: Tab::Utxos,
            size: Vec2D::default(),
            tip: 0,
            status: Status::LoadingBlockHeaders { height: 0 },
            message: String::new(),
            last_redraw: None,
            redraw: REDRAW_ALL,
        }
    }
}

impl Ui {
    pub fn redraw<D: db::Read, W: io::Write>(&mut self, db: &D, term: &mut W) -> Result<(), Error> {
        self.redraw = REDRAW_ALL;
        self.reset(term)?;
        self.decorations(term)?;

        refresh(self, db, term)?;

        Ok(())
    }

    pub fn offline(&mut self, offline: bool) {
        self.status = Status::Ready { height: 0, offline };
        self.redraw |= REDRAW_HEADER;
    }

    pub fn reset<W: io::Write>(&mut self, term: &mut W) -> io::Result<()> {
        self.size = termion::terminal_size()?.into();

        write!(
            term,
            "{}{}",
            termion::clear::All,
            termion::cursor::Goto(1, 1),
        )
    }

    pub fn decorations<W: io::Write>(&self, term: &mut W) -> io::Result<()> {
        let Vec2D { x: width, .. } = self.size;

        write!(
            term,
            "{}{}{}",
            termion::cursor::Goto(1, HEADER_ROW + 1),
            termion::color::Fg(termion::color::Red),
            "▔".repeat(width as usize)
        )
    }

    pub fn set_balance(&mut self, balance: u64) {
        self.balance = Balance(balance);
        self.redraw |= REDRAW_HEADER;
    }

    pub fn handle_ready(&mut self, height: Height, offline: bool) {
        self.tip = height;
        self.status = Status::Ready { height, offline };
        self.redraw |= REDRAW_HEADER;
    }

    pub fn handle_filter_processed(&mut self, height: Height) {
        self.status = Status::Scanning {
            height,
            tip: self.tip,
        };
        self.redraw |= REDRAW_HEADER;
    }

    pub fn handle_peer_height(&mut self, height: Height) {
        self.tip = height;
    }

    pub fn handle_synced(&mut self, height: Height, tip: Height) {
        self.status = if tip == height {
            Status::Synced { height }
        } else {
            Status::Syncing { height, tip }
        };
        self.tip = tip;
        self.redraw |= REDRAW_HEADER;
    }

    pub fn handle_input_event(&mut self, input: Event) -> io::Result<ControlFlow<()>> {
        match input {
            // Switch tabs.
            Event::Key(Key::Right | Key::Char('\t')) => {
                self.tab.next();
                self.redraw |= REDRAW_HEADER | REDRAW_MAIN;
            }
            Event::Key(Key::Left) => {
                self.tab.prev();
                self.redraw |= REDRAW_HEADER | REDRAW_MAIN;
            }
            Event::Mouse(MouseEvent::Press(_, _x, _y)) => {}
            _ => (),
        }
        Ok(ControlFlow::Continue(()))
    }

    pub fn handle_loading_event(&mut self, event: client::Loading) -> io::Result<ControlFlow<()>> {
        match event {
            client::Loading::BlockHeaderLoaded { height } => {
                self.status = Status::LoadingBlockHeaders { height };
            }
            client::Loading::FilterHeaderLoaded { height } => {
                self.status = Status::LoadingFilterHeaders { height };
            }
            client::Loading::FilterHeaderVerified { height } => {
                self.status = Status::VerifyingFilterHeaders { height };
            }
        }
        // Limit redraws to 60hz.
        if self
            .last_redraw
            .map_or(true, |t| t.elapsed() > time::Duration::from_millis(16))
        {
            self.redraw |= REDRAW_HEADER;
        }
        Ok(ControlFlow::Continue(()))
    }

    fn align(&self, text: impl ToString) -> Aligned {
        Aligned::new(text, self.size)
    }
}

#[derive(Debug)]
enum Status {
    Ready { height: Height, offline: bool },
    LoadingBlockHeaders { height: Height },
    LoadingFilterHeaders { height: Height },
    Scanning { height: Height, tip: Height },
    VerifyingFilterHeaders { height: Height },
    Syncing { height: Height, tip: Height },
    Synced { height: Height },
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn percent(a: f64, b: f64) -> String {
            format!("{:.2}%", a as f64 / b as f64 * 100.)
        }

        match self {
            Self::Ready { height, offline } => {
                if *offline {
                    write!(f, "Ready (offline)")
                } else {
                    write!(f, "Ready (height {})", height)
                }
            }
            Self::LoadingBlockHeaders { height } => {
                write!(f, "Loading block header {}..", height)
            }
            Self::LoadingFilterHeaders { height } => {
                write!(f, "Loading filter header {}..", height)
            }
            Self::VerifyingFilterHeaders { height } => {
                write!(f, "Verifying filter header {}..", height)
            }
            Self::Syncing { height, tip } => {
                write!(
                    f,
                    "Syncing {}/{} ({})",
                    height,
                    tip,
                    percent(*height as f64, *tip as f64)
                )
            }
            Self::Scanning { height, tip } => {
                write!(
                    f,
                    "Scanning {}/{} ({})",
                    height,
                    tip,
                    percent(*height as f64, *tip as f64)
                )
            }
            Self::Synced { height } => {
                write!(f, "Synced to block {}", height)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum Tab {
    Utxos,
    History,
    Addresses,
}

impl Tab {
    fn next(&mut self) {
        match self {
            Self::Utxos => *self = Self::History,
            Self::History => *self = Self::Addresses,
            Self::Addresses => *self = Self::Utxos,
        }
    }

    fn prev(&mut self) {
        match self {
            Self::Utxos => *self = Self::Addresses,
            Self::History => *self = Self::Utxos,
            Self::Addresses => *self = Self::History,
        }
    }
}

impl fmt::Display for Tab {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Utxos => write!(f, "UTXOs"),
            Self::History => write!(f, "History"),
            Self::Addresses => write!(f, "Addresses"),
        }
    }
}

pub fn refresh<D: db::Read, W: io::Write>(ui: &mut Ui, db: &D, term: &mut W) -> Result<(), Error> {
    ui.size = termion::terminal_size()?.into();

    if ui.redraw | REDRAW_HEADER == ui.redraw {
        draw_header(ui, term)?;
    }
    if ui.redraw | REDRAW_MAIN == ui.redraw {
        write!(term, "{}{}", cursor::Goto(1, MAIN_ROW), clear::AfterCursor)?;
        ui.redraw |= REDRAW_FOOTER;

        match ui.tab {
            Tab::Utxos => draw_utxo_tab(db, term)?,
            Tab::Addresses => draw_addresses_tab(ui, db, term)?,
            Tab::History => draw_history_tab(db, term)?,
        }
    }
    if ui.redraw | REDRAW_FOOTER == ui.redraw {
        draw_footer(ui, term)?;
    }
    if ui.redraw != REDRAW_NONE {
        write!(
            term,
            "{}{}{}",
            style::Reset,
            color::Bg(color::Reset),
            color::Fg(color::Reset),
        )?;
        term.flush()?;

        ui.redraw = REDRAW_NONE;
        ui.last_redraw = Some(time::Instant::now());
    }

    Ok(())
}

pub fn draw_header<W: io::Write>(ui: &Ui, term: &mut W) -> io::Result<()> {
    let balance = ui.align(&ui.balance).right();

    write!(
        term,
        "{}{}",
        cursor::Goto(1, HEADER_ROW),
        clear::CurrentLine
    )?;
    write!(
        term,
        "{}{}{}",
        color::Fg(color::Cyan),
        balance,
        style::Reset,
    )?;

    write!(
        term,
        "{}{}{}{}",
        color::Fg(color::Red),
        style::Faint,
        ui.align(&ui.status).center(),
        style::Reset,
    )?;

    let mut tabs = Vec::new();

    write!(term, "{}", cursor::Goto(1, HEADER_ROW))?;

    for tab in [Tab::Utxos, Tab::History, Tab::Addresses] {
        if ui.tab == tab {
            tabs.push(format!(
                "{}{} {} {}",
                color::Fg(color::Red),
                style::Invert,
                tab,
                style::Reset,
            ));
        } else {
            tabs.push(format!(
                "{} {} {}",
                color::Fg(color::Red),
                tab,
                style::Reset
            ));
        }
    }
    write!(term, "{}", tabs.join(" "))?;

    Ok(())
}

pub fn draw_utxo_tab<D: db::Read, W: io::Write>(db: &D, term: &mut W) -> Result<(), Error> {
    let utxos = db.utxos()?;

    for (i, (outpoint, txout)) in utxos.iter().enumerate() {
        let addr = Address::from_script(&txout.script_pubkey, bitcoin::Network::Bitcoin).unwrap();

        write!(
            term,
            "{}{}{}{}{:.7} {}{} {}{:>13}",
            cursor::Goto(1, MAIN_ROW + i as u16),
            clear::CurrentLine,
            color::Fg(color::Reset),
            style::Faint,
            outpoint.txid,
            style::NoFaint,
            addr,
            color::Fg(color::LightCyan),
            Balance(txout.value),
        )?;
    }
    Ok(())
}

pub fn draw_addresses_tab<D: db::Read, W: io::Write>(
    ui: &Ui,
    db: &D,
    term: &mut W,
) -> Result<(), Error> {
    let addresses = db.addresses()?;
    let mut table = Table::default();

    for address in addresses.iter() {
        table.push([
            address.index.to_string(),
            address.address.to_string(),
            Balance(address.received).to_string(),
        ]);
    }
    table.render(ui.size.x as usize, MAIN_ROW, term)?;

    Ok(())
}

pub fn draw_history_tab<D: db::Read, W: io::Write>(_db: &D, _term: &mut W) -> Result<(), Error> {
    Ok(())
}

pub fn draw_footer<W: io::Write>(ui: &Ui, term: &mut W) -> io::Result<()> {
    let Vec2D { y: height, .. } = ui.size;

    write!(
        term,
        "{}{}{}{}",
        cursor::Goto(1, height - 1),
        color::Bg(color::Reset),
        color::Fg(color::Blue),
        ui.message,
    )?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_balance_fmt() {
        let output = Balance(14912334245).to_string();
        assert_eq!(output, "149.1233 BTC");
    }
}
