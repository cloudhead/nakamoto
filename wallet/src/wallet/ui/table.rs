use std::io;

use termion::{clear, cursor};

#[derive(Debug)]
pub struct Table<const N: usize> {
    rows: Vec<[String; N]>,
    widths: [usize; N],
}

impl<const N: usize> Default for Table<N> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            widths: [0; N],
        }
    }
}

impl<const N: usize> Table<N> {
    pub fn push(&mut self, row: [String; N]) {
        for (i, cell) in row.iter().enumerate() {
            self.widths[i] = self.widths[i].max(cell.chars().count());
        }
        self.rows.push(row);
    }

    pub fn render<W: io::Write>(self, width: usize, start: u16, term: &mut W) -> io::Result<()> {
        use std::fmt::Write;

        for (i, row) in self.rows.iter().enumerate() {
            let mut output = String::new();
            let cells = row.len();

            for (i, cell) in row.iter().enumerate() {
                if i == cells - 1 {
                    write!(output, "{}", cell).ok();
                } else {
                    write!(output, "{:width$} ", cell, width = self.widths[i]).ok();
                }
            }
            if output.chars().count() <= width {
                write!(
                    term,
                    "{}{}{}",
                    cursor::Goto(1, i as u16 + start),
                    clear::CurrentLine,
                    output
                )?;
            } else {
                write!(
                    term,
                    "{}{}{:.width$}…",
                    cursor::Goto(1, i as u16 + start),
                    clear::CurrentLine,
                    &output,
                    width = width - 1
                )?;
            }
        }
        Ok(())
    }
}
