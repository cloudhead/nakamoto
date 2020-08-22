//! Address book. Keeps track of known peers.
use std::fmt;
use std::fs::File;
use std::io::{self, prelude::*};
use std::net;
use std::ops::Deref;
use std::path::Path;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct AddressBook {
    addrs: Vec<net::SocketAddr>,
}

impl AddressBook {
    pub const fn new() -> Self {
        Self { addrs: Vec::new() }
    }

    pub fn from<T: net::ToSocketAddrs + fmt::Debug>(seeds: &[T]) -> io::Result<Self> {
        let addrs = seeds
            .iter()
            .flat_map(|seed| match seed.to_socket_addrs() {
                Ok(addrs) => addrs.map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<io::Result<_>>()?;

        Ok(Self { addrs })
    }

    pub fn bootstrap(seeds: &[&str], port: u16) -> io::Result<Self> {
        let seeds = seeds.iter().map(|s| (*s, port)).collect::<Vec<_>>();

        AddressBook::from(&seeds)
    }

    pub fn push(&mut self, addr: net::SocketAddr) {
        self.addrs.push(addr);
    }

    pub fn pop(&mut self) -> Option<net::SocketAddr> {
        self.addrs.pop()
    }

    pub fn seed<S: net::ToSocketAddrs>(&mut self, seeds: Vec<S>) -> io::Result<()> {
        for seed in seeds.into_iter() {
            let addrs = seed.to_socket_addrs()?;
            self.addrs.extend(addrs);
        }
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        use std::io::BufReader;

        let mut addrs = Vec::new();

        let file = match File::open(path) {
            Ok(f) => f,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(Self { addrs });
            }
            Err(err) => return Err(err),
        };

        let reader = BufReader::with_capacity(32, file);

        for line in reader.lines() {
            let line = line?;
            let addr = line.parse().map_err(|err: net::AddrParseError| {
                io::Error::new(io::ErrorKind::InvalidData, err.to_string())
            })?;

            addrs.push(addr);
        }

        Ok(Self { addrs })
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let mut f = File::create(path)?;

        for addr in self.addrs.iter() {
            writeln!(&mut f, "{}", addr)?;
        }

        Ok(())
    }
}

impl Deref for AddressBook {
    type Target = Vec<net::SocketAddr>;

    fn deref(&self) -> &Self::Target {
        &self.addrs
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_save_and_load() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("address-book");

        let book = AddressBook::load(&path).unwrap();
        assert!(book.addrs.is_empty());

        let book = AddressBook::from(&[
            ("143.25.122.51", 8333),
            ("231.45.72.2", 8334),
            ("113.98.77.4", 8333),
        ])
        .unwrap();

        book.save(&path).unwrap();

        assert_eq!(AddressBook::load(&path).unwrap(), book);
    }
}
