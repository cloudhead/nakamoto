//! Client-related peer functionality.
use std::collections::HashMap;
use std::path::Path;
use std::{fs, io, net};

pub use nakamoto_common::p2p::peer::*;

/// A file-backed implementation of [`Store`].
pub struct Cache {
    addrs: HashMap<net::IpAddr, KnownAddress>,
    file: fs::File,
}

impl Cache {
    /// Open an existing cache.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        fs::OpenOptions::new()
            .create(true)
            .read(true)
            .open(path)
            .and_then(|file| Self::from(file))
    }

    /// Create a new cache.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = fs::File::create(path)?;

        Ok(Self {
            file,
            addrs: HashMap::new(),
        })
    }

    /// Create a new cache from a file.
    pub fn from(mut file: fs::File) -> io::Result<Self> {
        use io::Read;
        use microserde::json::Value;

        let mut s = String::new();
        let mut addrs = HashMap::new();

        file.read_to_string(&mut s)?;

        let val = microserde::json::from_str(&s)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
        match val {
            Value::Array(ary) => {
                for v in ary.into_iter() {
                    let ka = KnownAddress::from_json(v)
                        .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                    let ip = net::IpAddr::from(ka.addr.address);

                    addrs.insert(ip, ka);
                }
            }
            _ => return Err(io::ErrorKind::InvalidData.into()),
        }

        Ok(Self { file, addrs })
    }

    /// Flush data to disk.
    #[allow(dead_code)]
    fn flush<'a>(&mut self) -> io::Result<()> {
        use io::{Seek, Write};
        use microserde::json::Value;

        let list: microserde::json::Array = self.addrs.values().map(|a| a.to_json()).collect();
        let s = microserde::json::to_string(&Value::Array(list));

        self.file.set_len(0)?;
        self.file.seek(io::SeekFrom::Start(0))?;
        self.file.write_all(s.as_bytes())?;
        self.file.sync_data()?;

        Ok(())
    }
}

impl Store for Cache {
    fn get_mut(&mut self, ip: &net::IpAddr) -> Option<&mut KnownAddress> {
        self.addrs.get_mut(ip)
    }

    fn get(&self, ip: &net::IpAddr) -> Option<&KnownAddress> {
        self.addrs.get(ip)
    }

    fn remove(&mut self, ip: &net::IpAddr) -> Option<KnownAddress> {
        self.addrs.remove(ip)
    }

    fn insert(&mut self, ip: net::IpAddr, ka: KnownAddress) -> Option<KnownAddress> {
        self.addrs.insert(ip, ka)
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (&net::IpAddr, &KnownAddress)> + 'a> {
        Box::new(self.addrs.iter())
    }

    fn clear(&mut self) {
        self.addrs.clear()
    }

    fn len(&self) -> usize {
        self.addrs.len()
    }
}
