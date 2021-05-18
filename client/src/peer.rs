//! Client-related peer functionality.
use std::collections::HashMap;
use std::path::Path;
use std::{fs, io, net};

pub use nakamoto_common::p2p::peer::*;

/// A file-backed implementation of [`Store`].
#[derive(Debug)]
pub struct Cache {
    addrs: HashMap<net::IpAddr, KnownAddress>,
    file: fs::File,
}

impl Cache {
    /// Open an existing cache.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .and_then(Self::from)
    }

    /// Create a new cache.
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?;

        Ok(Self {
            file,
            addrs: HashMap::new(),
        })
    }

    /// Create a new cache from a file.
    pub fn from(mut file: fs::File) -> io::Result<Self> {
        use io::Read;
        use microserde::json::Value;
        use std::str::FromStr;

        let mut s = String::new();
        let mut addrs = HashMap::new();

        file.read_to_string(&mut s)?;

        if !s.is_empty() {
            let val = microserde::json::from_str(&s)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;

            match val {
                Value::Object(ary) => {
                    for (k, v) in ary.into_iter() {
                        let ka = KnownAddress::from_json(v)
                            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
                        let ip = net::IpAddr::from_str(k.as_str())
                            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;

                        addrs.insert(ip, ka);
                    }
                }
                _ => return Err(io::ErrorKind::InvalidData.into()),
            }
        }

        Ok(Self { file, addrs })
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

    fn insert(&mut self, ip: net::IpAddr, ka: KnownAddress) -> bool {
        let inserted = <HashMap<_, _> as Store>::insert(&mut self.addrs, ip, ka);
        if inserted {
            // TODO: Save to disk.
        }
        inserted
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

    fn flush<'a>(&mut self) -> io::Result<()> {
        use io::{Seek, Write};
        use microserde::json::Value;

        let peers: microserde::json::Object = self
            .addrs
            .iter()
            .map(|(ip, ka)| (ip.to_string(), ka.to_json()))
            .collect();
        let s = microserde::json::to_string(&Value::Object(peers));

        self.file.set_len(0)?;
        self.file.seek(io::SeekFrom::Start(0))?;
        self.file.write_all(s.as_bytes())?;
        self.file.write_all(&[b'\n'])?;
        self.file.sync_data()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use bitcoin::network::address::Address;
    use bitcoin::network::constants::ServiceFlags;
    use nakamoto_common::block::time::LocalTime;

    #[test]
    fn test_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache");

        Cache::create(&path).unwrap();
        let cache = Cache::open(&path).unwrap();

        assert!(cache.is_empty());
    }

    #[test]
    fn test_save_and_load() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("cache");
        let mut expected = Vec::new();

        {
            let mut cache = Cache::create(&path).unwrap();

            for i in 32..48 {
                let ip = net::IpAddr::from([127, 0, 0, i]);
                let sockaddr = net::SocketAddr::from((ip, 8333));
                let services = ServiceFlags::NETWORK;
                let ka = KnownAddress {
                    addr: Address::new(&sockaddr, services),
                    source: Source::Dns,
                    last_success: Some(LocalTime::from_secs(i as u64)),
                    last_attempt: None,
                };
                cache.insert(ip, ka);
            }
            cache.flush().unwrap();

            for (ip, ka) in cache.iter() {
                expected.push((*ip, ka.clone()));
            }
        }

        {
            let cache = Cache::open(&path).unwrap();
            let mut actual = cache
                .iter()
                .map(|(i, ka)| (*i, ka.clone()))
                .collect::<Vec<_>>();

            actual.sort_by_key(|(i, _)| *i);
            expected.sort_by_key(|(i, _)| *i);

            assert_eq!(actual, expected);
        }
    }
}
