//! Persistent storage backend for blocks.
use std::fs;
use std::io::{self, Read, Seek, Write};
use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::path::Path;

use nakamoto_common::bitcoin::consensus::encode::{Decodable, Encodable};

use nakamoto_common::block::store::{Error, Store};
use nakamoto_common::block::Height;

/// Append a block to the end of the stream.
fn put<H: Sized + Encodable, S: Seek + Write, I: Iterator<Item = H>>(
    mut stream: S,
    headers: I,
) -> Result<Height, Error> {
    let mut pos = stream.seek(io::SeekFrom::End(0))?;
    let size = std::mem::size_of::<H>();

    for header in headers {
        pos += header.consensus_encode(&mut stream)? as u64;
    }
    Ok(pos / size as u64)
}

/// Get a block from the stream.
fn get<H: Decodable, S: Seek + Read>(mut stream: S, ix: u64) -> Result<H, Error> {
    let size = std::mem::size_of::<H>();
    let mut buf = vec![0; size]; // TODO: Use an array when rust has const-generics.

    stream.seek(io::SeekFrom::Start(ix * size as u64))?;
    stream.read_exact(&mut buf)?;

    H::consensus_decode(&buf[..]).map_err(Error::from)
}

/// An iterator over block headers in a file.
#[derive(Debug)]
pub struct Iter<H> {
    height: Height,
    file: fs::File,

    _phantom: PhantomData<H>,
}

impl<H: Decodable> Iterator for Iter<H> {
    type Item = Result<(Height, H), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let height = self.height;

        assert!(height > 0);

        match get(&mut self.file, height - 1) {
            // If we hit this branch, it's because we're trying to read passed the end
            // of the file, which means there are no further headers remaining.
            Err(Error::Io(err)) if err.kind() == io::ErrorKind::UnexpectedEof => None,
            // If another kind of error occurs, we want to yield it to the caller, so
            // that it can be propagated.
            Err(err) => Some(Err(err)),
            Ok(header) => {
                self.height = height + 1;
                Some(Ok((height, header)))
            }
        }
    }
}

/// A `Store` backed by a single file.
#[derive(Debug)]
pub struct File<H> {
    file: fs::File,
    genesis: H,
}

impl<H> File<H> {
    /// Open a new file store from the given path and genesis header.
    pub fn open<P: AsRef<Path>>(path: P, genesis: H) -> io::Result<Self> {
        fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map(|file| Self { file, genesis })
    }

    /// Create a new file store at the given path, with the provided genesis header.
    pub fn create<P: AsRef<Path>>(path: P, genesis: H) -> Result<Self, Error> {
        let file = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(path)?;

        Ok(Self { file, genesis })
    }
}

impl<H: 'static + Copy + Encodable + Decodable> Store for File<H> {
    type Header = H;

    /// Get the genesis block.
    fn genesis(&self) -> H {
        self.genesis
    }

    /// Append a block to the end of the file.
    fn put<I: Iterator<Item = Self::Header>>(&mut self, headers: I) -> Result<Height, Error> {
        self::put(&mut self.file, headers)
    }

    /// Get the block at the given height. Returns `io::ErrorKind::UnexpectedEof` if
    /// the height is not found.
    fn get(&self, height: Height) -> Result<H, Error> {
        if let Some(ix) = height.checked_sub(1) {
            // Clone so this function doesn't have to take a `&mut self`.
            let mut file = self.file.try_clone()?;
            get(&mut file, ix)
        } else {
            Ok(self.genesis)
        }
    }

    /// Rollback the chain to the given height. Behavior is undefined if the given
    /// height is not contained in the store.
    fn rollback(&mut self, height: Height) -> Result<(), Error> {
        let size = mem::size_of::<H>();

        self.file
            .set_len((height) * size as u64)
            .map_err(Error::from)
    }

    /// Flush changes to disk.
    fn sync(&mut self) -> Result<(), Error> {
        self.file.sync_data().map_err(Error::from)
    }

    /// Iterate over all headers in the store.
    fn iter(&self) -> Box<dyn Iterator<Item = Result<(Height, H), Error>>> {
        // Clone so this function doesn't have to take a `&mut self`.
        match self.file.try_clone() {
            Ok(file) => Box::new(iter::once(Ok((0, self.genesis))).chain(Iter {
                height: 1,
                file,
                _phantom: PhantomData,
            })),
            Err(err) => Box::new(iter::once(Err(Error::Io(err)))),
        }
    }

    /// Return the number of headers in the store.
    fn len(&self) -> Result<usize, Error> {
        let meta = self.file.metadata()?;
        let len = meta.len();
        let size = mem::size_of::<H>();

        assert!(len <= usize::MAX as u64);

        if len as usize % size != 0 {
            return Err(Error::Corruption);
        }
        Ok(len as usize / size + 1)
    }

    /// Return the block height of the store.
    fn height(&self) -> Result<Height, Error> {
        self.len().map(|n| n as Height - 1)
    }

    /// Check the file store integrity.
    fn check(&self) -> Result<(), Error> {
        self.len().map(|_| ())
    }

    /// Attempt to heal data corruption.
    fn heal(&self) -> Result<(), Error> {
        let meta = self.file.metadata()?;
        let len = meta.len();
        let size = mem::size_of::<H>();

        assert!(len <= usize::MAX as u64);

        let extraneous = len as usize % size;
        if extraneous != 0 {
            self.file.set_len(len - extraneous as u64)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{io, iter};

    use super::{Error, File, Height, Store};
    use crate::block::BlockHeader;

    const HEADER_SIZE: usize = 80;

    fn store(path: &str) -> File<BlockHeader> {
        let tmp = tempfile::tempdir().unwrap();
        let genesis = BlockHeader {
            version: 1,
            prev_blockhash: Default::default(),
            merkle_root: Default::default(),
            bits: 0x2ffffff,
            time: 39123818,
            nonce: 0,
        };

        File::open(tmp.path().join(path), genesis).unwrap()
    }

    #[test]
    fn test_put_get() {
        let mut store = store("headers.db");

        let header = BlockHeader {
            version: 1,
            prev_blockhash: store.genesis.block_hash(),
            merkle_root: Default::default(),
            bits: 0x2ffffff,
            time: 1842918273,
            nonce: 312143,
        };

        assert_eq!(
            store.get(0).unwrap(),
            store.genesis,
            "when the store is empty, we can `get` the genesis"
        );
        assert!(
            store.get(1).is_err(),
            "when the store is empty, we can't get height `1`"
        );

        let height = store.put(iter::once(header)).unwrap();
        store.sync().unwrap();

        assert_eq!(height, 1);
        assert_eq!(store.get(height).unwrap(), header);
    }

    #[test]
    fn test_put_get_batch() {
        let mut store = store("headers.db");

        assert_eq!(store.len().unwrap(), 1);

        let count = 32;
        let header = BlockHeader {
            version: 1,
            prev_blockhash: store.genesis().block_hash(),
            merkle_root: Default::default(),
            bits: 0x2ffffff,
            time: 1842918273,
            nonce: 0,
        };
        let iter = (0..count).map(|i| BlockHeader { nonce: i, ..header });
        let headers = iter.clone().collect::<Vec<_>>();

        // Put all headers into the store and check that we can retrieve them.
        {
            let height = store.put(iter).unwrap();

            assert_eq!(height, headers.len() as Height);
            assert_eq!(store.len().unwrap(), headers.len() + 1); // Account for genesis.

            for (i, h) in headers.iter().enumerate() {
                assert_eq!(&store.get(i as Height + 1).unwrap(), h);
            }

            assert!(&store.get(32 + 1).is_err());
        }

        // Rollback and overwrite the history.
        {
            let h = headers.len() as Height / 2; // Some point `h` in the past.

            assert!(&store.get(h + 1).is_ok());
            assert_eq!(store.get(h + 1).unwrap(), headers[h as usize]);

            store.rollback(h).unwrap();

            assert!(
                &store.get(h + 1).is_err(),
                "after the rollback, we can't access blocks passed `h`"
            );
            assert_eq!(store.len().unwrap(), h as usize + 1);

            // We can now overwrite the block at position `h + 1`.
            let header = BlockHeader {
                nonce: 49219374,
                ..header
            };
            let height = store.put(iter::once(header)).unwrap();

            assert!(header != headers[height as usize]);

            assert_eq!(height, h + 1);
            assert_eq!(store.get(height).unwrap(), header);

            // Blocks up to and including `h` are unaffected by the rollback.
            assert_eq!(store.get(0).unwrap(), store.genesis);
            assert_eq!(store.get(1).unwrap(), headers[0]);
            assert_eq!(store.get(h).unwrap(), headers[h as usize - 1]);
        }
    }

    #[test]
    fn test_iter() {
        let mut store = store("headers.db");

        let count = 32;
        let header = BlockHeader {
            version: 1,
            prev_blockhash: store.genesis().block_hash(),
            merkle_root: Default::default(),
            bits: 0x2ffffff,
            time: 1842918273,
            nonce: 0,
        };
        let iter = (0..count).map(|i| BlockHeader { nonce: i, ..header });
        let headers = iter.clone().collect::<Vec<_>>();

        store.put(iter).unwrap();

        let mut iter = store.iter();
        assert_eq!(iter.next().unwrap().unwrap(), (0, store.genesis));

        for (i, result) in iter.enumerate() {
            let (height, header) = result.unwrap();

            assert_eq!(i as u64 + 1, height);
            assert_eq!(header, headers[height as usize - 1]);
        }
    }

    #[test]
    fn test_corrupt_file() {
        let mut store = store("headers.db");

        store.check().expect("checking always works");
        store.heal().expect("healing when there is no corruption");

        let headers = &[
            BlockHeader {
                version: 1,
                prev_blockhash: store.genesis().block_hash(),
                merkle_root: Default::default(),
                bits: 0x2ffffff,
                time: 1842918273,
                nonce: 312143,
            },
            BlockHeader {
                version: 1,
                prev_blockhash: Default::default(),
                merkle_root: Default::default(),
                bits: 0x1ffffff,
                time: 1842918920,
                nonce: 913716378,
            },
        ];
        store.put(headers.iter().cloned()).unwrap();
        store.check().unwrap();

        assert_eq!(store.len().unwrap(), 3);

        let size = std::mem::size_of::<BlockHeader>();
        assert_eq!(size, HEADER_SIZE);

        // Intentionally corrupt the file, by truncating it by 32 bytes.
        store
            .file
            .set_len(headers.len() as u64 * size as u64 - 32)
            .unwrap();

        assert_eq!(
            store.get(1).unwrap(),
            headers[0],
            "the first header is intact"
        );

        matches! {
            store
                .get(2)
                .expect_err("the second header has been corrupted"),
            Error::Io(err) if err.kind() == io::ErrorKind::UnexpectedEof
        };

        store.len().expect_err("data is corrupted");
        store.check().expect_err("data is corrupted");

        store.heal().unwrap();
        store.check().unwrap();

        assert_eq!(
            store.len().unwrap(),
            2,
            "the last (corrupted) header was removed"
        );
    }
}
