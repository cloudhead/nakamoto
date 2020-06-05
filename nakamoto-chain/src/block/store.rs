//! Block storage.

use crate::blocktree::Height;

use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::encode;
use thiserror::Error;

use std::fmt;

#[derive(Debug, Error)]
pub enum Error {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("error decoding block: {0}")]
    Decoding(#[from] encode::Error),
}

pub trait Store: fmt::Debug {
    /// Get the genesis block.
    fn genesis(&self) -> Result<BlockHeader, Error> {
        self.get(0)
    }

    /// Append a batch of consecutive block headers to the end of the chain.
    fn put<I: Iterator<Item = BlockHeader>>(&mut self, headers: I) -> Result<Height, Error>;

    /// Get the block at the given height.
    fn get(&self, height: Height) -> Result<BlockHeader, Error>;

    /// Rollback the chain to the given height.
    fn rollback(&mut self, height: Height) -> Result<(), Error>;

    /// Synchronize the changes to disk.
    fn sync(&mut self) -> Result<(), Error>;
}

#[derive(Debug, Clone)]
pub struct Dummy(pub BlockHeader);

impl Store for Dummy {
    /// Get the genesis block.
    fn genesis(&self) -> Result<BlockHeader, Error> {
        Ok(self.0)
    }

    /// Append a batch of consecutive block headers to the end of the chain.
    fn put<I: Iterator<Item = BlockHeader>>(&mut self, _headers: I) -> Result<Height, Error> {
        Ok(0)
    }

    /// Get the block at the given height.
    fn get(&self, _height: Height) -> Result<BlockHeader, Error> {
        Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "unexpected end of file",
        )))
    }

    /// Rollback the chain to the given height.
    fn rollback(&mut self, _height: Height) -> Result<(), Error> {
        Ok(())
    }

    /// Synchronize the changes to disk.
    fn sync(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

pub mod io {
    use super::{Error, Store};
    use crate::blocktree::Height;

    use bitcoin::blockdata::block::BlockHeader;
    use bitcoin::consensus::encode::{Decodable, Encodable};

    use std::fs::{self, File};
    use std::io::{self, Read, Seek, Write};
    use std::iter;
    use std::path::Path;

    // Size in bytes of a block header.
    const HEADER_SIZE: usize = 80;

    /// Append a block to the end of the file.
    fn put<S: Seek + Write, I: Iterator<Item = BlockHeader>>(
        mut stream: S,
        headers: I,
    ) -> Result<Height, Error> {
        let mut pos = stream.seek(io::SeekFrom::End(0))?;

        for header in headers {
            pos += header.consensus_encode(&mut stream)? as u64;
        }
        Ok(pos / HEADER_SIZE as u64 - 1)
    }

    /// A `Store` backed by a single file.
    #[derive(Debug)]
    pub struct FileStore {
        file: File,
    }

    impl FileStore {
        pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
            fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(path)
                .map(|file| Self { file })
        }

        pub fn create<P: AsRef<Path>>(path: P, genesis: BlockHeader) -> Result<Self, Error> {
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(path)?;

            put(&mut file, iter::once(genesis))?;

            Ok(Self { file })
        }
    }

    impl Store for FileStore {
        /// Append a block to the end of the file.
        fn put<I: Iterator<Item = BlockHeader>>(&mut self, headers: I) -> Result<Height, Error> {
            self::put(&mut self.file, headers)
        }

        /// Get the block at the given height. Returns `io::ErrorKind::UnexpectedEof` if
        /// the height is not found.
        fn get(&self, height: Height) -> Result<BlockHeader, Error> {
            let mut buf = [0; HEADER_SIZE];

            // Clone so this function doesn't have to take a `&mut self`.
            let mut file = self.file.try_clone()?;

            file.seek(io::SeekFrom::Start(height * HEADER_SIZE as u64))?;
            file.read_exact(&mut buf)?;

            BlockHeader::consensus_decode(&buf[..]).map_err(Error::from)
        }

        /// Rollback the chain to the given height. Behavior is undefined if  the given
        /// height is not contained in the store.
        fn rollback(&mut self, height: Height) -> Result<(), Error> {
            self.file
                .set_len((height + 1) * HEADER_SIZE as u64)
                .map_err(Error::from)
        }

        /// Flush changes to disk.
        fn sync(&mut self) -> Result<(), Error> {
            self.file.sync_data().map_err(Error::from)
        }
    }

    #[cfg(test)]
    mod test {
        use super::{BlockHeader, FileStore, Height, Store};
        use std::iter;

        #[test]
        fn test_put_get() {
            let tmp = tempfile::tempdir().unwrap();
            let mut store = FileStore::open(tmp.path().join("headers.db")).unwrap();

            let header = BlockHeader {
                version: 1,
                prev_blockhash: Default::default(),
                merkle_root: Default::default(),
                bits: 0x2ffffff,
                time: 1842918273,
                nonce: 312143,
            };

            assert!(
                store.get(0).is_err(),
                "when the store is empty, there is nothing to `get`"
            );

            let height = store.put(iter::once(header)).unwrap();
            store.sync().unwrap();

            assert_eq!(height, 0);
            assert_eq!(store.get(height).unwrap(), header);
            assert!(store.get(1).is_err());
        }

        #[test]
        fn test_put_get_batch() {
            let tmp = tempfile::tempdir().unwrap();
            let mut store = FileStore::open(tmp.path().join("headers.db")).unwrap();
            let mut headers = Vec::new();

            assert!(
                store.get(0).is_err(),
                "when the store is empty, there is nothing to `get`"
            );

            let count = 32;
            let header = BlockHeader {
                version: 1,
                prev_blockhash: Default::default(),
                merkle_root: Default::default(),
                bits: 0x2ffffff,
                time: 1842918273,
                nonce: 0,
            };

            (0..count).for_each(|i| headers.push(BlockHeader { nonce: i, ..header }));

            // Put all headers into the store and check that we can retrieve them.
            {
                let height = store.put(headers.clone().into_iter()).unwrap();

                assert_eq!(height, headers.len() as Height - 1);

                for (i, h) in headers.iter().enumerate() {
                    assert_eq!(&store.get(i as Height).unwrap(), h);
                }

                assert!(&store.get(32).is_err());
                assert!(&store.get(64).is_err());
            }

            // Rollback and overwrite the history.
            {
                let h = headers.len() as Height / 2; // Some point `h` in the past.

                assert!(&store.get(h + 1).is_ok());
                assert_eq!(store.get(h + 1).unwrap(), headers[h as usize + 1]);

                store.rollback(h).unwrap();

                assert!(
                    &store.get(h + 1).is_err(),
                    "after the rollback, we can't access blocks passed `h`"
                );

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
                assert_eq!(store.get(0).unwrap(), headers[0]);
                assert_eq!(store.get(1).unwrap(), headers[1]);
                assert_eq!(store.get(h).unwrap(), headers[h as usize]);
            }
        }
    }
}
