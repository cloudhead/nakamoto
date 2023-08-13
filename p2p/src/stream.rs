//! Message stream utilities.
use std::io;

use nakamoto_common::bitcoin::consensus::{encode, Decodable};

/// Message stream decoder.
///
/// Used to for example turn a byte stream into network messages.
#[derive(Debug)]
pub struct Decoder {
    unparsed: Vec<u8>,
}

impl Decoder {
    /// Create a new stream decoder.
    pub fn new(capacity: usize) -> Self {
        Self {
            unparsed: Vec::with_capacity(capacity),
        }
    }

    /// Input bytes into the decoder.
    pub fn input(&mut self, bytes: &[u8]) {
        self.unparsed.extend_from_slice(bytes);
    }

    /// Decode and return the next message. Returns [`None`] if nothing was decoded.
    pub fn decode_next<D: Decodable>(&mut self) -> Result<Option<D>, encode::Error> {
        match encode::deserialize_partial::<D>(&self.unparsed) {
            Ok((msg, index)) => {
                // Drain deserialized bytes only.
                self.unparsed.drain(..index);
                Ok(Some(msg))
            }

            Err(encode::Error::Io(ref err)) if err.kind() == io::ErrorKind::UnexpectedEof => {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use nakamoto_common::{bitcoin::network::{message::{NetworkMessage, RawNetworkMessage}, Magic}, block::Work};
    use quickcheck_macros::quickcheck;

    const MSG_VERACK: [u8; 24] = [
        0xf9, 0xbe, 0xb4, 0xd9, 0x76, 0x65, 0x72, 0x61, 0x63, 0x6b, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x5d, 0xf6, 0xe0, 0xe2,
    ];

    const MSG_PING: [u8; 32] = [
        0xf9, 0xbe, 0xb4, 0xd9, 0x70, 0x69, 0x6e, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x08, 0x00, 0x00, 0x00, 0x24, 0x67, 0xf1, 0x1d, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];

    #[quickcheck]
    fn prop_decode_next(chunk_size: usize) {
        let mut bytes = vec![];
        let mut msgs = vec![];
        let mut decoder = Decoder::new(1024);

        let chunk_size = 1 + chunk_size % decoder.unparsed.capacity();

        bytes.extend_from_slice(&MSG_VERACK);
        bytes.extend_from_slice(&MSG_PING);

        for chunk in bytes.as_slice().chunks(chunk_size) {
            decoder.input(chunk);

            while let Some(msg) = decoder.decode_next::<RawNetworkMessage>().unwrap() {
                msgs.push(msg);
            }
        }

        assert_eq!(decoder.unparsed.len(), 0);
        assert_eq!(msgs.len(), 2);
        assert_eq!(
            msgs[0],
            RawNetworkMessage {
                magic: Magic::REGTEST,
                payload: NetworkMessage::Verack
            }
        );
        assert_eq!(
            msgs[1],
            RawNetworkMessage {
                magic: Magic::REGTEST,
                payload: NetworkMessage::Ping(100),
            }
        );
    }
}
