use nakamoto_common::block::BlockHeader;
pub use nakamoto_common::block::*;

/// Solve a block's proof of work puzzle.
pub fn solve(header: &mut BlockHeader) {
    let target = header.target();
    while header.validate_pow(&target).is_err() {
        header.nonce += 1;
    }
}

pub mod cache {
    pub mod model;
}

pub mod gen {
    use std::collections::HashMap;

    use bitcoin::blockdata::script::Script;
    use bitcoin::blockdata::transaction::{OutPoint, TxIn, TxOut};
    use bitcoin::consensus::Decodable;
    use bitcoin::hash_types::{PubkeyHash, TxMerkleNode, Txid};
    use bitcoin::util::bip158;
    use bitcoin::util::uint::Uint256;

    use bitcoin_hashes::{hash160, Hash as _};

    use nonempty::NonEmpty;

    use nakamoto_common::block::filter::{BlockFilter, FilterHash, FilterHeader};
    use nakamoto_common::block::*;

    /// Generates a random transaction.
    pub fn transaction(rng: &mut fastrand::Rng) -> Transaction {
        let mut input = Vec::with_capacity(rng.usize(1..16));
        let mut output = Vec::with_capacity(rng.usize(1..16));

        // Inputs.
        for _ in 0..input.capacity() {
            let bytes = std::iter::repeat_with(|| rng.u8(..))
                .take(32)
                .collect::<Vec<_>>();

            let txid = Txid::from_slice(&bytes).unwrap();
            let inp = TxIn {
                previous_output: OutPoint {
                    txid,
                    vout: rng.u32(0..16),
                },
                script_sig: Script::new(),
                sequence: 0xFFFFFFFF,
                witness: vec![],
            };
            input.push(inp);
        }

        // Outputs.
        for _ in 0..output.capacity() {
            let out = tx_out(rng);
            output.push(out);
        }

        Transaction {
            version: 1,
            lock_time: 0,
            input,
            output,
        }
    }

    /// Generate a random transaction output.
    pub fn tx_out(rng: &mut fastrand::Rng) -> TxOut {
        let bytes = std::iter::repeat_with(|| rng.u8(..))
            .take(20)
            .collect::<Vec<_>>();
        let hash = hash160::Hash::from_slice(bytes.as_slice()).unwrap();
        let pkh = PubkeyHash::from_hash(hash);
        let script_pubkey = Script::new_p2pkh(&pkh);

        TxOut {
            value: rng.u64(1..),
            script_pubkey,
        }
    }

    /// Generate a random coinbase transaction.
    pub fn coinbase(rng: &mut fastrand::Rng) -> Transaction {
        let output = vec![tx_out(rng)];

        Transaction {
            version: 1,
            lock_time: 0,
            input: vec![],
            output,
        }
    }

    /// Generate a random block based on a previous header.
    pub fn block(prev_header: &BlockHeader, rng: &mut fastrand::Rng) -> Block {
        let mut txdata = Vec::with_capacity(rng.usize(1..16));
        txdata.push(coinbase(rng));
        for _ in 1..txdata.capacity() {
            txdata.push(transaction(rng));
        }

        let merkle_root =
            bitcoin::util::hash::bitcoin_merkle_root(txdata.iter().map(|tx| tx.txid().as_hash()));
        let merkle_root = TxMerkleNode::from_hash(merkle_root);
        let header = header(&prev_header, merkle_root, rng);

        Block { header, txdata }
    }

    /// Generate a random header based on a previous header and merkle root.
    pub fn header(
        prev_header: &BlockHeader,
        merkle_root: TxMerkleNode,
        rng: &mut fastrand::Rng,
    ) -> BlockHeader {
        let target_spacing = 60 * 10; // 10 minutes.
        let delta = rng.u32(target_spacing / 2..target_spacing * 2);

        let time = prev_header.time + delta;
        let bits = BlockHeader::compact_target_from_u256(&prev_header.target());

        let mut header = BlockHeader {
            version: 1,
            time,
            nonce: 0,
            bits,
            merkle_root,
            prev_blockhash: prev_header.block_hash(),
        };
        super::solve(&mut header);

        header
    }

    /// Generate a random minimum-difficulty genesis block.
    pub fn genesis(rng: &mut fastrand::Rng) -> Block {
        let txdata = vec![coinbase(rng)];
        let merkle_root =
            bitcoin::util::hash::bitcoin_merkle_root(txdata.iter().map(|tx| tx.txid().as_hash()));
        let merkle_root = TxMerkleNode::from_hash(merkle_root);

        let target = Uint256([
            0xffffffffffffffffu64,
            0xffffffffffffffffu64,
            0xffffffffffffffffu64,
            0x7fffffffffffffffu64,
        ]);
        let bits = BlockHeader::compact_target_from_u256(&target);

        let mut header = BlockHeader {
            version: 1,
            time: 0,
            nonce: 0,
            bits,
            merkle_root,
            prev_blockhash: BlockHash::default(),
        };
        super::solve(&mut header);

        Block { header, txdata }
    }

    /// Generate a random blockchain.
    pub fn blockchain(genesis: Block, rng: &mut fastrand::Rng) -> NonEmpty<Block> {
        let height = rng.usize(0..64);
        let mut prev_header = genesis.header;
        let mut chain = NonEmpty::new(genesis);

        for _ in 0..height {
            let block = block(&prev_header, rng);
            prev_header = block.header;

            chain.push(block);
        }
        chain
    }

    /// Create a filter from a block.
    pub fn cfilter(block: &Block) -> BlockFilter {
        let mut txmap = HashMap::new();
        for tx in block.txdata.iter().skip(1) {
            for input in tx.input.iter() {
                txmap.insert(input.previous_output, input.script_sig.clone());
            }
        }

        BlockFilter::new_script_filter(block, |out| {
            if let Some(s) = txmap.get(out) {
                Ok(s.clone())
            } else {
                Err(bip158::Error::UtxoMissing(*out))
            }
        })
        .unwrap()
    }

    /// Create a filter header from a previous header and a filter.
    pub fn cfheader(parent: &FilterHeader, cfilter: &BlockFilter) -> (FilterHash, FilterHeader) {
        let hash = FilterHash::hash(&cfilter.content);
        let header = hash.filter_header(parent);

        (hash, header)
    }

    /// Build a filter header chain from input blocks.
    pub fn cfheaders_from_blocks<'a>(
        mut parent: FilterHeader,
        blocks: impl Iterator<Item = &'a Block>,
    ) -> Vec<(FilterHash, FilterHeader)> {
        let mut cfheaders = Vec::new();

        for blk in blocks {
            let (cfhash, cfheader) = cfheader(&parent, &cfilter(blk));
            cfheaders.push((cfhash, cfheader));
            parent = cfheader;
        }
        cfheaders
    }

    /// Generates a random filter header chain starting from a parent filter header.
    pub fn cfheaders(
        mut parent: FilterHeader,
        rng: fastrand::Rng,
    ) -> impl Iterator<Item = (FilterHash, FilterHeader)> {
        std::iter::repeat_with(move || {
            let bytes = std::iter::repeat_with(|| rng.u8(..))
                .take(32)
                .collect::<Vec<_>>();
            let hash = FilterHash::consensus_decode(bytes.as_slice()).unwrap();
            let header = hash.filter_header(&parent);

            parent = header;

            (hash, header)
        })
    }
}
