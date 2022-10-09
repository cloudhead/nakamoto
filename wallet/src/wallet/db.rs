mod types;

use std::path::Path;
use std::str::FromStr;

use nakamoto_common::bitcoin::Address;
use nakamoto_common::bitcoin::OutPoint;
use nakamoto_common::bitcoin::TxOut;
use nakamoto_common::bitcoin::Txid;

use sqlite as sql;

pub use types::*;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database error: {0}")]
    Internal(#[from] sql::Error),
    #[error("database error: open: {0}")]
    Open(sql::Error),
    #[error("database error: {1}: {0}")]
    Query(sql::Error, &'static str),
    #[error("database error: schema: {0}")]
    Schema(sql::Error),
    #[error("decoding error for column `{0}`")]
    Decoding(&'static str),
}

/// Read from the database.
pub trait Read {
    /// Get the wallet balance.
    fn balance(&self) -> Result<u64, Error>;
    /// Get a UTXO.
    fn utxo(&self, outpoint: &OutPoint) -> Result<Option<(OutPoint, TxOut)>, Error>;
    /// Get all UTXOs.
    fn utxos(&self) -> Result<Vec<(OutPoint, TxOut)>, Error>;
    /// Get all addresses.
    fn addresses(&self) -> Result<Vec<AddressRecord>, Error>;
}

/// Write to the database.
pub trait Write {
    /// Add a UTXO. Returns `true` if it didn't exist.
    fn add_utxo(&self, txid: Txid, vout: u32, address: Address, value: u64) -> Result<bool, Error>;
    /// Remove a UTXO. Returns the removed UTXO.
    fn remove_utxo(&self, prev_out: &OutPoint) -> Result<Option<(OutPoint, TxOut)>, Error>;
}

/// Wallet database.
pub struct Db {
    raw: sql::Connection,
}

impl Read for Db {
    fn balance(&self) -> Result<u64, Error> {
        let mut stmt = self.raw.prepare("SELECT SUM(value) FROM utxos")?;
        stmt.next()?;

        let balance = stmt.read::<i64>(0)? as u64;

        Ok(balance)
    }

    fn utxo(&self, outpoint: &OutPoint) -> Result<Option<(OutPoint, TxOut)>, Error> {
        let row = self
            .raw
            .prepare(
                "SELECT address, value
                 FROM utxos
                 WHERE txid = ?
                 AND vout = ?",
            )?
            .into_cursor()
            .bind(&[
                sql::Value::String(outpoint.txid.to_string()),
                sql::Value::Integer(outpoint.vout as i64),
            ])?
            .next();

        if let Some(Ok(row)) = row {
            let address = row.get::<String, _>(0);
            let script_pubkey = Address::from_str(&address)
                .map_err(|_| Error::Decoding("address"))?
                .script_pubkey();
            let value = row.get::<i64, _>(1) as u64;

            return Ok(Some((
                *outpoint,
                TxOut {
                    script_pubkey,
                    value,
                },
            )));
        }
        Ok(None)
    }

    fn utxos(&self) -> Result<Vec<(OutPoint, TxOut)>, Error> {
        let mut stmt = self
            .raw
            .prepare("SELECT txid, vout, address, value FROM utxos")?
            .into_cursor();

        let mut utxos = Vec::new();
        while let Some(Ok(row)) = stmt.next() {
            let Record((txid, vout, address, value)): Record<(String, i64, String, Balance)> =
                row.try_into()?;
            let txid = txid.parse().map_err(|_| Error::Decoding("txid"))?;
            let address = address
                .parse::<Address>()
                .map_err(|_| Error::Decoding("address"))?;

            utxos.push((
                OutPoint {
                    txid,
                    vout: vout as u32,
                },
                TxOut {
                    script_pubkey: address.script_pubkey(),
                    value: *value,
                },
            ));
        }
        Ok(utxos)
    }

    fn addresses(&self) -> Result<Vec<AddressRecord>, Error> {
        let mut stmt = self
            .raw
            .prepare("SELECT `id`, `index`, `label`, `received`, `used` FROM `addresses`")
            .map_err(|e| Error::Query(e, "loading addresses"))?
            .into_cursor();
        let mut addrs = Vec::new();

        while let Some(Ok(row)) = stmt.next() {
            let addr = AddressRecord::try_from(&row)?;
            addrs.push(addr);
        }
        Ok(addrs)
    }
}

impl Write for Db {
    fn add_utxo(&self, txid: Txid, vout: u32, address: Address, value: u64) -> Result<bool, Error> {
        self.raw
            .prepare(
                "INSERT INTO utxos (txid, vout, address, value, date)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT DO NOTHING",
            )?
            .into_cursor()
            .bind(&[
                sql::Value::String(txid.to_string()),
                sql::Value::Integer(vout as i64),
                sql::Value::String(address.to_string()),
                sql::Value::Integer(value as i64),
                sql::Value::Integer(0), // TODO: Set transaction time
            ])?
            .next();

        Ok(self.raw.change_count() > 0)
    }

    fn remove_utxo(&self, prev_out: &OutPoint) -> Result<Option<(OutPoint, TxOut)>, Error> {
        // TODO: Should execute this in a transaction.
        let utxo = self.utxo(prev_out)?;
        self.raw
            .prepare("DELETE FROM utxos WHERE txid = ? AND vout = ?")?
            .bind(1, prev_out.txid.to_string().as_str())?
            .bind(2, prev_out.vout as i64)?
            .next()?;

        Ok(utxo)
    }
}

impl Db {
    /// The database schema.
    const SCHEMA: &str = include_str!("schema.sql");

    /// Open a wallet database at the given path. If none exists, an empty database is created.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let raw = sql::Connection::open_with_flags(
            path.as_ref(),
            sql::OpenFlags::new()
                .set_no_mutex()
                .set_create()
                .set_read_write(),
        )
        .map_err(Error::Open)?;

        raw.execute(Db::SCHEMA).map_err(Error::Schema)?;

        Ok(Self { raw })
    }

    /// Create a new in-memory database.
    pub fn memory() -> Result<Self, Error> {
        let raw = sql::Connection::open(":memory:")?;
        raw.execute(Self::SCHEMA)?;

        Ok(Self { raw })
    }

    /// Return the id of the last inserted row.
    #[allow(dead_code)]
    fn last_insert_rowid(&self) -> usize {
        unsafe { sqlite3_sys::sqlite3_last_insert_rowid(self.raw.as_raw()) as usize }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nakamoto_common::bitcoin::Network;
    use nakamoto_test::block::gen;
    use nakamoto_test::fastrand;

    #[test]
    fn test_add_utxo() {
        let db = Db::memory().unwrap();
        let mut rng = fastrand::Rng::new();
        let tx = gen::transaction(&mut rng);
        let address = Address::from_script(&tx.output[0].script_pubkey, Network::Bitcoin).unwrap();

        let out = OutPoint {
            txid: tx.txid(),
            vout: rng.u32(..),
        };

        let added = db
            .add_utxo(out.txid, out.vout, address.clone(), tx.output[0].value)
            .unwrap();
        assert!(added);

        let added = db
            .add_utxo(out.txid, out.vout, address.clone(), tx.output[0].value)
            .unwrap();
        assert!(!added);

        let (_outpoint, txout) = db.utxo(&out).unwrap().unwrap();
        assert_eq!(txout.script_pubkey, address.script_pubkey());
        assert_eq!(txout.value, tx.output[0].value);
    }

    #[test]
    fn test_add_remove_utxo() {
        let db = Db::memory().unwrap();
        let mut rng = fastrand::Rng::new();
        let tx = gen::transaction(&mut rng);
        let address = Address::from_script(&tx.output[0].script_pubkey, Network::Bitcoin).unwrap();

        let out = OutPoint {
            txid: tx.txid(),
            vout: rng.u32(..),
        };

        let added = db
            .add_utxo(out.txid, out.vout, address, tx.output[0].value)
            .unwrap();
        assert!(added);

        let (_, txout) = db.remove_utxo(&out).unwrap().unwrap();
        assert_eq!(txout, tx.output[0]);
        assert!(db.utxo(&out).unwrap().is_none());
    }

    #[test]
    fn test_utxos() {
        let db = Db::memory().unwrap();
        let mut rng = fastrand::Rng::new();
        let tx = gen::transaction(&mut rng);
        let address = Address::from_script(&tx.output[0].script_pubkey, Network::Bitcoin).unwrap();

        let out = OutPoint {
            txid: tx.txid(),
            vout: rng.u32(..),
        };

        db.add_utxo(out.txid, 1, address.clone(), tx.output[0].value)
            .unwrap();
        db.add_utxo(out.txid, 2, address.clone(), tx.output[0].value)
            .unwrap();
        db.add_utxo(out.txid, 3, address, tx.output[0].value)
            .unwrap();

        let utxos = db
            .utxos()
            .unwrap()
            .into_iter()
            .map(|(o, _)| o)
            .collect::<Vec<_>>();

        assert!(utxos.contains(&OutPoint {
            txid: out.txid,
            vout: 1
        }));
        assert!(utxos.contains(&OutPoint {
            txid: out.txid,
            vout: 2
        }));
        assert!(utxos.contains(&OutPoint {
            txid: out.txid,
            vout: 3
        }));
    }
}
