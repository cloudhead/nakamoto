use sqlite as sql;

use nakamoto_common::bitcoin::Address;
use nakamoto_common::bitcoin::address::NetworkUnchecked;

use super::Error;

/// Wraps a type, enabling it to be converted to SQL types.
pub struct Record<T>(pub T);

impl<A, B> TryFrom<sql::Row> for Record<(A, B)>
where
    A: sql::ValueInto,
    B: sql::ValueInto,
{
    type Error = Error;

    fn try_from(row: sql::Row) -> Result<Self, Self::Error> {
        let a = row.get(0);
        let b = row.get(1);

        Ok(Record((a, b)))
    }
}

impl<A, B, C> TryFrom<sql::Row> for Record<(A, B, C)>
where
    A: sql::ValueInto,
    B: sql::ValueInto,
    C: sql::ValueInto,
{
    type Error = Error;

    fn try_from(row: sql::Row) -> Result<Self, Self::Error> {
        let a = row.get(0);
        let b = row.get(1);
        let c = row.get(2);

        Ok(Record((a, b, c)))
    }
}

impl<A, B, C, D> TryFrom<sql::Row> for Record<(A, B, C, D)>
where
    A: sql::ValueInto,
    B: sql::ValueInto,
    C: sql::ValueInto,
    D: sql::ValueInto,
{
    type Error = Error;

    fn try_from(row: sql::Row) -> Result<Self, Self::Error> {
        let a = row.get(0);
        let b = row.get(1);
        let c = row.get(2);
        let d = row.get(3);

        Ok(Record((a, b, c, d)))
    }
}

/// An address table row.
pub struct AddressRecord {
    pub address: Address,
    pub index: usize,
    pub label: Option<String>,
    pub received: u64,
    pub used: bool,
}

impl<'a> TryFrom<&'a sql::Row> for AddressRecord {
    type Error = Error;

    fn try_from(row: &'a sql::Row) -> Result<Self, Self::Error> {
        let addr: Address<NetworkUnchecked> = row
                .get::<String, _>(0)
                .as_str()
                .parse()
                .map_err(|_| Error::Decoding("address"))?;
        Ok(Self {
            address: addr.assume_checked(),
            index: row.get::<i64, _>(1) as usize,
            label: row.get(2),
            received: row.get::<i64, _>(3) as u64,
            used: row.get::<i64, _>(4) > 0,
        })
    }
}

/// A balance in satoshis.
pub struct Balance(u64);

impl std::ops::Deref for Balance {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl sql::ValueInto for Balance {
    fn into(value: &sql::Value) -> Option<Self> {
        match value {
            sql::Value::Integer(i) => Some(Balance(*i as u64)),
            _ => None,
        }
    }
}
