use std::{ops::Range, str::FromStr};

use bitcoin::{util::bip32::DerivationPath, Address};
use nakamoto_common::bitcoin;

pub use coldcard::protocol::AddressFormat;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("no hardware device found")]
    NoDevice,
    #[error("failed to open hardware device")]
    Open,
    #[error("failed to decode address from device")]
    Address(#[from] bitcoin::util::address::Error),
    #[error("derivation path error")]
    DerivationPath(coldcard::protocol::derivation_path::Error),
    #[error("device error: {0}")]
    Device(#[from] coldcard::Error),
}

pub struct Hw {
    /// Hardware device. `None` when disconnected, and `Some` when connected.
    device: Option<coldcard::Coldcard>,
    /// Hardware device address derivation path.
    hd_path: DerivationPath,
}

impl Hw {
    pub fn new(hd_path: DerivationPath) -> Self {
        Self {
            hd_path,
            device: None,
        }
    }

    pub fn connect(&mut self) -> Result<&mut coldcard::Coldcard, Error> {
        if let Some(ref mut device) = self.device {
            return Ok(device);
        }
        self.reconnect()
    }

    pub fn reconnect(&mut self) -> Result<&mut coldcard::Coldcard, Error> {
        // Detect all connected Coldcards.
        let serials = coldcard::detect()?;
        let (coldcard, _) = serials.first().ok_or(Error::NoDevice)?.open(None)?;
        let coldcard = self.device.get_or_insert(coldcard);

        Ok(coldcard)
    }

    pub fn request_addresses(
        &mut self,
        range: impl Into<Range<usize>>,
        format: AddressFormat,
    ) -> Result<Vec<(usize, Address)>, Error> {
        let hd_path = self.hd_path.clone();
        let device = self.connect()?;

        let range: Range<usize> = range.into();
        let mut addrs = Vec::new();

        for (ix, child) in hd_path
            .normal_children()
            .enumerate()
            .skip(range.start)
            .take(range.len())
        {
            let child = coldcard::protocol::DerivationPath::new(child.to_string().as_str())
                .map_err(Error::DerivationPath)?;
            // TODO: This should be made to return `Address` type.
            let addr = device.address(child, format)?;
            let addr = Address::from_str(addr.as_str())?;

            log::debug!("Loaded address {addr} from device");

            addrs.push((ix, addr));
        }
        Ok(addrs)
    }
}
