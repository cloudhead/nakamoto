pub mod error;
pub mod peer;

use log::*;

pub struct Network {
    peer_config: peer::Config,
}

impl Network {
    pub fn new(peer_config: peer::Config) -> Self {
        Self { peer_config }
    }

    pub fn connect(&mut self, host: &str) -> Result<(), error::Error> {
        let addr = format!("{}:{}", host, self.peer_config.port());
        let mut p = peer::Peer::connect(&addr, &self.peer_config)?;

        debug!("Connected to {}", p.address);
        trace!("{:#?}", p);

        p.handshake(0)?;

        Ok(())
    }
}
