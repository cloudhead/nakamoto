pub mod peer;

use log::*;
use std::io;

pub struct Network {
    peer_config: peer::Config,
}

impl Network {
    pub fn new(peer_config: peer::Config) -> Self {
        Self { peer_config }
    }

    pub fn connect(&mut self, host: &str) -> io::Result<()> {
        let addr = format!("{}:{}", host, self.peer_config.port());
        let mut p = peer::Peer::connect(&addr, &self.peer_config)?;

        debug!("Connected to {}", p.address);
        trace!("{:#?}", p);

        p.handshake(0)?;

        Ok(())
    }
}
