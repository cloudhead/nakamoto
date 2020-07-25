use crossbeam_channel as chan;

use nakamoto_chain::block::{BlockHeader, Transaction};

use crate::error::Error;
use crate::handle::Handle;

/// A command or request that can be sent to the node process.
pub enum Command {
    GetTip(chan::Sender<BlockHeader>),
}

/// A light-node process.
pub struct Node {
    commands: chan::Receiver<Command>,
    handle: chan::Sender<Command>,
}

impl Node {
    /// Create a new node.
    pub fn new() -> Self {
        let (handle, commands) = chan::unbounded::<Command>();

        Self { commands, handle }
    }

    /// Start the node process. This function is meant to be
    /// run in a background thread.
    pub fn run(self) -> Result<(), Error> {
        loop {
            let cmd = self.commands.recv()?;

            match cmd {
                Command::GetTip(_tx) => todo!(),
            }
        }
    }

    /// Create a new handle to communicate with the node.
    pub fn handle(&mut self) -> NodeHandle {
        NodeHandle {
            commands: self.handle.clone(),
        }
    }
}

/// An instance of [`Handle`] for [`Node`].
pub struct NodeHandle {
    commands: chan::Sender<Command>,
}

impl Handle for NodeHandle {
    fn get_tip(&self) -> Result<BlockHeader, Error> {
        let (transmit, receive) = chan::bounded::<BlockHeader>(1);
        self.commands.send(Command::GetTip(transmit))?;

        Ok(receive.recv()?)
    }

    fn submit_transaction(&self, _tx: Transaction) -> Result<(), Error> {
        todo!()
    }

    fn wait_for_peers(&self, _count: usize) -> Result<chan::Receiver<()>, Error> {
        todo!()
    }

    fn wait_for_ready(&self) -> Result<chan::Receiver<()>, Error> {
        todo!()
    }

    fn shutdown(self) -> Result<(), Error> {
        todo!()
    }
}
