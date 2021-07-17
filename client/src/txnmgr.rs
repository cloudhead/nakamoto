//!
//! Manages transaction.
//!
#![warn(missing_docs)]

use bitcoin::{
    network::{message::NetworkMessage, message_blockdata::Inventory},
    Block, Transaction, Txid,
};
use core::time;
use crossbeam_channel as chan;
use nakamoto_common::block::Height;
use nakamoto_p2p::{event, protocol::syncmgr, protocol::Command, reactor::Reactor, PeerId};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

use crate::{
    handle::{self, Handle as _},
    Handle, Publisher,
};

#[derive(Clone, Debug)]
/// The status of a transaction.
pub enum Event {
    /// The transaction was sent to one or more peers on the network, and is unconfirmed.
    Pending {
        /// Number of announcements of this transaction to other peers.
        announcements: usize,
        /// Transaction hash.
        txid: Txid,
    },
    /// The transaction has been accepted by one or more peers on the network.
    Accepted {
        /// Number of peers that our transaction data was sent to.
        confirmations: usize,
        /// Transaction hash.
        txid: Txid,
    },
    /// Transaction has been confirmed on the blockchain.
    Confirmed {
        /// Confirmed block height.
        height: Height,
        /// Transaction hash.
        txid: Txid,
    },
}

/// Transaction related error.
#[derive(Debug, Error)]
pub enum Error {
    /// Error due to RwLock.
    Lock,
    /// Error due to unavailable relay peers.
    RelayPeer,
    /// Transaction not in store.
    NotFound,
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Lock => write!(fmt, "Poisonsed read/write lock"),
            Error::RelayPeer => write!(fmt, "Zero connected relay peers"),
            Error::NotFound => write!(fmt, "Transaction not stored to store"),
        }
    }
}

impl From<Error> for handle::Error {
    fn from(e: Error) -> Self {
        Self::Transaction(e)
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::Pending {
                announcements,
                txid,
            } => {
                write!(
                    fmt,
                    "Pending transaction ID {} broadcasted to {} peers",
                    txid, announcements
                )
            }
            Event::Accepted {
                confirmations,
                txid,
            } => {
                write!(
                    fmt,
                    "Transaction with ID {} sent to {} peers",
                    txid, confirmations,
                )
            }
            Event::Confirmed { height, txid } => {
                write!(
                    fmt,
                    "Transaction with ID {} has been confirmed on the blockchain at height {}",
                    txid, height,
                )
            }
        }
    }
}

/// Transaction manager
pub struct TxnManager<R: Reactor<Publisher>> {
    handle: Handle<R>,
}

impl<R: Reactor<Publisher>> Clone for TxnManager<R>
where
    R::Waker: Sync,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

impl<R: Reactor<Publisher>> TxnManager<R>
where
    R::Waker: Sync,
{
    /// Create a new TxnManager.
    pub fn new(handle: Handle<R>) -> Self {
        TxnManager { handle }
    }

    /// Submits a transaction.
    ///
    /// Transactions are broadcasted to as many connected peers as timeout duration
    /// allows. To continue tracking transaction status after submission, do call
    /// the wait method. This is useful if you want to track transactions till they
    /// are accepted by peers.
    /// ```ignore
    /// let tx: Transaction;
    /// let tx_id: tx.tx_id();
    /// let txn_mgr = TxnManager::new(handle);
    /// let status = txn_mgr.submit_transaction(tx).unwrap();
    /// // Wait for transaction till accepted by a peer.
    /// while let Ok(txnmgr::Event::Accepted {
    ///     confirmations,
    ///     txid,
    /// }) = txn_mgr.wait(txn_id, std::time::Duration::from_secs(10))
    /// {}
    ///```
    pub fn submit_transaction(
        &mut self,
        txn: Transaction,
        timeout: time::Duration,
    ) -> Result<Event, handle::Error> {
        let tx_id = txn.txid();

        let (transmitter, recvr) = chan::bounded(1);
        self.handle
            .command(Command::BroadcastTransactionInv(tx_id, transmitter))?;

        let peers = recvr.recv()?;
        if peers.is_empty() {
            return Err(Error::RelayPeer.into());
        }

        let mut txn_map = match self.handle.transactions.write() {
            Ok(e) => e,
            Err(e) => return Err(e.into()),
        };

        let events = self.handle.events();
        let mut peers_responded = HashSet::new();

        // Keep on listening for peers that want our transaction till timeout.
        let result = event::wait(
            &events,
            |e| match e {
                event::Event::Received(e, NetworkMessage::GetData(inv)) => {
                    if inv.contains(&Inventory::Transaction(tx_id)) {
                        let result = self
                            .handle
                            .command(Command::SubmitTransaction(e, txn.clone()));
                        peers_responded.insert(e);
                        return Some(result);
                    }

                    None
                }

                event::Event::SyncManager(syncmgr::Event::BlockReceived(_, block, height)) => {
                    self.on_blocks_received(tx_id, block, height, &mut *txn_map)
                }

                _ => None,
            },
            timeout,
        );

        let peers_responded_len = peers_responded.len();

        // We have broadcasted the tx ID but couldn't send
        // tx to peers probably due to a timeout.
        if peers_responded_len == 0 {
            return Ok(Event::Pending {
                announcements: peers.len(),
                txid: tx_id,
            });
        }

        match result? {
            Ok(_) => Ok(Event::Accepted {
                confirmations: peers_responded_len,
                txid: tx_id,
            }),
            Err(e) => Err(e),
        }
    }

    /// Wait for a change of event of specified transaction ID.
    pub fn wait(&self, tx_id: Txid, timeout: time::Duration) -> Result<Event, handle::Error> {
        let mut txn_map = match self.handle.transactions.write() {
            Ok(e) => e,
            Err(e) => return Err(e.into()),
        };

        let events = self.handle.events();

        let keys: Vec<Txid> = txn_map.keys().cloned().into_iter().collect();

        let result = event::wait(
            &events,
            |e| match e {
                event::Event::Received(e, NetworkMessage::Inv(inv)) => {
                    let mut found_requested_tx = false;

                    for key in &keys {
                        if inv.contains(&Inventory::Transaction(*key)) {
                            let (txn, peers_recvd, _) = txn_map.get_mut(key).unwrap();

                            let result = self
                                .handle
                                .command(Command::SubmitTransaction(e, txn.to_owned()));

                            peers_recvd.insert(e);

                            match result {
                                Ok(_) => {
                                    // We continue tracking for transactions in store even when
                                    // requested tx is found, so we dont miss any event.
                                    if *key == tx_id {
                                        found_requested_tx = true;
                                    }
                                }

                                Err(e) => return Some(Err(e)),
                            }
                        }
                    }

                    if found_requested_tx {
                        return Some(Ok(()));
                    }

                    None
                }

                event::Event::SyncManager(syncmgr::Event::BlockReceived(_, block, height)) => {
                    self.on_blocks_received(tx_id, block, height, &mut *txn_map)
                }

                _ => None,
            },
            timeout,
        );

        let (_, peers_recvd, height) = txn_map.get_mut(&tx_id).unwrap();

        // If the first unwrapped error is sent, it signifies event
        // timed out. The second error value is that when tracking transaction.
        match result? {
            Ok(_) => {
                if *height == 0 {
                    Ok(Event::Accepted {
                        confirmations: peers_recvd.len(),
                        txid: tx_id,
                    })
                } else {
                    Ok(Event::Confirmed {
                        height: *height,
                        txid: tx_id,
                    })
                }
            }

            Err(e) => Err(e),
        }
    }

    fn on_blocks_received(
        &self,
        tx_id: Txid,
        block: Block,
        height: Height,
        txn_store: &mut HashMap<Txid, (Transaction, HashSet<PeerId>, Height)>,
    ) -> Option<Result<(), handle::Error>> {
        let mut found_requested_tx = false;

        for (id, (txn, _, confirmed_height)) in txn_store.iter_mut() {
            if block.txdata.contains(txn) {
                *confirmed_height = height;
            }

            if *id == tx_id {
                found_requested_tx = true
            }
        }

        if found_requested_tx {
            return Some(Ok(()));
        }

        None
    }
}
