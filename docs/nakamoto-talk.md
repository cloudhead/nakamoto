---
author: "@cloudhead"
paging: Slide %d / %d
---

# Nakamoto: a new bitcoin light-client

Nakamoto is a Bitcoin light-client library written in Rust, with a focus on low
resource utilization and privacy.

It's built on BIP 157/158 and targeted at mobile and other resource constrained
environments.

---

# whoami

*@cloudhead*

Got into Bitcoin in 2017. Interested in light clients and user sovereignty.

Most of my time goes into Radicle, a peer-to-peer code collaboration stack.

---

# Why?

* Because an increasing amount of users are on mobile.
* Because decentralization is important.
* Because running a full node is not for everyone.

---

# Mapping the space

```
    Electrum <--------------------------------------------------> Full node
                    BIP 37         BIP 157         Utreexo
 👍 efficiency                                                  👎 efficiency
 👎 reliability                                                 👍 reliability
 👎 privacy                                                     👍 privacy
```

---

# Mapping the space

## BIP 37: Bloom filters

* User computes own per-peer filter and sends to peers
* Peers send filtered blocks with some false positives, plus proofs
* User checks transaction inclusion in filtered blocks

## BIP 157: Client-side filtering

* Full nodes compute generalized per-block filters
* User downloads all filters starting from wallet birth height
* User checks transaction inclusion locally
* If there's a match, user asks random peer for full block and updates UTXOs

---

# Mapping the space

## BIP 37: Bloom filters

A critical design flaw means that if two or more filters are collected by
an adversary, the intersection can be computed.

The flaws of BIP 37 and the lack of alternative for a long time has lead
to a migration to trusted third parties, ie. vendor-supplied Electrum
endpoints. A very small minority of users run their own nodes.

---

## Electrum

* Uses SPV proof of inclusion.
* Leaks transactions and associated IP address.
* Leaks public keys and/or addresses.
* Can lie by omission.

This is the default in most bitcoin & lightning wallets today. Until
recently, the only alternative was to run your own node or deal with
the broken privacy of BIP 37.

---

## Neutrino

A few years ago, lightning labs developed a new type of client called
*neutrino* and specified its protocol under BIP 157 and 158.

In 2021, support was added to bitcoin core (`0.21`).

Allows lightning nodes to operate without a full node.

---

# Defaults

*First time users need good defaults.*

This means an acceptable trade-off between sync speed, resource usage,
trust minimization and privacy. There's no one-size-fits-all.

*Experienced users need optionality.*

This means means more privacy-conscious users will opt for different
solutions to performance-conscious users.

---

## BIP 157/8

* Has a "one honest peer" assumption.
* Very good privacy (only leak the blocks you're interested in).
* No header commitment to the compact filter, so a good address book is important.

---

## BIP 157/8

* `~20MB` compact filter header chain.
* `~20KB/block` median compact filter size

Compact filters are usually not stored for a long time by light clients.

*Trades bandwidth for privacy.*

---

## Nakamoto: Example

```rust
// The network reactor we're going to use.
type Reactor = nakamoto::net::poll::Reactor<net::TcpStream>;

// Create a client using the above network reactor.
let client = Client::<Reactor>::new()?;
let handle = client.handle();

// Run the client on a different thread, to not block the main thread.
thread::spawn(|| client.run(Config::new(Network::Testnet)).unwrap());

// Wait for the client to be connected to a peer.
handle.wait_for_peers(1, Services::default())?;

// … Wallet code goes here …
```
---

## Nakamoto: Architecture

* Client doesn't spawn any threads, very small footprint
* API is event-based with a command channel
* Networking I/O is cleanly separated from protocol code
  * Allows the networking backend to be swapped out
  * Protocol code is fully deterministic
  * Allows for *discrete event simulation*
* Ships with a simple *poll*-based network reactor
* Minimal dependencies

---

## Nakamoto: API

```rust
trait Handle {
    // Wait for the node to be ready and in sync with the blockchain.
    fn wait_for_ready(&self) -> Result<(), Error>;
    // Rescan the blockchain for matching scripts.
    fn rescan(
        &self,
        range: impl RangeBounds<Height>,
        watch: impl Iterator<Item = Script>
    ) -> Result<(), Error>;
    // Submit a transaction to the network.
    fn submit_transaction(&self, tx: Transaction) -> Result<(), Error>;
    // Listen on events.
    fn events(&self) -> chan::Receiver<Event>;
}
```
---

## Nakamoto: API Events

```rust
enum Event {
    // Ready to process peer events and start receiving commands.
    Ready { … },

    // Peer events.
    PeerConnected { … },
    PeerDisconnected { … },
    PeerNegotiated { … },
    PeerHeightUpdated { … },

    // Block events.
    BlockConnected { … },
    BlockDisconnected { … },
    BlockMatched { transactions: Vec<Transaction>, … },

    // The status of a transaction has changed.
    TxStatusChanged { … }
    // Compact filters have been synced up to this point.
    Synced { … },
}
```
---

# What's next?

## Already stable

* Peer-to-peer networking, handshake, connection management
* Peer stochastic address selection
* Block header sync and verification
* Transaction management
* Compact filter fetching, caching and matching (BIP 157)

## Work in progress

* Filter verification protocol to prevent lies by omission
* Address derivation support in filter scan

---

# Thank you.

<https://cloudhead.io/nakamoto>

*@cloudhead* on twitter, github etc.

