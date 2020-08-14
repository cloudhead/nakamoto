nakamoto
========

Nakamoto is a high-assurance Bitcoin light-client implementation in Rust. Its primary
focus is on low resource utilization, reliability and ease of use.

## Goals

* __High assurance__: the library should be thoroughly tested using modern techniques such as *property*
  and *model-based testing* as well as *discrete event simulation* and *fuzzing*. These methods require a more
  thorough approach to testing, and a specific kind of architecture, but have been shown to
  catch more bugs than traditional approaches.

* __Security__: as a library that may find its way into wallet implementations, a primary goal is
  security and auditability. For this reason, we try to minimize its dependency footprint and use of
  unsafe code.

* __Performance and efficiency__: blockchain synchronization should be done as efficiently as possible,
  with low memory and disk overhead. We target resource-constrained environments, such as mobile devices.

* __Privacy__: when possible, privacy-preserving techniques should be employed. For example, *Client Side
  Block Filtering* (BIP 157/158) should be used over bloom filters (BIP 37) to ensure user privacy.
  When a trade-off exists between privacy and efficiency/bandwidth (as is often the case), we allow the user
  to choose.

* __Documentation__: special attention should be made to documentation, both of the code and
  APIs, for this library to be as developer-friendly as possible.

## Contributing

If you'd like to contribute to the development of Nakamoto, please get in touch!

## Usage

In your *Cargo.toml*:

    [dependencies]
    nakamoto = "0.1.0"

## Running the tests

    cargo test --all --release

## Donations

To fund the project and ensure its ongoing development and maintenance, send
Bitcoin to the following address:

    bc1qptvwp33hk9ulaf607l6wft64serj9rh0zfec7e

## License

Licensed under the MIT license.
&copy; 2020 Alexis Sellier (<https://cloudhead.io>)
