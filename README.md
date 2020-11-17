nakamoto
========

Nakamoto is a high-assurance Bitcoin light-client implementation in Rust. Its primary
focus is on low resource utilization, reliability and ease of use.

## Goals

* __High assurance__: the library should be thoroughly tested using modern techniques such as *property*
  and *model-based testing* as well as *discrete event simulation* and *fuzzing*. These approaches
  benefit from a clean separation between I/O and protocol logic and have been shown to
  catch more bugs than unit testing.

* __Security__: as a library that may find its way into wallet implementations, a primary goal is
  security and auditability. For this reason, we try to minimize the total dependency footprint,
  keep the code easy to read and forbid any unsafe code.

* __Efficiency__: blockchain synchronization should be done as efficiently as possible,
  with low memory and disk overhead. We target resource-constrained environments, such as mobile.

* __Privacy__: when possible, privacy-preserving techniques should be employed. For example, *Client Side
  Block Filtering* (BIP 157/158) should be used over bloom filters (BIP 37) to ensure user privacy.

## Contributing

If you'd like to contribute to the development of Nakamoto, please get in touch!

## Usage

In your *Cargo.toml*:

    [dependencies]
    nakamoto = "0.1.0"

## Running the tests

    cargo test --all --release

## Donations

To help fund the project and ensure its ongoing development and maintenance, your
support in Bitcoin is appreciated at the following address:

    bc1qptvwp33hk9ulaf607l6wft64serj9rh0zfec7e

## License

Licensed under the MIT license.
&copy; 2020 Alexis Sellier (<https://cloudhead.io>)
