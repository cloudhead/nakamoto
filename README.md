nakamoto
========

Nakamoto is a high-assurance Bitcoin light-client implementation in Rust. It can
function either as a library, or as a standalone daemon.

## Goals

* __High assurance__: the library should be thoroughly tested using modern techniques such as *property*
  and *model testing*[^0][^1] as well as *discrete event simulation*[^2]. These methods require a more
  thorough approach to testing, and a specific kind of software architecture, but have been shown to
  catch more bugs than traditional approaches.

* __Security__: as a library that may find its way into wallet implementations, a primary goal is
  security and auditability. For this reason, we try to minimize the dependency footprint and use of
  unsafe code.

* __Performance and memory__: the library should be able to synchronize the bitcoin blockchain as fast
  as possible, without consuming copious amounts of memory or disk space. We target
  resource-constrainted environments, such as mobile.

* __Privacy__: when possible, privacy-preserving techniques should be employed. For example, *Client Side
  Block Filtering* (BIP 157/158) should be used over bloom filters (BIP 37) to ensure user privacy.

* __Documentation__: special attention should be made to documentation, both of the code and
  APIs, for this library to be as developer-friendly as possible.

[^0]: https://en.wikipedia.org/wiki/QuickCheck
[^1]: https://en.wikipedia.org/wiki/Model-based_testing
[^2]: https://en.wikipedia.org/wiki/Discrete-event_simulation

## License

Licensed under the MIT license.
&copy; 2020 Alexis Sellier (<https://cloudhead.io>)
