# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/bddkit/bddkit-etcd/releases/tag/v0.1.0) - 2026-11-23

### Added

- add key assertions ([#5](https://github.com/bddkit/bddkit-etcd/pull/5))
- key steps with failure classes and per-operation evidence
- plugin skeleton, resource config validation and live probe ([#2](https://github.com/bddkit/bddkit-etcd/pull/2))

### Fixed

- say TLS did not agree when the server closes the handshake
- let the probe's own timeout fire before the client's

### Other

- version 0.1.0, the first release (bddkit-etcd#7)
- the plugin README, from the config keys to running in parallel
- run the example suites against two servers and a failing one
- the secure test servers, certificates and the token rule
- TLS, mutual TLS and password authentication against a real etcd
- *(tests)* move the handle helper to tests/common
- release workflow for five targets with a rebuild_tag input ([#3](https://github.com/bddkit/bddkit-etcd/pull/3))
- agent orientation for the plugin repository ([#2](https://github.com/bddkit/bddkit-etcd/pull/2))
- Initial commit
