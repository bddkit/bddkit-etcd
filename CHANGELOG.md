# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/bddkit/bddkit-etcd/compare/v0.1.0...v0.1.1) - 2026-11-23

### Added

- a failover example on three real nodes that the exec plugin starts and kills
- etcd should accept / refuse steps for put, read and compaction
- steps for one node of a cluster (leader, applied revision, HashKV, serializable get)

### Other

- README for the cluster and refusal steps and the failover example
- a three-node etcd cluster of real processes that tests can break
