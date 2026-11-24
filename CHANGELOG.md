# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0](https://github.com/bddkit/bddkit-etcd/compare/v0.2.0...v0.3.0) - 2026-11-24

### Added

- resume a watch after its stream breaks
- named etcd watches with a buffer and event assertions ([#22](https://github.com/bddkit/bddkit-etcd/pull/22))

### Other

- release 0.3.0
- the watch steps, their rules and the open points of watch resume
- watch examples, and a watch through a failover that kills the watched node
- layout of the watch module and its tests
