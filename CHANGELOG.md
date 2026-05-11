# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-05-11

### Changed

- **Breaking:** n0 public relay disabled by default
  (`iroh::endpoint::presets::N0DisableRelay`). `ConnectPeer` now honors
  the request's `addresses` and `relay_url` — at least one must be
  non-empty, or the call returns an explicit error instead of the
  prior silent 10-second wait + opaque 500.
- **Breaking:** `--peer` / `PEAT_NODE_PEERS` takes `endpoint_id@host:port`
  form. Bare endpoint IDs are rejected with a clear log message. One
  peer per entry; `--peer` repeats or comma-separates in
  `PEAT_NODE_PEERS`. Multi-address-per-peer goes through the
  `ConnectPeer` RPC at runtime.
- **Breaking:** Watcher TLS partial configuration now errors at
  startup. Setting only `PEAT_NODE_AGENT_TLS_CERT` (or only
  `_KEY`, or `_CA` alone) used to silently fall through to insecure
  h2c — exact footgun fixed. The watcher logs the error and disables
  itself; the rest of the sidecar keeps running.
- **Breaking (Go SDK consumers):** `sdk/go/` removed entirely. The repo
  is pure Rust. Generate typed clients from `proto/sidecar.proto` in
  your own repo, or front the sidecar with `peat-gateway` (ADR-043
  consumer-interface adapter).
- Pinned dependency `peat-mesh` bumped from `=0.9.0-rc.1` to
  `=0.9.0-rc.7`. Six RCs of upstream work — `Node` generic doc layer
  (rc.2), per-peer `LinkState` for ADR-032 §A (rc.5), peat-btle
  lockstep closes (rc.6, rc.7).
- `GetSyncStats.bytes_sent` / `bytes_received` now wired through to
  `AutomergeSyncCoordinator`'s real counters instead of returning zero.

### Added

- `PEAT_NODE_IROH_UDP_PORT` / `--iroh-udp-port` — pins Iroh's QUIC port
  for deployments where peers need a stable UDP host:port (Docker
  Compose, fleet-managed sidecars).
- `examples/compose/` — runnable two-node Docker Compose quickstart,
  peering over direct UDP across the compose bridge. No
  public-internet egress required.
- `docs/CONFIGURATION.md` — collected reference for every `PEAT_NODE_*`
  env var.
- Rust integration test surface expanded from 27 to 46 tests covering
  cross-peer encryption, formation isolation, typed-collection
  full-field round-trips, multi-subscriber + DELETE event fanout,
  sync stop/resume + peer disconnect, UDS listener bind path, watcher
  mTLS PEM handling, and two-node sync (in-process + subprocess) with
  real byte-counter assertions.

### Removed

- **Breaking:** `sdk/go/` (Go SDK) and `test/go/` (Go integration
  tests) deleted. The `Sync Test (two-sidecar)` Go-driven CI job is
  replaced by `tests/sync_test.rs` (in-process) and
  `tests/sync_subprocess_test.rs` (real binaries via
  `CARGO_BIN_EXE_peat-node`), both running under `cargo test`.

### Internal

- CI: Helm chart now exercised by both `helm lint` and
  `helm template` (catches missing values / bad template references
  that `lint` alone misses).
- CI: Claude-driven QA Review workflow (`.github/workflows/qa-review.yml`)
  comments on every PR with severity-tagged findings against
  peat-node-specific criteria.

## [0.1.0] - 2026-05-08

## [0.1.0] - 2026-05-08

Initial release of `peat-node`, the Peat mesh sidecar that exposes
`peat-protocol` as a gRPC API for co-located applications. Ships as a
single Rust binary, a multi-arch container image
(`ghcr.io/defenseunicorns/peat-node`), a Helm chart, and a Zarf manifest.

### Added

- gRPC / gRPC-Web / Connect API on a single port (default `50051/tcp`)
  serving the `peat-protocol` wire contract from `proto/sidecar.proto`.
- Automerge CRDT state and Iroh blob storage under `/data/peat-node`.
- Helm chart at `chart/peat-node/` for Kubernetes deployment.
- Zarf manifest for air-gapped delivery.
- Multi-stage `Dockerfile` producing a `debian:bookworm-slim` runtime
  image with `tini` as PID 1.
