# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking:** n0 public relay disabled by default (`presets::N0DisableRelay`).
  `ConnectPeer` now honors `addresses` and `relay_url` from the request — at
  least one is required. The `--peer` / `PEAT_NODE_PEERS` flag takes
  `endpoint_id@host:port` form; bare endpoint IDs are rejected. New
  `--iroh-udp-port` / `PEAT_NODE_IROH_UDP_PORT` flag pins Iroh's QUIC
  port for direct peer-to-peer reachability.
- `GetSyncStats.bytes_sent` / `bytes_received` now wired through to
  `AutomergeSyncCoordinator`'s real counters instead of returning zero.

### Removed

- **Breaking:** Go SDK (`sdk/go/`) and Go integration tests (`test/go/`)
  removed entirely. The repo is pure Rust now. Consumers in other
  languages talk to the sidecar directly over the Connect-RPC wire
  (generate from `proto/sidecar.proto`), or front it with `peat-gateway`
  for protocol-bridge adapters per ADR-043.
- The `Sync Test (two-sidecar)` Go-driven CI job replaced by the
  in-process `tests/sync_test.rs` integration test, which runs as part
  of `cargo test`.

### Documentation

- `docs/CONFIGURATION.md` — collected reference for every `PEAT_NODE_*`
  env var, including the new ones above.
- `examples/compose/` — runnable two-node Docker Compose quickstart,
  peering over direct UDP (no public-internet egress required).

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
