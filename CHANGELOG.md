# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-05-08

Initial release of `peat-node`, the Peat mesh sidecar that exposes
`peat-protocol` as a gRPC API for co-located applications. Ships as a
single Rust binary, a multi-arch container image
(`ghcr.io/defenseunicorns/peat-node`), a Helm chart, a Zarf manifest,
and a Go SDK generated from the wire proto.

### Added

- gRPC / gRPC-Web / Connect API on a single port (default `50051/tcp`)
  serving the `peat-protocol` wire contract from `proto/sidecar.proto`.
- Automerge CRDT state and Iroh blob storage under `/data/peat-node`.
- Helm chart at `chart/peat-node/` for Kubernetes deployment.
- Zarf manifest for air-gapped delivery.
- Go SDK at `sdk/go` generated via `buf`, with integration tests
  under `test/go`.
- Multi-stage `Dockerfile` producing a `debian:bookworm-slim` runtime
  image with `tini` as PID 1.
