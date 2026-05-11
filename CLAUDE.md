# CLAUDE.md — `peat-node`

Before doing any work in this repo, read **both** of:

1. `SKILL.md` (this repo) — the per-repo workflow, verification checklist, and scope guards.
2. `peat/SKILL.md` (in the sibling `peat` repo, if checked out alongside) — the ecosystem skill: hard invariants, FFI conventions, the skill router across all peat-* repos.

If `peat/SKILL.md` isn't accessible, say so before proceeding — most architectural invariants live there, not here.

## Quick orientation

- **Repo role:** Peat mesh node — exposes `peat-protocol` as a gRPC API for co-located applications (sidecar pattern). Single Rust binary that runs alongside an application and provides DDIL-resilient fleet state sync over Connect / gRPC / gRPC-Web on a single port. Ships with a Helm chart (`chart/peat-node/`) and a Zarf manifest. Pure Rust — consumers in other languages talk to it directly over the Connect/gRPC wire.
- **Primary language:** Rust. Wire contract is `proto/sidecar.proto`, compiled via `connectrpc_build` for the in-tree server.
- **Cheap sanity check:** `cargo build`. Re-runs the proto compile if `proto/sidecar.proto` changed.

## Hard rule

A task in this repo is not done until the verification checklist in `SKILL.md` produces evidence. "Seems right" or "the diff looks correct" is never sufficient.

GPG-signed commits are required by repo policy. Cross-repo changes require one PR per repo, linked through a tracking issue — not a single PR that reaches across repos.
