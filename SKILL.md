---
name: peat-node
description: Per-repo skill for the Peat sidecar node — Rust binary that exposes peat-protocol over Connect/gRPC/gRPC-Web for co-located applications, plus Helm/Zarf packaging.
when_to_use: Editing files under peat-node/, reviewing peat-node PRs, debugging the sidecar API or DDIL fleet sync, editing proto/sidecar.proto, or working on the Helm chart / Zarf packaging.
verifies_with: cargo fmt --check, cargo clippy -- -D warnings, cargo test, the cross-cluster-sync.sh functional test for sync-path changes, and helm template for chart changes.
---

# `peat-node` SKILL

`peat-node` is the **sidecar node** for the Peat ecosystem. It runs alongside a co-located application and exposes `peat-protocol` as a gRPC API (Connect + gRPC + gRPC-Web on a single port via ConnectRPC) so applications can read/write Peat documents and consume change events without embedding the mesh stack themselves. The repo ships:

- A Rust binary (`src/main.rs`) implementing the sidecar.
- The wire contract in `proto/sidecar.proto`, compiled via `connectrpc_build` (build.rs) for the in-tree server.
- A Helm chart in `chart/peat-node/` and a Zarf manifest for Kubernetes deployment.
- A cross-cluster functional test (`test/cross-cluster-sync.sh`) plus in-tree Rust integration tests under `tests/`. The canonical test files map to RPC surfaces:
  - `tests/grpc_test.rs` — generic Connect HTTP+JSON coverage for the document / peer / sync / typed-collection RPCs
  - `tests/attachments_*_test.rs` — PRD-006 attachment surface (smoke, acceptance, subscribe, multi-peer, deferred)
  - `tests/subscribe_test.rs`, `tests/subscribe_query_test.rs` — document Subscribe streaming RPC
  - `tests/sync_test.rs`, `tests/cross_peer_encryption_test.rs`, `tests/formation_isolation_test.rs`, `tests/partition_test.rs` — multi-node CRDT scenarios
  - `tests/node_test.rs`, `tests/uds_test.rs`, `tests/typed_collections_test.rs`, `tests/sync_control_test.rs` — in-process unit-ish integration tests against `SidecarNode`

Consumers in other languages talk to the sidecar directly over the Connect-RPC wire — no in-repo SDK. The `examples/compose/` quickstart shows the bash+curl+jq path. For typed clients, generate from `proto/sidecar.proto` in the consumer's own repo (or front it with `peat-gateway` per ADR-043).

## When this skill applies

- Editing any file under `src/` (sidecar implementation, agent watcher, encryption-at-rest)
- Editing `proto/sidecar.proto` — **the wire contract**; changes ripple to the Rust server code and any external consumer that generates from it
- Editing `chart/peat-node/` (Helm chart) or `zarf.yaml` (Zarf packaging)
- Editing `test/cross-cluster-sync.sh` or the in-tree Rust integration tests
- Bumping the pinned `peat-mesh` version (currently `=0.9.0-rc.7`)

## Scope

**In scope:**
- Sidecar API server (Connect / gRPC / gRPC-Web on single port)
- Wire-contract proto and the Rust server generated from it
- Agent watcher (Connect RPC JSON encoding)
- Encryption at rest (`aes-gcm`)
- Helm chart and Zarf packaging
- Rust integration tests + cross-cluster functional test

**Out of scope (route elsewhere):**
- Mesh transport / sync semantics → `peat-mesh/SKILL.md`
- BLE transport → `peat-btle/SKILL.md`
- OCI registry sync → `peat-registry/SKILL.md`
- Typed-client SDKs in other languages → consumer's repo, or `peat-gateway` for protocol-bridge adapters (ADR-043)
- Top-level shared types/traits — consider whether the change belongs in `peat/peat-protocol` or `peat/peat-schema`. **Dependency direction:** peat-node depends on `peat-mesh` (always) and `peat-protocol` (for the attachment substrate — `FileDistribution`, `IrohFileDistribution`, `DistributionHandle`, `TransferPriority`). `peat-protocol` is the layer *beneath* `peat-mesh` in the workspace, not a sibling; the two-dep arrangement is intentional. Sibling repos (`peat-btle`, `peat-registry`, `peat-gateway`) remain out-of-bounds — those still route through their own skills.
- Production cluster operations / GitOps configs that consume this chart — separate ops repos

## Workflow

1. **Orient.** Read `peat/SKILL.md` (ecosystem) if accessible. Read this file. Read `docs/DESIGN.md` if you're touching architectural surfaces. `git status`, `git log -10`.
2. **Locate the spec.** Confirm the task has a GitHub issue with Context / Scope / Acceptance / Constraints / Dependencies. If not, stop and ask the user.
3. **Plan.** Produce a 1–5 step plan. Cross-check against ecosystem hard invariants (transport agnosticism, dependency direction, async runtime is Tokio, Rust only) and the scope guards below. **Proto changes are contract changes**: if you're editing `proto/sidecar.proto`, plan how the Rust server and any external consumer will pick up the change.
4. **Implement.** Branch from `main` per the trunk-based convention. Vertical slices, one concern per commit.
5. **Verify.** Run every command in the verification checklist below. Capture output.
6. **Hand off.** Open PR against `main` referencing the issue. Single concern per PR — squash-merge applies.

## Verification (exit criteria)

A session in this repo is not done until each of these produces evidence:

- [ ] `cargo fmt --check` exits 0
- [ ] `cargo clippy -- -D warnings` exits 0
- [ ] `cargo test` exits 0 (this includes `tests/sync_test.rs`, the two-node in-process CRDT-sync test)
- [ ] If `proto/sidecar.proto` was touched: `cargo build` (re-runs proto compile via build.rs and confirms server code matches the new contract); external consumers regenerate from the new proto on their side
- [ ] If sync-path code or the chart changed: `./test/cross-cluster-sync.sh`
- [ ] If `chart/` or `zarf.yaml` was touched: `helm template chart/peat-node` renders cleanly
- [ ] If the change bumps `peat-mesh`: full integration suite, not just unit tests

"Seems right" or "the diff looks correct" is never sufficient.

## Anti-rationalization

| Excuse | Rebuttal |
|---|---|
| "This change is too small to need a test." | If it's worth changing, it's worth one assertion. Add the test. |
| "I'll fix the clippy warning later." | The CI gate is `-D warnings`. There is no later. |
| "I'll add this new endpoint as a Rust-only handler — easier than touching proto." | Proto-first. New endpoints go in `sidecar.proto` first; the Rust server follows. Consumers can't talk to a Rust-only endpoint. |
| "I'll skip the cross-cluster sync test — `cargo test` passes." | DDIL fleet sync is the product. Cross-cluster test catches network-partition / re-convergence bugs unit tests don't. |
| "I'll bump `peat-mesh` to the latest RC." | The pin (`=0.9.0-rc.7`) is intentional. Bumps need full integration validation and possibly chart/Zarf updates. |
| "I'll inline the encryption-at-rest call without zeroization — it's only briefly in memory." | `aes-gcm` material lives in security-sensitive paths. Use the established zeroization patterns; don't introduce un-zeroized handling. |
| "I'll add a Go/TS/Python SDK directly to this repo for a quick consumer integration." | No SDKs live in this repo. Typed clients generate from `proto/sidecar.proto` in the consumer's own repo, or front the sidecar with `peat-gateway` per ADR-043. |

## Scope guards

- Touch only files the issue/user asked you to touch.
- Do not edit other peat-* repos. Cross-repo work goes in a separate PR in that repo, linked through a tracking issue.
- Do not introduce a new language or runtime. peat-node is pure Rust.
- Do not break wire-contract backwards compatibility silently. Proto changes that affect existing fields/methods require explicit versioning consideration.
- Do not commit secrets, KMS material, or absolute paths in `chart/`, `zarf.yaml`, or test fixtures.
- Do not configure git to bypass GPG signing or use `--no-verify` to skip pre-commit hooks.

## Gotchas

Add an entry each time a session produces output that needed correction. One line per gotcha plus a `Why:` line.

- Read/write `IROH_DISTRIBUTION_COLLECTION` docs only via `peat_protocol::storage::{read_distribution_document, scan_distribution_documents, write_receiver_node_status}` — never `collection.get/scan` + `serde_json`.
  Why: as of peat-protocol 0.9.0-rc.9 the on-wire shape is structured Automerge (`ROOT.metadata` byte-scalar + typed `ROOT.node_statuses` Map), not a single JSON `ROOT.data` scalar; the old access pattern returns `None`/garbage against rc.9 docs and the inline wholesale RMW was the substrate root of peat#864.
- The four iroh two-node integration tests (`end_to_end_attachment_delivery_two_nodes`, `node_list_scope_only_delivers_to_listed_nodes`, `receiver_writes_node_status_into_distribution_doc`, `subscribe_emits_progress_then_terminal`) must carry `#[serial_test::serial(iroh_two_node)]`.
  Why: `cargo test` runs tests within a binary in parallel; each of these spins up a `#[tokio::test(flavor = "multi_thread")]` runtime + two real iroh endpoints, and the CPU contention deterministically stalls PRD-006 test 23's 60s budget on CI runners (cost: a closed PR #77 and three CI-fail rounds before the cause was nailed).
- Local `protoc` must support proto3 optional; the distro `protobuf-compiler` (3.12.x) does not. Install a prebuilt protoc ≥25 to `~/.local/bin` and pass `PROTOC=$HOME/.local/bin/protoc` to cargo.
  Why: `build.rs` runs `connectrpc_build` over `proto/sidecar.proto` which uses proto3 optional; an old protoc fails the build with `--experimental_allow_proto3_optional was not set`. CI installs a current protoc; local dev usually doesn't.
- The receiver-side contract is independently testable from the receiver's *local* Automerge doc — don't gate its test solely on the sender's `subscribe_progress` stream.
  Why: `receiver_writes_node_status_into_distribution_doc` reads the receiver's own doc via `read_distribution_document`, isolating peat-node's write contract from upstream sender-observation races; this is what made the peat#864 bisect tractable.

## References (read on demand, not by default)

- Ecosystem invariants: `peat/SKILL.md` (sibling repo)
- Architecture: `docs/DESIGN.md`
- Configuration reference: `docs/CONFIGURATION.md`
- Wire contract: `proto/sidecar.proto`
- Helm chart: `chart/peat-node/`
- Zarf packaging: `zarf.yaml`
- Cross-cluster functional test: `test/cross-cluster-sync.sh`
- Rust integration tests: `tests/`
- Compose quickstart: `examples/compose/`
- Repo: https://github.com/defenseunicorns/peat-node

---
*Last updated: 2026-05-11*
*Maintained by: Kit Plummer, VP Data and Autonomy, Defense Unicorns*
