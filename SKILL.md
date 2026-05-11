---
name: peat-node
description: Per-repo skill for the Peat sidecar node — Rust binary that exposes peat-protocol over Connect/gRPC/gRPC-Web for co-located applications, plus a Go SDK and Helm/Zarf packaging.
when_to_use: Editing files under peat-node/, reviewing peat-node PRs, debugging the sidecar API or DDIL fleet sync, editing proto/sidecar.proto, working on the Go SDK in sdk/go, or working on the Helm chart / Zarf packaging.
verifies_with: cargo fmt --check, cargo clippy -- -D warnings, cargo test, plus Go SDK regeneration when proto changes, the cross-cluster-sync.sh functional test for sync-path changes, and helm template for chart changes.
---

# `peat-node` SKILL

`peat-node` is the **sidecar node** for the Peat ecosystem. It runs alongside a co-located application and exposes `peat-protocol` as a gRPC API (Connect + gRPC + gRPC-Web on a single port via ConnectRPC) so applications can read/write Peat documents and consume change events without embedding the mesh stack themselves. The repo ships:

- A Rust binary (`src/main.rs`) implementing the sidecar.
- The wire contract in `proto/sidecar.proto`, compiled via `connectrpc_build` (build.rs) for Rust and via `buf` for the Go SDK.
- A Go SDK in `sdk/go/` (client, heartbeat, generated proto code) — this is the public surface for Go consumers.
- A Helm chart in `chart/peat-node/` and a Zarf manifest for Kubernetes deployment.
- A cross-cluster functional test (`test/cross-cluster-sync.sh`) plus Go integration tests in `test/go`.

## When this skill applies

- Editing any file under `src/` (sidecar implementation, agent watcher, encryption-at-rest)
- Editing `proto/sidecar.proto` — **the wire contract**; changes ripple to Rust server code, the Go SDK, and any external consumer
- Editing `sdk/go/` (Go client, heartbeat, generated code)
- Editing `chart/peat-node/` (Helm chart) or `zarf.yaml` (Zarf packaging)
- Editing `test/cross-cluster-sync.sh` or `test/go/`
- Bumping the pinned `peat-mesh` version (currently `=0.9.0-rc.1`)

## Scope

**In scope:**
- Sidecar API server (Connect / gRPC / gRPC-Web on single port)
- Wire-contract proto and the Rust + Go code generated from it
- Agent watcher (Connect RPC JSON encoding)
- Encryption at rest (`aes-gcm`)
- Helm chart and Zarf packaging
- Functional and integration tests

**Out of scope (route elsewhere):**
- Mesh transport / sync semantics → `peat-mesh/SKILL.md`
- BLE transport → `peat-btle/SKILL.md`
- OCI registry sync → `peat-registry/SKILL.md`
- Top-level shared types/traits — consider whether the change belongs in `peat/peat-protocol` or `peat/peat-schema`
- Production cluster operations / GitOps configs that consume this chart — separate ops repos

## Workflow

1. **Orient.** Read `peat/SKILL.md` (ecosystem) if accessible. Read this file. Read `docs/DESIGN.md` if you're touching architectural surfaces. `git status`, `git log -10`.
2. **Locate the spec.** Confirm the task has a GitHub issue with Context / Scope / Acceptance / Constraints / Dependencies. If not, stop and ask the user.
3. **Plan.** Produce a 1–5 step plan. Cross-check against ecosystem hard invariants (transport agnosticism, dependency direction, async runtime is Tokio, no new languages — Go SDK is grandfathered) and the scope guards below. **Proto changes are contract changes**: if you're editing `proto/sidecar.proto`, plan how the Rust server, the Go SDK, and any external consumer will pick up the change.
4. **Implement.** Branch from `main` per the trunk-based convention. Vertical slices, one concern per commit. For proto changes, regenerate Go bindings (`buf generate` from `sdk/go/`) and commit the generated code in the same PR.
5. **Verify.** Run every command in the verification checklist below. Capture output.
6. **Hand off.** Open PR against `main` referencing the issue. Single concern per PR — squash-merge applies.

## Verification (exit criteria)

A session in this repo is not done until each of these produces evidence:

- [ ] `cargo fmt --check` exits 0
- [ ] `cargo clippy -- -D warnings` exits 0
- [ ] `cargo test` exits 0
- [ ] If `proto/sidecar.proto` was touched: `cargo build` (re-runs proto compile via build.rs and confirms server code matches the new contract); regenerate the Go SDK (`cd sdk/go && buf generate`) and commit the regenerated `gen/` output in the same PR
- [ ] If `sdk/go/` was touched: `cd sdk/go && go build ./...` (compile-compatibility check). Note: `sdk/go/` contains no `_test.go` files — it's a thin typed wrapper. Surface-tier coverage of the SDK lives in `test/go/cmd/synctest/`, exercised in CI by the `Sync Test (two-sidecar)` job.
- [ ] If sync-path code or the chart changed: `./test/cross-cluster-sync.sh` (or the Go integration tests in `test/go/`)
- [ ] If `chart/` or `zarf.yaml` was touched: `helm template chart/peat-node` renders cleanly
- [ ] If the change bumps `peat-mesh`: full integration suite, not just unit tests

"Seems right" or "the diff looks correct" is never sufficient.

## Anti-rationalization

| Excuse | Rebuttal |
|---|---|
| "This change is too small to need a test." | If it's worth changing, it's worth one assertion. Add the test. |
| "I'll fix the clippy warning later." | The CI gate is `-D warnings`. There is no later. |
| "I'll modify `proto/sidecar.proto` and regenerate the Go SDK in a follow-up." | The proto **is** the contract. Server code, Go SDK, and any consumer must match. Regenerate `sdk/go/gen/` and commit it in the same PR; otherwise downstream consumers break silently. |
| "I'll add this new endpoint as a Rust-only handler — easier than touching proto." | Proto-first. New endpoints go in `sidecar.proto` first; Rust + Go follow. Consumers can't talk to a Rust-only endpoint. |
| "I'll skip the cross-cluster sync test — `cargo test` passes." | DDIL fleet sync is the product. Cross-cluster test catches network-partition / re-convergence bugs unit tests don't. |
| "Go SDK is just a client wrapper — I don't need to update it for a small server change." | The SDK is part of the public surface and ships with this repo. Server-side behavior changes that affect call semantics require SDK updates and Go integration test runs. |
| "I'll bump `peat-mesh` to the latest RC." | The pin (`=0.9.0-rc.1`) is intentional. Bumps need full integration validation and possibly chart/Zarf updates. |
| "I'll inline the encryption-at-rest call without zeroization — it's only briefly in memory." | `aes-gcm` material lives in security-sensitive paths. Use the established zeroization patterns; don't introduce un-zeroized handling. |
| "I'll add a fix in the generated Go code so we don't have to regenerate." | Generated code must stay generated. If `buf generate` doesn't produce the desired output, fix the proto or the buf config — not the generated file. |

## Scope guards

- Touch only files the issue/user asked you to touch.
- Do not edit other peat-* repos. Cross-repo work goes in a separate PR in that repo, linked through a tracking issue.
- Do not hand-edit generated Go code under `sdk/go/gen/` — regenerate via `buf generate`.
- Do not introduce a new language or runtime. The Go SDK is a grandfathered exception (sidecar consumers are commonly Go); new code goes in Rust.
- Do not break wire-contract backwards compatibility silently. Proto changes that affect existing fields/methods require explicit versioning consideration.
- Do not commit secrets, KMS material, or absolute paths in `chart/`, `zarf.yaml`, or test fixtures.
- Do not configure git to bypass GPG signing or use `--no-verify` to skip pre-commit hooks.

## Gotchas

Add an entry each time a session produces output that needed correction. One line per gotcha plus a `Why:` line.

- *(none recorded yet)*

## References (read on demand, not by default)

- Ecosystem invariants: `peat/SKILL.md` (sibling repo)
- Architecture: `docs/DESIGN.md`
- Wire contract: `proto/sidecar.proto`
- Go SDK: `sdk/go/` (with `buf.gen.yaml` and `buf.yaml` driving codegen)
- Helm chart: `chart/peat-node/`
- Zarf packaging: `zarf.yaml`
- Cross-cluster functional test: `test/cross-cluster-sync.sh`
- Go integration tests: `test/go/`
- Repo: https://github.com/defenseunicorns/peat-node

---
*Last updated: 2026-05-05*
*Maintained by: Kit Plummer, VP Data and Autonomy, Defense Unicorns*
