# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.2] - 2026-07-02

### Fixed

- **Blob fetch stall cycle broken** — peers in the cooling-off state are now skipped during `fetch_blob` peer selection rather than being tried and immediately failing the health check. A cooling peer that was the only candidate would previously cause `fetch_blob` to stall for the full stall-timeout duration before falling back; it is now bypassed immediately. Bumps `peat-mesh` to `0.9.0-rc.41`. ([peat-mesh#257](https://github.com/defenseunicorns/peat-mesh/pull/257))

### Fixed

- **`grpc_test` port collision eliminated** — `boot_server` now binds to `127.0.0.1:0` and reads the OS-assigned port via `BoundServer::local_addr()` rather than using hardcoded ports (50081–50087). Removes the `AddrInUse` flake that appeared when a prior test run left sockets in `TIME_WAIT`.

## [0.4.1] - 2026-06-16

### Added

- **`peat attach` subcommand** (`send` / `watch` / `status`) — distribute and receive binary files directly from the operator CLI over the Iroh blob store without a running `peat-node`. Supports `--scope` (`all` / `nodes:id1,id2` / `formation:id`), `--priority` (`critical` / `high` / `normal` / `low`), `--wait` (block until all targets confirm receipt), and `--dist-id` exit-on-delivery for `watch`. Updated README and QUICKSTART with command reference and two-terminal walkthrough. ([#155](https://github.com/defenseunicorns/peat-node/pull/155))

### Fixed

- **Peer health reset on reconnect** — peers that were marked `Unhealthy` before a QUIC idle-timeout blackout now re-enter `fetch_blob` ordering at `Neutral` after `dial_and_attach`, rather than carrying their pre-outage verdict forward. `BlobPeerIndex` and `known_peers` are preserved — only the health verdict is cleared. Bumps peat-mesh to `rc.40`, which also drops the `peer_health_cooldown` default from 120 s to 30 s (matching the watchdog reconnect cycle). ([#156](https://github.com/defenseunicorns/peat-node/pull/156))

### Changed

- **Reconnect watchdog — double-lock eliminated** — the watchdog error path no longer re-acquires `registered_peers` to read back the backoff it just wrote; the value is captured as a local before the write lock drops. ([#157](https://github.com/defenseunicorns/peat-node/pull/157))
- **`peat attach watch --dist-id` — filesystem polling eliminated** — replaced 500 ms directory-scan polling with a `tokio::sync::Notify` fired by `InboxSink::deliver()` on the target distribution's atomic rename; the CLI wakes within one async tick of delivery. ([#157](https://github.com/defenseunicorns/peat-node/pull/157))

### Fixed

- **Inbox-only nodes now get time-based bundle eviction** (closes [peat-node#149](https://github.com/defenseunicorns/peat-node/issues/149)). The `PEAT_NODE_ATTACHMENT_HANDLE_RETENTION_SECS` knob and the background eviction ticker now cover receive-only nodes (inbox configured, no roots) as well as send-side nodes. Previously the ticker was gated on `has_roots()` only, so a receive-only node accumulated terminal bundle handles indefinitely until LRU pressure removed them — time-based eviction was silently inert.
- **Chart: inbox without volume mount now fails at `helm install` time** (closes [peat-node#149](https://github.com/defenseunicorns/peat-node/issues/149)). Setting `attachment.inbox` without a corresponding `attachment.extraVolumeMounts` entry now produces a clear `helm install`/`helm upgrade` error rather than silently writing received blobs to ephemeral container storage (lost on pod restart).
- **Binary warns on `PEAT_NODE_ATTACHMENT_INBOX_POLL_SECS=0`** (closes [peat-node#149](https://github.com/defenseunicorns/peat-node/issues/149)). A poll interval of 0 is clamped to 1 s at startup and now emits a `WARN`-level log message so operators can distinguish "accepted" from "silently corrected".

### Changed

- **Chart: `disableMdns` defaults to `true` — bare-metal Helm users must opt in** (from [#148](https://github.com/defenseunicorns/peat-node/pull/148)). The peat-node binary defaults mDNS peer discovery to **on**; the Helm chart now defaults `disableMdns: true` (injecting `PEAT_NODE_DISABLE_MDNS=true`) because multicast is unavailable in Kubernetes and containers. **Bare-metal operators deploying via the chart** who relied on mDNS for same-host peer discovery must add `disableMdns: false` to their values override after a `helm upgrade`. No action is needed for Kubernetes deployments — mDNS was always a no-op there.

## [0.4.0] - 2026-06-10

### Added

- **`PEAT_NODE_BLOB_STALL_TIMEOUT_SECS`** env var / **`--blob-stall-timeout-secs`** CLI flag (closes [peat-node#131](https://github.com/defenseunicorns/peat-node/issues/131)). Sets the iroh blob-download stall threshold (`IrohConfig::download_stall_timeout`). Default is peat-mesh's built-in 30 s; lower it (e.g. 3–5 s) for redundant-peer deployments where an unreachable preferred peer would otherwise cost the full stall on the first fetch before the peer-health index demotes it.
- **QoS-priority relay fanout** (closes [peat-node#138](https://github.com/defenseunicorns/peat-node/issues/138)). peat-node now drains its relay-fanout queue highest-QoS-first (Critical before Bulk) instead of in arrival order. A latency-sensitive document enqueued behind a large Bulk backlog is fanned out ahead of it rather than head-of-line-blocked — mirroring the peat-mesh#247 / ADR-0013 fix already in the peat-mesh layer.
- **Operator tombstone TTL and GC config** (closes [peat-node#136](https://github.com/defenseunicorns/peat-node/issues/136)). Three new env vars / CLI flags control lifecycle behaviour of the backing CRDT store without a code rebuild:
  - `PEAT_NODE_TOMBSTONE_TTL_HOURS` / `--tombstone-ttl-hours` — tombstone retention window (default 168 h / 7 days; values below 24 h emit a startup warning per ADR-016).
  - `PEAT_NODE_GC_INTERVAL_SECS` / `--gc-interval-secs` — GC sweep cadence (default 300 s / 5 min).
  - `PEAT_NODE_GC_BATCH_SIZE` / `--gc-batch-size` — max tombstones processed per GC sweep (default 1 000).

### Changed

- **BREAKING (sidecar gRPC API): `Platform` → `Node` ([ADR-068](https://github.com/defenseunicorns/peat/blob/main/docs/adr/068-node-base-unit-vocabulary.md) Phase 4, epic [peat#968](https://github.com/defenseunicorns/peat/issues/968)).** Converges the sidecar surface on **Node** as the base-unit term. In `sidecar.proto` (field numbers preserved — binary stays tag-compatible; message/enum/RPC/field *names* break): `message Platform` → `Node`, `PutPlatform`/`GetPlatforms` → `PutNode`/`GetNodes`, `enum PlatformStatus` → `NodeStatus` (`PLATFORM_STATUS_*` → `NODE_STATUS_*`), `Platform.platform_type` → `Node.node_type`, `Cell.platform_count` → `node_count`, `Track.source_platform` → `source_node`. The document-collection name for node docs changes `"platforms"` → `"nodes"`. gRPC clients must regenerate. Pre-1.0 clean break, consistent with the ADR-066 rename in 0.3.8.
- **`peat-mesh = "=0.9.0-rc.35"`** (was `rc.33`) and **`peat-protocol` / `peat-schema` floor `>=0.9.0-rc.24`** (was `>=rc.22`). rc.35 carries the peat-mesh `HierarchyLevel::Platform` → `Node` rename ([ADR-068 Phase 3](https://github.com/defenseunicorns/peat-mesh/pull/237)); the peat-schema/peat-protocol floor bump to rc.23 carries the [ADR-068 Phase 1](https://github.com/defenseunicorns/peat/pull/969) rename — `peat-cli`'s schema validation requires the `node_type` schema, so rc.22 (`platform_type`) is excluded. rc.24 floor applied alongside rc.36 pin (see next entry). The `test/cross-cluster-sync.sh` e2e harness and the API docs were migrated to the new RPC/collection names in lockstep.
- **`peat-mesh = "=0.9.0-rc.37"`** (was `rc.36`). rc.37 carries the determinism fix in [peat-mesh#244](https://github.com/defenseunicorns/peat-mesh/pull/244) ([peat-mesh#243](https://github.com/defenseunicorns/peat-mesh/issues/243)): a burst of deletes no longer opens a peer's circuit breaker and silently drops the next write across that hop (in a relay topology, the post-burst write reached no downstream node). No peat-node code change; `peat-protocol`/`peat-schema` floors unchanged (`>=rc.24` already admits the new peat-mesh).
- **`peat-mesh = "=0.9.0-rc.38"`** (was `rc.37`). rc.38 ([peat-mesh#251](https://github.com/defenseunicorns/peat-mesh/pull/251)) ships three fixes: `SyncError::Document` excluded from the peer circuit breaker ([peat-mesh#246](https://github.com/defenseunicorns/peat-mesh/issues/246)) — document decode errors no longer advance `retry_attempt` or reset `consecutive_successes`; `prepare_doc_for_sync` skips `doc.save()` for `FullHistory` mode ([peat-mesh#236](https://github.com/defenseunicorns/peat-mesh/issues/236)) — eliminates an O(N) serialisation step on every sync cycle; `AutomergeBackendConfig` gains `ttl_config`/`gc_config` and is now `#[non_exhaustive]` with `Default` (peat-node#136). No peat-node API surface change.

## [0.3.9] - 2026-06-04

Picks up **peat-mesh v0.9.0-rc.33** — the dual-C2 blob-fetch failover fix. No peat-node code change; the new behaviour is consumed transparently through `peat_mesh::storage` blob fetching.

### Changed

- **`peat-mesh = "=0.9.0-rc.33"`** (was `rc.32`). rc.33 ([peat-mesh#220](https://github.com/defenseunicorns/peat-mesh/pull/220), closes [peat-mesh#137](https://github.com/defenseunicorns/peat-mesh/issues/137)) makes `fetch_blob` order peers by recent fetch health: a peer whose most recent attempt stalled/errored is deprioritized behind healthy and untried peers (deprioritize, not skip), so an unreachable preferred peer (e.g. a downed C2 in a dual-C2 deployment) no longer costs the full `download_stall_timeout` (~30s) on every attachment fetch — only on the first, after which it is demoted for `peer_health_cooldown` (new `IrohConfig` field, default 120s). peat-node consumes this transparently through `peat_mesh::storage` blob fetching; no peat-node code change is required and the cooldown default needs no tuning. `peat-protocol` / `peat-schema` stay at the `>=0.9.0-rc.22, <0.9.1` floor — their range already admits rc.33.

## [0.3.8] - 2026-06-02

Picks up **peat-mesh v0.9.0-rc.32** + **peat-schema / peat-protocol v0.9.0-rc.22** — the coordinated **ADR-066 hierarchy vocabulary rename** (Squad/Platoon/Company → Cell/Cohort/Federation, plus a new Coalition tier) together with peat-mesh's dependency refresh (`automerge` 0.7→0.9, `iroh` rc.0→rc.1). Adopted as a single release because the two upstreams must move in lockstep: peat-mesh rc.31's `automerge` 0.9 bump and peat-protocol rc.22's matching bump share the `Automerge` type across the re-export boundary, and `iroh` is pinned to one version process-wide.

### Changed

- **`peat-mesh = "=0.9.0-rc.32"`** (was `rc.30`). rc.31 ([peat-mesh#208](https://github.com/defenseunicorns/peat-mesh/pull/208)/[#209](https://github.com/defenseunicorns/peat-mesh/pull/209)/[#210](https://github.com/defenseunicorns/peat-mesh/pull/210)) ships the ADR-066 Phase 3 internal `HierarchyLevel` rename + Coalition tier (internal to peat-mesh; not part of peat-node's consumed surface), aligns QoS collection names to the hyphenated peat-schema convention peat-node already uses, and refreshes deps: `automerge` 0.7→0.9, the iroh rc-train rc.0→rc.1 (`iroh-blobs` 0.101→0.102, `iroh-mdns-address-lookup` 0.2→0.3). rc.32 ([peat-mesh#218](https://github.com/defenseunicorns/peat-mesh/pull/218), closes [peat-mesh#217](https://github.com/defenseunicorns/peat-mesh/issues/217)) adds the send-side tombstone guard in `initiate_sync` — fixes a resurrection race where a reconnect re-sent a just-deleted doc as a live snapshot, which had made the `quickstart-compose` delete gate flaky.
- **`peat-protocol` / `peat-schema` floor `>=0.9.0-rc.22, <0.9.1`** (were `>=rc.17` / `>=rc.21`). rc.22 ([peat#957](https://github.com/defenseunicorns/peat/pull/957)) carries the ADR-066 schema/protocol rename and the matching `automerge` 0.9 bump.
- **`iroh = "=1.0.0-rc.1"`** (was `rc.0`) in both the root and `peat-cli` manifests — kept in lockstep with peat-mesh because iroh's process-global crypto provider + ALPN registry break under version skew (peat#923/#924). Now requires a Rust 1.91+ toolchain.
- **`automerge = "0.9.0"`** (was `0.7.1`) in `peat-cli`, matching peat-mesh's bump so the `Automerge` type peat-cli names from the store is ABI-compatible with the backend.

### Migration

- **`CellState.platoon_id` → `CellState.cohort_id`** (peat-schema, ADR-066; same proto field number 5). Consumers writing the `cell-states` typed collection via `peat create`/`peat update --set` must use `cohort_id`. The e2e typed-lifecycle scenario (`crates/peat-cli/tests/e2e/scenarios.rs`) was updated accordingly. The sidecar gRPC contract (`proto/sidecar.proto`) is unchanged; the rename is confined to the peat-schema typed-collection field names peat-node passes through as opaque JSON.

### Resolved upstream

- **[peat#904](https://github.com/defenseunicorns/peat/issues/904)** — ADR-066 hierarchy vocabulary rename. Phases 1+2 (schema/protocol) landed in peat rc.22; Phase 3 (peat-mesh internal) in peat-mesh rc.31.

## [0.3.7] - 2026-06-01

Picks up **peat-mesh v0.9.0-rc.30** ([peat-mesh#205](https://github.com/defenseunicorns/peat-mesh/issues/205)/[#206](https://github.com/defenseunicorns/peat-mesh/pull/206)) — the release that makes `peat-cli` actually work end-to-end against an established sidecar over an airgapped network. Also ships the two automated walkthrough gates (`Quickstart Path A (compose)` + a new `Cross-cluster sync` Test 6) that pin the QUICKSTART documentation to verifiable behavior.

### Added

- **QUICKSTART walkthroughs.** Root [`QUICKSTART.md`](QUICKSTART.md) covers standing up `peat-node` itself via Docker Compose (Path A, ~3 min) or Helm + k3d (Path B, ~10 min); [`crates/peat-cli/QUICKSTART.md`](crates/peat-cli/QUICKSTART.md) walks operators through every `peat` subcommand against the compose example. Both gated by automated CI tests so doc claims stay honest.
- **Two new CI gates.** [`test/quickstart-compose.sh`](test/quickstart-compose.sh) (`.github/workflows/quickstart.yml`) runs the full Path A walkthrough on every push — schema discovery, creds bootstrap, mesh-touching `query`, CRUD via `create`/`update`/`delete`, and `observe` streaming. [`test/cross-cluster-sync.sh`](test/cross-cluster-sync.sh) Test 6 exercises the in-pod CLI workflow from QUICKSTART Path B (`kubectl exec deploy/peat-peat-node -- peat …`) against a real Helm-deployed k3d cluster.
- **`peat` operator CLI — Phase 2–6 build-out** ([peat-node ADR-001](docs/peat-node-adr-001-peat-cli.md), PR [#107](https://github.com/defenseunicorns/peat-node/pull/107)). 0.3.6 shipped the Phase 1 scaffolding (workspace conversion + skeleton via [peat-node#103](https://github.com/defenseunicorns/peat-node/pull/103)); this PR wires all subcommand handlers to the live mesh:
  - **Read commands:** `peat query <COLLECTION>[/<DOC_ID>]` (materialised current state then exit) and `peat observe <COLLECTION>[/<DOC_ID>]` (stream changes until SIGINT).
  - **Write commands:** `peat create`, `peat update --set` (upsert per ADR-021), and `peat delete` (tombstone per ADR-034). `peat update --from` is gated on [peat-mesh#187](https://github.com/defenseunicorns/peat-mesh/issues/187) and returns `NotImplemented` until that lands.
  - **Output formats:** `text` (default, generic CRDT renderer), `json` (canonical), `ndjson` (one record per line for `observe`).
  - **Shell discipline:** data → stdout, logs/errors → stderr; documented exit code table (0/1/2/3/4/130); SIGPIPE-ignore at startup; SIGINT silent-exit.
  - **Distribution:** included in the `peat-node` container image at `/usr/local/bin/peat`; standalone binaries (`.tar.gz` + SHA-256 on Unix, `.zip` + SHA-256 on Windows) for Linux x86_64 / Linux aarch64 / macOS x86_64 / macOS aarch64 / Windows x86_64 attached to each tagged release.
  - **Testing:** 70 tests — 34 unit, 15 in-process parser, 21 binary e2e (13 surface tests + 8 real-mesh CRUD-lifecycle scenarios spawning the `peat` subprocess against an in-process `AutomergeBackend` peer with formation auth over loopback Iroh).
- **Optional pre-commit hook** at `.githooks/pre-commit` runs `cargo fmt --check` + `cargo clippy --workspace --all-targets -- -D warnings` when Rust files are staged. Enable per-clone with `git config core.hooksPath .githooks`.
- **Cross-platform CI matrix.** `.github/workflows/ci.yaml` adds a `cross-platform` job covering macOS aarch64 (workspace build + `peat-cli` tests) and Windows x86_64 (`peat-cli` build). Closes the "Windows in Constraints but not in CI gates" ADR-001 review finding.

### Changed

- **`peat-mesh = "=0.9.0-rc.30"`** (was `rc.27` in 0.3.6). Spans rc.28 / rc.29 / rc.30:
  - rc.28 ([peat-mesh#187](https://github.com/defenseunicorns/peat-mesh/issues/187)/[#192](https://github.com/defenseunicorns/peat-mesh/issues/192)/[#195](https://github.com/defenseunicorns/peat-mesh/pull/195)/[#196](https://github.com/defenseunicorns/peat-mesh/pull/196)) — `AutomergeStore::diff` / `apply_delta` + `AutomergeDelta` (unblocks `peat update --from`); `AutomergeBackend::{next_lamport, current_lamport, observe_lamport}` node-local Lamport clock + persistence + receive-side wire-up (replaces peat-cli's wall-clock workaround in `peat delete`).
  - rc.29 ([peat-mesh#202](https://github.com/defenseunicorns/peat-mesh/issues/202)) — `AutomergeStore::delete` now fires `observer_tx`, satisfying the CDC contract documented on `subscribe_to_observer_changes` ("fires for ALL document changes"). `peat observe` now sees remote tombstone arrivals.
  - rc.30 ([peat-mesh#205](https://github.com/defenseunicorns/peat-mesh/issues/205)/[#206](https://github.com/defenseunicorns/peat-mesh/pull/206)) — two coordinated fixes that take peat-cli from "authenticates but pulls zero docs" to fully functional. Connect side: `connect_and_authenticate_with_addr(EndpointAddr)` skips the `AddressLookupServices` chain when the caller has a fully-populated address (airgapped consumers no longer time out on `DnsAddressLookup`). Accept side: `SyncProtocolHandler::accept` now spawns `sync_all_documents_with_peer` after registering inbound connections, so empty-store peers pull state on connect.
- **`sync_on_change` fans out sync-received writes.** `src/node.rs` switched from `subscribe_to_changes()` (local-only) to `subscribe_to_changes_with_origin()` so peat-node-b relays a doc it received from peer A to peer C, with echo-suppression against the source. Needed for the A→B→C gossip chain the new `peat observe` walkthrough exercises ([peat-mesh#891](https://github.com/defenseunicorns/peat-mesh/issues/891)/[#907](https://github.com/defenseunicorns/peat-mesh/issues/907) contract).
- **`GetDocument` tolerates two doc shapes.** `src/node.rs` accepts both `{"value": "<json-string>"}` (the `PutDocument` (gRPC) wire shape, with optional at-rest encryption) AND the structural Automerge shape peat-cli's `create --set` writes (`{"name": "alice"}`). Single API entry point regardless of which writer produced the record.
- **`render_query` keys output for `--all-collections` and collection scopes.** Was dropping the `collection:id` key on single-doc results, which broke `jq '.["hello:world"]'` against `--all-collections` whenever the result happened to be one doc. Bare rendering reserved for explicit-doc-id queries (`query contacts/alice`).
- **CI test job split.** `cargo test --workspace --exclude peat-cli` runs alongside the rest of the workspace in parallel; `cargo test -p peat-cli` runs in its own step to isolate the e2e binary's multi-process scenarios from cross-binary CPU contention on 2-core Linux runners. Mirrors the existing macOS isolation in the `Cross-platform` job.

### Upstream landed

- **[peat#940](https://github.com/defenseunicorns/peat/issues/940)** — operator credential bundle file format formalised in ADR-006 (via [peat#944](https://github.com/defenseunicorns/peat/pull/944)). `peat-cli`'s `crates/peat-cli/src/creds.rs` shape (`app_id` + `shared_key` + optional `peers`) is now the canonical bundle; file-system custody normative requirements (mode 0600, refuse on world/group-readable).

### Resolved upstream

- **[peat-mesh#187](https://github.com/defenseunicorns/peat-mesh/issues/187)** — Automerge delta API. Landed in rc.28; `peat update --from` now uses the delta path instead of returning `NotImplemented`.
- **[peat-mesh#192](https://github.com/defenseunicorns/peat-mesh/issues/192)** — Lamport-clock source. Landed in rc.28; `peat delete` uses `AutomergeBackend::next_lamport` instead of a wall-clock proxy.
- **[peat-mesh#202](https://github.com/defenseunicorns/peat-mesh/issues/202)** — `AutomergeStore::delete` fires `observer_tx`. Landed in rc.29; `peat observe` now sees remote tombstones.
- **[peat-mesh#205](https://github.com/defenseunicorns/peat-mesh/issues/205)** + **[#206](https://github.com/defenseunicorns/peat-mesh/pull/206)** — connect-side `AddressLookupServices` chain bypass + accept-side push of existing docs. Landed in rc.30; peat-cli now fully works against an established sidecar.

### Open upstream tracking

- [peat#941](https://github.com/defenseunicorns/peat/issues/941) — authorization model. **Deferred** pending ADR-006 Layer 1 device identity. `peat-cli`'s "exit 3 before content parse" path stays as scaffolding for the future design; today's access boundary is formation-key custody.
- [peat#946](https://github.com/defenseunicorns/peat/issues/946) — peat-schema runtime type metadata registry. Blocks typed renderer dispatch + schema-validated writes (filed P0).
- "Real ack" for peat-cli's `--wait-for-sync`. Current implementation is a fixed 750 ms post-write sleep (`POST_WRITE_SYNC_WAIT` in `crates/peat-cli/src/cli/writes.rs`); QA review flags this as a misleading doc claim. Tracked for a follow-up once peat-mesh surfaces a per-write "acknowledged by N peers" signal.

## [0.3.6] - 2026-05-28

Patch release. Picks up the **peat-mesh#175 closure follow-throughs** published in [peat-mesh v0.9.0-rc.27](https://github.com/defenseunicorns/peat-mesh/releases/tag/v0.9.0-rc.27) (was rc.26). Also rolls forward to consume the peat-cli scaffolding that merged on `main` between 0.3.5 and this release ([peat-node#103](https://github.com/defenseunicorns/peat-node/pull/103)). Sidecar gRPC surface and on-the-wire formats are unchanged from 0.3.5.

### Changed

- **`peat-mesh = "=0.9.0-rc.27"`** (was `rc.26`). Spans three peat-mesh#175 follow-through PRs:
  - [peat-mesh#189](https://github.com/defenseunicorns/peat-mesh/pull/189) — `AutomergeBackend::get` keyed-lookup override (closes [peat-mesh#186](https://github.com/defenseunicorns/peat-mesh/issues/186)). The trait-default `DocumentStore::get(collection, id)` routed through `query(Query::Eq{id})` → `scan_prefix(collection_prefix)` + Automerge-deserialize of every entry in the collection — O(N) per lookup, O(N²) when looped over a set of IDs. The override does one redb point lookup + one Automerge parse using the same `key_for(collection, id)` shape the upsert path already builds — O(1) w.r.t. collection size. Deletion semantics preserved (soft-deleted docs return `None`; `IncludeDeleted` callers continue to route through `query()`).
  - [peat-mesh#190](https://github.com/defenseunicorns/peat-mesh/pull/190) — `InMemoryBackend::get` analog. Same O(N)→O(1) shape on the in-memory backend (two HashMap point lookups vs `col.values()` iteration).
  - [peat-mesh#188](https://github.com/defenseunicorns/peat-mesh/pull/188) — UAT file-header doc-comment scope clarification distinguishing the in-CI symmetric two-peer / single-relay case from the peat-sim 7n-dual-c2 multi-emitter / oversubscribed-link case. Doc-only.

  Both perf overrides are pure behavioural improvements — no API surface change, no contract change. peat-node's `DocumentStore::get` call sites transparently get the faster path.
- **`peat-protocol`** resolves unchanged at `>=0.9.0-rc.17, <0.9.1` — peat-protocol rc.17's workspace pin `peat-mesh = ">=0.9.0-rc.25, <0.9.1"` already permits rc.27 transitively, so no floor advance is needed.
- **`iroh = "=1.0.0-rc.0"`** unchanged from 0.3.5.

### Added

- **`crates/peat-cli/`** ([peat-node#103](https://github.com/defenseunicorns/peat-node/pull/103), ADR-001). New workspace member: the `peat` operator CLI as a Peat node. Phase-1 scaffolding only — all subcommand handlers stub to `CliError::NotImplemented`; full mesh wiring lands in subsequent phases. Cargo workspace conversion landed alongside (root `[workspace] members = ["crates/*"]`); existing `peat-node` package remains in place.

### Impact on peat-node

**None at the surface level.** peat-node uses `peat_mesh::storage::*`, `peat_mesh::sync::AutomergeBackend`, and `peat_protocol::storage::*`. The peat-mesh rc.27 perf overrides preserve all existing contracts (return types, deletion-filter semantics, observer paths); any caller using `DocumentStore::get` transparently gets the faster path. peat-cli is a new workspace member not invoked from the sidecar runtime.

### Compatibility

No source changes for sidecar consumers. The `proto/sidecar.proto` wire contract, Connect RPC surface, and on-disk `ENC:v1:` envelope format are all unchanged from 0.3.5. Existing 0.3.5 sidecar clients can be redeployed against the 0.3.6 image with no code changes.

Cross-cluster sync validated end-to-end on the k3d × 2 integration suite under the bumped stack ([peat-node#108](https://github.com/defenseunicorns/peat-node/pull/108) CI, 7m53s). 183 tests pass across 27 test binaries.

## [0.3.5] - 2026-05-28

Patch release. Picks up the **persistent multiplexed sync streams + peat-mesh#175 regression coverage** trail published across peat-mesh and peat-protocol over 2026-05-26 → 2026-05-28: [peat-mesh v0.9.0-rc.26](https://github.com/defenseunicorns/peat-mesh/releases/tag/v0.9.0-rc.26) (was rc.24, two rc-step advance through rc.25's ADR-063 closure and rc.26's in-CI behavioural UAT) and [peat-protocol / peat v0.9.0-rc.17](https://github.com/defenseunicorns/peat/releases/tag/v0.9.0-rc.17) (was rc.16). Also lands the peat-node-side auto-reconnect-after-blackout fix. Sidecar gRPC surface and on-the-wire formats are unchanged from 0.3.4.

### Changed

- **`peat-mesh = "=0.9.0-rc.26"`** (was `rc.24`). Spans:
  - rc.25 ([peat-mesh#176](https://github.com/defenseunicorns/peat-mesh/pull/176), [peat-mesh#178](https://github.com/defenseunicorns/peat-mesh/pull/178), [peat-mesh#180](https://github.com/defenseunicorns/peat-mesh/pull/180); ADR-063): persistent multiplexed sync streams. `AutomergeBackend` owns a strong `Arc<SyncChannelManager>`, closing the dropped-`Arc` symptom from [peat-mesh#175](https://github.com/defenseunicorns/peat-mesh/issues/175); per-peer writer-task + bounded mpsc replaces per-message stream-open + per-peer mutex. Persistent path is internal to peat-mesh — peat-node API surface unchanged.
  - rc.26 ([peat-mesh#184](https://github.com/defenseunicorns/peat-mesh/pull/184)): in-CI behavioural UAT for peat-mesh#175 delivery-ratio thresholds under constrained-bandwidth shaping (256 kbps, ~MTU bucket, 100 ms one-way delay). Pairs with rc.25's architectural pin from [peat-mesh#180](https://github.com/defenseunicorns/peat-mesh/pull/180) for code-shape + measurement coverage. Test-only addition; no peat-node-side adaptation.
- **`peat-protocol` resolves to `0.9.0-rc.17`** (floor advanced from `>=0.9.0-rc.16` to `>=0.9.0-rc.17`; range upper bound `<0.9.1` unchanged): rc.17 ([defenseunicorns/peat#936](https://github.com/defenseunicorns/peat/pull/936)) ships the ADR-063 docs and advances the workspace's peat-mesh floor to rc.25 so peat-protocol picks up the persistent-stream wire-up.
- **`iroh = "=1.0.0-rc.0"`** unchanged from 0.3.4. The exact-pin to peat-mesh's iroh version is preserved — iroh's process-global crypto provider + ALPN registry have undefined behavior under split-version linkage.

### Fixed

- **Auto-reconnect peers after blackout** ([peat-node#99](https://github.com/defenseunicorns/peat-node/pull/99), closes [peat-node#91](https://github.com/defenseunicorns/peat-node/issues/91)): a peer that disconnected during a transport blackout was not being re-attempted automatically — operator had to restart the sidecar to recover the link. Reconnect loop now correctly resumes once iroh reports the peer reachable again.

### CI

- **QA review workflow** ([peat-node#101](https://github.com/defenseunicorns/peat-node/pull/101), refs [defenseunicorns/peat#937](https://github.com/defenseunicorns/peat/issues/937)): retrieve prior review content via `gh` rather than a sandboxed temp file, fixing a sandbox-permission-denied class of QA-review failures.

### Impact on peat-node

**None at the surface level.** peat-node uses `peat_mesh::storage::*`, `peat_mesh::sync::AutomergeBackend`, and `peat_protocol::storage::*`. The peat-mesh rc.25 persistent-stream refactor is internal to the backend; rc.26 is test-only. The bumps are pure version advances.

### Compatibility

No source changes for sidecar consumers. The `proto/sidecar.proto` wire contract, Connect RPC surface, and on-disk `ENC:v1:` envelope format are all unchanged from 0.3.4. Existing 0.3.4 sidecar clients can be redeployed against the 0.3.5 image with no code changes.

Cross-cluster sync validated end-to-end on the k3d × 2 integration suite under the bumped stack ([peat-node#104](https://github.com/defenseunicorns/peat-node/pull/104) CI, 8m16s). 161 tests pass across 22 test binaries.

## [0.3.4] - 2026-05-26

Patch release. Picks up the **ADR-062 Phase 2 follow-up trail** published across peat-mesh and peat-protocol over 2026-05-25 / 2026-05-26: [peat-mesh v0.9.0-rc.24](https://github.com/defenseunicorns/peat-mesh/releases/tag/v0.9.0-rc.24) (was rc.20) and [peat-protocol / peat v0.9.0-rc.16](https://github.com/defenseunicorns/peat/releases/tag/v0.9.0-rc.16) (was rc.14). Sidecar gRPC surface and on-the-wire formats are unchanged from 0.3.3.

### Changed

- **`peat-mesh = "=0.9.0-rc.24"`** (was `rc.20`). Spans the ADR-062 Phase 2 trail:
  - rc.21 ([peat-mesh#162](https://github.com/defenseunicorns/peat-mesh/pull/162)): `IrohMeshTransport` relocation from peat-protocol into `peat_mesh::transport::iroh_mesh`; `peat_mesh::network::EndpointId` re-export.
  - rc.22 ([peat-mesh#166](https://github.com/defenseunicorns/peat-mesh/pull/166)): `Connection` + `DiscoveryEvent` re-exports the rc.21 surface accidentally missed.
  - rc.23 ([peat-mesh#171](https://github.com/defenseunicorns/peat-mesh/pull/171)): `parse_close_reason` structured-variant refactor — exhaustive match on `iroh::endpoint::ConnectionError`, stable per-variant payload tags on `DisconnectReason::{NetworkError, ApplicationError}`. No behavior change for happy-path close handling; subtle wording-tweak resilience for log-parsing consumers.
  - rc.24 ([peat-mesh#173](https://github.com/defenseunicorns/peat-mesh/pull/173)): narrow `peat_mesh::network::QuicMeshConnection` trait + removed `pub use Connection`. The trait names exactly the four methods peat-protocol's formation-handshake uses; a future iroh `Connection` method addition no longer widens peat-protocol's reachable surface by default. Closes the transport-agnosticism-at-API-shape gap from the original ADR-062 work.
- **`peat-protocol` resolves to `0.9.0-rc.16`** (range floor advanced from `>=0.9.0-rc.14` to `>=0.9.0-rc.16`; range upper bound `<0.9.1` unchanged):
  - rc.15 ([defenseunicorns/peat#930](https://github.com/defenseunicorns/peat/pull/930)): ADR-062 Phase 2 consumer-side — `peat-protocol/src/transport/iroh.rs` deleted (918 lines), `iroh`/`iroh-blobs`/`iroh-mdns-address-lookup` dropped from peat-protocol's direct deps, 13 import sites rewired to `peat_mesh::network::*` re-exports.
  - rc.16 ([defenseunicorns/peat#933](https://github.com/defenseunicorns/peat/pull/933)): `formation_handshake.rs` signatures take `&dyn QuicMeshConnection` instead of `&Connection`; `network.rs` re-export list drops `Connection`, adds `QuicMeshConnection`.
- **`iroh = "=1.0.0-rc.0"`** unchanged from 0.3.3. The exact-pin to peat-mesh's iroh version is preserved — iroh's process-global crypto provider + ALPN registry have undefined behavior under split-version linkage.

### Impact on peat-node

**None at the surface level.** peat-node uses `peat_mesh::storage::*`, `peat_mesh::sync::AutomergeBackend`, and `peat_protocol::storage::*`. None of the narrow-trait, IrohMeshTransport-relocation, or `Connection`-re-export-removal work touches that surface. The bump is a pure version advance to track the underlying ecosystem cleanup.

### Compatibility

No source changes for sidecar consumers. The `proto/sidecar.proto` wire contract, Connect RPC surface, and on-disk `ENC:v1:` envelope format are all unchanged. Existing 0.3.3 sidecar clients can be redeployed against the 0.3.4 image with no code changes.

Cross-cluster sync validated end-to-end on the k3d × 2 integration suite under the new peat-mesh rc.24 / peat-protocol rc.16 stack ([peat-node#97](https://github.com/defenseunicorns/peat-node/pull/97) CI, 7m57s). 79 attachment-feature tests (53 unit + 26 integration across 6 test files) pass against the bumped stack with zero failures.

## [0.3.3] - 2026-05-24

Patch release. Picks up [peat-mesh v0.9.0-rc.20](https://github.com/defenseunicorns/peat-mesh/releases/tag/v0.9.0-rc.20) and [peat-protocol / peat v0.9.0-rc.14](https://github.com/defenseunicorns/peat/releases/tag/v0.9.0-rc.14). Sidecar gRPC surface and on-the-wire formats are unchanged from 0.3.2.

### Changed

- **`peat-mesh = "=0.9.0-rc.20"`** (was `rc.12`). Brings in the iroh 0.97 → 1.0.0-rc.0 cascade, the extracted `iroh-mdns-address-lookup` crate, and the `Endpoint::builder(presets::Empty).crypto_provider(...)` API. `aws-lc-rs` is now the active rustls crypto provider for every iroh QUIC handshake; the previous `ring`-backed path is no longer reachable through any peat-mesh endpoint constructor. peat-node inherits the FIPS-aligned provider through `AutomergeBackend::with_iroh` without local change. Residual `ring` symbol surface via `noq-proto` / `rcgen` / `rustls-webpki` is tracked for upstream removal under [defenseunicorns/peat#923](https://github.com/defenseunicorns/peat/issues/923#issuecomment-4528407237).
- **`peat-protocol` resolves to `0.9.0-rc.14`** (range floor advanced from `>=0.9.0-rc.10` to `>=0.9.0-rc.14`; range upper bound `<0.9.1` unchanged). rc.14 is the consumer-side cut of the FIPS-active provider + Android JNI hardening chain (peat#924 + peat#925).
- **`iroh = "=1.0.0-rc.0"`** (was `"0.97"`). The direct peat-node dep must match peat-mesh's exact iroh pin — iroh's process-global crypto provider + ALPN registry have undefined behavior under split-version linkage. Exact-pin enforces the constraint at resolve time rather than at runtime.

### Compatibility

No source changes for sidecar consumers. The `proto/sidecar.proto` wire contract, Connect RPC surface, and on-disk `ENC:v1:` envelope format are all unchanged. Existing 0.3.2 sidecar clients can be redeployed against the 0.3.3 image with no code changes.

Cross-cluster sync validated end-to-end on the k3d × 2 integration suite under the new iroh 1.0.0-rc.0 + aws-lc-rs stack (peat-node#92 CI). Field-tier scenario validation done in the peat workspace's QUICKSTART regression — all four scenarios pass on real LAN (laptop + 2 Raspberry Pi 5s, 192.168.228.0/24).

## [0.3.2] - 2026-05-19

Patch release. Picks up [peat-mesh v0.9.0-rc.12](https://github.com/defenseunicorns/peat-mesh/releases/tag/v0.9.0-rc.12) (FIPS-posture primitive swap) and [peat-protocol v0.9.0-rc.11](https://github.com/defenseunicorns/peat/releases/tag/v0.9.0-rc.11) (matching FIPS adaptation). Sidecar gRPC surface and on-the-wire formats are unchanged from 0.3.1.

### Changed

- **`peat-mesh = "=0.9.0-rc.12"`** (was `rc.11`). Brings in the swap from ChaCha20-Poly1305 + X25519 to AES-256-GCM + ECDH-P256 (FIPS 140-3 approved equivalents, per peat ADR-060 §5). peat-node doesn't construct `EncryptionKeypair` or call `establish_channel` directly, so the AEAD/DH swap is transparent at the sidecar boundary.
- **`peat-protocol` resolves to `0.9.0-rc.11`** (was `rc.10`). The peat-protocol release adapts its security re-exports to peat-mesh's new constant names (`X25519_PUBLIC_KEY_SIZE` → `ECDH_PUBLIC_KEY_SIZE`).
- **`AutomergeBackendConfig` construction in `src/node.rs`** explicitly passes `cipher: None` for the new optional at-rest cipher hook peat-mesh rc.12 introduced. peat-node's existing higher-level `StoreCipher` (AES-256-GCM via `aes-gcm`, used in `forward_store_changes`) keeps its current encrypt-before-store role; plumbing it into the lower-level peat-mesh hook is a separate follow-up the rc.12 changelog called out.

### Compatibility

No source changes for sidecar consumers. The `proto/sidecar.proto` wire contract, Connect RPC surface, and on-disk `ENC:v1:` envelope format are all unchanged. Existing 0.3.1 sidecar clients can be redeployed against the 0.3.2 image with no code changes.

## [0.3.1] - 2026-05-17

Closes [#68](https://github.com/defenseunicorns/peat-node/issues/68): the
receive-side distribution lifecycle moves upstream into `peat-protocol`,
where it belongs. peat-node is now a pure consumer of the distribution
surface rather than the layer that closed a `peat-protocol` gap (the
`[ARCH]` follow-up from the PRD-006 v1.1 QA review on #65).

Patch bump (not minor): no change to peat-node's gRPC surface or
observable behavior. This is an internal layering refactor plus
dependency-floor bumps; the full attachment acceptance suite passes
unchanged against the published dependencies.

### Changed

- **Receive lifecycle relocated to `peat-protocol` 0.9.0-rc.10 (#68).**
  `src/attachments/inbox.rs` shrank from 713 to ~290 lines: the polling
  watcher, distribution-doc scan, targeting check, per-receiver
  `Transferring`/`Completed` status writes, and the deterministic test
  fault seam are all gone — they now live in
  `peat_protocol::storage::IrohFileDistribution::start_receive_watcher`.
  What remains is `FilesystemInboxSink`, a thin
  `peat_protocol::storage::ReceiveSink` implementation (durable
  `already_delivered` gate + atomic tmp+rename `deliver`). The relocated
  `#[doc(hidden)]` test seam is re-exported from `attachments::inbox` so
  existing test imports keep resolving.
- **`src/node.rs`** builds the sink and calls `start_receive_watcher`;
  the distribution substrate is now constructed when **either** an
  attachment root **or** an inbox path is configured (a receive-only
  node still needs the instance).

### Dependencies

- `peat-protocol` pin raised to `>=0.9.0-rc.10, <0.9.1` (the receive
  API only exists in rc.10); the cross-repo `[patch.crates-io]` dev
  override used during development was removed.
- `peat-mesh` pinned `=0.9.0-rc.10` → `=0.9.0-rc.11`. The published
  `peat-protocol` 0.9.0-rc.10 requires `peat-mesh >=0.9.0-rc.10, <0.9.1`,
  so rc.11 satisfies it transitively with no `peat-protocol` re-release.

## [0.3.0] - 2026-05-17

Closes [defenseunicorns/peat#864](https://github.com/defenseunicorns/peat/issues/864)
end-to-end: the attachment receive path now records per-receiver
transfer status through `peat-protocol` 0.9.0-rc.9's typed
`node_statuses` Automerge-Map API, so a sender's
`SubscribeAttachmentBundle` stream reliably observes
`IN_PROGRESS → terminal` for real cross-peer transfers.

Minor bump (not patch) because this pulls in a **BREAKING wire-format
change** on the synced `file_distributions` collection — see Migration.

### Changed

- **Attachment inbox watcher consumes the `peat-protocol` 0.9.0-rc.9
  typed distribution-doc API (#78).** `attachments::inbox` now reads
  via `scan_distribution_documents` and writes per-receiver status via
  `write_receiver_node_status` (typed `ROOT.node_statuses` Automerge
  Map, per-key, lock-guarded) instead of the pre-rc.9 inline
  wholesale-scalar read-modify-write of `ROOT.data`. Dependency floor:
  `peat-protocol >=0.9.0-rc.9` (peat-mesh stays `=0.9.0-rc.10`).
- **Receiver writes its own `NodeTransferStatus` into the distribution
  document (#76)** on `Transferring` (pre-fetch) and `Completed`
  (post atomic-rename), which is what makes the sender-side progress
  watcher observable.

### Added

- PRD-006 test 23 (`subscribe_emits_progress_then_terminal`) and the
  receiver-local doc-state regression
  (`receiver_writes_node_status_into_distribution_doc`) un-ignored and
  passing against published `peat-protocol 0.9.0-rc.9` (#78).
- PRD-006 test 22 — `NodeList` scope only delivers to listed nodes
  (#73); `receiver_can_fetch_blob_pushed_by_sender` un-ignored (#72).
- `#[serial_test::serial(iroh_two_node)]` on the four iroh two-node
  integration tests so cargo's default per-binary parallelism can't
  CPU-starve their multi-second budgets (#78).

### Migration (BREAKING — mesh interop)

`peat-node 0.3.0` runs on `peat-protocol 0.9.0-rc.9`, whose
`file_distributions` Automerge collection uses a typed schema
(`ROOT.metadata` byte-scalar + typed `ROOT.node_statuses` Map) instead
of the pre-rc.9 single wholesale-scalar `ROOT.data`. A 0.3.0 node
**dual-reads** a legacy (≤0.2.0) document, but a ≤0.2.0 node **cannot**
read a 0.3.0-written document. In a mixed mesh that exercises the
attachment subsystem (`--attachment-root` set), upgrade all peat-node
instances together; do not run 0.3.0 and ≤0.2.0 side-by-side on a
formation that uses attachments. Operators not using attachments are
unaffected (the subsystem is opt-in and disabled by default).

### Packaging

- Helm chart `version`/`appVersion` and `values.yaml` image tag bumped
  to `0.3.0` / `v0.3.0`.
- `zarf.yaml` corrected: it was stale at `0.1.0` (two minors behind)
  and its image ref lacked the `v` prefix the release workflow
  actually pushes (`:vX.Y.Z`). Now `0.3.0` / `:v0.3.0` across
  metadata, image, and chart version.

## [0.2.0] - 2026-05-14

### Added

- **PRD-006 path-based attachment distribution API (#64).** Four new
  RPCs on `peat.sidecar.v1.PeatSidecar`: `SendAttachments`,
  `GetAttachmentDistribution`, `SubscribeAttachmentBundle`
  (server-streaming), `CancelAttachmentDistribution`. Disabled by
  default — operators opt in by setting `--attachment-root name=path`
  (one or more). With no root configured, all four RPCs return
  `Unimplemented`. Full validation pipeline (PRD §Validation Rules
  1-12 minus the rule-11 queue path and rule-25 discovery-grace
  promoter, both deferred), atomic-on-failure ingest with
  content-address-safe rollback, single-pass `O_NOFOLLOW` open +
  tee-style sha256 hashing, bundle handle table with retention + LRU,
  per-bundle progress fan-out, late-subscribe contract (snapshot
  already-terminal + live for in-flight). See
  [docs/CONFIGURATION.md#attachment-distribution-prd-006](docs/CONFIGURATION.md#attachment-distribution-prd-006).
- **Receive-side attachment delivery (#65).** New
  `--attachment-inbox <path>` config spawns a background watcher that
  polls the synced `file_distributions` Automerge collection and
  fetches blobs targeting this node into
  `{inbox}/{distribution_id}/{filename}`. Closes the silent gap left
  by #64 (sender-side ingest worked, but no automated path actually
  delivered files to peers). The polling watcher in peat-node is a
  documented stopgap — the long-term home is peat-protocol's
  receive-side observer hooks (`file_distribution.rs:617-621` TODO).
- 11 `--attachment-*` CLI flags / `PEAT_NODE_ATTACHMENT_*` env vars
  surfaced through `chart/peat-node/values.yaml` (with
  `extraVolumes` / `extraVolumeMounts` for operator-supplied volume
  sources).
- New `peat-protocol` direct dependency for `FileDistribution` /
  `IrohFileDistribution` / `DistributionHandle` / `TransferPriority`.
  `peat-mesh` does not re-export these. SKILL.md acknowledges the
  two-dep arrangement as permitted.
- Two-node compose quickstart at
  `examples/compose/attachments/docker-compose.two-node.yml` +
  `peer.sh` + per-size `send.sh` benchmark (1 / 10 / 100 MiB from
  `/dev/urandom`). Demonstrates actual cross-peer file delivery
  end-to-end.
- `tests/attachments_e2e_test.rs` — boots two `SidecarNode`s, peers
  them bidirectionally, sends from A, asserts byte-for-byte +
  sha256-match arrival on B's filesystem inbox. Runs in default
  `cargo test`; not `#[ignore]`'d. The acceptance gate that #64
  should have shipped with.

### Fixed

- **`SidecarNode::connect_peer` now calls
  `blob_store.add_peer(peer_id)`** after `start_sync_connection`. The
  iroh transport's connection list and the blob store's peer index are
  tracked separately upstream; before this fix the blob-store list
  stayed empty, so `IrohFileDistribution::resolve_targets(AllNodesScope)`
  always returned `target_nodes=[]`. Net: every multi-peer attachment
  scenario silently failed, which is also why the original deferred
  multi-peer test hit "no peers configured for remote fetch."
- Retention-eviction background task now runs. The
  `--attachment-handle-retention-secs` knob was operator-visible but
  inert in 0.1.x — `evict_expired()` was unit-tested but nothing in the
  running service called it. Terminal bundles lingered until LRU
  pressure removed them.
- `bytes_total` in `GetAttachmentDistributionResponse` falls back to
  the bundle identity's `size_bytes` when no per-peer state has been
  reported yet. Was returning the hex hash length (~64) for every
  pre-fetch query.
- `BundleRegistry::check_resubmit` uses two-phase locking — read lock
  for absent / conflicting branches, write lock only for the mutating
  branches (terminal-reuse drop, idempotent `last_touched_at` bump).
  Matches the module's documented concurrency contract.
- `handlers::in_flight_count` uses `BundleRegistry::non_terminal_count()`
  instead of `len()`. Terminal bundles within the retention window no
  longer count against `--attachment-max-concurrent-distributions`.
- Receive-side watcher's `already_delivered()` filesystem check
  short-circuits `fetch_blob` when `{inbox}/{distribution_id}/`
  already contains a matching-size file. Restart cost is now ~zero
  instead of re-fetching + re-writing every historical delivery.
- `send.sh` quickstart driver uses `openssl dgst -sha256 -binary`
  instead of `sha256sum | xxd`. xxd isn't installed by default on
  minimal Linux images.
- Compose quickstart at `examples/compose/attachments/docker-compose.yml`
  defaults to `build:` from the repo root instead of pinning a
  registry tag. The pre-0.2.0 image didn't have the attachment
  surface and operators following the pinned tag hit
  `unimplemented: method not found`.

### Changed

- `SidecarConfig` gained `attachment_config: AttachmentConfig`. Existing
  callers should pass `AttachmentConfig::default()` when not using the
  attachment surface (defaults are operator-safe — empty roots disable
  the RPCs).
- README API table: 21 → 25 RPCs with the new Attachments row.

### Notes

- **v1.1-honesty caveats** (intentional, documented in proto
  doc-comments and `docs/CONFIGURATION.md`):
  - `AttachmentPriority` is recorded on the distribution document but
    v1 does NOT enforce wire-level preemption between priority classes
    — that needs PRD-004 bandwidth allocation.
  - `DISTRIBUTION_STATUS_PARTIAL` is reserved for v2 (needs receive-
    side observer hooks); v1 senders emit `COMPLETED` on full
    sender-side success and `FAILED` on explicit transfer failure.
  - `DISTRIBUTION_STATUS_COMPLETED` reported by
    `GetAttachmentDistribution` never advances naturally against a real
    peer mesh — the sender's `is_complete()` check needs receiver-side
    state propagation. Files DO arrive within ~1s of `SendAttachments`
    returning; sender-side status just doesn't know.
  - `FormationScope` rejects `FailedPrecondition` (no async formation
    resolution in v1). `CapableScope` rejects `FailedPrecondition`
    (capability vocabulary deferred to a follow-on ADR).
  - The bundle handle table is in-memory only. A peat-node restart
    drops every `bundle_id`; subscribers re-attaching to pre-restart
    IDs receive `NotFound`. iroh content-addressed blobs and synced
    distribution documents are unaffected.

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
