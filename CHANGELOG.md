# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
