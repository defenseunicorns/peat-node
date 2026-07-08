<!-- refreshed: 2026-07-08 -->
# Architecture

**Analysis Date:** 2026-07-08

## System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                  Co-located Application                      │
│            (talks Connect/gRPC/gRPC-Web)                     │
└────────────────────────┬────────────────────────────────────┘
                         │ Connect RPC (TCP or Unix socket)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                   PeatSidecarService                         │
│               `src/service.rs`                               │
│  Implements proto trait; dispatches to SidecarNode + handlers│
├──────────┬──────────────┬──────────────┬────────────────────┤
│ Document │   Peer Mgmt  │  Attachments │  Subscriptions     │
│  CRUD    │              │  (PRD-006)   │  + Query           │
│          │              │ `src/attach  │ `src/query.rs`     │
│          │              │  ments/`     │                    │
└────┬─────┴──────┬───────┴──────┬───────┴────────────────────┘
     │            │              │
     ▼            ▼              ▼
┌─────────────────────────────────────────────────────────────┐
│                      SidecarNode                             │
│                  `src/node.rs`                                │
│  Lifecycle wrapper: encryption, fanout, peer registry,       │
│  reconnect watchdog, change broadcast, collection configs    │
└──────────────────────────┬──────────────────────────────────┘
                           │
     ┌─────────────────────┼─────────────────────┐
     ▼                     ▼                     ▼
┌──────────────┐ ┌──────────────────┐ ┌──────────────────────┐
│AutomergeBack │ │IrohFileDistrib.  │ │ Discovery            │
│end (peat-    │ │(peat-protocol)   │ │ mDNS / K8s           │
│mesh)         │ │blob send/receive │ │ EndpointSlice        │
│ Store+Sync+  │ │                  │ │ (peat-mesh)          │
│ Transport    │ │                  │ │                      │
└──────┬───────┘ └────────┬─────────┘ └──────────────────────┘
       │                  │
       ▼                  ▼
┌─────────────────────────────────────────────────────────────┐
│  iroh (QUIC P2P)  — Endpoint, Blobs, Connections             │
└─────────────────────────────────────────────────────────────┘
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| `main` | CLI parsing (clap), server bootstrap, listener loop | `src/main.rs` |
| `PeatSidecarService` | Connect RPC trait impl, request dispatch | `src/service.rs` |
| `SidecarNode` | Mesh lifecycle: backend init, peer mgmt, doc CRUD, encryption, change broadcast | `src/node.rs` |
| `StoreCipher` | AES-256-GCM encryption at rest for document content | `src/crypto.rs` |
| `PriorityFanout` | QoS-priority relay fanout queue for change propagation | `src/fanout.rs` |
| `Matcher` | Subscription query evaluation against JSON payloads | `src/query.rs` |
| `AgentWatcher` | Polls co-located UDS Remote Agent, syncs state to mesh | `src/watcher.rs` |
| `identity` | Deterministic iroh keypair derivation (HKDF-SHA256) | `src/identity.rs` |
| `attachments` | PRD-006 file distribution: validate, ingest, registry, inbox/outbox | `src/attachments/` |
| `peat-cli` | CLI client for interacting with a running peat-node | `crates/peat-cli/` |

## Pattern Overview

**Overall:** Sidecar pattern -- single Rust binary runs alongside an application, exposing a P2P CRDT mesh over Connect/gRPC/gRPC-Web on a single port.

**Key Characteristics:**
- Single-binary deployment (no separate gateway/proxy)
- Connect RPC serves Connect + gRPC + gRPC-Web on one port via `connectrpc` crate
- All mesh operations delegated to `peat-mesh` (`AutomergeBackend`) -- peat-node adds encryption, fanout, peer lifecycle
- Supports TCP and Unix domain socket listeners
- Background tasks (watchdog, fanout worker, inbox/outbox watchers) spawned as independent tokio tasks

## Layers

**Wire Layer (proto/service):**
- Purpose: Define and serve the RPC contract
- Location: `proto/sidecar.proto`, `src/service.rs`
- Contains: Proto definitions compiled by `connectrpc_build` in `build.rs`; service trait implementation
- Depends on: `SidecarNode`, `query::Matcher`, `attachments::handlers`
- Used by: Co-located applications over the network

**Node Layer (core logic):**
- Purpose: Mesh lifecycle management, document operations, peer connectivity
- Location: `src/node.rs`
- Contains: `SidecarNode` struct with all mesh state, peer registry, change broadcast, collection configs
- Depends on: `peat-mesh` (`AutomergeBackend`), `peat-protocol` (`IrohFileDistribution`), `crypto`, `fanout`, `identity`, `attachments`
- Used by: `service.rs`

**Cross-Cutting Modules:**
- Purpose: Encryption, identity derivation, query matching, agent watching, fanout
- Location: `src/crypto.rs`, `src/identity.rs`, `src/query.rs`, `src/watcher.rs`, `src/fanout.rs`
- Contains: Standalone utilities consumed by the node layer
- Depends on: Standard crypto crates, `peat-mesh` types
- Used by: `node.rs`, `service.rs`

**Attachment Subsystem:**
- Purpose: File distribution over the mesh (PRD-006)
- Location: `src/attachments/`
- Contains: Config, validation, ingest, registry, inbox/outbox watchers, runtime state, RPC handlers
- Depends on: `peat-protocol::IrohFileDistribution`, `peat-mesh` blob store
- Used by: `service.rs` (RPC handlers), `node.rs` (lifecycle)

**External Dependencies (upstream crates):**
- `peat-mesh`: Automerge CRDT store, sync coordinator, transport, blob store, discovery, QoS
- `peat-protocol`: Formation handshake, `IrohFileDistribution`
- `iroh`: QUIC P2P networking, blob transfer, endpoint identity

## Data Flow

### Primary Request Path (Document Write)

1. Application sends `PutDocument` RPC (`src/service.rs:PeatSidecar::put_document`)
2. Service calls `SidecarNode::put_document` (`src/node.rs`)
3. If cipher configured: encrypt JSON payload with AES-256-GCM (`src/crypto.rs`)
4. Write to `AutomergeStore` via `json_to_automerge` + `store.put`
5. Store fires observer notification on its broadcast channel
6. `forward_store_changes` task re-emits as `ChangeEvent` for subscribers (`src/node.rs`)
7. `sync_on_change` task enqueues onto `PriorityFanout` queue (`src/fanout.rs`)
8. Fanout worker drains highest-QoS-first, pushes doc to connected peers via `AutomergeSyncCoordinator`

### Peer Connection Flow

1. `ConnectPeer` RPC or discovery event (mDNS/K8s)
2. `SidecarNode::connect_peer` resolves addresses via DNS (`src/node.rs`)
3. `dial_and_attach`: authenticate via formation key handshake (retry up to 3x)
4. Register connection for CRDT sync via `MeshSyncTransport::start_sync_connection`
5. Register peer in blob store for attachment targeting
6. Record in `registered_peers` map for auto-reconnect watchdog

### Attachment Distribution (PRD-006)

1. `SendAttachments` RPC: validate request (`src/attachments/validate.rs`)
2. Ingest files: hash + stream into iroh blob store (`src/attachments/ingest.rs`)
3. Register bundle in handle table (`src/attachments/registry.rs`)
4. Create distribution doc via `IrohFileDistribution` (peat-protocol)
5. Distribution doc syncs to peers via Automerge CRDT
6. Receiver's inbox watcher polls `file_distributions` collection, fetches blob, writes to disk (`src/attachments/inbox.rs`)

**State Management:**
- Document state: Automerge CRDT store (persistent, file-backed in `data_dir`)
- Peer state: In-memory `registered_peers` HashMap protected by `RwLock`
- Collection configs: In-memory HashMap + JSON file persistence (`data_dir/collection_configs.json`)
- Bundle handles: In-memory registry with TTL eviction + LRU cap
- Change events: tokio broadcast channels (256-slot buffer)

## Key Abstractions

**AutomergeBackend (peat-mesh):**
- Purpose: Unified bootstrap of Automerge store + iroh endpoint + sync coordinator + transport + blob store
- Examples: `src/node.rs` -- `AutomergeBackend::with_iroh(backend_cfg)`
- Pattern: Builder config struct (`AutomergeBackendConfig`) -> `Arc<AutomergeBackend>` with accessor methods for sub-components

**SidecarNode:**
- Purpose: Application-level mesh node wrapping the backend with encryption, fanout, peer lifecycle
- Examples: `src/node.rs` -- constructed in `main.rs`, shared via `Arc<SidecarNode>`
- Pattern: Long-lived `Arc`-shared struct; spawns background tasks holding `Weak` refs for clean shutdown

**PriorityFanout:**
- Purpose: QoS-aware change propagation queue
- Examples: `src/fanout.rs` -- enqueue changes by doc key, worker drains highest-QoS-first
- Pattern: Lock-free enqueue + single-worker drain with `Notify`-based wake

## Entry Points

**Binary (`peat-node`):**
- Location: `src/main.rs`
- Triggers: Direct execution, container entrypoint, Kubernetes sidecar
- Responsibilities: Parse CLI args (clap), bootstrap `SidecarNode`, start Connect RPC server, spawn background tasks

**Subcommand (`derive-id`):**
- Location: `src/main.rs` (Command::DeriveId branch)
- Triggers: `peat-node derive-id --shared-key ... --node-id ...`
- Responsibilities: Offline computation of deterministic iroh EndpointId, prints to stdout

**CLI (`peat-cli`):**
- Location: `crates/peat-cli/src/main.rs`
- Triggers: `peat create|update|delete|query|observe|attach|schema` commands
- Responsibilities: Client-side interaction with a running peat-node over Connect RPC

**Proto compilation:**
- Location: `build.rs`
- Triggers: `cargo build` (rerun on `proto/sidecar.proto` change)
- Responsibilities: Compile proto to Rust via `connectrpc_build`, extract dep versions from `Cargo.lock`

## Architectural Constraints

- **Threading:** Tokio multi-thread async runtime. Background tasks (reconnect watchdog, fanout worker, inbox/outbox watchers, store change forwarder, peer status logger) are spawned as independent `tokio::spawn` tasks.
- **Global state:** `SidecarNode` is the single shared state object, wrapped in `Arc`. Internal fields use `RwLock` (std, not tokio) for synchronous access to peer registry and collection configs. `AtomicBool` for sync-active flag.
- **Iroh version lock:** peat-mesh and peat-node MUST share the same iroh version. Mixed versions cause undefined behavior in iroh's process-global crypto provider + ALPN registry.
- **Deterministic identity:** When `shared_key` + `node_id` are both set, the iroh keypair is derived deterministically via HKDF-SHA256. This derivation MUST stay byte-for-byte identical across peat-mesh and peat-node.
- **Attachment safety default:** No `--attachment-root` = all four attachment RPCs return `Unimplemented`. Operators must explicitly opt in.

## Anti-Patterns

### Echoing sync-received changes back to source

**What happens:** Subscribing to the local-only change channel misses sync-received writes, breaking transitive gossip.
**Why it's wrong:** An observer on node B never sees changes from node A that arrived via sync.
**Do this instead:** Subscribe to `subscribe_to_changes_with_origin` and use `FanoutKind::ExcludeSource` to suppress echo back to the originating peer. See `src/node.rs` `sync_on_change`.

### Blocking fanout in the change listener

**What happens:** Awaiting each document's full fanout inline head-of-line-blocks latency-sensitive docs behind lower-priority ones.
**Why it's wrong:** A priority-FLASH document waits behind a backlog of ROUTINE changes.
**Do this instead:** Enqueue onto `PriorityFanout` non-blockingly; single worker drains highest-QoS-first. See `src/fanout.rs`.

## Error Handling

**Strategy:** `anyhow::Result` for fallible operations; `ConnectError` at the RPC boundary via a `fn internal(e) -> ConnectError` helper that logs and wraps.

**Patterns:**
- Background tasks log errors via `tracing::warn`/`tracing::error` and continue (no panics)
- Poisoned `RwLock`s are recovered via `unwrap_or_else(|e| e.into_inner())` -- never panic on lock poison
- Peer connection failures are retried (3 attempts with 200ms backoff) before returning error
- Auto-reconnect watchdog uses exponential backoff (5s min, 120s max) per peer

## Cross-Cutting Concerns

**Logging:** `tracing` + `tracing-subscriber` with env-filter. Default filter: `peat_node=info,peat_mesh=info,peat_protocol=info,iroh=warn`. Override via `RUST_LOG`.

**Validation:** Attachment requests validated against 12 rules in `src/attachments/validate.rs`. Proto requests validated at the service layer in `src/service.rs`.

**Authentication:** Formation key handshake via `peat-protocol` (`connect_and_authenticate`). Shared key is base64-encoded 32-byte secret distributed out-of-band.

**Encryption:** Optional AES-256-GCM encryption at rest for document content (`src/crypto.rs`). CRDT metadata stays unencrypted so sync works. Format: `ENC:v1:<base64(nonce ++ ciphertext ++ tag)>`.

**Discovery:** mDNS (default on, `--disable-mdns` to turn off) and Kubernetes EndpointSlice watching (`--enable-kubernetes-discovery`). Both feed into the same `connect_peer` / `dial_and_attach` path.

---

*Architecture analysis: 2026-07-08*
