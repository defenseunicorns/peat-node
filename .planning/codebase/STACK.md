# Technology Stack

**Analysis Date:** 2026-07-08

## Languages

**Primary:**
- Rust (edition 2021) - All application code (`src/`, `crates/peat-cli/src/`)

**Secondary:**
- Protocol Buffers (proto3) - Wire contract (`proto/sidecar.proto`)

## Runtime

**Environment:**
- Rust 1.93 (per Dockerfile `rust:1.93-bookworm`)
- Tokio async runtime (full features, multi-threaded)

**Package Manager:**
- Cargo (workspace with `crates/*` members)
- Lockfile: `Cargo.lock` present

## Frameworks

**Core:**
- `connectrpc` 0.2 (server feature) - Connect RPC / gRPC / gRPC-Web on a single port
- `axum` 0.8 - HTTP framework underlying the RPC server
- `hyper` 1.x - HTTP/1.1 + HTTP/2 server
- `tower` 0.5 - Middleware / service layer

**Testing:**
- `tokio` test-util - Async test support
- `tempfile` 3.10 - Temporary directories in tests
- `serial_test` 3 - Serial execution for iroh two-node integration tests
- `rcgen` 0.13 - TLS certificate generation for tests

**Build/Dev:**
- `connectrpc-build` 0.2 - Proto compilation in `build.rs`
- `cargo-chef` - Docker layer caching for dependencies
- `mold` - Fast linker (in Docker build)

## Key Dependencies

**Critical (mesh networking):**
- `peat-mesh` =0.9.0-rc.46 (features: automerge-backend, kubernetes) - CRDT sync + Iroh blob transfer
- `peat-protocol` >=0.9.0-rc.29, <0.9.1 (features: automerge-backend) - FileDistribution trait + IrohFileDistribution
- `iroh` 1.0.2 - P2P networking (MUST match peat-mesh's iroh version exactly)

**Serialization:**
- `serde` 1 (derive) - Serialization framework
- `serde_json` 1 - JSON encoding/decoding

**Cryptography:**
- `aes-gcm` 0.10 - Encryption at rest
- `sha2` 0.10 - SHA-256 hashing for attachment ingest
- `hkdf` 0.12 - HKDF-SHA256 deterministic iroh keypair derivation
- `rand` 0.8 - Random number generation
- `base64` 0.22 - Base64 encoding

**Infrastructure:**
- `k8s-openapi` 0.24 (v1_32) - Kubernetes API (required by peat-mesh kubernetes feature)
- `reqwest` 0.12 (rustls-tls, http2, json) - HTTP client for agent watcher
- `clap` 4 (derive, env) - CLI argument parsing with env var support

**Utilities:**
- `uuid` 1 (v4, serde) - UUID generation
- `chrono` 0.4 (serde) - Date/time handling
- `thiserror` 1 / `anyhow` 1 - Error handling
- `tracing` 0.1 / `tracing-subscriber` 0.3 (env-filter) - Structured logging
- `libc` 0.2 - O_NOFOLLOW for attachment-ingest TOCTOU mitigation

## Workspace Crates

| Crate | Binary | Purpose |
|-------|--------|---------|
| `peat-node` (root) | `peat-node` | Mesh sidecar node (`src/main.rs`) |
| `peat-cli` | `peat` | Operator CLI (`crates/peat-cli/src/main.rs`) |

## Configuration

**Environment (all via `PEAT_NODE_*` env vars or CLI flags):**
- `PEAT_NODE_LISTEN` - Listen address (default: `tcp://0.0.0.0:50051`)
- `PEAT_NODE_DATA_DIR` - Data directory (default: `/data/peat-node`)
- `PEAT_NODE_NODE_ID` - Node identifier
- `PEAT_NODE_APP_ID` - Application ID (default: `peat-default`)
- `PEAT_NODE_SHARED_KEY` - Shared authentication key
- `PEAT_NODE_ENCRYPTION_KEY` - Encryption at rest key
- `PEAT_NODE_PEERS` - Comma-separated peer list
- `PEAT_NODE_DISABLE_MDNS` - Disable mDNS discovery (default: false)
- `PEAT_NODE_AUTO_SYNC` - Auto-sync on connect (default: true)
- `PEAT_NODE_IROH_UDP_PORT` - Iroh UDP port
- `PEAT_NODE_BLOB_STALL_TIMEOUT_SECS` - Blob transfer stall timeout
- `PEAT_NODE_TOMBSTONE_TTL_HOURS` - Tombstone TTL
- `PEAT_NODE_GC_INTERVAL_SECS` - GC interval
- `PEAT_NODE_GC_BATCH_SIZE` - GC batch size
- `PEAT_NODE_AGENT_ADDR` - Co-located agent address (watcher)
- `PEAT_NODE_AGENT_POLL_INTERVAL` - Agent poll interval (default: 10s)
- `PEAT_NODE_AGENT_TLS_CERT` / `_KEY` / `_CA` - Agent mTLS config
- `PEAT_NODE_ATTACHMENT_ROOT` - Attachment storage root
- `PEAT_NODE_ATTACHMENT_MAX_FILE_BYTES` / `_MAX_BUNDLE_BYTES` / `_MAX_FILES_PER_BUNDLE` - Attachment limits
- `PEAT_NODE_ATTACHMENT_MAX_NODE_LIST_LEN` - Max nodes per distribution
- `PEAT_NODE_ATTACHMENT_MAX_CONCURRENT_DISTRIBUTIONS` - Concurrency limit
- `PEAT_NODE_ATTACHMENT_QUEUE_WHEN_FULL` - Queue behavior when full
- `PEAT_NODE_ATTACHMENT_DEFAULT_PRIORITY` - Default attachment priority
- `PEAT_NODE_PRINT_CONFIG` - Log resolved config at startup (default: false)

**Build:**
- `build.rs` - Compiles `proto/sidecar.proto` via `connectrpc_build`
- `Cargo.toml` - Workspace resolver 2

## Platform Requirements

**Development:**
- Rust toolchain (1.93+)
- protoc (Protocol Buffers compiler)

**Production:**
- Linux (debian:bookworm-slim base image)
- `tini` init process
- Port 50051/tcp (gRPC API)
- Volume `/data/peat-node` (CRDT state + Iroh blobs)
- Helm chart: `chart/peat-node/`

---

*Stack analysis: 2026-07-08*
