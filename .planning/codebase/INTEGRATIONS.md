# External Integrations

**Analysis Date:** 2026-07-08

## APIs & External Services

**P2P Mesh (Iroh):**
- Iroh 1.0.2 - QUIC-based P2P networking for CRDT document sync and blob transfer
  - SDK/Client: `iroh` crate (direct dep, pinned to match `peat-mesh`)
  - Auth: Shared key via `PEAT_NODE_SHARED_KEY` env var
  - Discovery: mDNS (toggle via `PEAT_NODE_DISABLE_MDNS`) + Kubernetes EndpointSlice watch

**Co-located Agent (UDS Remote Agent):**
- Optional polling watcher for a co-located application agent
  - SDK/Client: `reqwest` HTTP client with Connect RPC JSON encoding (`src/watcher.rs`)
  - Auth: mTLS via `PEAT_NODE_AGENT_TLS_CERT` / `PEAT_NODE_AGENT_TLS_KEY` / `PEAT_NODE_AGENT_TLS_CA`
  - Endpoint: `PEAT_NODE_AGENT_ADDR`
  - Poll interval: `PEAT_NODE_AGENT_POLL_INTERVAL` (default 10s)

**Connect RPC API (served):**
- Single-port Connect / gRPC / gRPC-Web server for co-located application consumption
  - Proto: `proto/sidecar.proto` (package `peat.sidecar.v1`)
  - Service: `PeatSidecar` - lifecycle, peer management, document CRUD, subscriptions
  - Listen: `PEAT_NODE_LISTEN` (default `tcp://0.0.0.0:50051`)

## Data Storage

**Databases:**
- Automerge CRDT store (via `peat-mesh` automerge-backend)
  - Persisted to: `PEAT_NODE_DATA_DIR` (default `/data/peat-node`)
  - Client: `peat-mesh::storage::AutomergeStore`

**File Storage:**
- Iroh blob store for file/attachment distribution (PRD-006)
  - Location: Within `PEAT_NODE_DATA_DIR`
  - Attachment ingest: `src/attachments/` module
  - Root: `PEAT_NODE_ATTACHMENT_ROOT`

**Caching:**
- None (CRDT store is the primary state)

## Authentication & Identity

**Mesh Auth:**
- Shared-key authentication between mesh peers (`PEAT_NODE_SHARED_KEY`)
- Deterministic iroh keypair derivation via HKDF-SHA256 (`src/crypto.rs`, `src/identity.rs`)
  - Enables stable node identity across pod restarts in Kubernetes

**Encryption at Rest:**
- AES-GCM encryption via `PEAT_NODE_ENCRYPTION_KEY` (`aes-gcm` crate)

**Agent mTLS:**
- Optional mutual TLS for agent watcher connection
  - Cert/key/CA paths via env vars

## Monitoring & Observability

**Error Tracking:**
- None (no external error tracking service)

**Logs:**
- `tracing` + `tracing-subscriber` with env-filter
  - Structured logging, filter controlled via `RUST_LOG` env var

## CI/CD & Deployment

**Hosting:**
- Kubernetes (sidecar pattern) via Helm chart (`chart/peat-node/`)
- Also runs standalone or as systemd service
- Zarf manifest available (`bundle/`)

**Container:**
- Multi-stage Docker build (`Dockerfile`)
  - Builder: `rust:1.93-bookworm` with cargo-chef
  - Runtime: `debian:bookworm-slim` with tini

**CI Pipeline:**
- GitHub Actions (`.github/workflows/`)
  - `ci.yaml` - Main CI
  - `release.yml` - Release builds
  - `qa-review.yml` - QA review
  - `quickstart.yml` - Quickstart validation
  - `attachment-delivery.yml` - Attachment delivery tests
  - `cross-cluster-sync.yml` - Cross-cluster sync tests

## Kubernetes Integration

**Discovery:**
- `peat-mesh` kubernetes feature enables `KubernetesDiscovery` via EndpointSlice watch
- `k8s-openapi` v1_32 API version

**Deployment:**
- Helm chart: `chart/peat-node/Chart.yaml`, `chart/peat-node/values.yaml`
- Templates: `chart/peat-node/templates/`

## Environment Configuration

**Required env vars:**
- None strictly required (all have defaults), but operationally:
  - `PEAT_NODE_SHARED_KEY` - For mesh authentication
  - `PEAT_NODE_ENCRYPTION_KEY` - For encryption at rest
  - `PEAT_NODE_PEERS` - For initial peer connectivity

**Optional env vars:**
- See full list in STACK.md Configuration section

**Secrets location:**
- Passed via environment variables or Kubernetes secrets (Helm chart)

## Webhooks & Callbacks

**Incoming:**
- None (uses Connect RPC streaming, not webhooks)

**Outgoing:**
- None (agent watcher uses polling, not webhooks)

---

*Integration audit: 2026-07-08*
