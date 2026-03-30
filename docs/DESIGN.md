# peat-sidecar Design

## Overview

peat-sidecar is a Rust binary that runs as a Kubernetes sidecar container
alongside applications (primarily UDS Remote Agent). It participates as a
full CRDT mesh node via peat-mesh (Automerge + Iroh QUIC) and exposes a
gRPC API for co-located applications.

For UDS Remote Agent integration, the sidecar also acts as an **agent
watcher** — it connects to the local agent as a client, polls its state,
and syncs that state across the mesh to other clusters.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Kubernetes Pod                                                       │
│                                                                       │
│  ┌──────────────────────────┐     ┌────────────────────────────────┐ │
│  │ uds-remote-agent         │     │ peat-sidecar                   │ │
│  │ (unchanged, server-only) │     │                                │ │
│  │                          │     │  ┌──────────────────────────┐  │ │
│  │  Connect RPC :8080    ◄──┼─────┤  │ Agent Watcher            │  │ │
│  │  (ZarfAPI, RegistryAPI,  │     │  │ (Connect RPC client)     │  │ │
│  │   SettingsAPI, OSAPI)    │     │  │                          │  │ │
│  │                          │     │  │ Polls:                   │  │ │
│  │                          │     │  │  - ListPackages          │  │ │
│  │                          │     │  │  - DeploymentStatus      │  │ │
│  │                          │     │  │  - /status               │  │ │
│  │                          │     │  │  - ListPulledPackages    │  │ │
│  │                          │     │  └──────────┬───────────────┘  │ │
│  │                          │     │             │                   │ │
│  │                          │     │             ▼                   │ │
│  │                          │     │  ┌──────────────────────────┐  │ │
│  │                          │     │  │ CRDT Store (Automerge)   │  │ │
│  │                          │     │  │                          │  │ │
│  │                          │     │  │ Collections:             │  │ │
│  │                          │     │  │  platforms/   (agents)   │  │ │
│  │                          │     │  │  deployments/ (packages) │  │ │
│  │                          │     │  │  packages/   (pulled)    │  │ │
│  │                          │     │  └──────────┬───────────────┘  │ │
│  │                          │     │             │                   │ │
│  │                          │     │             ▼                   │ │
│  │                          │     │  ┌──────────────────────────┐  │ │
│  │                          │     │  │ Mesh Transport           │  │ │
│  │                          │     │  │ (Iroh QUIC + relay)      │  │ │
│  │                          │     │  └──────────┬───────────────┘  │ │
│  └──────────────────────────┘     └─────────────┼──────────────────┘ │
│                                                  │                    │
└──────────────────────────────────────────────────┼────────────────────┘
                                                   │
                                          Iroh QUIC (relay or direct)
                                                   │
                                                   ▼
                                          Other peat-sidecar instances
                                          on other clusters
```

## Who Connects to UDS Remote Agent Today

UDS Remote Agent is a **server-only** application. It does not make outbound
connections to other agents. Three clients connect to it:

| Client | Protocol | Auth | Code |
|--------|----------|------|------|
| `uds-agent-cli` (Go CLI) | Connect RPC / HTTP/2 | mTLS | `pkg/client/api.go` |
| Web UI (Svelte) | Connect RPC / HTTP/2 | mTLS (.p12) | Embedded, served from `/` |
| E2E tests | Connect RPC / HTTP/2 | mTLS (test certs) | `test/e2e/` |

All three use the same protocol: **Connect RPC with `connect.WithGRPC()`
over HTTP/2**. The canonical Go client is `pkg/client/api.go`.

## Agent Watcher Design

The peat-sidecar agent watcher is just **another client** of the local
UDS Remote Agent — it connects using the same Connect RPC protocol as the
CLI and UI. No modifications to UDS Remote Agent are needed.

### Connection

The watcher connects to the agent on localhost within the same pod:

- **Insecure mode** (`--disable-mtls`): `http://localhost:8080` with h2c
- **mTLS mode**: `https://localhost:8080` with shared certificates
  (mounted via the same Secret volume as the agent)

```
--agent-addr http://localhost:8080     # insecure (dev/test)
--agent-addr https://localhost:8080    # mTLS (production)
--agent-cert-file /pki/client.pem     # client cert for mTLS
--agent-key-file /pki/client-key.pem  # client key for mTLS
--agent-ca-file /pki/rootCA.pem       # CA cert for mTLS
```

### Poll Loop

The watcher runs a periodic poll loop (default: 10 seconds) that reads
agent state and writes it to the local CRDT store:

```
every poll_interval:
    1. GET /status
       → write to "platforms/{agent-id}" collection
         {
           "agent_id": "alpha-agent",
           "version": "0.74.0",
           "arch": "arm64",
           "k8s_version": "v1.31.4",
           "k8s_node_status": "READY",
           "cluster": "alpha",
           "last_seen": 1743206400
         }

    2. ListPackages (ZarfAPI)
       → for each deployed package, write to "deployments/{agent-id}:{package-name}"
         {
           "agent_id": "alpha-agent",
           "package": "mission-app",
           "version": "2.0.0",
           "status": "DEPLOYED",
           "components": [...],
           "namespace": "mission"
         }

    3. DeploymentStatus (ZarfAPI)
       → for active deployments, write to "deployments/{agent-id}:{deployment-id}"
         (transient — auto-expires or gets overwritten)

    4. ListPulledPackages (RegistryAPI)
       → for each pulled package, write to "packages/{agent-id}:{reference}"
         {
           "agent_id": "alpha-agent",
           "reference": "ghcr.io/defenseunicorns/packages/uds/dos-games:1.1.0",
           "arch": "amd64",
           "status": "PULLED",
           "total_bytes": 52428800
         }
```

### What Syncs Across the Mesh

Once written to the local Automerge store, documents automatically sync
to all connected peers via the existing `sync_on_change` loop (Iroh QUIC
transport). The CRDT merge semantics are last-writer-wins per field.

| Collection | Key Format | Content | Sync Value |
|------------|-----------|---------|------------|
| `platforms` | `{agent-id}` | Agent identity, health, cluster info | Fleet visibility |
| `deployments` | `{agent-id}:{package-name}` | What's deployed where | Fleet-wide deployment view |
| `packages` | `{agent-id}:{oci-reference}` | What's cached where | Coordinated pulls |

### What Does NOT Sync

- **Agent settings** (registry credentials) — security boundary
- **Active pod status** — cluster-local, from K8s informers
- **In-flight deployment progress** — transient, local concern
- **Kubernetes resources** — each cluster's API server is authoritative

### Fleet Queries

Any client can query the peat-sidecar's gRPC API to get the fleet-wide view:

```go
// See all agents in the mesh
platforms, _ := peatClient.GetPlatforms(ctx)
// → [{alpha-agent, v0.74.0, READY}, {bravo-agent, v0.74.0, READY}]

// See all deployments across all clusters
docs, _ := peatClient.ListDocuments(ctx, "deployments")
// → [alpha-agent:mission-app, alpha-agent:monitoring, bravo-agent:dos-games]

data, _ := peatClient.GetDocument(ctx, "deployments", "alpha-agent:mission-app")
// → {"agent_id":"alpha-agent","package":"mission-app","version":"2.0.0","status":"DEPLOYED"}
```

## Implementation Plan

### Phase 1: Agent Watcher (Rust, in peat-sidecar)

Add a new module `src/watcher.rs` that:

1. Uses `reqwest` to call the agent's REST endpoints (`/status`, `/healthz`)
2. Uses `tonic` (or raw HTTP/2 + prost) to call Connect RPC endpoints
   (`ListPackages`, `DeploymentStatus`, `ListPulledPackages`)
3. Runs a `tokio::time::interval` poll loop
4. Writes polled state to `SidecarNode::put_document()`

The watcher is **optional** — enabled only when `--agent-addr` is provided.
Without it, peat-sidecar works as a standalone CRDT node with the gRPC API.

**New CLI flags:**
```
--agent-addr <URL>              # Local UDS Remote Agent address
--agent-poll-interval <SECS>    # Poll interval (default: 10)
--agent-cert-file <PATH>        # Client cert for mTLS (optional)
--agent-key-file <PATH>         # Client key for mTLS (optional)
--agent-ca-file <PATH>          # CA cert for mTLS (optional)
```

### Phase 2: Proto Compatibility

The agent's Connect RPC services use these proto packages:
- `zarfapi.v1` — `ListPackages`, `DeploymentStatus`
- `registryapi.v1` — `ListPulledPackages`

The watcher needs to decode these responses. Options:
1. **Vendor the agent's proto files** and generate Rust types with `prost`
2. **Use raw JSON** — Connect RPC supports JSON encoding, parse with `serde_json`
3. **Use the Go client from peat-uds-remote-agent** as a sidecar process

**Recommendation:** Option 2 (raw JSON) for initial implementation.
Connect RPC with `content-type: application/json` returns human-readable
JSON. The watcher can parse it with `serde_json::Value` without needing
the agent's proto definitions. This avoids a tight coupling between the
sidecar's Rust code and the agent's proto schema.

For production, Option 1 (vendored protos) provides type safety and
efficient binary encoding.

### Phase 3: Fleet API

Optionally add fleet-specific RPCs to the peat-sidecar gRPC service:

```protobuf
// Fleet-wide queries (reads from CRDT mesh, not from local agent)
rpc GetFleetAgents(GetFleetAgentsRequest) returns (GetFleetAgentsResponse);
rpc GetFleetDeployments(GetFleetDeploymentsRequest) returns (GetFleetDeploymentsResponse);
rpc GetFleetPackages(GetFleetPackagesRequest) returns (GetFleetPackagesResponse);
```

These are convenience wrappers around `GetPlatforms` / `ListDocuments` /
`GetDocument` that return structured fleet state.

## Deployment

### Kubernetes (sidecar in UDS Remote Agent pod)

```yaml
# Injected via kubectl patch or Helm values overlay
containers:
  - name: peat-sidecar
    image: ghcr.io/defenseunicorns/peat-sidecar:latest
    env:
      - name: PEAT_SIDECAR_LISTEN
        value: "tcp://0.0.0.0:50051"
      - name: PEAT_SIDECAR_AGENT_ADDR
        value: "http://localhost:8080"   # same-pod agent
      - name: PEAT_SIDECAR_AGENT_POLL_INTERVAL
        value: "10"
      - name: PEAT_SIDECAR_AUTO_SYNC
        value: "true"
    ports:
      - containerPort: 50051
```

### mTLS Production Deployment

```yaml
    env:
      - name: PEAT_SIDECAR_AGENT_ADDR
        value: "https://localhost:8080"
      - name: PEAT_SIDECAR_AGENT_CERT_FILE
        value: "/pki/client.pem"
      - name: PEAT_SIDECAR_AGENT_KEY_FILE
        value: "/pki/client-key.pem"
      - name: PEAT_SIDECAR_AGENT_CA_FILE
        value: "/pki/rootCA.pem"
    volumeMounts:
      - name: mtls-certs       # shared with agent container
        mountPath: /pki
        readOnly: true
```

## Open Question: How Does Fleet State Get INTO the Agent?

### The Problem

The watcher solves outbound: local agent state → CRDT mesh → other clusters.
But what about inbound? When peat discovers other agents via `APP_ID` and
subscription, how does the **local** UDS Remote Agent learn about them?

Today: **it can't**. The agent has no API for receiving inbound fleet
information. Every RPC is either a query (ListPackages) or a command
(CreateDeployment). There is no `RegisterPeer`, `NotifyFleetState`, or
event subscription endpoint.

### Three Possible Answers

#### Option A: Fleet state lives only in the sidecar (no agent changes)

The agent stays unaware of the fleet. Fleet-wide queries go to the
sidecar's gRPC API, not the agent's. Clients (CLI, UI) that want fleet
visibility talk to the sidecar directly.

```
CLI ──► peat-sidecar :50051  →  fleet-wide view (platforms, deployments)
CLI ──► uds-remote-agent :8080  →  local-only view (this cluster's packages)
```

**Pros**: Zero changes to agent. Ship immediately.
**Cons**: Two endpoints for clients to know about. No fleet awareness
in the agent's UI.

#### Option B: Sidecar writes fleet state into agent settings (labels)

The agent already has `Labels` — user-defined key/value metadata. The
sidecar could use the `CreateLabel` / `UpdateLabel` RPCs to inject fleet
state as labels:

```
Label: peat.fleet/agents = "alpha-agent,bravo-agent,charlie-agent"
Label: peat.fleet/alpha-agent.status = "READY"
Label: peat.fleet/alpha-agent.packages = "mission-app:2.0.0,monitoring:1.0.0"
```

This uses existing APIs. The UI already displays labels. But it's a hack —
labels weren't designed for structured fleet state, and the agent has no
reactive behavior on label changes.

**Pros**: Uses existing agent API. UI shows labels.
**Cons**: Semantic abuse of labels. Not structured. No agent-side logic.

#### Option C: Add fleet-aware APIs to UDS Remote Agent (requires agent changes)

Add new RPCs to the agent that the sidecar can push fleet state into:

```protobuf
service FleetService {
  // Sidecar pushes fleet state into the agent
  rpc UpdateFleetAgent(UpdateFleetAgentRequest) returns (UpdateFleetAgentResponse);
  rpc UpdateFleetDeployment(UpdateFleetDeploymentRequest) returns (UpdateFleetDeploymentResponse);

  // Clients query fleet state through the agent
  rpc ListFleetAgents(ListFleetAgentsRequest) returns (ListFleetAgentsResponse);
  rpc ListFleetDeployments(ListFleetDeploymentsRequest) returns (ListFleetDeploymentsResponse);
}
```

The sidecar subscribes to CRDT changes and pushes them into the agent via
these RPCs. The agent stores fleet state in memory and exposes it through
the same gRPC server the CLI/UI already talk to.

**Pros**: Clean API. Fleet state visible in agent's UI. Single endpoint
for clients. Agent can react to fleet changes (e.g., show remote
deployments in package list).
**Cons**: Requires changes to UDS Remote Agent. New proto definitions.
New in-memory state management.

### Recommendation

**Start with Option A, plan for Option C.**

Option A ships immediately with zero agent changes and proves the value
of fleet-wide CRDT sync. The sidecar's gRPC API already serves fleet
queries. The Go client (`peat-uds-remote-agent`) already wraps it.

Once fleet visibility proves valuable, Option C adds clean APIs to the
agent. This is a natural progression — the same way the agent added
`RegistryService` and `AgentSettingsService` over time, a `FleetService`
is just another service wired into the existing `mux.Router`.

The sidecar's role evolves:
1. **Today**: watcher (polls agent) + mesh node (syncs state) + fleet API server
2. **Future**: watcher + mesh node + **fleet state pusher** (calls agent's FleetService)

### Data Flow (Option A → Option C transition)

**Option A (now):**
```
                      ┌── CLI/UI ──► agent :8080 (local view)
User queries ────────►│
                      └── CLI/UI ──► sidecar :50051 (fleet view)
```

**Option C (future):**
```
User queries ──► CLI/UI ──► agent :8080 (local + fleet view)
                                  ▲
                                  │ FleetService.UpdateFleetAgent()
                                  │
                            sidecar (pushes CRDT changes into agent)
```

## Precedent

This design follows the same pattern as **peat-registry mesh mode**:

| Aspect | peat-registry | peat-sidecar |
|--------|--------------|--------------|
| Local service | OCI registry (HTTP :5000) | UDS Remote Agent (Connect RPC :8080) |
| Rust binary | Sidecar container | Sidecar container |
| Watches local service | Enumerates registry digests | Polls agent APIs |
| Writes to CRDT | DigestSet documents | Platform/deployment/package documents |
| Syncs with peers | Automerge + Iroh | Automerge + Iroh |
| Zero changes to local service | Yes | Yes |

## References

- ADR-050 Amendment 1: Sidecar Pattern for Go/Cloud-Native Integration (#746)
- peat-registry mesh mode: `peat-registry/src/mesh/node.rs`
- UDS Remote Agent client: `uds-remote-agent/pkg/client/api.go`
- UDS Remote Agent protos: `uds-remote-agent/protos/*/v1/*.proto`

## GitHub Issues

| Issue | Repo | Description |
|-------|------|-------------|
| [#747](https://github.com/defenseunicorns/peat/issues/747) | peat | peat-sidecar umbrella |
| [#748](https://github.com/defenseunicorns/peat/issues/748) | peat | Agent watcher (poll UDS Remote Agent) |
| [#749](https://github.com/defenseunicorns/peat/issues/749) | peat | peat-uds-remote-agent Go client |
| [#750](https://github.com/defenseunicorns/peat/issues/750) | peat | Cluster-to-cluster integration test |
| [#751](https://github.com/defenseunicorns/peat/issues/751) | peat | Fleet state propagation design |
| [#533](https://github.com/defenseunicorns/uds-remote-agent/issues/533) | uds-remote-agent | FleetService (future) |
