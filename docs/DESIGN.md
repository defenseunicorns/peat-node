# peat-sidecar Design

## Overview

peat-sidecar is a general-purpose Rust binary that runs as a Kubernetes
sidecar, participates as a full CRDT mesh node via peat-mesh (Automerge +
Iroh QUIC), and exposes a gRPC API for co-located applications.

Its primary integration target is UDS Fleet Management, where it provides
the **DDIL-resilient transport layer** between edge agents and the Fleet
Command Hub.

## Relationship to UDS Fleet Management

### The Fleet Management Architecture

UDS Fleet Management (see `UDS Fleet Management` TDD) defines a
hub-spoke architecture:

```
┌─ Fleet Command Hub (central cluster) ──────────────────────┐
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ API Server   │  │ Agent Manager│  │ Postgres         │  │
│  │ (Svelte UI + │  │ (ConnectRPC) │  │ (fleet state)    │  │
│  │  REST API)   │  │              │  │                  │  │
│  └──────────────┘  └──────▲───────┘  └──────────────────┘  │
│                           │                                  │
└───────────────────────────┼──────────────────────────────────┘
                            │
              AgentService.Connect(AgentMessage)
              South → North only
              Heartbeat: status + workloads + labels
                            │
           ┌────────────────┼────────────────┐
           │                │                │
    ┌──────┴───────┐ ┌─────┴────────┐ ┌─────┴────────┐
    │ Edge Cluster │ │ Edge Cluster │ │ Edge Cluster │
    │  + Remote    │ │  + Remote    │ │  + Remote    │
    │    Agent     │ │    Agent     │ │    Agent     │
    └──────────────┘ └──────────────┘ └──────────────┘
```

Key characteristics:
- **South→North** — edge agents initiate; hub receives
- **Heartbeat model** — agents send `AgentMessage` containing status,
  workloads, and labels to the hub's `AgentService.Connect()` RPC
- **Postgres persistence** on the hub side
- **Keycloak** for agent identity and user RBAC
- **Scale**: 5,000 agents, 30s heartbeat interval

### The DDIL Problem

The Fleet Management TDD explicitly calls out DDIL (Denied, Degraded,
Intermittent, Limited) as a core constraint. The hub-spoke model has
a specific weakness here: **when an edge agent can't reach the hub,
heartbeats stop and the hub goes blind.** There is no peer-to-peer
fallback, no store-and-forward, and no way for agents to share state
with each other when the hub is unreachable.

Customer scenarios that expose this:
- ATOMS: Fighter jets with ephemeral clusters, no guaranteed connectivity
- CANES: Ships at sea with intermittent satellite links
- Forward-deployed kits that operate behind contested networks

### Where Peat Fits

Peat doesn't replace the Fleet Management architecture — it **provides
the DDIL-resilient transport layer underneath it.**

```
┌─ Fleet Command Hub ────────────────────────────────────────┐
│  API Server + Agent Manager + Postgres                      │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ peat-sidecar                                          │   │
│  │ (mesh participant — consumes fleet state from CRDT)   │   │
│  └────────────────────────┬─────────────────────────────┘   │
└───────────────────────────┼──────────────────────────────────┘
                            │
              Peat CRDT Mesh (Automerge + Iroh QUIC)
              ● Peer-to-peer — no central dependency
              ● Survives network partitions
              ● Multi-transport: QUIC, BLE, relay
              ● Eventually consistent
                            │
           ┌────────────────┼────────────────┐
           │                │                │
    ┌──────┴───────┐ ┌─────┴────────┐ ┌─────┴────────┐
    │ Edge Cluster │ │ Edge Cluster │ │ Edge Cluster │
    │              │ │              │ │              │
    │ Remote Agent │ │ Remote Agent │ │ Remote Agent │
    │ peat-sidecar │ │ peat-sidecar │ │ peat-sidecar │
    │ (watcher +   │ │ (watcher +   │ │ (watcher +   │
    │  mesh node)  │ │  mesh node)  │ │  mesh node)  │
    └──────────────┘ └──────────────┘ └──────────────┘
```

What Peat adds:
1. **Partition tolerance** — agents sync state via CRDT even when the
   hub is unreachable. When connectivity resumes, the hub receives
   the full converged state.
2. **Peer-to-peer** — agents on the same ship/base/network can sync
   with each other directly, without routing through the hub.
3. **Multi-transport** — Iroh QUIC for internet, BLE for local mesh,
   relay for NAT traversal. The same state flows over whatever
   transport is available.
4. **No data loss** — CRDT merge guarantees eventual consistency.
   Heartbeats that can't reach the hub are stored locally and sync
   when a path opens.

### Integration Model

The peat-sidecar **carries the same data** as the Fleet Management
heartbeat. The `AgentMessage` proto defines:

```protobuf
message Heartbeat {
  zarfapi.v1.AgentStatus agent_status = 1;
  repeated WorkloadInfo workloads = 2;
  string version = 3;
  map<string, string> labels = 4;
}
```

The sidecar's watcher polls the same data from the local agent
(`/status`, `ListPackages`) and writes it to CRDT collections that
map directly to the heartbeat fields:

| Heartbeat Field | CRDT Collection | Source |
|----------------|-----------------|--------|
| `agent_status` | `platforms/{agent-id}` | `GET /status` |
| `workloads` | `deployments/{agent-id}:{pkg}` | `ListPackages` |
| `version` | `platforms/{agent-id}.version` | `GET /status` |
| `labels` | `platforms/{agent-id}.labels` | Agent settings |

The Fleet Command Hub runs its own peat-sidecar. It reads the CRDT
mesh state and either:
- **(Option E)** Feeds it directly into the Agent Manager as if it
  were a heartbeat — the hub doesn't need to distinguish between
  "received via direct RPC" and "received via CRDT mesh"
- **(Option F)** The API Server queries the hub's peat-sidecar
  directly for fleet state, bypassing the Agent Manager for reads

### Connected vs DDIL Operation

| Scenario | Direct Heartbeat | Peat CRDT | Hub View |
|----------|-----------------|-----------|----------|
| **Connected** | Agent → Hub every 30s | Also syncs via mesh | Both paths deliver data; hub uses freshest |
| **Hub unreachable** | Fails silently | Agents sync peer-to-peer | Hub catches up when connectivity resumes |
| **Intermittent** | Sporadic delivery | Mesh fills gaps | Hub has continuous picture despite dropouts |
| **Air-gapped** | Never reaches hub | Local mesh only | Tablet/sneakernet reads from local sidecar |

### The Air-Gap / Tablet Scenario

The Fleet Management TDD describes a UDS Android tablet as an
"enrollment authority" for provisioning edge clusters. The same
tablet could run peat-sidecar and serve as a **mobile mesh bridge**:

```
Air-gapped environment                    Connected environment
─────────────────────                    ─────────────────────

Edge Cluster A ◄──BLE──► Tablet ···sneakernet···► Hub Cluster
Edge Cluster B ◄──BLE──►   (peat-sidecar)         (peat-sidecar)
Edge Cluster C ◄──BLE──►
```

The tablet syncs with edge clusters via BLE, physically moves to
a connected environment, and syncs with the hub via QUIC. Fleet
state flows without any real-time network connectivity.

---

## Architecture (Pod-Level)

```
┌──────────────────────────────────────────────────────────────────────┐
│  Kubernetes Pod                                                       │
│                                                                       │
│  ┌──────────────────────────┐     ┌────────────────────────────────┐ │
│  │ uds-remote-agent         │     │ peat-sidecar                   │ │
│  │                          │     │                                │ │
│  │  Connect RPC :8080    ◄──┼─────┤  Agent Watcher                 │ │
│  │  (ZarfAPI, RegistryAPI,  │     │  (Connect RPC client, polls    │ │
│  │   SettingsAPI, OSAPI)    │     │   /status, ListPackages, etc.) │ │
│  │                          │     │                                │ │
│  │  As client:              │     │  CRDT Store (Automerge)        │ │
│  │  - OCI registry pulls    │     │  ● platforms/  (agent state)   │ │
│  │  - Zarf registry proxy   │     │  ● deployments/(packages)     │ │
│  │  - Kubernetes API        │     │  ● packages/  (pulled cache)   │ │
│  │  - Fleet Mgmt heartbeat  │     │                                │ │
│  │    (AgentService.Connect)│     │  Mesh Transport (Iroh QUIC)    │ │
│  └──────────────────────────┘     └───────────┬──────────────────┘ │
│                                                │                    │
└────────────────────────────────────────────────┼────────────────────┘
                                                 │
                                        Iroh QUIC / BLE / relay
                                                 │
                                        Other peat-sidecar instances
```

## UDS Remote Agent: Server AND Client

UDS Remote Agent is both a **server** and a **client**:

**As a server** — listens on :8080 (Connect RPC) for CLI, UI, and API
clients.

**As a client** — connects outbound to:
- **OCI registries** — pulls packages via HTTP
- **Zarf Registry** — reverse-proxies OCI requests
- **Kubernetes API** — cluster state via informers
- **Fleet Command Hub** — `AgentService.Connect()` heartbeat (planned)

The reusable client SDK (`pkg/client/api.go`) is used by:

| Client | Protocol | Auth |
|--------|----------|------|
| `uds-agent-cli` (Go CLI) | Connect RPC / HTTP/2 | mTLS |
| Web UI (Svelte) | Connect RPC / HTTP/2 | mTLS (.p12) |
| E2E tests | Connect RPC / HTTP/2 | mTLS (test certs) |
| **peat-sidecar watcher** | Connect RPC / HTTP/2 | mTLS or insecure |

The CLI supports 1-to-1 (`--server` pointing at one agent) and 1-to-many
(pointing at different agents). This means the CLI could also point at a
peat-sidecar fleet endpoint for fleet-wide queries using the same protocol.

## Agent Watcher Design

The peat-sidecar agent watcher connects to the local agent using the
**same Connect RPC protocol** as the CLI and UI. It is just another client.

### Connection

```
--agent-addr http://localhost:8080     # insecure (dev/test)
--agent-addr https://localhost:8080    # mTLS (production)
--agent-cert-file /pki/client.pem
--agent-key-file /pki/client-key.pem
--agent-ca-file /pki/rootCA.pem
```

### Poll Loop

Every `--agent-poll-interval` seconds (default: 10):

1. `GET /status` → write to `platforms/{agent-id}`
2. `ListPackages` (ZarfAPI) → write to `deployments/{agent-id}:{pkg}`
3. `ListPulledPackages` (RegistryAPI) → write to `packages/{agent-id}:{ref}`

Changes sync automatically to all connected peers via `sync_on_change`.

### What Syncs / What Doesn't

| Syncs | Doesn't Sync |
|-------|-------------|
| Agent identity, health, K8s version | Agent settings (registry credentials) |
| Deployed packages (name, version, status) | Active pod status (cluster-local) |
| Pulled/cached packages | In-flight deployment progress |
| Labels | Kubernetes resources |

## How Fleet State Gets INTO the Agent

### The Problem

The watcher solves outbound: agent → sidecar → mesh. But how does fleet
state flow inbound to the local agent?

### Options

**Option A: Fleet state in sidecar only (now)**

Fleet-wide queries go to the sidecar. The agent stays unaware.

```
CLI ──► sidecar :50051  →  fleet-wide view
CLI ──► agent :8080     →  local-only view
```

**Option B: Sidecar feeds Agent Manager on the hub (planned)**

The hub's peat-sidecar reads CRDT mesh state and feeds it into the
Agent Manager as synthetic heartbeats. The Agent Manager doesn't
need to know the data came from CRDT vs. direct RPC. Postgres gets
the same fleet-wide data either way.

This aligns with the Fleet Management TDD: the Agent Manager already
expects `AgentMessage` payloads. The peat-sidecar on the hub simply
constructs these from CRDT state.

**Option C: Agent FleetService (future)**

Add fleet-aware RPCs to the agent for the sidecar to push into.
See `defenseunicorns/uds-remote-agent#533`.

**Option D: Sidecar implements agent APIs**

peat-sidecar serves `zarfapi.v1.ZarfAPIService.ListPackages` but
returns fleet-wide aggregated data from the CRDT mesh. The existing
CLI works without modification — just point `--server` at the sidecar.

### Recommendation

**Start with A. Implement B when Fleet Command Hub ships.**

Option B is the natural integration: the hub already has an Agent Manager
that receives heartbeats and writes to Postgres. Adding a peat-sidecar
to the hub that feeds CRDT state into the same pipeline requires no
architectural changes to Fleet Management — just a new input source for
the Agent Manager.

## Deployment

### Edge Cluster (sidecar in UDS Remote Agent pod)

```yaml
containers:
  - name: peat-sidecar
    image: ghcr.io/defenseunicorns/peat-sidecar:latest
    env:
      - name: PEAT_SIDECAR_LISTEN
        value: "tcp://0.0.0.0:50051"
      - name: PEAT_SIDECAR_AGENT_ADDR
        value: "http://localhost:8080"
      - name: PEAT_SIDECAR_AUTO_SYNC
        value: "true"
    ports:
      - containerPort: 50051
```

### Fleet Command Hub (sidecar in Agent Manager pod)

```yaml
containers:
  - name: peat-sidecar
    image: ghcr.io/defenseunicorns/peat-sidecar:latest
    env:
      - name: PEAT_SIDECAR_LISTEN
        value: "tcp://0.0.0.0:50051"
      - name: PEAT_SIDECAR_AUTO_SYNC
        value: "true"
      # No --agent-addr: hub sidecar doesn't watch a local agent,
      # it receives state from the mesh and feeds the Agent Manager
```

## Precedent

| Aspect | peat-registry mesh mode | peat-sidecar |
|--------|------------------------|--------------|
| Local service | OCI registry (HTTP :5000) | UDS Remote Agent (Connect RPC :8080) |
| Watches local service | Enumerates registry digests | Polls agent APIs |
| Writes to CRDT | DigestSet documents | Platform/deployment/package documents |
| Syncs with peers | Automerge + Iroh | Automerge + Iroh |
| Zero changes to local service | Yes | Yes |

## References

- ADR-050 Amendment 1: Sidecar Pattern for Go/Cloud-Native Integration (#746)
- UDS Fleet Management TDD (Notion → `docs/UDS Fleet Management*.md`)
- Observability Engineering Design (Notion → `docs/Observability (Engineering Design)*.md`)
- peat-registry mesh mode: `peat-registry/src/mesh/node.rs`
- UDS Remote Agent client: `uds-remote-agent/pkg/client/api.go`
- UDS Remote Agent protos: `uds-remote-agent/protos/*/v1/*.proto`
- Fleet Management Agent Manager proto: `agent_manager_api.v1.AgentService`

## GitHub Issues

| Issue | Repo | Description |
|-------|------|-------------|
| [#747](https://github.com/defenseunicorns/peat/issues/747) | peat | peat-sidecar umbrella |
| [#748](https://github.com/defenseunicorns/peat/issues/748) | peat | Agent watcher (poll UDS Remote Agent) |
| [#750](https://github.com/defenseunicorns/peat/issues/750) | peat | Cluster-to-cluster integration test |
| [#751](https://github.com/defenseunicorns/peat/issues/751) | peat | Fleet state propagation design |
| [#533](https://github.com/defenseunicorns/uds-remote-agent/issues/533) | uds-remote-agent | FleetService (future) |
