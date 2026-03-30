# Cluster-to-Cluster Peat Sync Test

Tests cross-cluster CRDT synchronization between two UDS Remote Agent
deployments via peat-sidecar.

## Architecture

```
┌─ k3d cluster "peat-alpha" ──────────────┐
│  Namespace: zarf                         │
│  ┌─────────────────────────────────────┐ │
│  │ Pod: uds-remote-agent-deployment    │ │
│  │  ┌───────────────────────────────┐  │ │
│  │  │ uds-remote-agent (:8080)      │  │ │
│  │  └───────────────────────────────┘  │ │
│  │  ┌───────────────────────────────┐  │ │
│  │  │ peat-sidecar (:50051)         │  │ │
│  │  └───────────────────────────────┘  │ │
│  └─────────────────────────────────────┘ │
│  NodePorts: 32582 (agent), 32551 (peat)  │
└──────────────────────────────────────────┘
           │ Iroh QUIC (relay or direct)
           ▼
┌─ k3d cluster "peat-bravo" ──────────────┐
│  Namespace: zarf                         │
│  ┌─────────────────────────────────────┐ │
│  │ Pod: uds-remote-agent-deployment    │ │
│  │  ┌───────────────────────────────┐  │ │
│  │  │ uds-remote-agent (:8080)      │  │ │
│  │  └───────────────────────────────┘  │ │
│  │  ┌───────────────────────────────┐  │ │
│  │  │ peat-sidecar (:50051)         │  │ │
│  │  └───────────────────────────────┘  │ │
│  └─────────────────────────────────────┘ │
│  NodePorts: 33582 (agent), 33551 (peat)  │
└──────────────────────────────────────────┘
```

## Prerequisites

- k3d
- Docker
- Zarf CLI (or `uds` wrapper)
- Go 1.25+
- peat-sidecar container image (built from `../../peat-sidecar/`)

## Usage

```bash
# Build peat-sidecar image
cd ../../../peat-sidecar && docker build -t peat-sidecar:dev .

# Run the full test (creates clusters, deploys, tests, cleans up)
./setup.sh

# Run just the Go test (assumes clusters are already up)
ALPHA_PEAT_ADDR=http://localhost:32551 \
BRAVO_PEAT_ADDR=http://localhost:33551 \
go test -v -run TestCrossClusterSync ./...
```
