# peat-sidecar

Peat mesh sidecar — a Rust binary that runs alongside applications in Kubernetes pods, participates as a full CRDT mesh node ([Automerge](https://automerge.org/) + [Iroh](https://iroh.computer/) QUIC), and exposes a gRPC API for co-located apps to read/write shared state that syncs across clusters.

## How It Works

```
┌──────────────────────────────────────────────────────────────┐
│  Kubernetes Pod                                               │
│                                                               │
│  ┌────────────────────┐     ┌──────────────────────────────┐ │
│  │ Your Application    │     │ peat-sidecar                 │ │
│  │                     │     │                              │ │
│  │  gRPC client    ────┼─────┤  gRPC API (:50051)          │ │
│  │  (any language)     │     │  21 RPCs: documents, peers,  │ │
│  │                     │     │  subscriptions, sync control │ │
│  └────────────────────┘     │                              │ │
│                              │  CRDT Store (Automerge)      │ │
│                              │  P2P Transport (Iroh QUIC)   │ │
│                              └───────────┬──────────────────┘ │
└──────────────────────────────────────────┼────────────────────┘
                                           │
                                  Iroh QUIC (relay or direct)
                                           │
                                  Other peat-sidecar instances
                                  on other clusters
```

Documents written on one cluster automatically sync to all connected peers via Automerge CRDT — no central server, works through network partitions, eventually consistent.

## Quick Start

```bash
# Build
cargo build --release

# Run (TCP mode)
./target/release/peat-sidecar \
  --listen tcp://0.0.0.0:50051 \
  --data-dir /tmp/peat-sidecar \
  --node-id my-node \
  --auto-sync

# Run (Unix socket mode)
./target/release/peat-sidecar \
  --listen unix:///var/run/peat.sock \
  --data-dir /tmp/peat-sidecar
```

## gRPC API

The sidecar exposes `peat.sidecar.v1.PeatSidecar` with 21 RPCs:

| Category | RPCs |
|----------|------|
| **Lifecycle** | `GetStatus` |
| **Peers** | `ConnectPeer`, `DisconnectPeer`, `ListPeers` |
| **Documents** | `PutDocument`, `GetDocument`, `DeleteDocument`, `ListDocuments` |
| **Typed Collections** | `PutPlatform`, `GetPlatforms`, `PutCell`, `GetCells`, `PutTrack`, `GetTracks`, `PutCommand`, `GetCommands` |
| **Subscriptions** | `Subscribe` (server-streaming) |
| **Sync Control** | `StartSync`, `StopSync`, `GetSyncStats` |

Proto definition: [`proto/sidecar.proto`](proto/sidecar.proto)

## Agent Watcher (Optional)

When `--agent-addr` is set, the sidecar polls a co-located service (e.g., [UDS Remote Agent](https://github.com/defenseunicorns/uds-remote-agent)) via Connect RPC and syncs its state to the mesh:

```bash
./target/release/peat-sidecar \
  --listen tcp://0.0.0.0:50051 \
  --agent-addr http://localhost:8080 \
  --agent-poll-interval 10 \
  --auto-sync
```

The watcher uses the same Connect RPC protocol as the agent's CLI and UI — no modifications to the watched service required.

## Docker

```bash
docker build -t peat-sidecar:latest .

docker run -p 50051:50051 peat-sidecar:latest \
  --listen tcp://0.0.0.0:50051
```

## Helm

Deploy as a standalone pod or inject as a sidecar:

```bash
# Standalone
helm install peat-sidecar chart/peat-sidecar/

# As a sidecar (use the injectable template)
# See chart/peat-sidecar/templates/_helpers.tpl for
# peat-sidecar.container and peat-sidecar.volumes templates
```

## Configuration

All flags can be set via environment variables with `PEAT_SIDECAR_` prefix:

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--listen` | `PEAT_SIDECAR_LISTEN` | `tcp://0.0.0.0:50051` | gRPC listen address |
| `--data-dir` | `PEAT_SIDECAR_DATA_DIR` | `/data/peat-sidecar` | Persistent data directory |
| `--node-id` | `PEAT_SIDECAR_NODE_ID` | Random UUID | Node identifier |
| `--app-id` | `PEAT_SIDECAR_APP_ID` | `peat-default` | Formation/group ID |
| `--shared-key` | `PEAT_SIDECAR_SHARED_KEY` | | Base64 shared key |
| `--peer` | `PEAT_SIDECAR_PEERS` | | Peer endpoint IDs (comma-separated) |
| `--auto-sync` | `PEAT_SIDECAR_AUTO_SYNC` | `true` | Start sync on boot |
| `--agent-addr` | `PEAT_SIDECAR_AGENT_ADDR` | | Agent address to watch |
| `--agent-poll-interval` | `PEAT_SIDECAR_AGENT_POLL_INTERVAL` | `10` | Poll interval (seconds) |

## Design

See [docs/DESIGN.md](docs/DESIGN.md) for the full architecture, agent watcher design, and fleet state propagation options.

## Client Libraries

- **Go**: [peat-uds-remote-agent](https://github.com/defenseunicorns/peat-uds-remote-agent) — pure Go, Connect RPC client
- **Any language**: generate a gRPC client from [`proto/sidecar.proto`](proto/sidecar.proto)

## License

[Apache License 2.0](LICENSE)
