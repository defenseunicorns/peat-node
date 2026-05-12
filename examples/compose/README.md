# peat-node — Docker Compose example

Runnable two-node mesh demonstrating CRDT sync between two peat-node sidecars on a single Docker host. No Kubernetes, no UDS Remote Agent — just two containers, a bootstrap script, and `curl`.

## Prerequisites

- Docker (or Docker Desktop) with `docker compose`
- `curl` and `jq` on the host
- No public-internet egress required. The two containers peer over direct UDP on the compose bridge network; Iroh's QUIC port is pinned per node and Docker's embedded DNS resolves `peat-node-a` / `peat-node-b` between them. (Works on macOS Docker Desktop with corporate proxies / air-gapped networks.)

## Run

```sh
docker compose up -d
./bootstrap.sh    # peer node-a <-> node-b
./demo.sh         # write on node-a, read on node-b, verify sync
```

Expected output from `demo.sh`:

```
[node-a] PutDocument hello/world
  wrote: {"msg":"sync via CRDT","from":"node-a"}
[node-b] Polling GetDocument hello/world...
  PASS: node-b received it after 2s
  data: {"msg":"sync via CRDT","from":"node-a"}
```

## Teardown

```sh
docker compose down -v    # -v drops the data volumes too
```

## What's happening

| Container | Host port | Role |
|---|---|---|
| `peat-node-a` | `localhost:50061` | First mesh node, writes the document |
| `peat-node-b` | `localhost:50062` | Second mesh node, receives via CRDT sync |

Both nodes are in the same Peat *formation* (`PEAT_NODE_APP_ID=compose-demo` with a shared `PEAT_NODE_SHARED_KEY`), so they authenticate each other on connect. `bootstrap.sh` fetches node-a's Iroh endpoint ID via `GetStatus` and feeds it to node-b's `ConnectPeer`. After that, any document write on either node propagates to the other automatically via Automerge CRDT over Iroh QUIC.

The wire is [Connect RPC](https://connectrpc.com/) on a single port — the same TCP listener speaks Connect-over-HTTP+JSON (what the scripts use), gRPC, and gRPC-Web. Use whichever the client library prefers.

## Talking to a peat-node from your own service

The shape of every unary RPC is the same:

```sh
curl -X POST http://localhost:50061/peat.sidecar.v1.PeatSidecar/<Method> \
  -H 'Content-Type: application/json' \
  -d '<json-body>'
```

See `proto/sidecar.proto` for the full RPC list. Common ones:

| RPC | Body |
|---|---|
| `GetStatus` | `{}` |
| `PutDocument` | `{"collection":"...","docId":"...","jsonData":"..."}` |
| `GetDocument` | `{"collection":"...","docId":"..."}` |
| `ListDocuments` | `{"collection":"..."}` |
| `DeleteDocument` | `{"collection":"...","docId":"..."}` |
| `ListPeers` | `{}` |
| `GetSyncStats` | `{}` |

For typed clients in other languages, generate from [`proto/sidecar.proto`](../../proto/sidecar.proto) in the consumer's own repo — peat-node is pure Rust and does not ship language-specific SDKs.

For protocol bridges (NATS, MQTT, etc.), use [`peat-gateway`](https://github.com/defenseunicorns/peat-gateway) in front of peat-node rather than re-implementing the wire here — `peat-gateway` is the ADR-043 consumer-interface adapter and owns those concerns.

## Configuration

Every environment variable used in `docker-compose.yml` is documented in [`docs/CONFIGURATION.md`](../../docs/CONFIGURATION.md). The demo key (`AAAA...=`) is 32 zero bytes — fine for a local demo, never use it for anything that goes over a real network. Generate your own:

```sh
head -c 32 /dev/urandom | base64
```

## What this example does NOT cover

- Operator runbook (key rotation, peer bootstrap on cold start, backup/restore, upgrade flow)
- Encryption-at-rest (`PEAT_NODE_ENCRYPTION_KEY`) — supported; not exercised in this demo
- mTLS to a co-located agent (`PEAT_NODE_AGENT_TLS_*`) — supported; not exercised in this demo
- More than two nodes — works the same way, just bootstrap each new node against an existing one
