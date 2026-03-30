# Go Integration Tests

Go module containing a client library and test tools for peat-sidecar.

## Client Library

`client.go` provides an idiomatic Go client for the peat-sidecar gRPC API.
Uses [Connect RPC](https://connectrpc.com/) over HTTP/2 (h2c), matching the
protocol used by UDS Remote Agent's CLI and UI.

```go
import peat "github.com/defenseunicorns/peat-sidecar/test/go"

client, _ := peat.Connect("http://localhost:50051")
status, _ := client.Status(ctx)
```

## Test Tools

| Command | Description |
|---------|-------------|
| `cmd/smoketest/` | Single sidecar: gRPC round-trip (status, put/get/delete, typed collections) |
| `cmd/synctest/` | Two sidecars: peer connection, bidirectional CRDT sync |
| `cmd/watchertest/` | Full stack: real UDS Remote Agent + watcher + mesh sync |
| `cmd/query/` | Inspect a running sidecar's CRDT store |
| `cluster/` | Two k3d clusters with UDS Remote Agent + peat-sidecar e2e test |

## Running

```bash
# Prerequisites: peat-sidecar binary
cd test/go

# Smoke test
PEAT_SIDECAR_BIN=../../target/release/peat-sidecar go run ./cmd/smoketest/

# Two-node sync test
PEAT_SIDECAR_BIN=../../target/release/peat-sidecar go run ./cmd/synctest/

# Full stack with real UDS Remote Agent
UDS_AGENT_BIN=/path/to/uds-remote-agent \
PEAT_SIDECAR_BIN=../../target/release/peat-sidecar \
  go run ./cmd/watchertest/

# Query a running sidecar
PEAT_SIDECAR_ADDR=http://localhost:50051 go run ./cmd/query/

# Cross-cluster e2e (creates k3d clusters, deploys, tests, cleans up)
cd cluster && ./setup.sh
```

## Proto Generation

Stubs are pre-generated in `gen/`. To regenerate:

```bash
make generate
```

Requires `protoc`, `protoc-gen-go`, and `protoc-gen-connect-go`.
