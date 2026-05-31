# peat-node — Quickstart

A 10-minute walkthrough from zero to a running two-node mesh, with documents syncing between peers. Two paths:

- **Path A — Docker Compose** (single host, two sidecars). Easiest. ~3 minutes. Good for a smoke test or local development.
- **Path B — Helm + k3d** (two Kubernetes clusters, sidecars in each). ~10 minutes. Mirrors the production sidecar pattern.

Pick the one that matches what you're trying to validate.

> **Known limitation.** Driving the running mesh from the `peat` CLI in the same container or pod is currently blocked on [peat-mesh#205](https://github.com/defenseunicorns/peat-mesh/issues/205) — `connect_and_authenticate` resolves peer addresses through iroh's `address_lookup` chain in an order that doesn't reach `MemoryLookup`-registered direct addresses in airgapped contexts. The two walkthroughs below validate the sidecar↔sidecar CRDT sync via the production gRPC + Iroh path; `peat schema list` / `peat schema describe` run offline inside the container without dialling the mesh. CLI-driven CRUD against a running sidecar will be restored when peat-mesh#205 ships.

> **What is `peat-node`?** A Rust sidecar that runs alongside an application, participates as a full CRDT mesh node (Automerge + Iroh QUIC), and exposes a gRPC API for the co-located app to read/write shared state. State syncs across clusters automatically. See [`README.md`](README.md) for the architectural overview and [`docs/DESIGN.md`](docs/DESIGN.md) for the integration design.

---

## Path A — Docker Compose (3 minutes)

### Prereqs

- Docker (or Docker Desktop) with `docker compose`
- `curl` and `jq` on the host
- No public-internet egress required — both containers peer over the compose bridge network.

### Run

```sh
cd examples/compose

# Boots two peat-node sidecars (node-a on :50061, node-b on :50062).
# The `-f docker-compose.dev.yml` override builds peat-node:dev locally
# — the `peat` CLI (used in step 4 below) shipped after v0.3.0 and
# isn't in the base compose's pinned image yet. Once a release with
# the CLI publishes, drop the override and bump the base image tag.
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build --wait

# Tells node-b to dial node-a. Idempotent; safe to re-run.
./bootstrap.sh

# Writes `hello/world` on node-a, polls node-b until it appears.
./demo.sh
```

If you don't need the CLI walkthrough below and just want the fastest CRDT-sync demo, omit the `-f docker-compose.dev.yml` override — `docker compose up -d` uses the published image at `:v0.3.0`.

Expected `./demo.sh` output (last few lines):

```
==> Polling node-b for hello/world…
==> Found on node-b after 2 attempts.
==> Document body on node-b:
{ "greeting": "hi from node-a" }
==> CRDT sync verified.
```

The document was authored on node-a and reached node-b over the loopback Iroh QUIC mesh — no central server, no relay. This is the same code path that runs in production.

### Offline schema discovery inside the container

The `peat` binary ships at `/usr/local/bin/peat` in the sidecar image. Two subcommands run **offline** (no creds, no mesh dial) and are useful for confirming the build and discovering registered types:

```sh
# Enumerate every peat-schema type the CLI knows about.
docker exec peat-node-a peat schema list

# Drill into one type's field shape.
docker exec peat-node-a peat schema describe capabilities
```

Driving the running mesh through the CLI from inside the container (`peat query`, `peat create`, etc. against a sidecar) is currently blocked on [peat-mesh#205](https://github.com/defenseunicorns/peat-mesh/issues/205); the CLI commands themselves work and the offline subcommands cover the discovery half of the operator workflow.

### Teardown

```sh
docker compose -f docker-compose.yml -f docker-compose.dev.yml down -v
```

### Automated end-to-end check

The full Path A walkthrough (every command in this section) is encoded in [`test/quickstart-compose.sh`](test/quickstart-compose.sh) — run it to verify the walkthrough works against your local checkout:

```sh
./test/quickstart-compose.sh
```

Each step is asserted; failure on any step is a doc/code drift signal. CI runs this on every push, so the QUICKSTART above stays honest.

---

## Path B — Helm + k3d (10 minutes)

Two Kubernetes clusters on a shared Docker network, each running a `peat-node` sidecar, peered for CRDT sync across cluster boundaries. This is the test harness CI uses (`test/cross-cluster-sync.sh`) so it's already battle-tested.

### Prereqs

- `k3d`, `helm`, `kubectl`, `docker`, and `python3` on the host.
- ~2 GB of free Docker memory budget.

### Run

```sh
# Builds the image (if needed), creates two k3d clusters on a shared
# network, deploys peat-node to each, peers them, runs a CRDT-sync
# verification, and tears the clusters down.
./test/cross-cluster-sync.sh
```

The script is idempotent in parts and split into phases so you can stop after deploy:

```sh
# Just stand up the clusters + sidecars, no test run, no teardown.
./test/cross-cluster-sync.sh create

# Run the CRUD-sync test against the already-running clusters.
./test/cross-cluster-sync.sh test

# Tear everything down when you're done.
./test/cross-cluster-sync.sh cleanup
```

### What you have after `create`

Two k3d clusters — `peat-sync-alpha` and `peat-sync-bravo` — each with a `peat-node` deployment in the `peat` namespace, peered via direct Iroh UDP on the shared `peat-mesh-net` Docker network.

Inspect either side:

```sh
# Cluster alpha
kubectl --context k3d-peat-sync-alpha get pods -n peat
kubectl --context k3d-peat-sync-alpha logs -n peat -l app.kubernetes.io/name=peat-node -c peat-node

# Cluster bravo
kubectl --context k3d-peat-sync-bravo get pods -n peat
```

### Offline schema discovery inside the pod

The `peat` binary ships inside the `peat-node` container at `/usr/local/bin/peat`. Two subcommands run **offline** (no creds, no mesh dial) and are useful for confirming the build and discovering registered types:

```sh
kubectl --context k3d-peat-sync-alpha exec -n peat deploy/peat-peat-node -c peat-node -- \
  peat schema list

kubectl --context k3d-peat-sync-alpha exec -n peat deploy/peat-peat-node -c peat-node -- \
  peat schema describe capabilities
```

Driving the running mesh through the CLI from inside the pod (`peat query`, `peat create`, etc. against the sidecar at `localhost:51071`) is currently blocked on [peat-mesh#205](https://github.com/defenseunicorns/peat-mesh/issues/205) — same root cause as Path A. Sidecar-to-sidecar CRDT sync via the production gRPC + Iroh path (what `cross-cluster-sync.sh` Tests 1-5 verify) works in this scenario; CLI-driven CRUD will be restored when peat-mesh#205 ships.

> **Helm release / deployment naming.** The chart's `fullname` is `<release>-<chart>` — with release name `peat` it produces `peat-peat-node`. Adjust if you installed with a different release name.

### Teardown

```sh
./test/cross-cluster-sync.sh cleanup
```

### Automated end-to-end check

`./test/cross-cluster-sync.sh all` is the executable contract for Path B — it spins up both clusters and runs Tests 1-5 against the production gRPC + Iroh path (peer connectivity, document sync, fleet convergence, sync-byte counters). The `Cross-cluster sync` GitHub Actions workflow runs the same script on every PR that touches `chart/`, `src/`, `Cargo.toml`/`Cargo.lock`, or the script itself. The CLI-in-pod workflow (formerly Test 6) is held out pending peat-mesh#205.

---

## Production deployment (beyond the quickstart)

When you're past the smoke-test stage and ready to deploy peat-node alongside a real application:

- **Helm chart**: [`chart/peat-node/`](chart/peat-node/) — includes injectable templates for adding peat-node as a sidecar container to any pod. See [`README.md` § Deployment](README.md#deployment).
- **Zarf package**: `zarf package create .` builds an offline-installable bundle for air-gapped clusters.
- **UDS bundle**: [`bundle/uds-bundle.yaml`](bundle/uds-bundle.yaml) wraps the Helm chart in a UDS Package CR with the NetworkPolicies for Iroh QUIC mesh traffic.
- **Configuration**: [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md) — every flag, every `PEAT_NODE_*` env var, every chart value.
- **Credentials at scale**: ADR-006 covers the formation key + bundle file format. The compose example's zero-byte key is a test-only convenience — generate a real one with `openssl rand -base64 32`.

## Talking to peat-node from your application

The sidecar exposes `peat.sidecar.v1.PeatSidecar` on port 50051 — Connect RPC over HTTP/2, so any gRPC / Connect-compatible client works. See [`proto/sidecar.proto`](proto/sidecar.proto) for the full surface and [`examples/compose/README.md#talking-to-a-peat-node-from-your-own-service`](examples/compose/README.md) for client-side patterns.

The 25 RPCs split into five categories:

| Category | What it covers |
|---|---|
| Lifecycle | `GetStatus` (node id, endpoint id, peer count) |
| Peers | `ConnectPeer`, `DisconnectPeer`, `ListPeers` |
| Documents | `PutDocument`, `GetDocument`, `DeleteDocument`, `ListDocuments` |
| Subscriptions | `Subscribe` (server-streaming, every change to a collection or doc) |
| Sync control | `StartSync`, `StopSync`, `GetSyncStats` |

Plus typed convenience RPCs over peat-schema collections (`PutPlatform`, `GetCells`, etc.) and attachment-distribution RPCs (PRD-006, disabled by default).

## Common operator patterns

**Verify CRDT sync between two nodes**: write on one, query on the other with `--timeout 30s` to ride out cold-link warm-up:

```sh
# From any peer with the right creds
peat --creds creds.yaml create contacts --id smoke --set ts=$(date +%s) --wait-for-sync
peat --creds creds.yaml query contacts/smoke --output json
```

**Watch a single collection across the mesh**:

```sh
peat --creds creds.yaml observe deployments --output ndjson | jq .
```

**Sweep the entire reachable store** (useful when debugging "where is this doc?"):

```sh
peat --creds creds.yaml query --all-collections --output json | jq 'keys'
```

**Connect to a peer that wasn't in the original bundle**:

```sh
# Via the sidecar's gRPC surface (curl + JSON works as well as a real client)
curl -X POST http://localhost:50051/peat.sidecar.v1.PeatSidecar/ConnectPeer \
  -H 'Content-Type: application/json' \
  -d '{"endpointId":"…","addresses":["host:port"]}'
```

## Where to next

- **CLI quickstart**: [`crates/peat-cli/QUICKSTART.md`](crates/peat-cli/QUICKSTART.md) — the operator side, end-to-end.
- **CLI reference**: [`crates/peat-cli/README.md`](crates/peat-cli/README.md) — every flag, every exit code, the troubleshooting matrix.
- **Architecture**: [`docs/DESIGN.md`](docs/DESIGN.md) — how peat-node fits into UDS Fleet Management and the DDIL story.
- **CLI design**: [peat-node ADR-001](docs/peat-node-adr-001-peat-cli.md) — why the CLI is shaped the way it is.

## Troubleshooting

**`docker compose up` succeeds but `./demo.sh` times out polling node-b.**
Check `docker compose logs node-a node-b` for an Iroh handshake failure. If you see `relay connection refused`, the compose network may be blocked — try `docker network prune` and re-run.

**`./test/cross-cluster-sync.sh` fails with `context deadline exceeded` on Helm install.**
Pod isn't going Ready within 90s. The script now captures `kubectl describe pods` / events / container logs on failure; check the workflow output for the actual cause. Usually a missing `protoc` on the build host or a stale `peat-node:dev` image — `docker rmi peat-node:dev && ./test/cross-cluster-sync.sh` forces a rebuild.

**`peat … query` exits 1 with `no peers reachable`.**
The endpoint id in `creds.yaml` doesn't match what the peer is advertising, or the peer's Iroh UDP port isn't reachable from your host. `GetStatus` on the peer prints the current endpoint id; cross-check.

**`peat … create capabilities` exits 4 with `schema validation failed`.**
You omitted a required field. Run `peat schema describe capabilities` to see the field shape and required formats. Note that scalar fields default to their proto3 zero, but `id` and `name` are required-non-empty by the validator.
