# peat CLI — Quickstart

A 5-minute walkthrough from zero to reading/writing documents on a Peat mesh. Stops along the way exercise everything you'll actually use in day-to-day operations.

> **What is `peat`?** A CRUD-shaped operator CLI that joins a Peat mesh as a real node, runs one command, and exits. Same protocol stack as `peat-node` — no admin-side API. See [peat-node ADR-001](../../docs/peat-node-adr-001-peat-cli.md) for the design.

## What you'll need

| | What | Why |
|---|---|---|
| 1 | The `peat` binary | The CLI itself |
| 2 | A credential bundle YAML | Identifies you to the mesh |
| 3 | A reachable peer | Something for the CLI to sync with — can be a local `peat-node`, a compose example, or a remote cluster |

For (1), grab the pre-built binary for your platform from the [GitHub Releases page](https://github.com/defenseunicorns/peat-node/releases). Each release attaches archives for:

| Platform | Archive |
|---|---|
| Linux x86_64 | `peat-<version>-linux-x86_64.tar.gz` |
| Linux aarch64 | `peat-<version>-linux-aarch64.tar.gz` |
| macOS aarch64 (Apple Silicon) | `peat-<version>-macos-aarch64.tar.gz` |
| Windows x86_64 | `peat-<version>-windows-x86_64.zip` |

Each archive contains the `peat` binary, `README.md`, and `LICENSE`. Extract and place the binary somewhere on your `PATH`.

**Build from source** (if you need an unreleased version or a platform not listed above): you'll need a recent stable Rust toolchain — install via [rustup](https://rustup.rs) if you don't have one. Then from the root of this repo:

```sh
cargo install --path crates/peat-cli
# installs to ~/.cargo/bin/peat
```

For (3), if you don't already have a `peat-node` to talk to, the fastest path is the compose example at [`examples/compose/`](../../examples/compose/) — `docker compose up -d && ./bootstrap.sh` boots two sidecars on a single host in about 30 seconds. The rest of this guide assumes that's running.

---

## Step 0 — Offline sanity check (no creds needed)

`peat` ships a registry inspector that runs entirely offline. Use it before anything else to confirm the binary works and to discover what schema-typed collections exist.

```sh
peat schema list
```

Expected output:

```
COLLECTION    TYPE        VERSION  ID
capabilities  Capability  v1       peat.capability.v1.Capability
cell-configs  CellConfig  v1       peat.cell.v1.CellConfig
cell-states   CellState   v1       peat.cell.v1.CellState
node-configs  NodeConfig  v1       peat.node.v1.NodeConfig
node-states   NodeState   v1       peat.node.v1.NodeState
```

Drill into one type:

```sh
peat schema describe capabilities
```

```
Capability (v1)
  id:         peat.capability.v1.Capability
  collection: capabilities
  fields:
    ID          id               text
    Name        name             text
    Type        capability_type  enum[Unspecified|Sensor|Compute|Communication|Mobility|Payload|Emergent]
    Confidence  confidence       percentage
    Metadata    metadata_json    json-string
    Registered  registered_at    timestamp
```

The field column on the right is the `--set` path you'll use in step 3.

---

## Step 1 — Write a credential bundle

`peat` won't join the mesh anonymously. You need a YAML credential bundle that identifies your formation (the group of peers you're authorized to talk to).

Bundle shape:

```yaml
# Required. The formation/app id that peers share. Must match the
# peers' `PEAT_NODE_APP_ID`.
app_id: <your-formation-id>

# Required. 32-byte shared key, base64-encoded. Must match peers.
# Generate a real one with `openssl rand -base64 32`.
shared_key: <base64-32-bytes>

# Optional. Explicit peers to dial in `<endpoint-id>@<host>:<port>` form,
# where host:port is the peer's Iroh UDP socket. Omit entirely if peers
# are discoverable via mDNS — the CLI will find them automatically.
peers:
  - <endpoint-id>@<host>:<udp-port>
```

> **Default location:** `peat` looks for credentials in this order:
> 1. `--creds <PATH>` flag
> 2. `PEAT_CREDS` environment variable (path to the YAML file)
> 3. Platform config dir — checked in order, first match wins:
>    - `$XDG_CONFIG_HOME/peat/credentials.yaml` (if `$XDG_CONFIG_HOME` is set)
>    - `~/Library/Application Support/peat/credentials.yaml` (macOS native)
>    - `~/.config/peat/credentials.yaml` (Linux default; also checked on macOS as a fallback)
>
> Place the file at the platform default and you won't need to pass `--creds` on every invocation.

> **File-permission discipline (ADR-006):** `peat` refuses to read a bundle that is world- or group-readable. `chmod 600` is the path forward.

### Concrete: drive the compose example from inside the container

The [`examples/compose/`](../../examples/compose/) demo only maps the gRPC TCP ports to the host — Iroh UDP stays on the compose bridge network. The cleanest way to drive it with `peat` is to run the CLI *inside* one of the sidecar containers (the `peat-node` image ships `/usr/local/bin/peat`):

```sh
# Write a creds.yaml inside the container — host paths don't help here.
# The peer we're dialing is peat-node-b, so fetch *its* NodeId (via
# Docker DNS to its in-container gRPC on 50051) and pair it with the
# port peat-node-b's iroh actually binds to (51072 in the compose
# example — peat-node-a uses 51071, b uses 51072 to avoid a port
# clash on the shared bridge).
docker exec peat-node-a sh -c 'cat > /tmp/creds.yaml <<EOF
app_id: compose-demo
shared_key: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=
peers:
  - $(curl -s -X POST http://peat-node-b:50051/peat.sidecar.v1.PeatSidecar/GetStatus \
      -H "Content-Type: application/json" -d "{}" | grep -o "\"endpointAddr\":\"[^\"]*\"" | cut -d\" -f4)@peat-node-b:51072
EOF
chmod 600 /tmp/creds.yaml'
```

(`compose-demo` is the `PEAT_NODE_APP_ID` the compose example uses; if you reuse the bundle template elsewhere, swap it for your formation id.)

From here, every `peat …` invocation in the remaining steps runs as `docker exec peat-node-a peat --creds /tmp/creds.yaml …`. To keep the examples below readable I'll write them as plain `peat …`; mentally prepend the `docker exec` prefix.

### Concrete: drive a peat-node you control

If you have your own `peat-node` running on a network the CLI can reach (UDP and TCP both open), write the bundle on the host and supply `--creds ./creds.yaml` directly. The Helm + k3d path in the [container quickstart](../../QUICKSTART.md#path-b--helm--k3d-10-minutes) shows the `kubectl exec` variant of the same pattern.

---

## Step 2 — Read state from the mesh

The simplest mesh-touching command — confirms the handshake works and the local store sees existing documents.

```sh
peat --creds ./creds.yaml query --all-collections --output json
```

`--all-collections` scans every collection reachable with this bundle. The first run on a fresh peer may return `{}` if the mesh is empty; that's a successful empty result, not an error.

Same shape on a specific collection:

```sh
peat --creds ./creds.yaml query contacts
```

Or a specific doc:

```sh
peat --creds ./creds.yaml query contacts/c-1234 --output text
```

---

## Step 3 — Write a document

`create` adds a new document. `--set path=value` builds the document from key/value pairs (works on both arbitrary JSON and peat-schema-registered types).

Arbitrary JSON (any collection name you make up):

```sh
peat --creds ./creds.yaml create contacts \
  --id alice \
  --set name=alice \
  --set rank=1 \
  --wait-for-sync
```

Schema-registered type (peat-schema validates the result):

```sh
peat --creds ./creds.yaml create capabilities \
  --id cap-thermal \
  --set id=cap-thermal \
  --set name=thermal-sensor \
  --set confidence=0.92 \
  --wait-for-sync
```

`--wait-for-sync` blocks until at least one peer has acknowledged the write. Drop it for fire-and-forget.

Read it back:

```sh
peat --creds ./creds.yaml query capabilities/cap-thermal
```

Edit one field:

```sh
peat --creds ./creds.yaml update capabilities/cap-thermal \
  --set confidence=0.98 \
  --wait-for-sync
```

Tombstone it (ADR-034 tombstone semantics — the doc is removed and the deletion syncs):

```sh
peat --creds ./creds.yaml delete capabilities/cap-thermal --wait-for-sync
```

---

## Step 4 — Watch changes live

`observe` opens a subscription and streams events to stdout until you `Ctrl-C`. Default output is line-buffered ndjson — pipe it to `jq` for filtering.

```sh
peat --creds ./creds.yaml observe capabilities --output ndjson | jq .
```

Or across every collection at once:

```sh
peat --creds ./creds.yaml observe --all-collections --output ndjson \
  | jq 'select(.key | startswith("capabilities:"))'
```

In a second terminal, run a `create` from step 3 — you should see the new record appear in the observer's stdout within ~1 second. Same with `delete`: the observer emits `{"key":"…","deleted":true}`.

`peat observe contacts | head -n 5` exits cleanly with status 0 after the consumer closes its end. No `broken pipe` line on stderr.

---

## Step 5 — Common operator patterns

**Round-trip-edit** — fetch a doc, edit it in `jq`, write it back as a minimal delta:

```sh
peat --creds ./creds.yaml query capabilities/cap-thermal --output json \
  | jq '.confidence = 0.99' \
  | peat --creds ./creds.yaml update capabilities/cap-thermal --from - --wait-for-sync
```

The Automerge delta path preserves the doc's operation history — this is *not* the same as deleting and recreating.

**Dry-run a write** — validate locally without joining the mesh:

```sh
peat create capabilities --id cap-x --set name=foo --set id=cap-x --dry-run
```

Prints the canonical operation JSON to stdout. Exit 0 = the write would be valid; exit 4 = schema validation failed (e.g. missing required field).

**In-container debug** — the binary is at `/usr/local/bin/peat` inside the `peat-node` container, so `kubectl exec` reaches a debug surface without an extra sidecar. The chart doesn't auto-mount a CLI credential bundle, so bootstrap one inside the pod first (full recipe in [`crates/peat-cli/README.md` § In-cluster debug surface](README.md#in-cluster-debug-surface)):

```sh
kubectl exec -n peat -it deploy/peat-peat-node -c peat-node -- \
  peat --creds /tmp/creds.yaml query --all-collections --output json
```

---

## Where to next

- **Reference docs**: [`crates/peat-cli/README.md`](README.md) covers every flag, every exit code, the credential schema, output-format contracts, and the troubleshooting matrix.
- **Container quickstart**: [`QUICKSTART.md`](../../QUICKSTART.md) at the repo root walks through standing up `peat-node` itself (the thing the CLI talks to) via Docker Compose or Helm.
- **Design**: [peat-node ADR-001](../../docs/peat-node-adr-001-peat-cli.md) for why the CLI is shaped the way it is.

## Exit codes (quick reference)

| Code | Means | Common cause |
|---|---|---|
| 0 | OK | — |
| 1 | timeout / no peers / generic failure | unreachable peer in `creds.yaml` |
| 2 | authentication failure | missing/unreadable `creds.yaml`, unknown field in the bundle |
| 3 | permission denied | reserved for future per-collection scopes |
| 4 | malformed request | bad target syntax, schema validation failure, duplicate `--id` on `create` |
| 130 | SIGINT | Ctrl-C while streaming |

Data goes to **stdout**; logs, errors, and status to **stderr**. `peat … > file.json` produces a clean file with no log noise.
