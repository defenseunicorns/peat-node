# `peat` — operator CLI as a Peat node

Per [peat-node ADR-001](../../docs/peat-node-adr-001-peat-cli.md). `peat` is the operator CLI for a Peat mesh deployment. It joins the mesh as a real Peat node (no admin sidecar API), runs a CRUD-shaped command, and exits.

## Install

`peat` ships in two forms:

- **In the `peat-node` container image** — the binary lives at `/usr/local/bin/peat`. Reach it with `kubectl exec` (or equivalent) for in-cluster debugging.
- **Standalone binary** — attached to each tagged GitHub release for Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64). Download the matching archive, verify its SHA-256, extract, and place `peat` on your `PATH`.

Build from source:

```sh
cargo build --release -p peat-cli
# binary lands at target/release/peat
```

### In-cluster debug surface

For a sidecar already deployed with the `peat-node` Helm chart, write a CLI credential bundle inside the pod (the chart doesn't auto-mount one — the sidecar reads its formation config from env vars) and `exec` against the running container. The chart's default deployment name is `<release>-peat-node` (e.g. `peat-peat-node` for a release named `peat`).

```sh
# Bootstrap a creds.yaml inside the pod, pointing the CLI at the
# sidecar's own loopback endpoint. Adjust app_id / shared_key to match
# whatever the chart was installed with.
kubectl exec -n peat deploy/peat-peat-node -c peat-node -- sh -c '
  cat > /tmp/creds.yaml <<EOF
app_id: <your-formation-id>
shared_key: <base64-32-bytes>
peers:
  - $(curl -s -X POST http://localhost:50051/peat.sidecar.v1.PeatSidecar/GetStatus \
      -H "Content-Type: application/json" -d "{}" \
      | grep -o "\"endpointAddr\":\"[^\"]*\"" | cut -d\" -f4)@localhost:51071
EOF
  chmod 600 /tmp/creds.yaml'

# One-shot read.
kubectl exec -n peat -it deploy/peat-peat-node -c peat-node -- \
  peat --creds /tmp/creds.yaml query contacts/c-1234

# Streaming observe (Ctrl-C to exit; binary handles SIGINT cleanly).
kubectl exec -n peat -it deploy/peat-peat-node -c peat-node -- \
  peat --creds /tmp/creds.yaml observe contacts
```

`peat` joins as an ephemeral observer node — no persistent state survives the `exec` invocation, presence records carry a short TTL, and the mesh does not plan around the CLI as a durable participant.

## Credentials

`peat` will not join a mesh without credentials. Resolution chain:

1. `--creds <PATH>` argument
2. `PEAT_CREDS` environment variable (path to YAML)
3. `$XDG_CONFIG_HOME/peat/credentials.yaml` (platform default)

### Bundle format

Pending formalisation in [peat#940](https://github.com/defenseunicorns/peat/issues/940). `peat-cli` rejects unknown fields strictly, so today's shape is the source of truth until the upstream ADR-006 amendment lands. **You will need to migrate when peat#940 ships** — track the issue.

Getting a bundle today: the `app_id` is your formation identifier; the `shared_key` is the base64-encoded 32-byte formation key that the rest of the mesh uses (the same value passed to `peat-node` via `--shared-key`). For a fresh deployment, generate one with:

```sh
openssl rand -base64 32
```

…and distribute the same value to every node that participates in the formation, including this CLI.

```yaml
app_id: my-app
shared_key: <base64-formation-key>

# Optional: explicit peers in <endpoint_id>@<host>:<port> form.
# Not needed when peers are on the same host/LAN — mDNS discovers them.
peers:
  - <endpoint_id>@10.0.0.5:4242

# Optional: persist the local store across CLI restarts.
data_dir: ~/.local/share/peat/my-app

# Optional: disable mDNS peer discovery (for containers where multicast
# is unavailable). Default: mDNS is enabled.
# disable_mdns: true

# encryption_key: <base64-32-byte-key>   # accepted by schema but rejected at
# load time; at-rest cipher layering vs. peat-node's app-level encryption is
# being resolved in peat#940. Setting this field today exits 2 with a clear
# error rather than silently bypassing encryption.
```

### mDNS zero-config peer discovery

`peat` processes with the same `app_id` and `shared_key` on the same host or LAN discover each other automatically via mDNS (`_peat._udp.local.`). The formation key HMAC validates that only same-formation peers connect. The `peers:` list is therefore optional — omit it entirely for local workflows.

Set `disable_mdns: true` in the bundle to opt out. This is necessary in containers where multicast routing is typically unavailable.

## Commands

### Read

```sh
# Materialised current state for one collection or doc.
peat query <COLLECTION>[/<DOC_ID>] [--limit N] [--output json|text|ndjson]

# Scan every collection reachable with the credential bundle.
peat query --all-collections [--limit N] [--output json|text|ndjson]
peat query --all              [--limit N] [--output json|text|ndjson]   # short alias

# Live stream of updates until SIGINT.
peat observe <COLLECTION>[/<DOC_ID>] [--mode latest-only|windowed|full-history]

# Live stream across every collection.
peat observe --all-collections [--mode latest-only|windowed|full-history]
peat observe --all             [--mode latest-only|windowed|full-history]
```

Exactly one of `<TARGET>` or `--all-collections` is required; combining them is a parse-time error.

### Write

```sh
# Strict-create: errors if the doc already exists.
# <DOC_ID> is optional — omit for an auto-generated UUID.
# For schema-registered types, the `id` field is auto-injected from <DOC_ID>.
peat create <COLLECTION>[/<DOC_ID>] (--from PATH|- | --set PATH=VALUE...) \
            [--dry-run] [--wait-for-sync] [--no-validate]

# Upsert: applies path=value updates; creates the doc if missing.
peat update <COLLECTION>/<DOC_ID> (--from PATH|- | --set PATH=VALUE...) \
            [--dry-run] [--wait-for-sync] [--no-validate]

# Tombstone the doc per ADR-034.
peat delete <COLLECTION>/<DOC_ID> [--wait-for-sync]
```

`--wait-for-sync` works with both explicit peers and mDNS-discovered peers.

### Schema discovery

```sh
# List every peat-schema type the CLI knows about.
peat schema list [--output text|json|ndjson]

# Describe one type's field-level shape (label, format, proto field name).
peat schema describe <COLLECTION | TYPE_ID>
```

Both run offline — no credential bundle required. Useful to audit which types are registered before staging a write, or to discover field names + formats for a `--set` payload. Address by collection (`capabilities`) or canonical id (`peat.capability.v1.Capability`); a typo returns exit 4 with `no registered type matches`.

Schema commands produce human-readable text output by default regardless of the global `--output` setting — they are offline inspectors, not data pipelines. Pass `--output json` explicitly if you need machine-readable schema output.

### Output formats

| Format | Use |
|---|---|
| `json` **(default)** | Single canonical JSON value. Stable schema for scripts. Pipe directly to `jq`. |
| `text` | Human-readable labeled output. Pass `--output text` for terminal-friendly display. |
| `ndjson` | One JSON record per line, with a `key` field: `{"key": "capabilities:cap-1", "doc": {...}}`. Natural for `observe \| jq` and log shipping; the `key` field is useful for CDC or multi-collection streams. |

### Exit codes

Per ADR-001 "Shell integration discipline":

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | timeout / no peers / generic failure |
| 2 | authentication failure |
| 3 | permission denied |
| 4 | malformed request (bad target, conflicting flags, doc already exists on `create`, …) |
| 130 | SIGINT (Ctrl-C while streaming) |

Data goes to **stdout**; logs, errors, and status to **stderr**. `peat … > file.json` produces a clean file with no log noise.

## Examples

```sh
# Show the current state of a doc (JSON by default).
peat query contacts/c-1234

# Pipe directly to jq — no --output flag needed.
peat query contacts/c-1234 | jq '.name'

# Sweep every collection reachable with the bundle.
peat query --all | jq 'keys'

# Stream every update to the contacts collection.
peat observe contacts | jq 'select(.doc.rank > 3)'

# Cross-collection observer — route on the key field.
peat observe --all | jq 'select(.key | startswith("contacts:"))'

# Create a doc with a specific id (slash syntax).
peat create contacts/c-1234 --from contact.json --wait-for-sync

# Create a doc with an auto-generated UUID.
peat create contacts --set name=alice --wait-for-sync

# Create a schema-registered doc — id is auto-injected from the doc_id.
peat create capabilities/cap-thermal --set name=thermal-sensor --set confidence=0.92 --wait-for-sync

# Tweak a single field, leaving everything else alone.
peat update contacts/c-1234 --set rank=2 --wait-for-sync

# Tombstone.
peat delete contacts/c-1234
```

### Round-trip edit

```sh
peat query contacts/c-1234 \
  | jq '.position.lat = 40.7128' \
  | peat update contacts/c-1234 --from -
```

`update --from` computes a minimal Automerge delta against the document's current state and applies only the new changes — the existing operation history is preserved. Updates against a missing doc fall back to initial creation.

## Operational notes

- **Posture.** `peat` joins as a short-TTL observer; it advertises no application capabilities, and other peers can filter it out of routing decisions. Writes are author-stamped with the credential identity (`--as <id>` overrides).
- **Tempdir.** Each invocation without `--data-dir` gets an ephemeral data dir that is removed on exit. No persistent state survives a CLI run unless `data_dir` is configured.
- **`--wait-for-sync`.** Approximates per-write peer-acknowledgement with a brief fixed wait. Works with both explicit peers and mDNS-discovered peers. Real ack tracking lands when [peat-mesh](https://github.com/defenseunicorns/peat-mesh) exposes it.
- **`observe` deduplication.** `peat observe` emits only when document content actually changes. The Automerge multi-hop sync protocol internally exchanges 2-3 messages; only distinct document states produce output events.

## Troubleshooting

Errors print on stderr with the exit code controlling the response. A few patterns operators hit early:

**Exit 2 — `authentication failure: could not read credentials file …`**

The resolution chain found a path but couldn't open it. Confirm the path is correct, the file exists, and the process has read permission. `peat` does not silently fall back to anonymous join — a missing creds file is fatal.

**Exit 2 — `authentication failure: could not parse credentials file …: unknown field …`**

The bundle has a key the schema doesn't recognise. The schema is intentionally narrow ([peat#940](https://github.com/defenseunicorns/peat/issues/940)) — remove the unknown key or migrate when the upstream amendment lands.

**Exit 2 — `… peat-cli does not yet apply [encryption_key] …`**

The bundle sets `encryption_key`, but the at-rest cipher path is still being resolved upstream. Remove the field to proceed; the ephemeral tempdir backing each CLI invocation lives only for the duration of the command.

**Exit 1 — `no peers reachable (configured: N)`**

`peat` joined as a fresh node and could not reach any peer in `creds.peers`. Check:
- Peer endpoint id matches what `peat-node` advertises (`GET /status` on the sidecar's gRPC surface, field `endpointAddr`).
- The `host:port` resolves and the host is reachable on UDP (Iroh transport).
- `--timeout` is long enough — default `10s` is brief on cold links; pass `--timeout 60s` for slow networks.

If you're relying on mDNS rather than explicit peers, confirm that `disable_mdns` is not set and that multicast is available on the interface (mDNS does not work in most container networking configurations).

**Local store is locked**

```
the local store at /path/to/data is locked — another `peat` process is likely running. Stop it and retry.
```

Only one `peat` process can hold a redb store at a time. When using `observe` with `--data-dir`, writer processes must use a different `--data-dir` or omit it entirely (ephemeral). Stop the conflicting process and retry.

**Query returns empty / observe never fires**

`peat` polls its local store after joining. If the seeded data lives on a peer that doesn't push proactively, the CLI's `--timeout` budget may expire before sync drains the doc into the local store. Bump `--timeout` for slow links; confirm the peer is actively syncing (`peat observe <collection>` will show non-zero traffic if it is).

**SIGPIPE behaviour**

`peat observe contacts | head -n 5` exits cleanly with status 0 after the consumer closes its end — no `broken pipe` line on stderr. If you see one, it likely came from the downstream tool, not `peat`.

## Upstream tracking issues

| ID | What | Affects |
|---|---|---|
| [peat#940](https://github.com/defenseunicorns/peat/issues/940) | ADR-006 amendment for the credential bundle format | `peat-cli` ships a placeholder format until this lands |
| [peat#941](https://github.com/defenseunicorns/peat/issues/941) | Per-collection write authorization scopes | Phase 4a's exit-3 path is coarse-grained today |
