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

For a sidecar already deployed with the `peat-node` Helm chart, mount the operator credentials into the pod and `exec` against the running container:

```sh
# One-shot read.
kubectl exec -n peat -it deploy/peat-node -- peat \
  --creds /etc/peat/credentials.yaml \
  query contacts/c-1234 --output json

# Streaming observe (Ctrl-C to exit; binary handles SIGINT cleanly).
kubectl exec -n peat -it deploy/peat-node -- peat \
  --creds /etc/peat/credentials.yaml \
  observe contacts --output ndjson
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
peers:
  - <endpoint_id>@10.0.0.5:4242
# encryption_key: <base64-32-byte-key>   # accepted by schema but rejected at
# load time; at-rest cipher layering vs. peat-node's app-level encryption is
# being resolved in peat#940. Setting this field today exits 2 with a clear
# error rather than silently bypassing encryption.
```

## Commands

### Read

```sh
# Materialised current state for one collection or doc.
peat query <COLLECTION>[/<DOC_ID>] [--limit N] [--output text|json|ndjson]

# Scan every collection reachable with the credential bundle.
peat query --all-collections [--limit N] [--output text|json|ndjson]
peat query --all              [--limit N] [--output text|json|ndjson]   # short alias

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
peat create <COLLECTION> [--id DOC_ID] (--from PATH|- | --set PATH=VALUE...) \
            [--dry-run] [--wait-for-sync] [--no-validate]

# Upsert: applies path=value updates; creates the doc if missing.
peat update <COLLECTION>/<DOC_ID> --set PATH=VALUE... \
            [--dry-run] [--wait-for-sync]

# Tombstone the doc per ADR-034.
peat delete <COLLECTION>/<DOC_ID> [--wait-for-sync]
```

### Output formats

| Format | Use |
|---|---|
| `text` (default) | Human-readable. Pretty-printed JSON in v1; richer typed renderers land as `peat-schema` exposes a type registry. |
| `json` | Single canonical JSON value. Stable schema for scripts. |
| `ndjson` | One JSON record per line. Natural for `observe \| jq` and log shipping. |

### Exit codes

Per ADR-001 "Shell integration discipline":

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | timeout / no peers / generic failure |
| 2 | authentication failure |
| 3 | permission denied |
| 4 | malformed request (bad target, conflicting flags, duplicate `--id`, …) |
| 130 | SIGINT (Ctrl-C while streaming) |

Data goes to **stdout**; logs, errors, and status to **stderr**. `peat … > file.json` produces a clean file with no log noise.

## Examples

```sh
# Show the current state of a doc.
peat query contacts/c-1234 --output json

# Sweep every collection reachable with the bundle.
peat query --all --output json | jq 'keys'

# Stream every update to the contacts collection as ndjson.
peat observe contacts --output ndjson | jq 'select(.doc.rank > 3)'

# Cross-collection observer — route on the key field.
peat observe --all --output ndjson | jq 'select(.key | startswith("contacts:"))'

# Create a doc from a JSON file.
peat create contacts --id c-1234 --from contact.json --wait-for-sync

# Tweak a single field, leaving everything else alone.
peat update contacts/c-1234 --set rank=2 --wait-for-sync

# Tombstone.
peat delete contacts/c-1234
```

### Round-trip edit

```sh
peat query contacts/c-1234 --output json \
  | jq '.position.lat = 40.7128' \
  | peat update contacts/c-1234 --from -
```

`update --from` computes a minimal Automerge delta against the document's current state and applies only the new changes — the existing operation history is preserved. Updates against a missing doc fall back to initial creation.

## Operational notes

- **Posture.** `peat` joins as a short-TTL observer; it advertises no application capabilities, and other peers can filter it out of routing decisions. Writes are author-stamped with the credential identity (`--as <id>` overrides).
- **Tempdir.** Each invocation gets an ephemeral data dir that is removed on exit. No persistent state survives a CLI run.
- **`--wait-for-sync`.** Approximates per-write peer-acknowledgement with a brief fixed wait. Real ack tracking lands when [peat-mesh](https://github.com/defenseunicorns/peat-mesh) exposes it.

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

**Query returns empty / observe never fires**

`peat` polls its local store after joining. If the seeded data lives on a peer that doesn't push proactively, the CLI's `--timeout` budget may expire before sync drains the doc into the local store. Bump `--timeout` for slow links; confirm the peer is actively syncing (`peat observe <collection>` will show non-zero traffic if it is).

**SIGPIPE behaviour**

`peat observe contacts | head -n 5` exits cleanly with status 0 after the consumer closes its end — no `broken pipe` line on stderr. If you see one, it likely came from the downstream tool, not `peat`.

## Upstream tracking issues

| ID | What | Affects |
|---|---|---|
| [peat#940](https://github.com/defenseunicorns/peat/issues/940) | ADR-006 amendment for the credential bundle format | `peat-cli` ships a placeholder format until this lands |
| [peat#941](https://github.com/defenseunicorns/peat/issues/941) | Per-collection write authorization scopes | Phase 4a's exit-3 path is coarse-grained today |
