# `peat` — operator CLI as a Peat node

Per [peat-node ADR-001](../../docs/peat-node-adr-001-peat-cli.md). `peat` is the operator CLI for a Peat mesh deployment. It joins the mesh as a real Peat node (no admin sidecar API), runs a CRUD-shaped command, and exits.

## Install

`peat` ships in two forms:

- **In the `peat-node` container image** — the binary lives at `/usr/local/bin/peat`. Reach it with `kubectl exec` (or equivalent) for in-cluster debugging.
- **Standalone binary** — attached to each tagged GitHub release for Linux (x86_64, aarch64), macOS (x86_64, aarch64), and Windows (x86_64).

Build from source:

```sh
cargo build --release -p peat-cli
# binary lands at target/release/peat
```

## Credentials

`peat` will not join a mesh without credentials. Resolution chain:

1. `--creds <PATH>` argument
2. `PEAT_CREDS` environment variable (path to YAML)
3. `$XDG_CONFIG_HOME/peat/credentials.yaml` (platform default)

Bundle format (pending formalisation in [peat#940](https://github.com/defenseunicorns/peat/issues/940); `peat-cli` rejects unknown fields strictly):

```yaml
app_id: my-app
shared_key: <base64-formation-key>
peers:
  - <endpoint_id>@10.0.0.5:4242
encryption_key: <base64-32-byte-key>   # optional
```

## Commands

### Read

```sh
# Materialised current state.
peat query <COLLECTION>[/<DOC_ID>] [--limit N] [--output text|json|ndjson]

# Live stream of updates until SIGINT.
peat observe <COLLECTION>[/<DOC_ID>] [--mode latest-only|windowed|full-history]
```

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

`peat update --from <PATH>` is gated on [peat-mesh#187](https://github.com/defenseunicorns/peat-mesh/issues/187) (Automerge delta API). Use `--set` until that lands.

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

# Stream every update to the contacts collection as ndjson.
peat observe contacts --output ndjson | jq 'select(.doc.rank > 3)'

# Create a doc from a JSON file.
peat create contacts --id c-1234 --from contact.json --wait-for-sync

# Tweak a single field, leaving everything else alone.
peat update contacts/c-1234 --set rank=2 --wait-for-sync

# Tombstone.
peat delete contacts/c-1234
```

### Round-trip edit (planned)

```sh
# Currently gated on peat-mesh#187 — listed here so the pattern survives in docs.
peat query contacts/c-1234 --output json \
  | jq '.position.lat = 40.7128' \
  | peat update contacts/c-1234 --from -
```

## Operational notes

- **Posture.** `peat` joins as a short-TTL observer; it advertises no application capabilities, and other peers can filter it out of routing decisions. Writes are author-stamped with the credential identity (`--as <id>` overrides).
- **Tempdir.** Each invocation gets an ephemeral data dir that is removed on exit. No persistent state survives a CLI run.
- **`--wait-for-sync`.** Approximates per-write peer-acknowledgement with a brief fixed wait. Real ack tracking lands when [peat-mesh](https://github.com/defenseunicorns/peat-mesh) exposes it.

## Upstream tracking issues

| ID | What | Affects |
|---|---|---|
| [peat#940](https://github.com/defenseunicorns/peat/issues/940) | ADR-006 amendment for the credential bundle format | `peat-cli` ships a placeholder format until this lands |
| [peat#941](https://github.com/defenseunicorns/peat/issues/941) | Per-collection write authorization scopes | Phase 4a's exit-3 path is coarse-grained today |
| [peat-mesh#187](https://github.com/defenseunicorns/peat-mesh/issues/187) | Automerge delta API | Blocks `update --from` |
