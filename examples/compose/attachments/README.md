# Attachment distribution quickstart (PRD-006)

Single-node `peat-node` with the attachment surface enabled. Sends a
local file through `SendAttachments`, then verifies the distribution
with `GetAttachmentDistribution`.

> **Image version requirement.** This quickstart needs a `peat-node`
> image that contains the PRD-006 attachment RPCs — i.e. the first
> release tagged at or after the PR that lands the attachment surface.
> The compose file pins `ghcr.io/defenseunicorns/peat-node:v0.1.1` as a
> placeholder; bump that tag to the actual release before sharing this
> quickstart externally. To run against a local build, uncomment the
> `build:` block in `docker-compose.yml` and comment out `image:`.

The two-node CRDT sync demo lives one directory up at
[`../docker-compose.yml`](../docker-compose.yml); this one is the
smallest possible attachment-only example.

## Run it

```bash
docker compose up -d
./send.sh                    # ingests outbox/hello.txt
docker compose logs peat-node # see the attachment events
docker compose down -v
```

`send.sh` reads `outbox/hello.txt`, computes its sha256 + size, POSTs a
`SendAttachments` request via the Connect JSON wire, and prints the
response. It then calls `GetAttachmentDistribution` to confirm the
bundle reached its terminal state (here, COMPLETED — zero peers means
the watcher's initial-status shortcut fires immediately).

## What's configured

`docker-compose.yml` sets one `--attachment-root` and accepts every
other PRD-006 default:

```yaml
PEAT_NODE_ATTACHMENT_ROOT: outbox=/var/lib/peat/outbox
```

The host directory `./outbox` is bind-mounted (read-only) into the
container at `/var/lib/peat/outbox`. Drop additional files into
`./outbox/` to attach them — they're addressable from
`SendAttachments` as `root_name=outbox` + `relative_path=<filename>`.

Without this env var, the four attachment RPCs return `Unimplemented` —
the PRD-006 safety default operators opt out of by naming the readable
roots.

## What gets exercised end-to-end

- **Wire encoding.** The Connect JSON shape (camelCase fields, base64
  for the `sha256` bytes field, the `scope` oneof as
  `{"allNodes":{}}`).
- **Path validation.** `outbox/hello.txt`'s resolved path stays inside
  the canonicalised root.
- **Streaming ingest.** Tee-style hash + iroh content-address
  (`create_blob_from_stream`).
- **Hash verification.** The declared sha256 matches the stream's
  computed sha256.
- **Distribution document creation.** `IrohFileDistribution::distribute`
  publishes the record under `file_distributions` (Automerge).
- **Status lookup.** `GetAttachmentDistribution(distribution_id)`
  resolves through the registry's reverse index and the runtime's
  per-distribution state.
- **Retention background task.** Default 24h — eviction sweeps once a
  minute. Override to a short value via
  `PEAT_NODE_ATTACHMENT_HANDLE_RETENTION_SECS` if you want to see the
  bundle age out before `docker compose down -v`.

## Multi-peer

Receiving peers can't auto-pull blobs in v1 — the receive-side observer
hooks in `peat-protocol` are a v2 follow-up. The substrate works
(`NetworkedIrohBlobStore::fetch_blob` resolves across connected peers),
but the trigger that would call it on receivers from a synced
distribution document is deferred. See
[`../../tests/attachments_deferred_test.rs`](../../../tests/attachments_deferred_test.rs)
for the test inventory tracking that gap.
