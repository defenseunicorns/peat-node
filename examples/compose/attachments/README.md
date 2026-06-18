# Attachment distribution quickstart (PRD-006)

Two compose files live here:

- **`docker-compose.yml`** — single node. Demonstrates sender-side
  ingest + status lookup. `DISTRIBUTION_STATUS_COMPLETED` here is the
  vacuous-zero-peer case (no targets, no real transfer).
- **`docker-compose.two-node.yml`** — A and B peered together.
  Demonstrates **actual file delivery**: files sent from A appear on
  B's filesystem inbox. This is the flow operators care about.

The single-node setup below is the per-size benchmark for sender-side
ingest. **For the real delivery demo, jump to "Two-node delivery"
below.**

> **Uses published `v0.3.1` image by default.** The PRD-006 attachment
> surface first shipped in `v0.2.0`, but `v0.2.x` carries the peat#864
> substrate bug: the sender's `SubscribeAttachmentBundle` stream stalls
> one frame short of terminal on a real cross-peer transfer. `v0.3.0`
> closes that end-to-end (and `v0.3.1` relocates the receive lifecycle
> into peat-protocol with no behavior change), so the two-node delivery
> demo needs `v0.3.0+`.
> Earlier `v0.1.x` images predate PRD-006 entirely and fail with
> `unimplemented: method not found`. To test local changes ahead of a
> release, comment out the `image:` line and uncomment the `build:`
> block in `docker-compose.yml` (or `docker-compose.two-node.yml` for
> the cross-peer demo) to build from the repo root.

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

## Two-node delivery

Real cross-peer file delivery (PRD-006 v1.1, post the inbox-watcher
landing).

```bash
docker compose -f docker-compose.two-node.yml up -d
./peer.sh                                  # bidirectional ConnectPeer
ENDPOINT=http://127.0.0.1:50061 OUTBOX_DIR=outbox-a ./send.sh
ls inbox-b/                                # files appear here per distribution_id
docker compose -f docker-compose.two-node.yml down -v
```

(Pulls `ghcr.io/defenseunicorns/peat-node:v0.4.3`. For testing local
changes, swap the `image:` line for the commented `build:` block in
both services.)

What the two-node setup wires:

- `peat-node-a` (`127.0.0.1:50061`) — sender. `./outbox-a` bind-mounted
  read-only at `/var/lib/peat/outbox`. `--attachment-root outbox=...`
  set; no inbox.
- `peat-node-b` (`127.0.0.1:50062`) — receiver. `./inbox-b`
  bind-mounted read-write at `/var/lib/peat/inbox`. `--attachment-inbox`
  set; the receive-side watcher polls the synced `file_distributions`
  collection every 1s and fetches anything targeting B's iroh endpoint.

`peer.sh` issues `ConnectPeer` in both directions, which is required
for `AllNodes`-scoped distributions: A's `resolve_targets` builds
`target_nodes` from `A.blob_store.known_peers()`, which is populated
only by `ConnectPeer` *into* A. Without A → B as well, A's
distribution doc carries an empty `target_nodes` and B correctly
concludes "not for me."

When `send.sh` is pointed at A, B's inbox accumulates
`inbox-b/{distribution_id}/{filename}` for each successful delivery.
Apps watching the inbox can correlate a `distribution_id` back to the
sender via `GetAttachmentDistribution`.

## Multi-host delivery (separate machines, no mDNS)

The two-node setup above puts both nodes on one Docker network and peers them
with `peer.sh` (a runtime `ConnectPeer` that reads each node's `endpoint_id`
from `GetStatus`). That doesn't work when the nodes are on **different
machines** and you can't reach the remote one to read its output, or when mDNS
is unavailable across subnets.

[`docker-compose.multi-host.yml`](./docker-compose.multi-host.yml) solves this
with **deterministic identity**: a node's iroh `endpoint_id` is
`HKDF-SHA256(shared_key, "iroh:" + node_id)`, so you compute *both* nodes' ids
offline, on one machine, before anything boots — then bake them into each
machine's `PEAT_NODE_PEERS`:

```bash
K="<base64 32-byte shared key>"
A_ID=$(peat-node derive-id --shared-key "$K" --node-id node-a)
B_ID=$(peat-node derive-id --shared-key "$K" --node-id node-b)
# Put $B_ID in machine A's PEAT_NODE_PEERS, $A_ID in machine B's. Done.
```

Each machine runs one node with its own `.env` (node id, shared key, iroh UDP
port, and the peer's derived id @ its IP:port). No `peer.sh`, no `GetStatus`
round-trip, no mDNS. See the header of `docker-compose.multi-host.yml` for the
full per-machine `.env` and the firewall/UDP-publish note, and
[`docs/CONFIGURATION.md` → Deterministic identity](../../../docs/CONFIGURATION.md#deterministic-identity--offline-peer-id-derivation)
for the full reference.

> Deterministic identity + `derive-id` require **peat-node v0.4.3+**. To run
> local changes ahead of a release, use the commented `build:` block in
> `docker-compose.multi-host.yml` to build from the repo root.
