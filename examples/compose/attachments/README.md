# Attachment distribution quickstart (PRD-006)

## Two-minute quick start

`node-a` and `node-b` are **two separate compose projects** simulating two
devices. Each `docker compose up` would otherwise create its own isolated
network, so the two containers could never reach each other. They instead join
a **shared external network** (`peat-mesh`) and dial each other by container
name — which is why that network must be created **first**, before either node
boots. (See ["Why a shared external network"](#why-a-shared-external-network)
below.)

```bash
# Step 1 — create the shared network both nodes attach to (once)
docker network create peat-mesh
```

Then start each node from its own directory. Open two terminals:

```bash
# Terminal 1 — sender (node A)
cd node-a && mkdir -p outbox
docker compose up -d

# Terminal 2 — receiver (node B)
cd node-b && mkdir -p inbox
docker compose up -d
```

Then drop a file — it auto-delivers:

```bash
cp myfile.txt node-a/outbox/
ls node-b/inbox/          # appears here within seconds
```

Teardown (remove the network last, after both nodes are down):
```bash
(cd node-a && docker compose down -v)
(cd node-b && docker compose down -v)
docker network rm peat-mesh
```

No `peer.sh`. No `send.sh`. Peering is pre-configured via `PEAT_NODE_PEERS`
(deterministic endpoint IDs derived from the shared key + node IDs). The outbox
watcher auto-distributes any file dropped in `node-a/outbox/`.

**Expected startup log noise:** both nodes dial each other simultaneously at
boot. You may see an `ERROR peat_node: failed to connect to peer … after 3
attempts` in the first ~15 seconds — this is a red herring. The connection
succeeds immediately after via the peer's simultaneous inbound dial. Look for
`INFO peat_node::node: connected to peer` to confirm peering. Once you see
that, file delivery works normally.

**Alternative (both nodes in one compose project):** `docker-compose.two-node.yml`
— same zero-friction approach but both nodes share a single Docker network
automatically, so there's no separate `docker network create` step.

### Why a shared external network

Each `docker compose up` creates a default network named after its project
(`node-a_default`, `node-b_default`) — **isolated** bridge networks with no
route between them and per-network DNS, so `node-a` can't even resolve or reach
`node-b`. That isolation is the whole point of Compose networks; it's also why
two independent projects can't talk by default.

A `peat-mesh` network declared `external: true` in both files is the fix: an
externally-created network that neither project owns, that both attach to. Now
both containers sit on **one subnet** and route directly to each other (no host
gateway, no published UDP ports, no NAT) — the faithful "two devices on the same
LAN" model. Because it's `external`, Compose won't create it, so it must exist
before either `up` (and you remove it manually after both are down).

> An earlier revision bridged the two isolated networks via
> `host.docker.internal` + published UDP ports. That fails on Docker Desktop:
> its userspace proxy doesn't cleanly forward the iroh QUIC/UDP handshake
> between networks, so the dial reaches a foreign endpoint and the TLS handshake
> fails with `error 48: invalid peer certificate: UnknownIssuer`. The shared
> network removes the host hop entirely.

---

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

> **All compose files here pin `v0.4.8`** (the latest release), which satisfies
> every attachment feature in the table below. To test local changes ahead of a
> release, comment out the `image:` line and uncomment the `build:` block in any
> of the compose files to build from the repo root.

### Attachment feature version minimums

| Capability | Min version | Notes |
|---|---|---|
| PRD-006 attachment RPCs (`SendAttachments`, status lookup) | `v0.2.0` | `v0.1.x` predates PRD-006 and fails with `unimplemented: method not found`. |
| Reliable cross-peer delivery | `v0.3.0` | `v0.2.x` carries the peat#864 substrate bug — the sender's `SubscribeAttachmentBundle` stream stalls one frame short of terminal on a real transfer. `v0.3.1` relocated the receive lifecycle into peat-protocol (no behavior change). |
| Deterministic identity + `derive-id` (multi-host peering) | `v0.4.4` | Offline peer-id derivation; see [Multi-host delivery](#multi-host-delivery-separate-machines-no-mdns) below. |
| Hands-off outbox watcher (`PEAT_NODE_ATTACHMENT_OUTBOX_WATCH`) | `v0.4.5` | Auto-distributes any stable new file dropped in an outbox root — no `SendAttachments` call. |
| Inbox mirrors the sender's outbox layout | `v0.4.8` | [#173](https://github.com/defenseunicorns/peat-node/issues/173); earlier images nested every delivery under `inbox/{distribution_id}/{filename}`. |

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
mkdir -p outbox-a inbox-b
docker compose -f docker-compose.two-node.yml up -d
cp myfile.txt outbox-a/   # PEAT_NODE_ATTACHMENT_OUTBOX_WATCH auto-distributes
ls inbox-b/               # files mirror the sender's outbox layout
docker compose -f docker-compose.two-node.yml down -v
```

(Pulls `ghcr.io/defenseunicorns/peat-node:v0.4.8`. For testing local
changes, swap the `image:` line for the commented `build:` block in
both services.)

Peering is pre-configured via `PEAT_NODE_PEERS` using deterministic endpoint
IDs (derived offline from the shared key + node IDs — same mechanism as
[multi-host delivery](#multi-host-delivery-separate-machines-no-mdns)). No
`peer.sh` step, no `GetStatus` round-trip. `PEAT_NODE_ATTACHMENT_OUTBOX_WATCH`
eliminates `send.sh` — any stable file written to the outbox triggers an
automatic `AllNodes` distribution.

What the two-node setup wires:

- `peat-node-a` (`127.0.0.1:50061`) — sender. `./outbox-a` bind-mounted
  read-only at `/var/lib/peat/outbox`. `PEAT_NODE_ATTACHMENT_ROOT outbox=...`
  set; `PEAT_NODE_ATTACHMENT_OUTBOX_WATCH=true`; no inbox.
- `peat-node-b` (`127.0.0.1:50062`) — receiver. `./inbox-b`
  bind-mounted read-write at `/var/lib/peat/inbox`. `PEAT_NODE_ATTACHMENT_INBOX`
  set; the receive-side watcher polls the synced `file_distributions`
  collection every 1s and fetches anything targeting B's iroh endpoint.

Each delivered file lands in B's inbox **mirroring the sender's outbox
layout**: `outbox-a/hello.txt` arrives at `inbox-b/hello.txt`, byte-identical,
latest-wins on re-delivery. Apps watching the inbox can still correlate a
delivery back to the sender via `GetAttachmentDistribution` — the
`distribution_id` travels in the synced `file_distributions` doc and the
receive-side log line, not in the on-disk path.

`peer.sh` is still present for manual-peering workflows (e.g. ad-hoc
`ConnectPeer` calls, scripted testing against arbitrary nodes). It is not
needed for the two-node compose above.

> **Inbox layout changed in v0.4.8 ([#173](https://github.com/defenseunicorns/peat-node/issues/173)).**
> Earlier images nested every delivery under `inbox-b/{distribution_id}/{filename}`;
> v0.4.8+ mirrors the sender's outbox path instead. A sender-supplied name that
> can't be safely resolved (absolute, or containing `..`) falls back to a flat
> `{distribution_id}.bin` at the inbox root.

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
round-trip, no mDNS.

> **Both machines must list each other** (`A_ID` in B's `PEAT_NODE_PEERS`, `B_ID`
> in A's) — and this is mandatory, not symmetry for its own sake. A node's
> `known_peers` is populated *only when it dials out*; accepting an inbound
> connection doesn't register the peer. Attachment delivery reads `known_peers`
> on both ends — the **sender's** set decides who a distribution targets, the
> **receiver's** set is where it fetches the blob. List only one side and the
> distribution *document* still syncs (CRDT gossip is transitive) but the file
> is never written: the "synced but nothing delivered" symptom. One iroh QUIC
> connection carries the bytes either way, so this is two *dials*, not two
> connections. The `peer status` log line (`connected_peers` vs `known_peers`,
> every 30s) shows whether each side actually dialed the other. The requirement
> for adjacent peers is tracked for removal upstream in
> [peat-node#170](https://github.com/defenseunicorns/peat-node/issues/170).

See the header of `docker-compose.multi-host.yml` for the
full per-machine `.env` and the firewall/UDP-publish note, and
[`docs/CONFIGURATION.md` → Deterministic identity](../../../docs/CONFIGURATION.md#deterministic-identity--offline-peer-id-derivation)
for the full reference.

> Deterministic identity + `derive-id` require **peat-node v0.4.4+**. To run
> local changes ahead of a release, use the commented `build:` block in
> `docker-compose.multi-host.yml` to build from the repo root.

### Attachment delivery across hosts

The multi-host nodes above sync CRDT documents. To also deliver **files**
(PRD-006), give the **sender** an outbox and the **receiver** an inbox — the
matching opt-in lines are stubbed in `docker-compose.multi-host.yml`:

- Sender: set `PEAT_NODE_ATTACHMENT_ROOT=outbox=/var/lib/peat/outbox` and mount
  a host dir read-only at `/var/lib/peat/outbox`.
- Receiver: set `PEAT_NODE_ATTACHMENT_INBOX=/var/lib/peat/inbox` and mount a
  writable host dir at `/var/lib/peat/inbox`.

Send with `SendAttachments` (scope `allNodes`); the receiver's inbox watcher
fetches the blob over iroh and writes it to `{inbox}/{relative_path}` —
mirroring the sender's outbox layout (v0.4.8+; see the [#173](https://github.com/defenseunicorns/peat-node/issues/173)
note under "Two-node delivery" above) — byte-identical to the source.

**Hands-off (synced-folder) mode.** Set `PEAT_NODE_ATTACHMENT_OUTBOX_WATCH=true`
on the sender (requires v0.4.5+) and you don't call `SendAttachments` at all:
the sender's **outbox watcher** auto-distributes any stable new file in its
root, and the receiver's inbox watcher writes it — drop a file in the outbox,
it appears in the peer's inbox. The receive side is already automatic; this adds
the symmetric send side. Both are pure-polling (no inotify), reliable across
container bind mounts.

> ⚠️ A `COMPLETED` status with **no connected peers** is the *vacuous*
> zero-target case — nothing was transferred. Confirm
> `GetStatus.connectedPeers >= 1` before trusting delivery.

This exact flow (outbox → inbox, sha256-validated on the receive side) runs in
CI via [`test/attachment-delivery-compose.sh`](../../../test/attachment-delivery-compose.sh)
— the regression guard for "passes tests but delivers nothing."
