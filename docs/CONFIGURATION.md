# peat-node configuration reference

Every flag accepted by the `peat-node` binary has an `--env` equivalent. The
table below is generated from `src/main.rs::Args` at the time of writing —
if you add or rename a flag, update this file in the same PR.

## Listen address & storage

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_LISTEN` | `--listen` | string | `tcp://0.0.0.0:50051` | Listen address. Use `unix:///path/to/sock` for a Unix socket or `tcp://HOST:PORT` for TCP. The single port serves Connect RPC, gRPC, and gRPC-Web. |
| `PEAT_NODE_DATA_DIR` | `--data-dir` | path | `/data/peat-node` | Persistent data directory. Contains the Automerge CRDT store under `automerge/` and the Iroh blob store under `iroh/`. Note: the iroh **endpoint ID is not derived from this directory** — it is seeded deterministically from `(PEAT_NODE_SHARED_KEY, PEAT_NODE_NODE_ID)` (see [Deterministic identity](#deterministic-identity--offline-peer-id-derivation)), so it is stable across restarts and even across data-dir wipes, as long as those two inputs are unchanged. |

## Identity & formation

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_NODE_ID` | `--node-id` | string | random UUID | Stable identifier for this node. Surfaces in `GetStatus.nodeId`, **and seeds the deterministic iroh identity** (see [Deterministic identity](#deterministic-identity--offline-peer-id-derivation)). Set this to a stable value in any peered deployment — the default random UUID changes every boot, so the endpoint ID would too. |
| `PEAT_NODE_APP_ID` | `--app-id` | string | `peat-default` | Formation / application identifier. Two nodes must share this AND the shared key to authenticate as peers. |
| `PEAT_NODE_SHARED_KEY` | `--shared-key` | base64 | `""` | Base64-encoded 32-byte shared secret used to derive the formation key. Generate with `head -c 32 /dev/urandom \| base64`. Also the HKDF input keying material for the deterministic iroh identity — any holder of this key can compute any node's endpoint ID from its `node_id`. |

## Peering

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_IROH_UDP_PORT` | `--iroh-udp-port` | u16 | unset (ephemeral) | Bind the Iroh QUIC endpoint to a specific UDP port. **Pin this** for any deployment where peers reach this node via a stable host:port — Docker Compose, fleet-managed sidecars, anywhere the n0 public relay isn't (and shouldn't be) in the picture. |
| `PEAT_NODE_PEERS` | `--peer` | comma-separated | `""` | Peers in `endpoint_id@host:port` form, one per entry. The comma separates peers, not addresses within a peer — for multiple reachable addresses for one peer, use the `ConnectPeer` RPC at runtime. A bare endpoint ID is rejected (logged as an error and skipped); the n0 public relay is no longer used by default, so the peer's reachable address must be supplied alongside its ID. **You don't have to boot a peer to learn its `endpoint_id`** — compute it offline with `peat-node derive-id` (see [Deterministic identity](#deterministic-identity--offline-peer-id-derivation)). |
| `PEAT_NODE_AUTO_SYNC` | `--auto-sync` | bool | `true` | If true, `StartSync` is invoked once startup completes. Set `false` to require an explicit `StartSync` RPC. |

### Relay

The n0 public relay pool (`*.relay.iroh.network`) is **disabled by default**. Two endpoints peer either via direct UDP addresses (passed through `ConnectPeer.addresses`) or via an explicit relay URL the caller provides through `ConnectPeer.relay_url`. There is no implicit public-internet dependency.

Production deployments that need relay-assisted NAT traversal can run their own relay (or use a known one) and pass its URL on each `ConnectPeer` call. A future env var may pin a default relay URL — track [#41](https://github.com/defenseunicorns/peat-node/issues/41) for the design.

### Deterministic identity & offline peer-id derivation

iroh is an **identity-addressed** transport: a peer is dialed by its public key (`endpoint_id`), and the IP:port in `PEAT_NODE_PEERS` is only a routing hint. The QUIC/TLS handshake verifies the node answering at that address actually holds the private key for that `endpoint_id` — so peering *requires* knowing each peer's id, and IP:port alone can never authenticate a peer.

To make that id knowable without an out-of-band exchange, peat-node derives the iroh keypair deterministically:

```
endpoint_id = public_key( HKDF-SHA256(salt = none,
                                      IKM  = base64_decode(PEAT_NODE_SHARED_KEY),
                                      info = "iroh:" + PEAT_NODE_NODE_ID) )
```

Consequences:

- **Stable across restarts.** Given a stable `PEAT_NODE_NODE_ID`, a node presents the same `endpoint_id` on every boot — and even after a `data_dir` wipe. (Without this, iroh mints a fresh random keypair on every start, so a hardcoded `PEAT_NODE_PEERS` entry goes stale the moment a peer restarts.)
- **Computable offline.** Any holder of the shared key can compute *any* node's `endpoint_id` from its `node_id` alone — no booting the node, no network, no access to the remote machine.
- **Identical to peat-mesh's discovery derivation**, so deterministic-identity nodes interoperate with mDNS/Kubernetes-discovered peers.

> Requires a **stable `PEAT_NODE_NODE_ID`** and a non-empty `PEAT_NODE_SHARED_KEY`. If `PEAT_NODE_NODE_ID` is left unset (random UUID per boot), identity is *not* stable and peat-node logs a startup warning. With an empty shared key, identity falls back to iroh's random per-process key.

#### `peat-node derive-id`

Compute a peer's `endpoint_id` offline, for filling in `PEAT_NODE_PEERS`:

```bash
peat-node derive-id --shared-key "$PEAT_NODE_SHARED_KEY" --node-id node-b
# 4229afe8d9c12d0acfd98cb56d4e2edd0e844442651a70a6995c2ed7ef100684
```

It prints only the id to stdout (pipe/`$(...)`-friendly) and never touches the network.

#### Cross-machine peering (no mDNS, no access to the remote node)

Two machines on different hosts — `node-a` at `10.0.0.10`, `node-b` at `10.0.0.20` — sharing a formation key `$K`. You configure both from one machine, knowing only IPs, ports, and the names you assign:

```bash
# Offline, on any machine that has $K:
A_ID=$(peat-node derive-id --shared-key "$K" --node-id node-a)
B_ID=$(peat-node derive-id --shared-key "$K" --node-id node-b)
```

**node-a** (`10.0.0.10`):
```
PEAT_NODE_NODE_ID=node-a
PEAT_NODE_SHARED_KEY=$K
PEAT_NODE_IROH_UDP_PORT=51071
PEAT_NODE_PEERS=$B_ID@10.0.0.20:51072
```

**node-b** (`10.0.0.20`):
```
PEAT_NODE_NODE_ID=node-b
PEAT_NODE_SHARED_KEY=$K
PEAT_NODE_IROH_UDP_PORT=51072
PEAT_NODE_PEERS=$A_ID@10.0.0.10:51071
```

Publish the **UDP** `PEAT_NODE_IROH_UDP_PORT` on each host (and open it in the firewall) — iroh's QUIC traffic must cross the host boundary; the gRPC TCP port alone is not enough. When `node-b` boots, it derives the same keypair from `(K, "node-b")` and presents exactly the `$B_ID` you wrote into `node-a`'s `PEAT_NODE_PEERS`. Nothing was discovered; everything was assigned. See `examples/compose/attachments/docker-compose.multi-host.yml`.

## Encryption at rest

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_ENCRYPTION_KEY` | `--encryption-key` | base64 | unset | Base64-encoded 32-byte AES-256-GCM key. When set, document payloads are encrypted before storage (Automerge envelope stays unencrypted so sync still works) and decrypted transparently on read. See `src/crypto.rs` for the `ENC:v1:` envelope format. |

## Core NATS bridge (optional)

The bridge is opt-in and uses Core NATS only. With no mappings configured,
peat-node creates no NATS connection, retry timer, or bridge task. A URL by
itself is validated but does not enable the subsystem.

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_NATS_URL` | `--nats-url` | URL | unset | Local Core NATS endpoint. Only explicit `nats://` and `tls://` schemes are accepted. URL user-info authentication is permitted. |
| `PEAT_NODE_NATS_MAPPING` | `--nats-mapping` | `subject=collection` (repeatable; comma-delimited in the environment) | *empty (bridge disabled)* | Literal subject-to-Peat-collection routes. Repeat `--nats-mapping` on the command line or separate environment entries with commas. When both sources are present, CLI mappings replace the environment mappings; they are not merged. |

Example:

```bash
peat-node \
  --nats-url 'nats://bridge-user:bridge-password@127.0.0.1:4222' \
  --nats-mapping vision.summary=vision_frames \
  --nats-mapping node.health=node_health
```

Authenticated user-info is retained only for the client connection. The URL
appears only in the opt-in `--print-config` / `PEAT_NODE_PRINT_CONFIG` resolved
configuration dump, where the entire user-info is replaced with `<redacted>`;
ordinary startup output does not render a NATS URL.

Bridge configuration is validated before data-directory or mesh bootstrap.
Startup reports all detected safe issues together, including blank or
ambiguous mappings, embedded whitespace, wildcard or reserved subjects,
collection names outside `[A-Za-z0-9][A-Za-z0-9._-]*`, and exact duplicate
subjects or collections. Accepted subjects and collections remain exact and
case-sensitive after outer whitespace trimming.

Bridge readiness is internal to the bridge subsystem and does not change the
public `GetStatusResponse` or reinterpret `NodePhase`. The runtime creates one
generation containing every configured literal subscription, with subscriber
capacity 1 per mapping, before it attempts to establish readiness. It then
sends an empty Core NATS request to the reserved
`_PEAT.NATS_BRIDGE.READINESS` subject with a two-second timeout. A broker
`503 No Responders` reply (or a normal response) confirms the post-subscription
round trip; only then are all configured subjects marked established in one
atomic transition. A timeout or client error leaves readiness false. Messages
may still be consumed before this barrier completes because readiness is an
establishment signal, not an ingestion gate. Application mappings cannot use
the reserved `_PEAT` namespace.

The NATS account used by the bridge must be allowed to subscribe to every
configured application subject, publish to the exact
`_PEAT.NATS_BRIDGE.READINESS` subject, and subscribe to its async-nats request
inbox (`_INBOX.>` in a permission allow-list). The barrier carries an empty
payload and no application document, credentials, or error detail. It is never
part of a configured subscription generation and therefore never enters a Peat
collection or the later egress path. Brokers that deny either request
permission or do not return a no-responder status when no service is listening
will leave the bridge not ready.

Readiness is not an authorization guarantee under the pinned async-nats
0.49.1 client. `subscribe` returns after locally enqueueing `SUB`; the broker
does not acknowledge that command, and async-nats sends `CONNECT verbose=false`.
Broker `-ERR` frames are forwarded through async-nats's bounded 128-event
`try_send` queue, while the later 503 or normal readiness response completes on
a separate request path. If that upstream queue drops a subscription-permission
`-ERR`, the outcome is indistinguishable from authorized subscriptions even
though the bridge invalidates every client/server error that reaches its event
callback. Operators must provision and independently validate subscribe
permission for every configured mapping. Under the developer-approved residual
risk dated 2026-07-15, `ING-01: partial` and internal bridge readiness must not
be used as proof that every application `SUB` was authorized.

Ingress admits payloads of at most 1,048,576 bytes (1 MiB). A larger frame is
rejected before the bridge clones it into an ingress item or awaits the shared
queue; every rejection increments the label-free `oversized_payloads` counter
and produces only bounded, rate-limited route/length diagnostics. Rejection
does not stop the subscription reader from accepting a later within-cap frame.

Accepted input is the bounded `serde_json::Value` subset, not every
grammar-valid JSON value. Default recursion protection accepts at most 127
nested arrays/objects and rejects the 128th. Numbers must fit serde_json's
enabled `Number` modes, including finite `f64` fallback (`1e308` is accepted;
`1e309` is rejected). Every accepted UTF-8 JSON message creates one immutable
Peat document with a fresh UUID v4. The five-field envelope contains fixed
`kind`, numeric `version`, literal `subject`, effective operator-visible
`source_node_id`, and `payload`; the payload preserves every accepted byte,
including whitespace, key order, numeric spelling, escapes, and Unicode.

For `M` configured mappings and a compliant broker, **1 MiB × (257 + 2M)** is
the **bridge-owned post-dispatch raw-body subtotal** only: 256 bodies in the
shared FIFO, one serial processor `IngressItem`, and per mapping at most one
blocked-reader clone plus one capacity-1 async-nats subscriber body. This is
not a maximum total raw-body value, a process-memory bound, or a memory
ceiling. Readers await shared FIFO capacity rather than deliberately dropping
at that boundary.

Before dispatch and before the bridge can inspect or enforce its 1 MiB policy,
async-nats connection parser/read buffering retains the broker-declared
transport frame. That pre-policy retention is additional to the subtotal.
Consequently a hostile or misbehaving broker has no strict bridge-enforced
bound on total raw-body or process retention; broker payload policy remains an
operational requirement.

The active serial processor also has payload-dependent transient allocations:
its raw `Vec`, the serde_json validation tree, the copied
`BridgeEnvelope.payload` `String`, the escaped serialized-envelope `String`,
the node-side parsed `Value`, optional ciphertext/base64/wrapper values, and
Automerge conversion/document allocations. Serial processing limits this
amplification to one active item. Small fixed route/item structs and allocator
metadata are separate fixed overhead; none of the payload-dependent terms
above is included in that label.

The regression budget for this one-active-item work is a
**scoped Rust-global-allocator live-byte delta** of 41,943,040 bytes (40 MiB).
The 2026-07-15 calibration maximum was 32,863,033 bytes, so the committed
threshold adds 9,080,007 bytes (27.6%) of conservative allocator/platform
headroom. Reproduce the ordinary fixed-threshold assertion with:

```bash
cargo test --test nats_bridge_memory_test -- --nocapture
```

Recalibration is intentionally separate and ignored by default:

```bash
cargo test --test nats_bridge_memory_test calibrate_scoped_allocator_delta -- --ignored --nocapture
```

The scoped measurement covers only Rust global-allocator activity on the
enabled current OS thread during a no-yield window. It explicitly excludes
mmap allocations, native-library and kernel buffers, allocations on other OS
threads, RSS, async-nats transport retention, and whole-process memory; it is
not a transport or process-memory bound.

### Remote-origin egress and loop safety

The bridge publishes only a private node event classified as a remote Peat
upsert. Local NATS ingress, local `PutDocument`/`DeleteDocument` operations,
all deletes and tombstones, and other local mutations never enter that event
stream. A remote document is eligible only when its stored envelope has the
exact `peat.nats-bridge` kind and numeric version `1`, its collection is a
configured route, its envelope subject exactly and case-sensitively matches
that route, and its durable `source_node_id` differs from the receiving node's
ID. Ordinary JSON, malformed or unsupported envelopes, route mismatches, and
documents returning to their durable source are skipped. The immediate Peat
peer is diagnostic transport context only; it is never trusted as durable
provenance.

The eligible envelope's `payload` string is moved directly to the configured
Core NATS subject as bytes. It is never parsed or reserialized on egress. The
wire body therefore contains no Peat envelope, document ID, timestamp, source
identity, or transformation. One serial worker publishes events in their
observed FIFO order; concurrent Peat activity and event loss mean this is not
a global or durable ordering guarantee.

Each publication has exactly one private
`Peat-Nats-Bridge-Origin: <local-node-id>` header. Ingress suppresses a message
only when that header has exactly one value and it equals the local node ID
byte-for-byte. An absent header, a foreign/case-variant/empty/unfamiliar value,
or repeated values are accepted as ordinary input. The marker is deliberately
unauthenticated: an application able to publish the exact local value can
cause that local message to be dropped. The shared async-nats connection also
uses `no_echo` (`CONNECT echo=false`) as defense in depth; `no_echo` alone does
not protect another connection and does not replace the exact marker check.

The remote event broadcast retains at most 256 pending events. Before an event
enters that ring, its serialized document is capped at 2,101,248 bytes and
each retained collection, document, and immediate-peer identity is capped at
1,024 bytes; an over-limit remote document is skipped and later valid changes
continue. The allowance preserves a 1,048,576-byte ingress payload even when
its JSON string needs escaping in the durable envelope while preventing the
broadcast from retaining attacker-sized documents or identities. The egress
FIFO retains at most 256 eligible payloads of at most 1,048,576 bytes each.
Collection, document, and immediate-peer identity limits are checked before
the bridge reads the store. Before recursive Automerge-to-JSON hydration, the
bridge also rejects a `save_nocompress()` representation larger than 8 MiB.
This prevents an oversized current document from creating an unbounded JSON
Value tree and serialized event string.

The pinned peat-mesh/Automerge API has an explicit residual limitation:
`DocChange` carries only a key and origin, `AutomergeStore::get()` deep-clones
the cached document, and there is no borrowed store read, encoded-size
metadata, bounded iterator, or limited serializer. `save_nocompress()` itself
also returns a newly allocated full-document vector. Consequently those two
pre-gate transient allocations can scale with an attacker-controlled stored
document even though they are dropped immediately and never enter bridge
queues. A current-thread allocator regression uses a 16 MiB forged remote
document to bound observed amplification to four document sizes plus 1 MiB,
prove no more than 1 MiB remains live after rejection, and verify a later
valid event continues. Eliminating the two inherited transient allocations
requires a new bounded/borrowed peat-mesh store API and is not an RSS or
whole-process memory guarantee.

Admission from the Peat listener is non-blocking. Broadcast lag, queue-full,
queue-closed, unavailable-client, publish, and negotiated `max_payload`
failures are terminal losses for that document; later events continue. There
is no bridge-owned retry, disconnected backlog, broker acknowledgement,
per-item flush, replay, or reconciliation in this phase. Success means
async-nats accepted the publish into its bounded client command path, not that
the broker or a subscriber received it. The two supervisor signal FIFOs each
hold 64 events, each configured async-nats subscriber holds one message, the
shared ingress FIFO holds 256, the private and public node broadcasts each
hold 256, and lifecycle/readiness use watch channels that retain only their
latest snapshot. The pinned async-nats client separately uses its bounded
128-event callback queue described above.

Egress classification and delivery counters remain label-free monotonic
counters. Diagnostic emission allocates exactly 16 fixed classification
buckets for route-less events plus 16 buckets for each finite validated
startup route. The first event is emitted, subsequent events in the same
classification and route are aggregated for 60 seconds, and the next periodic
event reports the suppressed count. Cross-route floods cannot be attributed
to another route; route-less broadcast lag remains explicitly route-less and
is never rendered as route zero. No document, peer, payload, marker,
credential, parser text, or source error becomes a diagnostic label. A
delivery diagnostic carries the finite validated startup route index preserved
with its FIFO item; it does not infer or default the route after publication.

The required origin header counts toward the broker's negotiated
`max_payload`. Consequently an exact 1,048,576-byte message accepted on
ingress can be rejected on egress by a broker whose `max_payload` is also
1,048,576, because the HPUB header block increases the total publish size.
Provision broker headroom for the NATS header. The bridge reports a fixed
`max_payload` loss classification and never truncates, rewrites, or retries the
payload.

Remote duplicate suppression retains exactly 4,096 SHA-256 document digests
(4,096 × 32 = 131,072 bytes) for the process lifetime. Its domain-separated,
big-endian length-framed input is `(collection, document ID)`; it retains no
attacker-controlled strings. A digest collision is treated as a duplicate.
The digest is inserted before FIFO admission, so queue or publication failure
cannot turn a later notification into an implicit retry. There is no eviction:
after the table fills, all previously unseen documents fail closed until
restart, while an existing digest remains a duplicate. Restart clears this
in-memory table and does not provide durable exactly-once behavior.

Origin attribution has a separate local-revision guard with exactly 4,096
SHA-256 slots (also 4,096 × 32 = 131,072 retained digest bytes). Its
domain-separated, length-framed digest covers the document key and exact
canonical Automerge heads. It retains neither document keys nor head vectors.
Local writes record their completed heads while holding the same per-key store
lock used by the remote event snapshot. The remote forwarder rereads and
classifies the current exact heads under that lock, so a later local snapshot
cannot be mislabeled remote. If a remote revision is superseded before capture,
it may be lost; there is no historical snapshot recovery.

`MAX_REVISION_HEADS=64` bounds only digest iteration and retained/admitted
journal work after `get_heads()` returns. Under the locked Automerge pin,
`get_heads()` first allocates and sorts all current heads. That inherited
transient exposure has no bounded iterator and is explicitly not a 2,048-byte
temporary cap, peak-memory limit, RSS guarantee, or whole-process bound. Both
fixed digest tables add small fixed metadata beyond their 131,072-byte slot
arrays; neither statement is a whole-runtime memory ceiling.

Before 1.0, `SidecarNode::document_store()` changed from a mutable raw-store
handle to `DocumentStoreReader`. Existing reads (`get`, `scan_prefix`,
`keys_with_prefix`, and observer subscription) remain available, but callers
can no longer recover the backing `AutomergeStore` or call raw `put`/`delete`.
This closes the public mutation path that could bypass local-origin
attribution.

The exact, case-sensitive collection name `file_distributions` is reserved and
rejected in NATS bridge mappings. Public `IrohFileDistribution`/attachment
mutation can preserve arbitrary pre-existing root fields, including content
that resembles a bridge envelope, so this boundary must not depend on a fixed
attachment schema or unknown-field rejection. The attachment mutation facade
is restricted to that canonical collection. Inventory found no other public
facade that mutates document collections: watcher writes are mediated through
the node, while blob and bundle APIs do not mutate Automerge documents.

Egress logs and metrics use fixed enums, monotonic label-free counters,
startup-bounded route indexes, and byte counts. They do not include payload or
envelope text, origin-header values, peer or document IDs, credentials,
untrusted NATS/store/parser error text, or error chains.

Core NATS remains at-most-once: sustained overload can still trigger client
slow-consumer loss. Such events are counted and warned about at a rate-limited
cadence without changing readiness, and lost Core NATS messages cannot be
replayed.

Peat storage is serial and preserves FIFO processing. The envelope is stored
and synchronized through the existing Peat document path; it is not published
to NATS. A transient store failure gets three total attempts using the same
generated document ID and envelope, with 50 ms then 200 ms delays. Final loss
is reported once with only safe route metadata, payload byte length, document
ID, bounded attempt values, and a fixed error classification. Payload text,
NATS credentials, unrestricted parser or store errors, parser source text, and
source chains are never logged. Invalid UTF-8, malformed JSON, excessive
nesting, and out-of-range numbers are counted safely and never stored or
relayed.

Phase 3 ends with remote-only Core NATS egress and in-memory loop safety.
Bounded shutdown draining, persisted reconciliation, and the complete metrics
surface are Phase 4. This Core NATS bridge provides no durable input,
acknowledgement, replay, ordering, exactly-once delivery, or zero-loss overload
guarantee; JetStream is out of scope.

## Agent watcher (optional)

The watcher polls a co-located service (e.g. UDS Remote Agent) and mirrors its
state into the CRDT mesh. Enable by setting `PEAT_NODE_AGENT_ADDR`; otherwise
the watcher is not started.

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_AGENT_ADDR` | `--agent-addr` | URL | unset | Address of the agent to watch, e.g. `http://localhost:8080` or `https://localhost:8080`. Unset disables the watcher. |
| `PEAT_NODE_AGENT_POLL_INTERVAL` | `--agent-poll-interval` | seconds | `10` | How often to poll the agent. |
| `PEAT_NODE_AGENT_TLS_CERT` | `--agent-tls-cert` | path | unset | PEM-encoded client cert for mTLS to the agent. |
| `PEAT_NODE_AGENT_TLS_KEY` | `--agent-tls-key` | path | unset | PEM-encoded client private key for mTLS. |
| `PEAT_NODE_AGENT_TLS_CA` | `--agent-tls-ca` | path | unset | PEM-encoded CA cert for verifying the agent's server certificate. |

mTLS is engaged only when **both** `PEAT_NODE_AGENT_TLS_CERT` and
`PEAT_NODE_AGENT_TLS_KEY` are set. Partial configurations — CA-only,
cert-without-key, or key-without-cert — are silently treated as insecure
h2c; the unmatched TLS env vars are ignored. If cert + key are both set
but either PEM is malformed, startup panics. See
`src/watcher.rs::build_client` ([#37](https://github.com/defenseunicorns/peat-node/issues/37) tracks hardening this to error on partial TLS config).

## Attachment distribution (PRD-006)

Path-based attachment submission over gRPC. A co-located application
hands `peat-node` a list of file paths plus their declared sha256 + size;
the sidecar validates, content-hashes via iroh-blobs (BLAKE3), and
queues them for distribution to other mesh peers.

**Safety default:** with no `--attachment-root` configured, the four
attachment RPCs (`SendAttachments`, `GetAttachmentDistribution`,
`SubscribeAttachmentBundle`, `CancelAttachmentDistribution`) return
`Unimplemented`. Operators must explicitly name the directory roots the
RPC may read.

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_ATTACHMENT_ROOT` | `--attachment-root` | `name=path` (repeatable, comma-delimited) | *empty (RPC disabled)* | Allowlisted roots, e.g. `outbox=/var/lib/peat/outbox,media=/var/lib/peat/media`. Each path is canonicalised at startup; bad inputs (missing dir, non-directory, malformed name) fail the process before the mesh starts. |
| `PEAT_NODE_ATTACHMENT_MAX_FILE_BYTES` | `--attachment-max-file-bytes` | u64 | `268435456` (256 MiB) | Per-file size cap. Larger files reject `ResourceExhausted`. |
| `PEAT_NODE_ATTACHMENT_MAX_BUNDLE_BYTES` | `--attachment-max-bundle-bytes` | u64 | `1073741824` (1 GiB) | Per-request total-bytes cap (`Σ size_bytes`). |
| `PEAT_NODE_ATTACHMENT_MAX_FILES_PER_BUNDLE` | `--attachment-max-files-per-bundle` | u32 | `64` | Per-request file-count cap. |
| `PEAT_NODE_ATTACHMENT_MAX_NODE_LIST_LEN` | `--attachment-max-node-list-len` | u32 | `256` | Cap on `NodeListScope.node_ids.len()`. |
| `PEAT_NODE_ATTACHMENT_MAX_CONCURRENT_DISTRIBUTIONS` | `--attachment-max-concurrent-distributions` | u32 | `4` | In-flight cap. Over-cap requests reject `ResourceExhausted` unless `--attachment-queue-when-full` is set. |
| `PEAT_NODE_ATTACHMENT_QUEUE_WHEN_FULL` | `--attachment-queue-when-full` | bool | `false` | When true, accept beyond the in-flight cap. v1 honors the knob but the queue wait itself is deferred — accepts pass through immediately. |
| `PEAT_NODE_ATTACHMENT_DEFAULT_PRIORITY` | `--attachment-default-priority` | enum | `routine` | Default `AttachmentPriority` when the caller leaves it `UNSPECIFIED`. Values: `bulk` \| `low` \| `routine` \| `priority` \| `critical`. v1 records the classification on the distribution document but does NOT enforce wire-level preemption between classes — that needs PRD-004 (bandwidth allocation). |
| `PEAT_NODE_ATTACHMENT_DISCOVERY_GRACE_SECS` | `--attachment-discovery-grace-secs` | u32 | `30` | Grace window for unknown node IDs in `NodeListScope` before they're marked `FAILED` in per-node status. **Recognised but inert in v1** — the background promoter task is not yet implemented. |
| `PEAT_NODE_ATTACHMENT_HANDLE_RETENTION_SECS` | `--attachment-handle-retention-secs` | u32 | `86400` (24h) | How long a terminal bundle's handle table is retained for `bundle_id` lookups, `SubscribeAttachmentBundle` late-attach, and `AlreadyExists` enforcement. `0` disables retention (no idempotency, no late-subscribe — discouraged). A background sweep evicts terminal bundles past the window; non-terminal bundles only age out under LRU pressure. |
| `PEAT_NODE_ATTACHMENT_MAX_KNOWN_BUNDLES` | `--attachment-max-known-bundles` | u32 | `4096` | Hard cap on the handle-table size; LRU eviction triggers before the retention window expires when exceeded. Protects long-running edge nodes from unbounded growth proportional to lifetime send volume. |

### Scope variants

`SendAttachmentsRequest.scope` accepts:

- `AllNodesScope` — distribute to every reachable peer.
- `NodeListScope { node_ids: [...] }` — distribute to the listed IDs. Unknown IDs aren't a request-time error; they record in per-node status and (when the discovery-grace task ships) age to FAILED after `--attachment-discovery-grace-secs`.
- `FormationScope { formation_id }` — **rejected `FailedPrecondition` in v1.** Formation membership resolution awaits a live data source.
- `CapableScope {}` — **rejected `FailedPrecondition` in v1.** Reserved-but-empty variant; the capability vocabulary is deferred to a follow-on ADR. The empty marker exists so the oneof can grow without renumbering once the schema lands.

An unset scope (oneof or the `scope` field omitted entirely) is rejected `InvalidArgument` — there is no silent fallback to `AllNodes`.

### Handle-table durability

The bundle handle table is **in-memory only** in v1. A `peat-node` restart drops every `bundle_id` lookup. Consequences:

- `SubscribeAttachmentBundle(bundle_id)` returns `NotFound` for any `bundle_id` whose subscriber re-attaches after a *server-side* restart, even within the retention window.
- `AlreadyExists` enforcement resets — a `bundle_id` ingested before the restart can be resubmitted with a different `FileSpec` set immediately after.
- Iroh content-addressed blobs and in-flight Automerge distribution documents are unaffected.

Consumers should not build durable-bundle assumptions against this surface. Durable handle tables would be a separate v2 spec.

### Deployment example

Two operator wiring patterns:

- **Docker Compose** — see [`examples/compose/attachments`](../examples/compose/attachments) for the simplest possible quickstart: one `peat-node` container with an `outbox` volume mounted at `/var/lib/peat/outbox` and the attachment env vars set.
- **Helm** — see the `attachment:` section in [`chart/peat-node/values.yaml`](../chart/peat-node/values.yaml). Operators provide the volumes via `attachment.extraVolumes` / `attachment.extraVolumeMounts` and map `attachment.roots` entries to the matching mount paths. The chart only wires env vars and threads the mounts — volume provisioning (PVC, hostPath, emptyDir, configMap, CSI, etc.) is operator-supplied.

## Logging

`peat-node` uses `tracing` with an env filter. Set `RUST_LOG` directly:

```
RUST_LOG=peat_node=debug,peat_mesh=info
```

Default: `peat_node=info,peat_mesh=info`.

## Examples

A working two-node config is in [`examples/compose/docker-compose.yml`](../examples/compose/docker-compose.yml). The Helm chart at [`chart/peat-node/`](../chart/peat-node/) maps these env vars to chart values.

For typed clients in other languages, generate from [`proto/sidecar.proto`](../proto/sidecar.proto) in your own repo. peat-node ships no language-specific SDKs — consumers hit the Connect-RPC wire directly, or use [`peat-gateway`](https://github.com/defenseunicorns/peat-gateway) for protocol-bridge adapters.
