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

Bridge readiness is internal to the bridge subsystem: an enabled bridge is
ready only while its NATS connection is active and every configured subject
subscription is established. Phase 1 does not change the public
`GetStatusResponse` or reinterpret `NodePhase`. Actual NATS subscriptions and
message ingestion arrive in Phase 2.

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
