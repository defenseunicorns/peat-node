# peat-node configuration reference

Every flag accepted by the `peat-node` binary has an `--env` equivalent. The
table below is generated from `src/main.rs::Args` at the time of writing —
if you add or rename a flag, update this file in the same PR.

## Listen address & storage

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_LISTEN` | `--listen` | string | `tcp://0.0.0.0:50051` | Listen address. Use `unix:///path/to/sock` for a Unix socket or `tcp://HOST:PORT` for TCP. The single port serves Connect RPC, gRPC, and gRPC-Web. |
| `PEAT_NODE_DATA_DIR` | `--data-dir` | path | `/data/peat-node` | Persistent data directory. Contains the Automerge CRDT store under `automerge/` and the Iroh blob store under `iroh/`. Endpoint ID is stable across restarts as long as this directory persists. |

## Identity & formation

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_NODE_ID` | `--node-id` | string | random UUID | Stable identifier for this node. Surfaces in `GetStatus.nodeId`. |
| `PEAT_NODE_APP_ID` | `--app-id` | string | `peat-default` | Formation / application identifier. Two nodes must share this AND the shared key to authenticate as peers. |
| `PEAT_NODE_SHARED_KEY` | `--shared-key` | base64 | `""` | Base64-encoded 32-byte shared secret used to derive the formation key. Generate with `head -c 32 /dev/urandom \| base64`. |

## Peering

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_IROH_UDP_PORT` | `--iroh-udp-port` | u16 | unset (ephemeral) | Bind the Iroh QUIC endpoint to a specific UDP port. **Pin this** for any deployment where peers reach this node via a stable host:port — Docker Compose, fleet-managed sidecars, anywhere the n0 public relay isn't (and shouldn't be) in the picture. |
| `PEAT_NODE_PEERS` | `--peer` | comma-separated | `""` | Peers in `endpoint_id@host:port` form, one per entry. The comma separates peers, not addresses within a peer — for multiple reachable addresses for one peer, use the `ConnectPeer` RPC at runtime. A bare endpoint ID is rejected (logged as an error and skipped); the n0 public relay is no longer used by default, so the peer's reachable address must be supplied alongside its ID. |
| `PEAT_NODE_AUTO_SYNC` | `--auto-sync` | bool | `true` | If true, `StartSync` is invoked once startup completes. Set `false` to require an explicit `StartSync` RPC. |

### Relay

The n0 public relay pool (`*.relay.iroh.network`) is **disabled by default**. Two endpoints peer either via direct UDP addresses (passed through `ConnectPeer.addresses`) or via an explicit relay URL the caller provides through `ConnectPeer.relay_url`. There is no implicit public-internet dependency.

Production deployments that need relay-assisted NAT traversal can run their own relay (or use a known one) and pass its URL on each `ConnectPeer` call. A future env var may pin a default relay URL — track [#41](https://github.com/defenseunicorns/peat-node/issues/41) for the design.

## Encryption at rest

| Env var | Flag | Type | Default | Description |
|---|---|---|---|---|
| `PEAT_NODE_ENCRYPTION_KEY` | `--encryption-key` | base64 | unset | Base64-encoded 32-byte AES-256-GCM key. When set, document payloads are encrypted before storage (Automerge envelope stays unencrypted so sync still works) and decrypted transparently on read. See `src/crypto.rs` for the `ENC:v1:` envelope format. |

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

## Logging

`peat-node` uses `tracing` with an env filter. Set `RUST_LOG` directly:

```
RUST_LOG=peat_node=debug,peat_mesh=info
```

Default: `peat_node=info,peat_mesh=info`.

## Examples

A working two-node config is in [`examples/compose/docker-compose.yml`](../examples/compose/docker-compose.yml). The Helm chart at [`chart/peat-node/`](../chart/peat-node/) maps these env vars to chart values.
