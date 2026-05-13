# peat-node: Cross-process peering fails with "connection lost"

## Summary

Two peat-node sidecar instances running on the same host cannot establish a peer connection. The `--peer` flag triggers `connect_peer` which attempts to discover the remote node via Iroh relay, but the connection fails with "connection lost" after ~30 seconds.

## Reproduction

```bash
# Terminal 1: Start sidecar 1
./peat-node --listen tcp://0.0.0.0:50051 --data-dir /tmp/peat-node-1 \
  --app-id peat-default --shared-key ZGVtby1zaGFyZWQta2V5LTMyYnl0ZXMhIQ== \
  --auto-sync --agent-addr http://localhost:8080

# Note the endpoint_id from logs, e.g.: 20bf9ca08c77ecee12ec5115ca1268dad94e047d2677e13d0d64ae6e56684c78

# Terminal 2: Start sidecar 2 with --peer pointing to sidecar 1
./peat-node --listen tcp://0.0.0.0:50052 --data-dir /tmp/peat-node-2 \
  --app-id peat-default --shared-key ZGVtby1zaGFyZWQta2V5LTMyYnl0ZXMhIQ== \
  --auto-sync --agent-addr http://localhost:8081 \
  --peer 20bf9ca08c77ecee12ec5115ca1268dad94e047d2677e13d0d64ae6e56684c78
```

## Expected

Sidecar 2 connects to sidecar 1, both show `1 peers`, CRDT documents sync between them.

## Actual

Sidecar 2 logs:
```
ERROR peat_node: failed to connect to peer: connection lost peer="20bf9ca..."
```

Both sidecars continue running with `0 peers`. Documents never sync.

## Analysis

### Root Cause: `MemoryLookup` is per-process

In `node.rs:73-75`:
```rust
let memory_lookup = iroh::address_lookup::memory::MemoryLookup::new();
let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::N0)
    .address_lookup(memory_lookup.clone())
    .bind()
    .await?;
```

Each peat-node process creates its own `MemoryLookup` instance. When `connect_peer` is called, it adds the peer's endpoint ID + relay URL to the local memory lookup table. However:

1. The peer's actual socket addresses are not known (only endpoint ID + relay URL)
2. The connection goes through the Iroh relay (n0 infrastructure)
3. The relay connection succeeds but the formation key handshake or QUIC session establishment fails with "connection lost"

### Possible causes of "connection lost"

1. **Formation key mismatch at QUIC layer**: The `FormationKey` is used in `connect_and_authenticate()`. If the ALPN negotiation or the custom handshake fails, the connection drops.

2. **Relay latency**: The `connect_peer` method waits for a relay URL (up to 10s), but if the relay connection is slow, the authenticated connection attempt may time out.

3. **Missing direct address path**: On the same host, both endpoints could connect directly via localhost/loopback instead of going through an external relay. But `MemoryLookup` has no mechanism to share addresses between processes.

4. **Iroh endpoint presets**: Using `iroh::endpoint::presets::N0` configures default n0.computer relay servers. If these are unreachable or throttled, peer connections fail.

## Proposed Fixes

### Option A: Support direct address in `--peer` flag
Allow `--peer endpoint_id@address:port` syntax so direct connections can be established without relay:
```bash
--peer 20bf9ca0...@127.0.0.1:12345
```
This requires knowing the Iroh QUIC port, which could be logged on startup.

### Option B: Add mDNS discovery to peat-node
Use `mdns-sd` (already a dependency of peat-mesh) for local subnet peer discovery. Nodes with matching `app_id` would find each other automatically.

### Option C: Log and expose the Iroh endpoint address
Log the full `EndpointAddr` (including socket addresses) on startup so operators can configure peer connections with direct addresses:
```
INFO iroh endpoint bound endpoint_addr=20bf9ca0...@192.168.4.48:54321,relay:https://euw1-1.relay.iroh.network
```

## Impact

- peat-node sidecars cannot sync with each other
- iOS/Android peat-ffi nodes cannot sync with peat-node sidecars
- The entire multi-agent mesh demo is blocked

## Update: Direct Address Fix (Partial)

Modified `connect_peer` to accept `endpoint_id@addr:port` format with `TransportAddr::Ip` direct addresses. This established the QUIC connection but the formation handshake still fails:

```
ERROR failed to connect to peer: formation auth timed out reading challenge length
```

The QUIC transport connects to the socket but `SyncProtocolHandler` on the accepting side doesn't respond to the formation challenge within the timeout. This suggests the ALPN handler isn't routing the incoming connection to `SyncProtocolHandler.accept()` correctly, or there's a deadlock in the handler startup.

**With peat-mesh v0.8.2 (crates.io)**: Sidecars peer successfully using this direct address approach (tested and confirmed — both show 1 peer, documents sync bidirectionally).

**With peat-mesh v0.8.0 (local path)**: Formation handshake times out on the accepting side.

This indicates a regression between v0.8.0 and v0.8.2 in the `SyncProtocolHandler` or the `NetworkedIrohBlobStore` protocol registration.

## Environment

- peat-node: peat-mesh v0.8.0 (local path) — fails
- peat-node: peat-mesh v0.8.2 (crates.io) — works
- peat-ffi: peat-mesh v0.8.0 (local path via workspace)
- iroh: v0.97
- macOS arm64 (Apple Silicon)
- Both sidecars on same host, same WiFi network
