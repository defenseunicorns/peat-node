# peat-ffi: CRDT-synced documents have empty content when read via getRawDocument

## Summary

When peat-node writes documents using `AutomergeStore.put(key, automerge_doc)` with `json_to_automerge({"value": "json_string"})`, and these documents are CRDT-synced to a peat-ffi node via Iroh, the receiving node can see the document **keys** via `store.scan_prefix()` but the **content** is empty — `store.get(key)` returns an Automerge doc where the "value" field is missing.

## Reproduction

1. peat-node sidecar writes a document:
```rust
let json_value = serde_json::json!({ "value": store_value });
let doc = json_to_automerge(&json_value, existing.as_ref())?;
self.store.put(&key, &doc)?;
```

2. peat-ffi node connects as a peer via Iroh QUIC
3. CRDT sync runs — sidecar logs show `syncing deployments:... with 1 peers`
4. On the peat-ffi side:
   - `store.scan_prefix("deployments:")` → returns 3 document keys ✅
   - `store.get("deployments:key")` → returns `Some(doc)` but the Automerge doc has no "value" field ❌
   - `automerge_to_json(&doc).get("value")` → `None`

## Console Evidence

```
[PeatLive] rawStore deployments: 3 docs
[PeatLive] rawStore deployments/573922c9:core-identity-authorization: getRawDocument returned nil
[PeatLive] rawStore deployments/573922c9:init: getRawDocument returned nil
[PeatLive] rawStore deployments/573922c9:uds-k3d-dev: getRawDocument returned nil
```

The document keys exist in the store (scan_prefix finds them), but reading the document content returns nil because the Automerge "value" field is empty.

## Analysis

### Root Cause: Automerge sync only syncs document structure, not the full document

When `AutomergeSyncCoordinator.sync_document_with_all_peers()` syncs a document, it sends the Automerge sync messages. But the receiving node creates a new Automerge document from the sync messages. If the sync protocol doesn't fully exchange all operations, the receiving document may be missing data.

### Possible causes:

1. **Sync protocol incomplete**: The Automerge sync protocol requires multiple round-trips to fully sync a document. If the peer connection is intermittent (which we see — peers=1 toggles to peers=0), the sync may not complete.

2. **Different Automerge document format**: peat-node creates documents via `json_to_automerge()` which creates Automerge operations. The sync may create a new document with the correct key but without applying all operations.

3. **Sync cooldown**: We see `Sync cooldown active, 91ms remaining` warnings, suggesting the sync is rate-limited and may not complete for all documents before the peer disconnects.

4. **One-way sync**: The `sync_document_with_all_peers` on the sidecar pushes changes outward, but the Automerge sync protocol is bidirectional. The receiving node needs to also pull, which requires a stable connection.

## Impact

- Documents sync their keys but not their content
- The iOS app can see that documents exist but cannot read their values
- The Health tab shows 0 agents/deployments even though the mesh is connected

## Proposed Fix

The `getRawDocument` implementation in peat-ffi should handle the case where the Automerge document was synced but the "value" field hasn't been fully replicated yet. Additionally, the sync protocol should ensure full document replication before reporting completion.

Alternatively, peat-node should write documents using the same `collection().upsert()` API as peat-ffi, which may use a different Automerge document structure that syncs correctly. However, initial testing showed that `collection().upsert()` documents also don't appear via `collection().scan()` on the receiving node.

## Environment

- peat-node: peat-mesh v0.8.2, peat-protocol (local)
- peat-ffi: peat-mesh v0.8.2
- iOS 17+ on physical device
- Peer connection intermittent (mDNS discovery over WiFi)
