//! PRD-006 multi-peer integration test.
//!
//! Boots two `SidecarNode`s on distinct iroh UDP ports, peers them, then
//! sends an attachment from A and verifies B can pull the blob via the
//! iroh blob store. Confirms the receiver-side fetch substrate works
//! end-to-end with content matching the sender's declared sha256.
//!
//! # Why one test (PRD specifies three)
//!
//! PRD §Testing Plan tests 21 (`send_all_nodes_distributes_to_two_peers`)
//! and 22 (`send_node_list_only_delivers_to_listed`) both rely on the
//! receiver-side observer pattern from `peat-protocol::storage::
//! file_distribution.rs:617-621` which is explicitly *not implemented* in
//! v1 — receivers don't auto-pull blobs from the synced distribution
//! document. The test that runs here covers the substrate-level
//! invariant the PRD tests actually rely on: when the sender's
//! NetworkedIrohBlobStore has a blob and another peer is connected at
//! the iroh layer, the receiver can call `fetch_blob(token)` and iroh
//! pulls the bytes across the wire with content-address verification.
//!
//! The handler-level distinction between AllNodes and NodeList scopes
//! (PRD test 22) lives in `IrohFileDistribution::resolve_targets`; that's
//! covered indirectly by the per-node-state aggregation in
//! `handlers::get_attachment_distribution` and unit-tested via the
//! `ValidatedScope` parser. Once peat-protocol grows the receive-side
//! observer hooks, this file should grow to mirror PRD tests 21 and 22
//! directly.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use peat_mesh::storage::blob_traits::{BlobHash, BlobMetadata, BlobStore, BlobToken};
use peat_node::attachments::config::{AttachmentConfig, AttachmentPriorityCli};
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;
use sha2::{Digest, Sha256};

struct BootedNode {
    node: Arc<SidecarNode>,
    base: String,
    _root_dir: tempfile::TempDir,
    root_path: PathBuf,
}

async fn boot(grpc_port: u16, iroh_port: u16) -> BootedNode {
    let data_dir = tempfile::tempdir().unwrap();
    let root_dir = tempfile::tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();
    let attachment_config = AttachmentConfig::from_raw(
        &[format!("outbox={}", root_path.display())],
        None, // inbox_path
        peat_node::attachments::config::DEFAULT_MAX_FILE_BYTES,
        peat_node::attachments::config::DEFAULT_MAX_BUNDLE_BYTES,
        peat_node::attachments::config::DEFAULT_MAX_FILES_PER_BUNDLE,
        peat_node::attachments::config::DEFAULT_MAX_NODE_LIST_LEN,
        peat_node::attachments::config::DEFAULT_MAX_CONCURRENT_DISTRIBUTIONS,
        peat_node::attachments::config::DEFAULT_QUEUE_WHEN_FULL,
        AttachmentPriorityCli::Routine,
        peat_node::attachments::config::DEFAULT_DISCOVERY_GRACE_SECS,
        peat_node::attachments::config::DEFAULT_HANDLE_RETENTION_SECS,
        peat_node::attachments::config::DEFAULT_MAX_KNOWN_BUNDLES,
        peat_node::attachments::config::DEFAULT_INBOX_POLL_SECS,
    )
    .unwrap();

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("multi-{grpc_port}"),
            app_id: "multi-peer-test".into(),
            shared_key: String::new(),
            data_dir: data_dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: Some(iroh_port),
            attachment_config,
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
        })
        .await
        .unwrap(),
    );

    let service = Arc::new(PeatSidecarService::new(Arc::clone(&node)));
    let router = service.register(connectrpc::Router::new());
    let addr: std::net::SocketAddr = format!("127.0.0.1:{grpc_port}").parse().unwrap();
    tokio::spawn(async move {
        connectrpc::Server::new(router).serve(addr).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    BootedNode {
        node,
        base: format!("http://127.0.0.1:{grpc_port}"),
        _root_dir: root_dir,
        root_path,
    }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap()
}

async fn call(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/{method}");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "{method} returned {status}: {text}");
    serde_json::from_str(&text).unwrap()
}

fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Substrate-level test: B can pull a blob A ingested.
///
/// Previously `#[ignore]`'d because the receiver hit
/// `"no peers configured for remote fetch"` — `BlobPeerIndex` was empty.
/// PR #65 fixed the underlying cause: `SidecarNode::connect_peer` now
/// calls `blob_store.add_peer(peer_id)` after `start_sync_connection`,
/// so the blob store's known-peers list is populated alongside iroh's
/// transport-layer connection list. With both populated,
/// `NetworkedIrohBlobStore::fetch_blob` iterates known_peers and tries
/// iroh-blobs' downloader against each — exactly the path this test
/// exercises.
///
/// Distinct from `attachments_e2e_test.rs`: that one drives the full
/// SendAttachments → inbox watcher → filesystem path. This one bypasses
/// the watcher and calls `fetch_blob` directly to prove the underlying
/// substrate works for any future consumer that wants targeted blob
/// pulls without the document watcher in the loop.
#[tokio::test]
async fn receiver_can_fetch_blob_pushed_by_sender() {
    const A_GRPC: u16 = 50121;
    const A_IROH: u16 = 51221;
    const B_GRPC: u16 = 50122;
    const B_IROH: u16 = 51222;

    let a = boot(A_GRPC, A_IROH).await;
    let b = boot(B_GRPC, B_IROH).await;
    let http = http_client();

    // Wire B → A over direct UDP. The connect_peer handshake establishes
    // bidirectional iroh connectivity, so A also learns B as a known
    // peer (which is what `IrohFileDistribution::resolve_targets` needs
    // for AllNodes scope).
    let status_a = call(&http, &a.base, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"]
        .as_str()
        .expect("endpointAddr missing on GetStatus")
        .to_string();
    call(
        &http,
        &b.base,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_a,
            "addresses": [format!("127.0.0.1:{A_IROH}")],
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Sender A sends an attachment.
    let payload = vec![0x5Au8; 4096]; // 4 KiB — large enough to exercise streaming, small enough to keep tests fast
    let payload_sha = sha256_bytes(&payload);
    let file_path = a.root_path.join("multi-peer.bin");
    std::fs::write(&file_path, &payload).unwrap();

    let resp = call(
        &http,
        &a.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "multi-peer.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&payload_sha),
            }],
            "scope": { "allNodes": {} }
        }),
    )
    .await;
    let blob_token_hex = resp["handles"][0]["blobToken"]
        .as_str()
        .expect("response must carry the blob token (BLAKE3 hex)")
        .to_string();
    assert!(
        !blob_token_hex.is_empty(),
        "blob_token must be populated for an ingested file"
    );

    // Sender's local store has the blob (sanity).
    let blob_hash = BlobHash(blob_token_hex.clone());
    assert!(
        a.node.blob_store().blob_exists_locally(&blob_hash),
        "sender must hold the blob locally after ingest"
    );
    assert!(
        !b.node.blob_store().blob_exists_locally(&blob_hash),
        "receiver must not hold the blob before fetch"
    );

    // Receiver B fetches the blob via iroh. The token contains
    // hash + size + metadata — size we know from the request; metadata
    // is opaque here (peat-protocol doesn't sync the metadata side-band
    // separately, but iroh's content-addressing doesn't require it for
    // fetch).
    let token = BlobToken::new(
        blob_hash.clone(),
        payload.len() as u64,
        BlobMetadata::default(),
    );
    let handle = tokio::time::timeout(
        Duration::from_secs(15),
        b.node.blob_store().fetch_blob(&token, |_| {}),
    )
    .await
    .expect("fetch_blob must complete within 15s")
    .expect("fetch_blob must succeed against the connected sender");

    // Verify content matches the sender's declared sha256.
    let fetched = std::fs::read(&handle.path)
        .expect("fetched blob must materialize at the handle's local path");
    let fetched_sha = sha256_bytes(&fetched);
    assert_eq!(
        fetched_sha, payload_sha,
        "fetched bytes must hash to the sender's declared sha256"
    );
    assert_eq!(
        fetched.len(),
        payload.len(),
        "fetched byte count must match"
    );

    // After fetch, the receiver's blob store has the blob locally.
    assert!(
        b.node.blob_store().blob_exists_locally(&blob_hash),
        "receiver's blob store must hold the blob after fetch_blob"
    );
}
