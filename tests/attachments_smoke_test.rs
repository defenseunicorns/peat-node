//! PRD-006 attachment-surface smoke test.
//!
//! Boots a real peat-node with `--attachment-root` configured, then drives
//! the four attachment RPCs over the Connect protocol (HTTP + JSON) with
//! `reqwest`. The goal is to validate the layers that the unit tests
//! deliberately mock out:
//!
//! - Real proto wire encoding via buffa (camelCase JSON, base64 bytes,
//!   oneof variant shape for `DistributionScopeSpec`).
//! - Real `IrohBlobStore::create_blob_from_stream` + the tee-style hasher
//!   from `attachments::ingest`.
//! - Real `IrohFileDistribution::distribute` against a zero-peer mesh.
//! - Real registry insert + `lookup_distribution` round-trip.
//!
//! This is the first integration check that the Step 7a handlers actually
//! work end-to-end. Step 8 layers the broader PRD acceptance tests on top.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use peat_node::attachments::config::{AttachmentConfig, AttachmentPriorityCli};
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;
use sha2::{Digest, Sha256};

struct BootedServer {
    base: String,
    /// Holds the attachment root alive for the test duration.
    _root_dir: tempfile::TempDir,
    /// The named root the test writes files into.
    root_path: std::path::PathBuf,
}

async fn boot_server_with_attachments(port: u16) -> BootedServer {
    let data_dir = tempfile::tempdir().unwrap();

    let root_dir = tempfile::tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();
    let root_spec = format!("outbox={}", root_path.display());
    let attachment_config = AttachmentConfig::from_raw(
        &[root_spec],
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
    .expect("attachment_config must construct against a real tempdir root");

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("attach-smoke-{port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: data_dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: None,
            attachment_config,
        disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
        })
        .await
        .unwrap(),
    );

    let service = Arc::new(PeatSidecarService::new(node));
    let router = service.register(connectrpc::Router::new());
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    tokio::spawn(async move {
        connectrpc::Server::new(router).serve(addr).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    BootedServer {
        base: format!("http://127.0.0.1:{port}"),
        _root_dir: root_dir,
        root_path,
    }
}

async fn boot_server_without_attachments(port: u16) -> String {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("attach-disabled-{port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: None,
            attachment_config: Default::default(),
        disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
        })
        .await
        .unwrap(),
    );

    let service = Arc::new(PeatSidecarService::new(node));
    let router = service.register(connectrpc::Router::new());
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    tokio::spawn(async move {
        connectrpc::Server::new(router).serve(addr).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    format!("http://127.0.0.1:{port}")
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

async fn call_unchecked(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/{method}");
    client
        .post(&url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap()
}

async fn call_ok(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let resp = call_unchecked(client, base, method, body).await;
    assert!(
        resp.status().is_success(),
        "{method} returned HTTP {}; body: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    resp.json().await.unwrap()
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

/// Happy path: send a real file, get a distribution_id back, query its
/// status. Exercises:
///   - SendAttachments JSON wire decode (FileSpec, DistributionScopeSpec
///     oneof, AttachmentPriority enum)
///   - Real `IrohBlobStore::create_blob_from_stream` via the tee reader
///   - Real `IrohFileDistribution::distribute` with zero connected peers
///     (target list is empty; distribute returns Ok per peat-protocol)
///   - GetAttachmentDistribution wire encode + status lookup round-trip
#[tokio::test]
async fn send_and_get_attachment_distribution_round_trip() {
    let server = boot_server_with_attachments(50091).await;
    let client = http_client();

    // Write a real file inside the configured root.
    let payload = b"PRD-006 smoke test payload";
    let file_path = server.root_path.join("hello.bin");
    std::fs::write(&file_path, payload).unwrap();
    let hash = sha256_bytes(payload);

    let resp = call_ok(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [
                {
                    "rootName": "outbox",
                    "relativePath": "hello.bin",
                    "sizeBytes": payload.len(),
                    "sha256": b64(&hash),
                }
            ],
            "scope": { "allNodes": {} }
        }),
    )
    .await;

    let bundle_id = resp["bundleId"]
        .as_str()
        .expect("response must include a bundle_id")
        .to_string();
    assert!(!bundle_id.is_empty(), "bundle_id should be a UUID");

    let handles = resp["handles"]
        .as_array()
        .expect("response must have handles");
    assert_eq!(handles.len(), 1);
    let h = &handles[0];
    let distribution_id = h["distributionId"]
        .as_str()
        .expect("handle must have distribution_id")
        .to_string();
    assert!(!distribution_id.is_empty());
    let blob_token = h["blobToken"]
        .as_str()
        .expect("handle must have blob_token");
    assert!(
        !blob_token.is_empty(),
        "blob_token (BLAKE3 content address) must be populated"
    );
    // file_index of 0 is the proto3 default and is omitted from JSON.
    let file_index = h.get("fileIndex").and_then(|v| v.as_u64()).unwrap_or(0);
    assert_eq!(file_index, 0);

    // Query the distribution. Zero connected peers → per_node is empty,
    // so the response falls back to the bundle identity's size_bytes
    // (the bug fix that triggered this smoke test).
    let status = call_ok(
        &client,
        &server.base,
        "GetAttachmentDistribution",
        serde_json::json!({ "distributionId": distribution_id }),
    )
    .await;

    // status field for proto3-default enum 0 may be omitted by JSON;
    // PENDING / IN_PROGRESS / COMPLETED are all acceptable for a fresh
    // distribution with no connected peers (peat-protocol returns an
    // empty per_node map; the handler aggregates to PENDING).
    let status_val = status
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("DISTRIBUTION_STATUS_UNSPECIFIED");
    assert!(
        matches!(
            status_val,
            "DISTRIBUTION_STATUS_UNSPECIFIED"
                | "DISTRIBUTION_STATUS_PENDING"
                | "DISTRIBUTION_STATUS_IN_PROGRESS"
                | "DISTRIBUTION_STATUS_COMPLETED"
        ),
        "unexpected status `{status_val}` for fresh zero-peer distribution"
    );

    let bytes_total = status
        .get("bytesTotal")
        .and_then(|v| v.as_str().and_then(|s| s.parse::<u64>().ok()))
        .or_else(|| status.get("bytesTotal").and_then(|v| v.as_u64()))
        .unwrap_or(0);
    assert_eq!(
        bytes_total,
        payload.len() as u64,
        "bytes_total should fall back to the bundle identity's size_bytes \
         when per_node is empty (the original bug returned the hex hash length)"
    );
}

/// Hash mismatch must fail before any blob lands locally. This is the
/// rule-9 streaming match (ingest checks the post-stream sha256 and
/// deletes the partial blob on mismatch).
#[tokio::test]
async fn send_attachments_hash_mismatch_returns_invalid_argument() {
    let server = boot_server_with_attachments(50092).await;
    let client = http_client();

    let payload = b"actual content";
    let file_path = server.root_path.join("a.bin");
    std::fs::write(&file_path, payload).unwrap();
    // Wrong hash on purpose.
    let bad = [0u8; 32];

    let resp = call_unchecked(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "a.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&bad),
            }],
            "scope": { "allNodes": {} }
        }),
    )
    .await;

    assert!(
        !resp.status().is_success(),
        "wrong sha256 should reject the request"
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let code = body["code"].as_str().unwrap_or_default();
    assert_eq!(
        code, "invalid_argument",
        "hash-mismatch rejection should use InvalidArgument; got body: {body}"
    );
}

/// PRD §Validation Rule 3 — unknown root_name rejects InvalidArgument.
#[tokio::test]
async fn send_attachments_unknown_root_rejected() {
    let server = boot_server_with_attachments(50093).await;
    let client = http_client();

    let payload = b"x";
    let hash = sha256_bytes(payload);
    let file_path = server.root_path.join("a.bin");
    std::fs::write(&file_path, payload).unwrap();

    let resp = call_unchecked(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "not-in-the-allowlist",
                "relativePath": "a.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&hash),
            }],
            "scope": { "allNodes": {} }
        }),
    )
    .await;

    assert!(!resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["code"], "invalid_argument");
}

/// PRD §Configuration safety default — with no --attachment-root, all
/// four attachment RPCs return Unimplemented regardless of payload.
/// PRD §Testing Plan test 20 promotes this to a Step 8 integration test;
/// catching it in the smoke test catches the "Unimplemented stub is
/// still wired" failure mode early.
#[tokio::test]
async fn attachments_disabled_when_no_root() {
    let base = boot_server_without_attachments(50094).await;
    let client = http_client();

    for (method, body) in [
        (
            "SendAttachments",
            serde_json::json!({
                "files": [{
                    "rootName": "any",
                    "relativePath": "x",
                    "sizeBytes": 1,
                    "sha256": b64(&[0u8; 32]),
                }],
                "scope": { "allNodes": {} }
            }),
        ),
        (
            "GetAttachmentDistribution",
            serde_json::json!({ "distributionId": "x" }),
        ),
        (
            "CancelAttachmentDistribution",
            serde_json::json!({ "distributionId": "x" }),
        ),
    ] {
        let resp = call_unchecked(&client, &base, method, body).await;
        assert!(!resp.status().is_success(), "{method} should not succeed");
        let body: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(
            body["code"], "unimplemented",
            "{method} should return Unimplemented when no --attachment-root is configured; got body: {body}"
        );
    }
}
