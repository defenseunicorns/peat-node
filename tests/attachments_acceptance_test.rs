//! PRD-006 §Testing Plan integration tests — single-node scenarios that
//! don't need a multi-peer mesh.
//!
//! Multi-peer tests (PRD §Testing Plan 21, 22, 23) live in
//! `attachments_multi_peer_test.rs`. Late-subscribe variants (28, 29) live
//! in `attachments_subscribe_test.rs`. The safety-default (20) is covered
//! by `attachments_smoke_test.rs::attachments_disabled_when_no_root`.
//! Tests marked deferred in `attachments_deferred_test.rs` are #[ignore]'d
//! pending the supporting functionality (discovery-grace background task,
//! fault injection).
//!
//! Driven over HTTP+JSON (Connect protocol) so the proto wire codec is
//! also exercised end-to-end.

use std::path::PathBuf;
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
    _root_dir: tempfile::TempDir,
    root_path: PathBuf,
}

/// Boot a peat-node with attachments enabled and the supplied config
/// knobs. `cfg_override` runs before construction so individual tests
/// can dial down caps (e.g. `max_concurrent_distributions=1`).
async fn boot_server(port: u16, cfg_override: impl FnOnce(&mut AttachmentConfig)) -> BootedServer {
    let data_dir = tempfile::tempdir().unwrap();
    let root_dir = tempfile::tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();

    let mut attachment_config = AttachmentConfig::from_raw(
        &[format!("outbox={}", root_path.display())],
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
    )
    .unwrap();
    cfg_override(&mut attachment_config);

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: format!("accept-{port}"),
            app_id: "test".into(),
            shared_key: String::new(),
            data_dir: data_dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: None,
            attachment_config,
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

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

async fn call_raw(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/{method}");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.json().await.unwrap_or(serde_json::Value::Null);
    (status, body)
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

fn write_file(root: &std::path::Path, name: &str, bytes: &[u8]) -> [u8; 32] {
    let path = root.join(name);
    std::fs::write(&path, bytes).unwrap();
    sha256_bytes(bytes)
}

fn send_body(root_name: &str, rel: &str, size: u64, sha256: &[u8; 32]) -> serde_json::Value {
    serde_json::json!({
        "files": [{
            "rootName": root_name,
            "relativePath": rel,
            "sizeBytes": size,
            "sha256": b64(sha256),
        }],
        "scope": { "allNodes": {} }
    })
}

// ─────────────────────────────────────────────────────────────────────
// PRD test 26 — concurrent_cap_returns_resource_exhausted
// ─────────────────────────────────────────────────────────────────────

/// With `max_concurrent_distributions = 1` and `queue_when_full = false`,
/// the second SendAttachments must fail `ResourceExhausted` because the
/// first bundle is still resident in the registry. The handler counts
/// resident bundles via the registry as the v1 in-flight proxy (the
/// PRD-006 docs note this over-counts at the boundary but errs on the
/// stricter side, which is what we assert here).
#[tokio::test]
async fn concurrent_cap_returns_resource_exhausted() {
    let server = boot_server(50111, |cfg| {
        cfg.max_concurrent_distributions = 1;
        cfg.queue_when_full = false;
    })
    .await;
    let client = http_client();

    let bytes = b"first";
    let h1 = write_file(&server.root_path, "f1.bin", bytes);
    let (status1, _b1) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        send_body("outbox", "f1.bin", bytes.len() as u64, &h1),
    )
    .await;
    assert!(
        status1.is_success(),
        "first SendAttachments must succeed; got {status1}"
    );

    let bytes2 = b"second";
    let h2 = write_file(&server.root_path, "f2.bin", bytes2);
    let (status2, body2) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        send_body("outbox", "f2.bin", bytes2.len() as u64, &h2),
    )
    .await;
    assert!(
        !status2.is_success(),
        "second SendAttachments must hit the concurrency cap"
    );
    assert_eq!(
        body2["code"], "resource_exhausted",
        "concurrency-cap rejection must be ResourceExhausted; got body: {body2}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// PRD test 27 — lookup_unknown_ids_return_not_found
// ─────────────────────────────────────────────────────────────────────

/// `GetAttachmentDistribution`, `CancelAttachmentDistribution`, and
/// `SubscribeAttachmentBundle` each return `NotFound` (not
/// `InvalidArgument`, `Internal`, or an empty success) when called with
/// an unknown ID. PRD §Validation Rule 11's catch-all NotFound rule.
#[tokio::test]
async fn lookup_unknown_ids_return_not_found() {
    let server = boot_server(50112, |_| {}).await;
    let client = http_client();

    let (status_get, body_get) = call_raw(
        &client,
        &server.base,
        "GetAttachmentDistribution",
        serde_json::json!({ "distributionId": "missing" }),
    )
    .await;
    assert!(!status_get.is_success());
    assert_eq!(body_get["code"], "not_found", "Get body: {body_get}");

    let (status_cancel, body_cancel) = call_raw(
        &client,
        &server.base,
        "CancelAttachmentDistribution",
        serde_json::json!({ "distributionId": "missing" }),
    )
    .await;
    assert!(!status_cancel.is_success());
    assert_eq!(
        body_cancel["code"], "not_found",
        "Cancel body: {body_cancel}"
    );

    // SubscribeAttachmentBundle is server-streaming — Connect's wire
    // routing for streaming RPCs differs from unary (different
    // content-type, request framing). The unary HTTP+JSON harness here
    // would just see "method not found" rather than the underlying
    // NotFound. Subscribe's NotFound semantic is covered in-process by
    // `attachments_subscribe_test.rs::subscribe_unknown_bundle_returns_not_found`;
    // we exercise the two unary RPCs over the wire here and rely on the
    // in-process test for the streaming RPC.
}

// ─────────────────────────────────────────────────────────────────────
// PRD test 30 — evicted_bundle_id_treated_as_fresh_request
// ─────────────────────────────────────────────────────────────────────

/// With `max_known_bundles = 1`, ingesting a second bundle evicts the
/// first under LRU pressure. A resubmit of the evicted bundle_id with a
/// different FileSpec set is accepted as a fresh request (the registry
/// returns NotFound, the handler runs a fresh ingest, the original
/// distribution_ids are not resolvable). The PRD acceptance criterion
/// also requires the prior distribution_id to lookup as NotFound, which
/// we verify via GetAttachmentDistribution against the captured ID.
#[tokio::test]
async fn evicted_bundle_id_treated_as_fresh_request() {
    let server = boot_server(50113, |cfg| {
        cfg.max_known_bundles = 1;
        // Avoid the concurrent-cap tripping before LRU does.
        cfg.max_concurrent_distributions = 16;
    })
    .await;
    let client = http_client();

    // Bundle X (caller-supplied bundle_id so we can re-target it).
    let payload_x = b"bundle X content";
    let hash_x = write_file(&server.root_path, "x.bin", payload_x);
    let (status_x, body_x) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "x.bin",
                "sizeBytes": payload_x.len(),
                "sha256": b64(&hash_x),
            }],
            "scope": { "allNodes": {} },
            "bundleId": "X",
        }),
    )
    .await;
    assert!(
        status_x.is_success(),
        "bundle X must ingest; body: {body_x}"
    );
    let dist_x = body_x["handles"][0]["distributionId"]
        .as_str()
        .expect("bundle X must return a distribution_id")
        .to_string();

    // Bundle Y forces eviction of X under LRU. Re-use the same root.
    let payload_y = b"bundle Y content";
    let hash_y = write_file(&server.root_path, "y.bin", payload_y);
    let (status_y, body_y) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "y.bin",
                "sizeBytes": payload_y.len(),
                "sha256": b64(&hash_y),
            }],
            "scope": { "allNodes": {} },
            "bundleId": "Y",
        }),
    )
    .await;
    assert!(
        status_y.is_success(),
        "bundle Y must ingest; body: {body_y}"
    );

    // After Y, X is evicted. GetAttachmentDistribution against X's
    // prior distribution_id must return NotFound.
    let (status_get, body_get) = call_raw(
        &client,
        &server.base,
        "GetAttachmentDistribution",
        serde_json::json!({ "distributionId": dist_x }),
    )
    .await;
    assert!(
        !status_get.is_success(),
        "evicted distribution_id must not resolve; status: {status_get}"
    );
    assert_eq!(
        body_get["code"], "not_found",
        "evicted distribution_id must lookup as NotFound; body: {body_get}"
    );

    // Resubmit bundle_id = X with a different FileSpec set. Because X
    // was evicted, this is treated as a fresh request (no AlreadyExists).
    let payload_x_v2 = b"bundle X content v2 with different size";
    let hash_x_v2 = write_file(&server.root_path, "x.bin", payload_x_v2);
    let (status_x2, body_x2) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "x.bin",
                "sizeBytes": payload_x_v2.len(),
                "sha256": b64(&hash_x_v2),
            }],
            "scope": { "allNodes": {} },
            "bundleId": "X",
        }),
    )
    .await;
    assert!(
        status_x2.is_success(),
        "resubmit of evicted bundle_id must be accepted as fresh; body: {body_x2}"
    );
    let dist_x2 = body_x2["handles"][0]["distributionId"]
        .as_str()
        .expect("resubmit must return a distribution_id");
    assert_ne!(
        dist_x2, dist_x,
        "resubmit must produce a new distribution_id"
    );
}
