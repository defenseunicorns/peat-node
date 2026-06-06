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
    /// Direct handle to the SidecarNode behind the HTTP server — used by
    /// tests that need to seed registry state past the wire layer
    /// (PRD test 26 in particular, where the only way to deterministically
    /// keep a bundle non-terminal is to insert it directly).
    node: Arc<SidecarNode>,
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
    cfg_override(&mut attachment_config);

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
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

    let service = Arc::new(PeatSidecarService::new(Arc::clone(&node)));
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
        node,
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

/// `max_concurrent_distributions = 1` plus a non-terminal bundle resident
/// in the registry must cause SendAttachments to reject with
/// `ResourceExhausted`. The earlier HTTP-driven version of this test
/// exploited an over-count bug (`registry.len()` instead of
/// `non_terminal_count`) — flagged by the PRD-006 QA review and fixed in
/// the same commit. With the honest counter, two sequential HTTP Sends
/// don't both stay in-flight because the watcher's zero-peer short-
/// circuit drives the first bundle to Completed before the second
/// arrives. The test is now in-process so it can seed a Pending bundle
/// directly via the registry — deterministic, and exercises the same
/// `in_flight_count`-vs-cap path the original HTTP test was reaching.
#[tokio::test]
async fn concurrent_cap_returns_resource_exhausted() {
    use peat_node::attachments::handlers;
    use peat_node::attachments::registry::{
        BundleIdentity, BundleRecord, BundleStatus, FileIdentity,
    };
    use peat_node::pb;

    let server = boot_server(50111, |cfg| {
        cfg.max_concurrent_distributions = 1;
        cfg.queue_when_full = false;
    })
    .await;
    // Reach past the HTTP layer for direct-Arc access to the node so we
    // can seed a Pending bundle into the registry. The HTTP path stays
    // in scope below (the real send_attachments call goes through it).
    let node = server.node.clone();

    // Seed a non-terminal bundle. `BundleRecord::new` defaults to
    // `BundleStatus::Pending`, which the in_flight_count helper picks up
    // because it filters by `!is_terminal()`.
    let identity = BundleIdentity {
        files: vec![FileIdentity {
            root_name: "synthetic".into(),
            relative_path: "synthetic.bin".into(),
            size_bytes: 1,
            sha256: [0u8; 32],
        }],
    };
    let synthetic = BundleRecord::new("synthetic-in-flight".into(), identity, vec![]);
    assert!(!synthetic.status.is_terminal()); // Pending — counts against the cap
    let _ = BundleStatus::Pending; // silence unused-import in the test
    node.bundle_registry().insert(synthetic);
    assert_eq!(node.bundle_registry().non_terminal_count(), 1);

    // Now a real SendAttachments must hit the cap. Going through the
    // in-process handler exercises the same in_flight_count call the
    // HTTP path uses; the only thing skipped is buffa wire encoding,
    // which the smoke test already covers.
    let payload = b"would have been the second send";
    let hash = write_file(&server.root_path, "second.bin", payload);
    let req = pb::SendAttachmentsRequest {
        files: vec![pb::FileSpec {
            root_name: "outbox".into(),
            relative_path: "second.bin".into(),
            size_bytes: payload.len() as u64,
            sha256: hash.to_vec(),
            ..Default::default()
        }],
        scope: buffa::MessageField::some(pb::DistributionScopeSpec {
            scope: Some(pb::distribution_scope_spec::Scope::AllNodes(Box::default())),
            ..Default::default()
        }),
        ..Default::default()
    };

    let err = handlers::send_attachments(&node, req).await.unwrap_err();
    assert_eq!(
        err.code,
        connectrpc::ErrorCode::ResourceExhausted,
        "non-terminal bundle resident + max_concurrent=1 must reject ResourceExhausted; got: {err}"
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

// ─────────────────────────────────────────────────────────────────────
// Retention eviction wiring
// ─────────────────────────────────────────────────────────────────────

/// PRD §Configuration retention semantic — the background task wired in
/// `SidecarNode::new` calls `bundle_registry.evict_expired()` periodically
/// so terminal bundles age out. Earlier, `evict_expired` existed and was
/// unit-tested but nothing actually called it from the running service;
/// the `--attachment-handle-retention-secs` knob was operator-visible but
/// inert. This test set retention=1s, drives a bundle to a terminal state
/// (zero-peer scope → immediate Completed via the watcher's initial
/// status shortcut), then waits long enough for the periodic sweep to
/// fire AT LEAST once and verifies the prior distribution_id no longer
/// resolves.
#[tokio::test]
async fn retention_eviction_fires_in_background() {
    let server = boot_server(50114, |cfg| {
        cfg.handle_retention_secs = 1;
        // Concurrent cap on its own would prevent the second bundle from
        // ingesting until the first ages out — but we only need one
        // bundle here, so leave defaults.
    })
    .await;
    let client = http_client();

    let payload = b"retention candidate";
    let hash = write_file(&server.root_path, "retain.bin", payload);
    let (status, body) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        send_body("outbox", "retain.bin", payload.len() as u64, &hash),
    )
    .await;
    assert!(status.is_success(), "send must succeed; body: {body}");
    let dist_id = body["handles"][0]["distributionId"]
        .as_str()
        .expect("distribution_id must be present")
        .to_string();

    // Brief settle for the watcher's zero-peer terminal-frame fire.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Sanity: distribution_id resolves immediately after send.
    let (_, body_now) = call_raw(
        &client,
        &server.base,
        "GetAttachmentDistribution",
        serde_json::json!({ "distributionId": dist_id }),
    )
    .await;
    assert!(
        body_now.get("code").is_none(),
        "distribution_id must resolve immediately after send; body: {body_now}"
    );

    // Wait past retention + at least one eviction tick (interval is
    // max(1, retention/2) = max(1, 0) = 1s for retention=1, so a 3-second
    // wait observes at minimum two ticks past the retention cutoff).
    tokio::time::sleep(Duration::from_secs(3)).await;

    let (status_after, body_after) = call_raw(
        &client,
        &server.base,
        "GetAttachmentDistribution",
        serde_json::json!({ "distributionId": dist_id }),
    )
    .await;
    assert!(
        !status_after.is_success(),
        "after retention window, prior distribution_id must NOT resolve; status: {status_after}, body: {body_after}"
    );
    assert_eq!(
        body_after["code"], "not_found",
        "evicted bundle's distribution_id must now lookup as NotFound; body: {body_after}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// PRD §Validation Rule 12 — idempotent resubmit over the wire
// ─────────────────────────────────────────────────────────────────────

/// Same `bundle_id` + identical FileSpec set → identical handles. The
/// registry's unit test confirms `check_resubmit` returns Idempotent;
/// this drives the path over the Connect wire so a JSON-encoding mismatch
/// between request and the registry's `BundleIdentity` cannot regress
/// silently.
#[tokio::test]
async fn idempotent_resubmit_returns_same_handles_over_http() {
    let server = boot_server(50115, |_| {}).await;
    let client = http_client();

    let payload = b"idempotency payload";
    let hash = write_file(&server.root_path, "ide.bin", payload);
    let req = serde_json::json!({
        "files": [{
            "rootName": "outbox",
            "relativePath": "ide.bin",
            "sizeBytes": payload.len(),
            "sha256": b64(&hash),
        }],
        "scope": { "allNodes": {} },
        "bundleId": "idem-X",
    });

    let (s1, b1) = call_raw(&client, &server.base, "SendAttachments", req.clone()).await;
    assert!(s1.is_success(), "first send must succeed; body: {b1}");
    let dist_1 = b1["handles"][0]["distributionId"]
        .as_str()
        .expect("first response must include a distribution_id")
        .to_string();
    let blob_1 = b1["handles"][0]["blobToken"]
        .as_str()
        .unwrap_or("")
        .to_string();

    let (s2, b2) = call_raw(&client, &server.base, "SendAttachments", req).await;
    assert!(
        s2.is_success(),
        "second (idempotent) send must succeed; body: {b2}"
    );
    let dist_2 = b2["handles"][0]["distributionId"].as_str().unwrap_or("");
    let blob_2 = b2["handles"][0]["blobToken"].as_str().unwrap_or("");
    assert_eq!(
        dist_1, dist_2,
        "idempotent resubmit must return the same distribution_id"
    );
    assert_eq!(
        blob_1, blob_2,
        "idempotent resubmit must return the same blob_token"
    );
    assert_eq!(b1["bundleId"], b2["bundleId"]);
}

/// Same `bundle_id` + deviating FileSpec set (different size_bytes) →
/// AlreadyExists. The conflict path in `check_resubmit` is unit-tested;
/// here we drive the wire path to confirm the handler maps
/// `BundleLookup::Conflict` to the right gRPC code.
#[tokio::test]
async fn bundle_id_reuse_with_different_files_returns_already_exists_over_http() {
    let server = boot_server(50116, |_| {}).await;
    let client = http_client();

    let original = b"original payload";
    let h1 = write_file(&server.root_path, "conf.bin", original);
    let (s1, b1) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "conf.bin",
                "sizeBytes": original.len(),
                "sha256": b64(&h1),
            }],
            "scope": { "allNodes": {} },
            "bundleId": "conflict-X",
        }),
    )
    .await;
    assert!(s1.is_success(), "first send must succeed; body: {b1}");

    // Resubmit same bundle_id with a different file (different size).
    let mutated = b"original payload extended"; // different size_bytes
    let h2 = write_file(&server.root_path, "conf.bin", mutated);
    let (s2, b2) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "conf.bin",
                "sizeBytes": mutated.len(),
                "sha256": b64(&h2),
            }],
            "scope": { "allNodes": {} },
            "bundleId": "conflict-X",
        }),
    )
    .await;
    assert!(!s2.is_success(), "deviating resubmit must fail");
    assert_eq!(
        b2["code"], "already_exists",
        "bundle-id conflict must map to AlreadyExists; body: {b2}"
    );
}

// ─────────────────────────────────────────────────────────────────────
// Wire-format coverage for AttachmentPriority / scope enums
// ─────────────────────────────────────────────────────────────────────

/// Proto-enum AttachmentPriority round-trips through the JSON wire. The
/// proto3 canonical JSON for enums is the unqualified short name (e.g.
/// "ATTACHMENT_PRIORITY_CRITICAL"). buffa accepts both the enum name and
/// the numeric form; the test drives the explicit-priority path to
/// confirm the BULK/LOW/ROUTINE/PRIORITY/CRITICAL mapping the handler
/// uses (1:1 onto peat-protocol TransferPriority with BULK collapsed
/// onto Low — see handlers::proto_priority_to_transfer's v1-honesty
/// note) actually decodes from the wire.
#[tokio::test]
async fn attachment_priority_critical_round_trips_over_http() {
    let server = boot_server(50117, |_| {}).await;
    let client = http_client();

    let payload = b"priority test";
    let hash = write_file(&server.root_path, "pri.bin", payload);
    let (s, b) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "pri.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&hash),
            }],
            "scope": { "allNodes": {} },
            "priority": "ATTACHMENT_PRIORITY_CRITICAL",
        }),
    )
    .await;
    assert!(
        s.is_success(),
        "request with explicit AttachmentPriority must succeed; body: {b}"
    );
    // The wire-side success is the primary assertion (the priority
    // wasn't rejected as an unknown enum value). A second-order check
    // is not directly possible without exposing the registry's stored
    // priority through an RPC — out of scope for v1.
    assert!(
        b["handles"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false),
        "response must include handles; body: {b}"
    );
}

/// PRD §Validation Rule 10 — FormationScope is rejected with
/// FAILED_PRECONDITION in v1 (no async resolution).
#[tokio::test]
async fn formation_scope_rejected_over_http() {
    let server = boot_server(50118, |_| {}).await;
    let client = http_client();

    let payload = b"x";
    let hash = write_file(&server.root_path, "f.bin", payload);
    let (s, b) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "f.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&hash),
            }],
            "scope": { "formation": { "formationId": "alpha-squad" } }
        }),
    )
    .await;
    assert!(!s.is_success(), "Formation scope must be rejected in v1");
    assert_eq!(
        b["code"], "failed_precondition",
        "Formation scope rejection must use FailedPrecondition; body: {b}"
    );
}

/// PRD §Validation Rule 10 — CapableScope is rejected with
/// FAILED_PRECONDITION; the capability vocabulary is deferred to a
/// follow-on ADR. Mirrors the validate-unit test
/// `validate_rejects_capable_scope_v1` over the wire.
#[tokio::test]
async fn capable_scope_rejected_over_http() {
    let server = boot_server(50119, |_| {}).await;
    let client = http_client();

    let payload = b"x";
    let hash = write_file(&server.root_path, "c.bin", payload);
    let (s, b) = call_raw(
        &client,
        &server.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "c.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&hash),
            }],
            "scope": { "capable": {} }
        }),
    )
    .await;
    assert!(!s.is_success(), "Capable scope must be rejected in v1");
    assert_eq!(
        b["code"], "failed_precondition",
        "Capable scope rejection must use FailedPrecondition; body: {b}"
    );
}
