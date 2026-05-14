//! End-to-end attachment delivery test.
//!
//! This is the test that should have existed in the PRD-006 v1 PR. The
//! v1 surface as shipped proved sender-side correctness in isolation
//! (validate, ingest, registry, runtime) but never verified that a file
//! sent from node A actually arrives on node B. PRD §Testing Plan tests
//! 21 and 22 were deferred because they assumed peat-protocol's
//! receive-side observer hooks would auto-pull on receivers — those
//! hooks aren't implemented, so I marked them `#[ignore]`'d. Result:
//! the merged surface passed every test, satisfied the QA reviewer,
//! and didn't deliver any files.
//!
//! `attachments::inbox` closes that gap inside peat-node: a polling
//! watcher observes the synced `file_distributions` collection and
//! pulls targeting blobs via `NetworkedIrohBlobStore::fetch_blob`. This
//! test proves the watcher actually works against a real two-peer iroh
//! mesh: boot A and B, peer them, send from A, then assert that the
//! exact file content appears under B's `--attachment-inbox` directory.
//!
//! No `#[ignore]` here — this is the acceptance gate.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use peat_node::attachments::config::{AttachmentConfig, AttachmentPriorityCli};
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;
use sha2::{Digest, Sha256};

struct BootedNode {
    base: String,
    /// Kept for tests that need to reach past the HTTP layer (none in
    /// this file currently — the e2e test is wire-driven — but future
    /// tests adding mid-stream registry probes or fault injection will
    /// want it).
    #[allow(dead_code)]
    node: Arc<SidecarNode>,
    _data_dir: tempfile::TempDir,
    _root_dir: tempfile::TempDir,
    _inbox_dir: tempfile::TempDir,
    root_path: PathBuf,
    inbox_path: PathBuf,
}

async fn boot(grpc_port: u16, iroh_port: u16, label: &str, enable_inbox: bool) -> BootedNode {
    let data_dir = tempfile::tempdir().unwrap();
    let root_dir = tempfile::tempdir().unwrap();
    let inbox_dir = tempfile::tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();
    let inbox_path = inbox_dir.path().to_path_buf();

    let attachment_config = AttachmentConfig::from_raw(
        &[format!("outbox={}", root_path.display())],
        if enable_inbox {
            Some(inbox_path.clone())
        } else {
            None
        },
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
        // Tight poll interval so the test doesn't wait 1s+ for each
        // watcher tick. Real deployments use the default (1s).
        1,
    )
    .unwrap();

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: format!("e2e-{label}"),
            app_id: "e2e-attachments".into(),
            shared_key: String::new(),
            data_dir: data_dir.path().to_path_buf(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: Some(iroh_port),
            attachment_config,
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
        base: format!("http://127.0.0.1:{grpc_port}"),
        node,
        _data_dir: data_dir,
        _root_dir: root_dir,
        _inbox_dir: inbox_dir,
        root_path,
        inbox_path,
    }
}

fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
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

fn sha256_of(data: &[u8]) -> [u8; 32] {
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

/// Poll the inbox until a file with `expected` bytes appears under
/// `{inbox}/{distribution_id}/...`. Returns the path. Times out after
/// 30 seconds to bound failure-mode flakiness — successful delivery
/// usually lands in under 3 seconds (iroh handshake + one watcher
/// tick).
async fn await_inbox_file(
    inbox: &std::path::Path,
    distribution_id: &str,
    expected: &[u8],
    deadline: Duration,
) -> PathBuf {
    let deadline_at = Instant::now() + deadline;
    let dist_dir = inbox.join(distribution_id);
    while Instant::now() < deadline_at {
        if dist_dir.is_dir() {
            // Look for any file in the distribution-id subdirectory.
            if let Ok(mut iter) = tokio::fs::read_dir(&dist_dir).await {
                while let Ok(Some(entry)) = iter.next_entry().await {
                    let path = entry.path();
                    // Skip our own in-flight tmp marker
                    if path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|s| s.starts_with('.'))
                    {
                        continue;
                    }
                    if path.is_file() {
                        if let Ok(actual) = tokio::fs::read(&path).await {
                            if actual == expected {
                                return path;
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "no file with the expected content appeared in {} within {:?}",
        dist_dir.display(),
        deadline
    );
}

/// Boot A + B, peer them, send a real file from A, assert the *same
/// bytes* land on B's filesystem inbox under the distribution_id
/// subdirectory. This is the missing acceptance: prior to this test,
/// no automated check verified that any peer ever received an
/// attachment.
fn init_test_tracing() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "peat_node=info".into()),
            )
            .with_test_writer()
            .try_init();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_to_end_attachment_delivery_two_nodes() {
    init_test_tracing();
    const A_GRPC: u16 = 50131;
    const A_IROH: u16 = 51231;
    const B_GRPC: u16 = 50132;
    const B_IROH: u16 = 51232;

    // A is the sender (no inbox needed). B is the receiver (inbox on).
    let a = boot(A_GRPC, A_IROH, "sender", false).await;
    let b = boot(B_GRPC, B_IROH, "receiver", true).await;
    let http = http_client();

    // Get each node's iroh endpoint and peer them BOTH directions so
    // that A's `known_peers` (used by `resolve_targets` for
    // AllNodesScope) includes B. If only B → A is wired, B knows
    // about A but A doesn't have B in its known_peers at distribute
    // time, so the distribution doc's target_nodes will be empty and
    // B won't trigger on the targeting check.
    let status_a = call(&http, &a.base, "GetStatus", serde_json::json!({})).await;
    let status_b = call(&http, &b.base, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"].as_str().unwrap().to_string();
    let endpoint_b = status_b["endpointAddr"].as_str().unwrap().to_string();

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
    call(
        &http,
        &a.base,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_b,
            "addresses": [format!("127.0.0.1:{B_IROH}")],
        }),
    )
    .await;

    // Give iroh a moment to settle the handshake so both sides see
    // each other as known peers. 2 seconds matches the existing
    // sync_test convention.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Start sync on both nodes. Test fixtures don't auto-sync the way
    // `main.rs` does, so the file_distributions doc would never reach
    // B without an explicit StartSync — same pattern as sync_test.rs.
    call(&http, &a.base, "StartSync", serde_json::json!({})).await;
    call(&http, &b.base, "StartSync", serde_json::json!({})).await;
    // Brief settle so the sync coordinator latches the flag before we
    // do the local write.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write a real file on A's outbox and send it.
    let payload =
        b"end-to-end attachment delivery - this exact byte sequence must arrive on B".to_vec();
    let hash = sha256_of(&payload);
    let file_path = a.root_path.join("delivery.bin");
    std::fs::write(&file_path, &payload).unwrap();

    let resp = call(
        &http,
        &a.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "delivery.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&hash),
            }],
            "scope": { "allNodes": {} }
        }),
    )
    .await;
    let distribution_id = resp["handles"][0]["distributionId"]
        .as_str()
        .expect("SendAttachments must return a distribution_id")
        .to_string();
    assert!(!distribution_id.is_empty());

    // The assertion that matters: B's filesystem inbox eventually
    // contains a file with the same bytes. 30-second timeout gives
    // iroh + watcher headroom but successful runs land in <3 seconds.
    let received_path = await_inbox_file(
        &b.inbox_path,
        &distribution_id,
        &payload,
        Duration::from_secs(30),
    )
    .await;

    // Per-byte and sha256 cross-check on the received file.
    let received = tokio::fs::read(&received_path).await.unwrap();
    assert_eq!(
        received.len(),
        payload.len(),
        "received byte count mismatch"
    );
    assert_eq!(received, payload, "received bytes don't match sent bytes");
    assert_eq!(
        sha256_of(&received),
        hash,
        "sha256 of received file doesn't match sender's declared hash"
    );

    // Sender side: file is also accessible locally (the sender wrote
    // the original; the watcher self-skip means it isn't duplicated
    // into A's inbox even if A had one).
    let on_sender = std::fs::read(&file_path).unwrap();
    assert_eq!(on_sender, payload);

    // Keep `_data_dir` etc. guards alive across the assertion phase by
    // touching them; tempfile cleans up on drop after the test exits.
    drop((a, b));
}
