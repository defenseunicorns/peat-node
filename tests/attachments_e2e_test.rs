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
use peat_protocol::storage::{read_distribution_document, TransferState};
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
            blob_stall_timeout: None,
            node_id: format!("e2e-{label}"),
            app_id: "e2e-attachments".into(),
            shared_key: String::new(),
            data_dir: data_dir.path().to_path_buf(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: Some(iroh_port),
            attachment_config,
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
            ..Default::default()
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
#[serial_test::serial(iroh_two_node)]
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

/// `iroh::EndpointId::fmt_short` emits the first 10 hex chars of the
/// full 64-char endpoint id. `IrohFileDistribution::resolve_targets`
/// formats peers this way when building `target_nodes`, and the inbox
/// watcher compares this form against its own endpoint, so a
/// `NodeListScope` caller must use the same 10-char prefix.
fn short_endpoint(full: &str) -> String {
    full.chars().take(10).collect()
}

/// PRD §Testing Plan test 22 — `send_node_list_only_delivers_to_listed`.
///
/// Three-node mesh A / B / C, all peered. A sends with
/// `NodeListScope{[B_short]}`. The acceptance contract has two halves:
///
///   1. B receives the file on its filesystem inbox.
///   2. C does NOT receive — its `target_nodes` exclusion is honoured
///      by the inbox watcher's targeting check (the doc still syncs to
///      C, but C's short endpoint id isn't in `target_nodes`, so it
///      records the doc as handled and short-circuits before fetch).
///
/// "C does not receive" is asserted after B's delivery completes — by
/// that point the distribution doc has flowed through Automerge sync
/// to every connected peer (including C), and C's watcher has had
/// enough sweeps to act on it. Then we wait an extra 3 watcher
/// intervals as a buffer before asserting C's inbox is still empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(iroh_two_node)]
async fn node_list_scope_only_delivers_to_listed_nodes() {
    init_test_tracing();
    const A_GRPC: u16 = 50141;
    const A_IROH: u16 = 51241;
    const B_GRPC: u16 = 50142;
    const B_IROH: u16 = 51242;
    const C_GRPC: u16 = 50143;
    const C_IROH: u16 = 51243;

    let a = boot(A_GRPC, A_IROH, "sender", false).await;
    let b = boot(B_GRPC, B_IROH, "receiver-listed", true).await;
    let c = boot(C_GRPC, C_IROH, "receiver-excluded", true).await;
    let http = http_client();

    let endpoint_a = call(&http, &a.base, "GetStatus", serde_json::json!({})).await["endpointAddr"]
        .as_str()
        .unwrap()
        .to_string();
    let endpoint_b = call(&http, &b.base, "GetStatus", serde_json::json!({})).await["endpointAddr"]
        .as_str()
        .unwrap()
        .to_string();
    let endpoint_c = call(&http, &c.base, "GetStatus", serde_json::json!({})).await["endpointAddr"]
        .as_str()
        .unwrap()
        .to_string();

    // Bidirectional peering: A needs B and C in its known_peers for
    // resolve_targets to include them; B and C need A so they can
    // pull blobs from it (and so Automerge sync flows the
    // file_distributions doc into their store).
    for (from_base, peer_id, peer_iroh_port) in [
        (&b.base, &endpoint_a, A_IROH),
        (&a.base, &endpoint_b, B_IROH),
        (&c.base, &endpoint_a, A_IROH),
        (&a.base, &endpoint_c, C_IROH),
    ] {
        call(
            &http,
            from_base,
            "ConnectPeer",
            serde_json::json!({
                "endpointId": peer_id,
                "addresses": [format!("127.0.0.1:{peer_iroh_port}")],
            }),
        )
        .await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    for node_base in [&a.base, &b.base, &c.base] {
        call(&http, node_base, "StartSync", serde_json::json!({})).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload = b"NodeList test - only B is in the target list".to_vec();
    let hash = sha256_of(&payload);
    std::fs::write(a.root_path.join("listed.bin"), &payload).unwrap();

    let b_short = short_endpoint(&endpoint_b);

    let resp = call(
        &http,
        &a.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "listed.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&hash),
            }],
            "scope": { "nodeList": { "nodeIds": [b_short.clone()] } }
        }),
    )
    .await;
    let distribution_id = resp["handles"][0]["distributionId"]
        .as_str()
        .expect("SendAttachments must return a distribution_id")
        .to_string();

    // B receives.
    let b_path = await_inbox_file(
        &b.inbox_path,
        &distribution_id,
        &payload,
        Duration::from_secs(30),
    )
    .await;
    let b_bytes = tokio::fs::read(&b_path).await.unwrap();
    assert_eq!(
        sha256_of(&b_bytes),
        hash,
        "B's content must match sender's sha256"
    );

    // Buffer past B's delivery so we're not racing C's watcher. 3
    // watcher intervals (default 1s) is enough headroom for C to act
    // on the synced doc if it were going to.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // C does NOT receive.
    let c_dist_dir = c.inbox_path.join(&distribution_id);
    let c_has_payload = if c_dist_dir.is_dir() {
        // Allow the directory to exist but assert no non-hidden file of
        // the payload's size lives there. The watcher's `already_delivered`
        // gate uses size-matching, and a sender-controlled empty dir
        // wouldn't be a delivery anyway. Inspecting size is more direct
        // than relying on the absence of the dir itself.
        let mut iter = tokio::fs::read_dir(&c_dist_dir).await.unwrap();
        let mut found_payload = false;
        while let Ok(Some(entry)) = iter.next_entry().await {
            let p = entry.path();
            if p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|s| s.starts_with('.'))
            {
                continue;
            }
            if let Ok(md) = entry.metadata().await {
                if md.is_file() && md.len() == payload.len() as u64 {
                    found_payload = true;
                    break;
                }
            }
        }
        found_payload
    } else {
        false
    };
    assert!(
        !c_has_payload,
        "C must NOT receive the file — NodeListScope was [{b_short}] only. \
         C's endpoint short is {} and was not in target_nodes.",
        short_endpoint(&endpoint_c)
    );

    drop((a, b, c));
}

/// Receiver-side regression for the `attachments::inbox` node-status
/// writes (the contract this PR introduces in `src/attachments/inbox.rs`).
///
/// The end-to-end byte-delivery test (`end_to_end_attachment_delivery_two_nodes`)
/// only proves bytes-on-disk; it can't fail if a future refactor silently
/// drops the `write_node_status(Transferring)` or `_(Completed)` call sites
/// — `serde_json::to_vec` errors are best-effort-logged-and-skipped, an
/// early-return added to the fetch path before the second write would go
/// undetected, and the cargo-test suite would still pass green.
///
/// PRD-006 test 23 (`subscribe_emits_progress_then_terminal` in
/// `attachments_deferred_test.rs`) IS that contract test from the
/// **sender's** observation perspective — assert ≥1 IN_PROGRESS + 1
/// terminal frame on `SubscribeAttachmentBundle`. But test 23 is blocked
/// on an upstream peat-mesh substrate race that drops the second of two
/// back-to-back receiver doc writes when they fire within the 100ms
/// sync_cooldown (defenseunicorns/peat#864). So we cannot rely on test 23
/// today to catch a regression in *this* PR.
///
/// This test verifies the receiver-local half of the contract — by the
/// time the inbox file is on disk, the receiver MUST have written both
/// `Transferring` (before fetch) and `Completed` (after atomic rename
/// inbox write) into its local copy of the distribution document. We
/// read the receiver's `AutomergeStore` directly, bypassing the sync
/// path that the upstream race affects. A regression that silently
/// drops the Completed write (or both) fails this test deterministically.
///
/// Once peat#864 lands and test 23 un-ignores, the two tests cover
/// complementary halves: this one pins the receiver's local doc state,
/// test 23 pins the sender's observable broadcast.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(iroh_two_node)]
async fn receiver_writes_node_status_into_distribution_doc() {
    init_test_tracing();
    const A_GRPC: u16 = 50151;
    const A_IROH: u16 = 51251;
    const B_GRPC: u16 = 50152;
    const B_IROH: u16 = 51252;

    let a = boot(A_GRPC, A_IROH, "sender", false).await;
    let b = boot(B_GRPC, B_IROH, "receiver", true).await;
    let http = http_client();

    let status_a = call(&http, &a.base, "GetStatus", serde_json::json!({})).await;
    let status_b = call(&http, &b.base, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"].as_str().unwrap().to_string();
    let endpoint_b = status_b["endpointAddr"].as_str().unwrap().to_string();
    let b_short = b.node.endpoint_short_id();

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
    tokio::time::sleep(Duration::from_secs(2)).await;

    call(&http, &a.base, "StartSync", serde_json::json!({})).await;
    call(&http, &b.base, "StartSync", serde_json::json!({})).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload = b"receiver-node-status: this exact byte sequence must arrive on B and B must record its delivery into the distribution doc".to_vec();
    let hash = sha256_of(&payload);
    let file_path = a.root_path.join("recv-status.bin");
    std::fs::write(&file_path, &payload).unwrap();

    let resp = call(
        &http,
        &a.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "recv-status.bin",
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

    // First, wait for byte-on-disk delivery — that's the sync point
    // after which both node-status writes must have run.
    let _ = await_inbox_file(
        &b.inbox_path,
        &distribution_id,
        &payload,
        Duration::from_secs(30),
    )
    .await;

    // Poll the receiver's local distribution doc until `node_statuses`
    // for this node reaches `Completed`, with a 15-second timeout. The
    // delivery-complete signal (`await_inbox_file`) returns when the
    // atomic-rename inbox write lands; the `Completed` node-status
    // write fires *immediately after* — so on a quiet local run the
    // very next poll sees `Completed`. CI runs on shared hardware where
    // the inbox-watcher task can be preempted between the rename and
    // the `write_node_status(Completed)` site by minutes of other
    // work, and inbound automerge-sync round-trips may overwrite the
    // local doc's `"data"` scalar before the second write lands.
    // Polling pins the contract on the *eventual* state rather than
    // exact timing, and surfaces the pre-Completed snapshot in the
    // failure message so a regression that *drops* the write is
    // distinguishable from sheer slowness.
    let poll_deadline = Instant::now() + Duration::from_secs(15);
    let entry = loop {
        let doc = read_distribution_document(b.node.document_store().as_ref(), &distribution_id)
            .expect("read_distribution_document must succeed")
            .expect("distribution doc must exist on the receiver after delivery");
        if let Some(entry) = doc.node_statuses.get(&b_short) {
            if entry.status == TransferState::Completed {
                break entry.clone();
            }
        }
        if Instant::now() >= poll_deadline {
            panic!(
                "receiver's local node_status never reached Completed within 15s of \
                 byte-on-disk delivery. b_short={b_short}. Last-seen \
                 doc.node_statuses: {:?}. A regression here means the Completed write \
                 site in `attachments::inbox::scan_once` was dropped, its `?` chain \
                 swallowed an error, or the local doc is being overwritten by \
                 inbound sync after the write.",
                doc.node_statuses
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert_eq!(
        entry.node_id, b_short,
        "node_id in the written NodeTransferStatus must match the receiver's \
         own short endpoint id (peer.fmt_short())"
    );
    assert_eq!(
        entry.total_bytes,
        payload.len() as u64,
        "receiver should have stamped total_bytes from the distribution doc's \
         blob_size, not from a hardcoded zero or the wrong field"
    );
    assert!(
        entry.completed_at.is_some(),
        "receiver should have stamped completed_at on the Completed write"
    );

    drop((a, b));
}

/// peat-node#69 acceptance: the sender's **unary** `GetAttachmentDistribution`
/// poll must reach `DISTRIBUTION_STATUS_COMPLETED` against a real
/// two-peer transfer.
///
/// Test 23 (`subscribe_emits_progress_then_terminal`) covers the
/// server-streaming `SubscribeAttachmentBundle` path and
/// `receiver_writes_node_status_into_distribution_doc` covers the
/// receiver's local doc write — but #69 is specifically about the
/// operator-facing unary poll (`GetAttachmentDistribution`), which
/// `send.sh` and any polling client hit. Pre-#864 it stayed
/// PENDING/IN_PROGRESS forever because no receiver→sender status
/// propagation existed. The peat-protocol rc.9 typed-`node_statuses`
/// substrate + the sender-side watcher feed the same in-memory
/// `IrohFileDistribution::status()` that `get_attachment_distribution`
/// reads, so this path now advances — this test is the missing
/// evidence that closes #69.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(iroh_two_node)]
async fn sender_get_attachment_distribution_reaches_completed_two_nodes() {
    init_test_tracing();
    const A_GRPC: u16 = 50161;
    const A_IROH: u16 = 51261;
    const B_GRPC: u16 = 50162;
    const B_IROH: u16 = 51262;

    let a = boot(A_GRPC, A_IROH, "sender", false).await;
    let b = boot(B_GRPC, B_IROH, "receiver", true).await;
    let http = http_client();

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
    tokio::time::sleep(Duration::from_secs(2)).await;

    call(&http, &a.base, "StartSync", serde_json::json!({})).await;
    call(&http, &b.base, "StartSync", serde_json::json!({})).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload = b"peat-node#69: sender GetAttachmentDistribution must reach COMPLETED".to_vec();
    let hash = sha256_of(&payload);
    let file_path = a.root_path.join("unary-complete.bin");
    std::fs::write(&file_path, &payload).unwrap();

    let resp = call(
        &http,
        &a.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "unary-complete.bin",
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

    // Sync point: bytes land on B's inbox, after which B's
    // Completed node-status write propagates back to A.
    let _ = await_inbox_file(
        &b.inbox_path,
        &distribution_id,
        &payload,
        Duration::from_secs(30),
    )
    .await;

    // Poll A's unary GetAttachmentDistribution until it reports
    // COMPLETED. 30s ceiling: byte delivery already happened, so this
    // is just waiting for the receiver's node-status write to CRDT-sync
    // back to A and the sender watcher to fold it into the in-memory
    // status the handler reads. A regression that breaks receiver→sender
    // propagation leaves this stuck at PENDING/IN_PROGRESS until the
    // deadline, and the panic surfaces the last-seen status.
    let poll_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let body = call(
            &http,
            &a.base,
            "GetAttachmentDistribution",
            serde_json::json!({ "distributionId": distribution_id }),
        )
        .await;
        let status_val = body
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("DISTRIBUTION_STATUS_UNSPECIFIED");
        if status_val == "DISTRIBUTION_STATUS_COMPLETED" {
            break;
        }
        if Instant::now() >= poll_deadline {
            panic!(
                "sender's GetAttachmentDistribution never reached \
                 DISTRIBUTION_STATUS_COMPLETED within 30s of byte-on-disk \
                 delivery (peat-node#69). Last-seen status: {status_val}, \
                 full body: {body}. A regression here means receiver→sender \
                 node_status propagation broke — the substrate write \
                 (peat-protocol write_receiver_node_status), the CRDT \
                 sync-back, or the sender-side watcher folding it into \
                 IrohFileDistribution::status()."
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    drop((a, b));
}
