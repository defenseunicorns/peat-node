//! PRD-006 §Testing Plan tests that need functionality or fault-injection
//! hooks the v1 attachment surface doesn't yet expose. Each test that
//! remains `#[ignore]`'d documents the exact gap inline.
//!
//! Tests covered by other files:
//!
//! - 20 → `attachments_smoke_test::attachments_disabled_when_no_root`
//! - 21 → `attachments_multi_peer_test::receiver_can_fetch_blob_pushed_by_sender`
//!   covers the substrate; `attachments_e2e_test::end_to_end_attachment_delivery_two_nodes`
//!   covers the full sender→inbox path
//! - 22 → `attachments_e2e_test::node_list_scope_only_delivers_to_listed_nodes`
//! - 23 → `subscribe_emits_progress_then_terminal` (below — un-ignored 2026-05-16
//!   once `peat-protocol 0.9.0-rc.7` wired sender-side progress frames via
//!   the receiver-written `node_statuses` map, peat-node's
//!   `attachments::inbox` watcher began writing into that map, and
//!   `peat-mesh 0.9.0-rc.10` (via peat-mesh#118) fixed the sync_cooldown
//!   silent-drop that was preventing the receiver's `Completed` write
//!   from reaching the sender within the broadcast lifecycle).
//! - 26, 27, 30 → `attachments_acceptance_test`
//! - 28 → `attachments_subscribe_test`
//!
//! Tests deferred here: 24, 25, 29.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
use futures::StreamExt;
use peat_node::attachments::config::{AttachmentConfig, AttachmentPriorityCli};
use peat_node::attachments::handlers;
use peat_node::attachments::inbox::{
    clear_receive_test_directives, set_receive_test_directive, ReceiveTestDirective,
};
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb;
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;
use sha2::{Digest, Sha256};

struct BootedNode {
    base: String,
    node: Arc<SidecarNode>,
    _data_dir: tempfile::TempDir,
    _root_dir: tempfile::TempDir,
    _inbox_dir: tempfile::TempDir,
    root_path: PathBuf,
}

async fn boot(grpc_port: u16, iroh_port: u16, label: &str, enable_inbox: bool) -> BootedNode {
    let data_dir = tempfile::tempdir().unwrap();
    let root_dir = tempfile::tempdir().unwrap();
    let inbox_dir = tempfile::tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();
    let inbox_path = inbox_dir.path().to_path_buf();
    let attachment_config = AttachmentConfig::from_raw(
        &[format!("outbox={}", root_path.display())],
        if enable_inbox { Some(inbox_path) } else { None },
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
        1,
    )
    .unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("d23-{label}"),
            app_id: "d23".into(),
            shared_key: String::new(),
            data_dir: data_dir.path().to_path_buf(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: Some(iroh_port),
            iroh_secret_key: None,
            attachment_config,
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
        })
        .await
        .unwrap(),
    );
    let svc = Arc::new(PeatSidecarService::new(Arc::clone(&node)));
    let router = svc.register(connectrpc::Router::new());
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
    }
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

fn sha256_of(d: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(d);
    let o = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(&o);
    a
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn distribution_status(progress: &pb::AttachmentProgress) -> Option<pb::DistributionStatus> {
    progress.status.as_known()
}

fn is_terminal(progress: &pb::AttachmentProgress) -> bool {
    matches!(
        distribution_status(progress),
        Some(pb::DistributionStatus::DISTRIBUTION_STATUS_COMPLETED)
            | Some(pb::DistributionStatus::DISTRIBUTION_STATUS_PARTIAL)
            | Some(pb::DistributionStatus::DISTRIBUTION_STATUS_FAILED)
            | Some(pb::DistributionStatus::DISTRIBUTION_STATUS_CANCELLED)
    )
}

fn is_in_progress(progress: &pb::AttachmentProgress) -> bool {
    matches!(
        distribution_status(progress),
        Some(pb::DistributionStatus::DISTRIBUTION_STATUS_IN_PROGRESS)
    )
}

/// PRD test 23 — `subscribe_emits_progress_then_terminal`.
///
/// Send a 4 MiB file, subscribe, assert at least one IN_PROGRESS frame
/// and exactly one terminal frame.
///
/// This test became driveable end-to-end with `peat-protocol 0.9.0-rc.8`
/// (which floor-bumps `peat-mesh >= 0.9.0-rc.10`). The chain of fixes:
///
/// 1. **peat-protocol 0.9.0-rc.7** — `IrohFileDistribution::new` spawns
///    a watcher subscribed to `AutomergeStore::subscribe_to_observer_changes`
///    that re-reads the distribution document on every `node_statuses`
///    update and publishes a fresh `DistributionStatus` to the
///    broadcast channel ([defenseunicorns/peat#864](https://github.com/defenseunicorns/peat/issues/864)).
/// 2. **peat-node** (this branch's earlier commit `a0334ac`) —
///    `attachments::inbox` performs a read-modify-write into the same
///    distribution document around `fetch_blob`: `Transferring` before,
///    `Completed` after the inbox write atomic-renames into place.
/// 3. **peat-mesh 0.9.0-rc.10** ([peat-mesh#118](https://github.com/defenseunicorns/peat-mesh/pull/118))
///    — fixes the `AutomergeSyncCoordinator::initiate_sync` silent-drop
///    on sync cooldown. Without this, the receiver's `Completed` write
///    (fired ~60 ms after `Transferring` around a sub-second 4 MiB local
///    blob fetch) was dropped by the auto-sync push's 100 ms (peer, doc)
///    cooldown, and the sender's broadcast watcher stalled one frame
///    short of terminal.
/// 4. **peat-protocol 0.9.0-rc.8** (this PR's pin bump) — picks up the
///    rc.10 substrate fix.
///
/// The 4 MiB payload is the PRD-prescribed size: large enough that the
/// sender's watcher observes both the Transferring and Completed states
/// (giving the contract a real IN_PROGRESS frame to assert on), but
/// small enough that a successful run lands in well under 30 seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(iroh_two_node)]
async fn subscribe_emits_progress_then_terminal() {
    const A_GRPC: u16 = 50141;
    const A_IROH: u16 = 51241;
    const B_GRPC: u16 = 50142;
    const B_IROH: u16 = 51242;

    let a = boot(A_GRPC, A_IROH, "sender", false).await;
    let b = boot(B_GRPC, B_IROH, "receiver", true).await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

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

    // 4 MiB of deterministic-but-incompressible bytes via a simple LCG.
    let payload: Vec<u8> = {
        let mut v = Vec::with_capacity(4 * 1024 * 1024);
        let mut state: u32 = 0x1234_5678;
        for _ in 0..(4 * 1024 * 1024) {
            state = state.wrapping_mul(1_103_515_245).wrapping_add(12345);
            v.push((state >> 16) as u8);
        }
        v
    };
    let hash = sha256_of(&payload);
    let file_path = a.root_path.join("progress.bin");
    std::fs::write(&file_path, &payload).unwrap();

    let resp = call(
        &http,
        &a.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "progress.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&hash),
            }],
            "scope": { "allNodes": {} }
        }),
    )
    .await;
    let bundle_id = resp["bundleId"]
        .as_str()
        .expect("SendAttachments must return a bundle_id")
        .to_string();
    assert!(!bundle_id.is_empty());

    let mut stream = handlers::subscribe_attachment_bundle(
        &a.node,
        pb::SubscribeAttachmentBundleRequest {
            bundle_id: bundle_id.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("subscribe_attachment_bundle on the sender must succeed");

    let mut in_progress_count = 0usize;
    let mut terminal_count = 0usize;
    let deadline = Instant::now() + Duration::from_secs(60);

    while Instant::now() < deadline && terminal_count == 0 {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(frame))) => {
                if is_in_progress(&frame) {
                    in_progress_count += 1;
                } else if is_terminal(&frame) {
                    terminal_count += 1;
                }
            }
            Ok(Some(Err(e))) => panic!("subscribe stream yielded an error frame: {e:?}"),
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    assert!(
        in_progress_count >= 1,
        "PRD §test 23: expected ≥1 IN_PROGRESS frame, got 0 (terminal={terminal_count})"
    );
    assert_eq!(
        terminal_count, 1,
        "PRD §test 23: expected exactly 1 terminal frame, got {terminal_count} (in_progress={in_progress_count})"
    );
}

/// PRD test 24 — `cancel_in_flight_stops_transfer`.
///
/// Start a large transfer, cancel mid-flight, assert status flips to
/// CANCELLED within 1s.
///
/// **Remaining gap:** peat#864's sender-side progress frames landed in
/// peat-protocol 0.9.0-rc.7 (and peat-node's `attachments::inbox` now
/// writes Transferring/Completed into the distribution doc), so the
/// observability half of this contract works. The blocker is a
/// **bandwidth-controlled receiver fixture** — no in-tree way to throttle
/// a single peer's iroh-blob fetch to keep the transfer in-flight long
/// enough to issue Cancel and verify the flip within 1s. A real fixture
/// needs either a mock `NetworkedIrohBlobStore` implementing throttled
/// fetch, OS-level traffic shaping (`tc netem`, brittle in CI), or a
/// tokio sleep injection into the receiver's `BlobStore::fetch_blob` path.
/// All three are non-trivial design decisions deferred from this PR.
/// Un-ignored 2026-05-17 (#71). The receive-path test seam
/// (`inbox::ReceiveTestDirective::HoldInFlight`) holds the receiver
/// in the in-flight state after it writes `Transferring`, giving a
/// deterministic window to cancel into — no OS traffic shaping or mock
/// blob store. Cancel is sender-side: `CancelAttachmentDistribution`
/// flips the registry to `Cancelled` and `GetAttachmentDistribution`
/// returns CANCELLED via the terminal-precedence branch, so the
/// assertion is independent of the (paused) receiver; the pause only
/// guarantees the cancel is genuinely *mid-flight* (IN_PROGRESS, not
/// already terminal). On wake the paused receiver re-reads the doc,
/// sees status != "distributing", and skips delivery — so it can't
/// race a Completed over the CANCELLED.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(iroh_two_node)]
async fn cancel_in_flight_stops_transfer() {
    const A_GRPC: u16 = 50171;
    const A_IROH: u16 = 51271;
    const B_GRPC: u16 = 50172;
    const B_IROH: u16 = 51272;

    clear_receive_test_directives();
    let a = boot(A_GRPC, A_IROH, "sender", false).await;
    let b = boot(B_GRPC, B_IROH, "receiver", true).await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let sa = call(&http, &a.base, "GetStatus", serde_json::json!({})).await;
    let sb = call(&http, &b.base, "GetStatus", serde_json::json!({})).await;
    let ea = sa["endpointAddr"].as_str().unwrap().to_string();
    let eb = sb["endpointAddr"].as_str().unwrap().to_string();
    call(
        &http,
        &b.base,
        "ConnectPeer",
        serde_json::json!({ "endpointId": ea, "addresses": [format!("127.0.0.1:{A_IROH}")] }),
    )
    .await;
    call(
        &http,
        &a.base,
        "ConnectPeer",
        serde_json::json!({ "endpointId": eb, "addresses": [format!("127.0.0.1:{B_IROH}")] }),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    call(&http, &a.base, "StartSync", serde_json::json!({})).await;
    call(&http, &b.base, "StartSync", serde_json::json!({})).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let payload = b"cancel-in-flight-test payload".to_vec();
    let hash = sha256_of(&payload);
    std::fs::write(a.root_path.join("cancel.bin"), &payload).unwrap();
    let resp = call(
        &http,
        &a.base,
        "SendAttachments",
        serde_json::json!({
            "files": [{
                "rootName": "outbox",
                "relativePath": "cancel.bin",
                "sizeBytes": payload.len(),
                "sha256": b64(&hash),
            }],
            "scope": { "allNodes": {} }
        }),
    )
    .await;
    let distribution_id = resp["handles"][0]["distributionId"]
        .as_str()
        .unwrap()
        .to_string();
    let blob_token = resp["handles"][0]["blobToken"]
        .as_str()
        .unwrap()
        .to_string();

    // Arm the receiver to pause in-flight. Safe to arm now (post-send):
    // the distribution doc still has to CRDT-sync A→B and B's scan tick
    // (~1s) to fire before B consults the seam — well after this set.
    set_receive_test_directive(&blob_token, ReceiveTestDirective::HoldInFlight);

    // Wait until A observes IN_PROGRESS (B wrote Transferring, synced
    // back) — i.e. the transfer is genuinely mid-flight.
    // Generous: the deferred suite runs serially (`#[serial]`), so this
    // can execute on a loaded box after the other two-node tests; the
    // *assertion* (cancel → CANCELLED in 1s) is sender-local and fast,
    // this only waits for the receiver to reach IN_PROGRESS first.
    let inflight_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let body = call(
            &http,
            &a.base,
            "GetAttachmentDistribution",
            serde_json::json!({ "distributionId": distribution_id }),
        )
        .await;
        if body.get("status").and_then(|v| v.as_str()) == Some("DISTRIBUTION_STATUS_IN_PROGRESS") {
            break;
        }
        if Instant::now() >= inflight_deadline {
            panic!(
                "distribution never reached IN_PROGRESS (mid-flight) — \
                 cannot test mid-flight cancel. Last body: {body}"
            );
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // Cancel mid-flight, then assert CANCELLED within 1s.
    call(
        &http,
        &a.base,
        "CancelAttachmentDistribution",
        serde_json::json!({ "distributionId": distribution_id }),
    )
    .await;
    let cancel_deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let body = call(
            &http,
            &a.base,
            "GetAttachmentDistribution",
            serde_json::json!({ "distributionId": distribution_id }),
        )
        .await;
        if body.get("status").and_then(|v| v.as_str()) == Some("DISTRIBUTION_STATUS_CANCELLED") {
            break;
        }
        if Instant::now() >= cancel_deadline {
            panic!(
                "PRD §test 24: status did not flip to CANCELLED within 1s \
                 of CancelAttachmentDistribution. Last-seen: {body}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    clear_receive_test_directives();
    drop((a, b));
}

/// PRD test 25 — `unknown_node_id_marked_failed_after_grace`.
///
/// `NodeList{[nonexistent]}` with `--attachment-discovery-grace-secs=2`;
/// assert that after the grace window the per-node status for the
/// undiscovered ID is FAILED and the bundle finalizes FAILED.
///
/// Un-ignored 2026-05-17 (#70). The discovery-grace promoter
/// (`handlers::spawn_discovery_grace_promoter`) now spawns per
/// NodeList distribution: after `discovery_grace_secs`, any declared
/// ID with no `node_statuses` entry (never discovered — peat-protocol
/// `resolve_targets` filtered it out as an unknown peer) is written
/// `Failed` into the distribution doc via the rc.9 typed API. The
/// sender's own peat-protocol watcher folds that into the in-memory
/// `DistributionStatus`; the per-distribution watcher's zero-peer
/// COMPLETED short-circuit is suppressed for NodeList scopes with
/// declared IDs, so the FAILED — not a vacuous COMPLETED — is what
/// drives `maybe_finalize_bundle`.
///
/// Single-node, no real peer needed: the bogus ID never resolves, so
/// the grace promoter is the only thing that can produce its status.
#[tokio::test]
async fn unknown_node_id_marked_failed_after_grace() {
    let data_dir = tempfile::tempdir().unwrap();
    let root_dir = tempfile::tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();

    // grace = 2s so the test's poll budget stays small.
    let attachment_config = AttachmentConfig::from_raw(
        &[format!("outbox={}", root_path.display())],
        None,
        peat_node::attachments::config::DEFAULT_MAX_FILE_BYTES,
        peat_node::attachments::config::DEFAULT_MAX_BUNDLE_BYTES,
        peat_node::attachments::config::DEFAULT_MAX_FILES_PER_BUNDLE,
        peat_node::attachments::config::DEFAULT_MAX_NODE_LIST_LEN,
        peat_node::attachments::config::DEFAULT_MAX_CONCURRENT_DISTRIBUTIONS,
        peat_node::attachments::config::DEFAULT_QUEUE_WHEN_FULL,
        AttachmentPriorityCli::Routine,
        2, // discovery_grace_secs
        peat_node::attachments::config::DEFAULT_HANDLE_RETENTION_SECS,
        peat_node::attachments::config::DEFAULT_MAX_KNOWN_BUNDLES,
        peat_node::attachments::config::DEFAULT_INBOX_POLL_SECS,
    )
    .unwrap();

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: "grace-test".into(),
            app_id: "test".into(),
            shared_key: String::new(),
            data_dir: data_dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: None,
            iroh_secret_key: None,
            attachment_config,
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
        })
        .await
        .unwrap(),
    );

    // SendAttachments with NodeList{["bogus-never-connects"]}.
    let payload = b"grace-promoter-test";
    std::fs::write(root_path.join("g.bin"), payload).unwrap();
    let mut h = Sha256::new();
    h.update(payload);
    let send_req = pb::SendAttachmentsRequest {
        files: vec![pb::FileSpec {
            root_name: "outbox".into(),
            relative_path: "g.bin".into(),
            size_bytes: payload.len() as u64,
            sha256: h.finalize().to_vec(),
            ..Default::default()
        }],
        scope: buffa::MessageField::some(pb::DistributionScopeSpec {
            scope: Some(pb::distribution_scope_spec::Scope::NodeList(Box::new(
                pb::NodeListScope {
                    node_ids: vec!["bogus-never-connects".to_string()],
                    ..Default::default()
                },
            ))),
            ..Default::default()
        }),
        ..Default::default()
    };

    let resp = handlers::send_attachments(&node, send_req)
        .await
        .expect("SendAttachments with NodeList{[bogus]} must succeed (PRD Rule 10: unknown IDs tolerated at request time)");
    let distribution_id = resp.handles[0].distribution_id.clone();
    assert!(!distribution_id.is_empty());

    // Poll GetAttachmentDistribution until the bogus node shows FAILED.
    // grace=2s + a margin; if it never flips the panic surfaces the
    // last-seen status so a promoter regression is obvious.
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let got = handlers::get_attachment_distribution(
            &node,
            pb::GetAttachmentDistributionRequest {
                distribution_id: distribution_id.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("GetAttachmentDistribution must resolve the just-sent distribution");

        let bogus = got
            .per_node
            .iter()
            .find(|n| n.node_id == "bogus-never-connects");
        let bogus_failed = bogus.map(|n| n.status.as_known())
            == Some(Some(pb::DistributionStatus::DISTRIBUTION_STATUS_FAILED));
        let dist_failed =
            got.status.as_known() == Some(pb::DistributionStatus::DISTRIBUTION_STATUS_FAILED);

        if bogus_failed && dist_failed {
            // PRD Rule 10 acceptance: per-node FAILED for the
            // undiscovered ID + bundle-level FAILED finalization.
            // (The proto NodeTransferState carries no error string —
            // node_id/status/bytes_transferred only — so the
            // grace-expiry reason isn't wire-asserted here.)
            break;
        }

        if Instant::now() >= deadline {
            panic!(
                "discovery-grace promoter never marked `bogus-never-connects` \
                 FAILED within 8s (grace=2s). Last-seen: dist_status={:?}, \
                 per_node={:?}. A regression here means the grace promoter \
                 didn't spawn / didn't write, or the zero-peer COMPLETED \
                 short-circuit fired for a NodeList scope and vacuously \
                 finalized the bundle before grace.",
                got.status.as_known(),
                got.per_node
            );
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// PRD test 29 — `subscribe_mixed_state_emits_snapshot_for_terminal_then_live_for_inflight`.
///
/// Bundle with one distribution driven to FAILED via fault injection
/// while a second is still IN_PROGRESS; subscribe; assert snapshot
/// frame for the terminal one then live frames for the in-flight one.
///
/// **Remaining gap:** the IN_PROGRESS half is now reliably driveable
/// (peat-protocol 0.9.0-rc.8 + peat-mesh 0.9.0-rc.10). The FAILED half
/// still needs a deterministic way to drive a single distribution to
/// FAILED — peat-protocol's `IrohFileDistribution::distribute` does not
/// expose fault injection. A clean mechanism (test-only hook or a
/// deterministic timeout flag on the distribution document) is the
/// follow-up.
///
/// `attachments_subscribe_test::subscribe_after_terminal_emits_snapshot_then_eof`
/// covers the "all-terminal at subscribe time" half (with two Completed
/// distributions instead of Completed+Failed). The mixed-state ordering
/// (snapshot before live) is exercised in unit tests on the
/// `StreamCloser` adapter and `BundleRuntime::per_distribution_snapshot`.
/// Un-ignored 2026-05-17 (#71). The receive-path test seam drives the
/// mixed state: file 1's blob is armed `FailFetch` (receiver writes a
/// Failed node_status instead of fetching → that distribution goes
/// terminal FAILED) while file 2 is armed `HoldInFlight`
/// (receiver writes Transferring then holds → that distribution stays
/// IN_PROGRESS). A late subscriber must then see, per the
/// SubscribeAttachmentBundle late-subscribe contract: the *snapshot*
/// frame for the already-terminal distribution (file 1, FAILED) before
/// any *live* frame for the still-in-flight distribution (file 2,
/// IN_PROGRESS).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(iroh_two_node)]
async fn subscribe_mixed_state_emits_snapshot_for_terminal_then_live_for_inflight() {
    const A_GRPC: u16 = 50181;
    const A_IROH: u16 = 51281;
    const B_GRPC: u16 = 50182;
    const B_IROH: u16 = 51282;

    clear_receive_test_directives();
    let a = boot(A_GRPC, A_IROH, "sender", false).await;
    let b = boot(B_GRPC, B_IROH, "receiver", true).await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let sa = call(&http, &a.base, "GetStatus", serde_json::json!({})).await;
    let sb = call(&http, &b.base, "GetStatus", serde_json::json!({})).await;
    let ea = sa["endpointAddr"].as_str().unwrap().to_string();
    let eb = sb["endpointAddr"].as_str().unwrap().to_string();
    call(
        &http,
        &b.base,
        "ConnectPeer",
        serde_json::json!({ "endpointId": ea, "addresses": [format!("127.0.0.1:{A_IROH}")] }),
    )
    .await;
    call(
        &http,
        &a.base,
        "ConnectPeer",
        serde_json::json!({ "endpointId": eb, "addresses": [format!("127.0.0.1:{B_IROH}")] }),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    call(&http, &a.base, "StartSync", serde_json::json!({})).await;
    call(&http, &b.base, "StartSync", serde_json::json!({})).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Two distinct files in one bundle.
    let p1 = b"mixed-state file ONE (will be FAILED via fault seam)".to_vec();
    let p2 = b"mixed-state file TWO (held IN_PROGRESS via pause seam)".to_vec();
    let h1 = sha256_of(&p1);
    let h2 = sha256_of(&p2);
    std::fs::write(a.root_path.join("m1.bin"), &p1).unwrap();
    std::fs::write(a.root_path.join("m2.bin"), &p2).unwrap();
    let resp = call(
        &http,
        &a.base,
        "SendAttachments",
        serde_json::json!({
            "files": [
                { "rootName": "outbox", "relativePath": "m1.bin", "sizeBytes": p1.len(), "sha256": b64(&h1) },
                { "rootName": "outbox", "relativePath": "m2.bin", "sizeBytes": p2.len(), "sha256": b64(&h2) },
            ],
            "scope": { "allNodes": {} }
        }),
    )
    .await;
    let bundle_id = resp["bundleId"].as_str().unwrap().to_string();
    let handles = resp["handles"].as_array().unwrap();
    assert_eq!(
        handles.len(),
        2,
        "expected 2 handles, got {}",
        handles.len()
    );
    // Handles preserve the request's file order through ingest;
    // index positionally (proto3-JSON omits `fileIndex` when 0).
    let (dist1, blob1) = (
        handles[0]["distributionId"].as_str().unwrap().to_string(),
        handles[0]["blobToken"].as_str().unwrap().to_string(),
    );
    let (dist2, blob2) = (
        handles[1]["distributionId"].as_str().unwrap().to_string(),
        handles[1]["blobToken"].as_str().unwrap().to_string(),
    );

    set_receive_test_directive(
        &blob1,
        ReceiveTestDirective::FailFetch("fault-injected for PRD test 29".to_string()),
    );
    set_receive_test_directive(&blob2, ReceiveTestDirective::HoldInFlight);

    // Wait until A's view shows dist1 FAILED and dist2 IN_PROGRESS.
    let setup_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let b1 = call(
            &http,
            &a.base,
            "GetAttachmentDistribution",
            serde_json::json!({ "distributionId": dist1 }),
        )
        .await;
        let b2 = call(
            &http,
            &a.base,
            "GetAttachmentDistribution",
            serde_json::json!({ "distributionId": dist2 }),
        )
        .await;
        let s1 = b1.get("status").and_then(|v| v.as_str());
        let s2 = b2.get("status").and_then(|v| v.as_str());
        if s1 == Some("DISTRIBUTION_STATUS_FAILED") && s2 == Some("DISTRIBUTION_STATUS_IN_PROGRESS")
        {
            break;
        }
        if Instant::now() >= setup_deadline {
            panic!("mixed state never set up: dist1(FAILED?)={s1:?} dist2(IN_PROGRESS?)={s2:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Late-subscribe while the mixed state holds: dist1 already
    // terminal (FAILED), dist2 still in-flight (held IN_PROGRESS).
    // Contract: the subscribe handler subscribes to the live broadcast
    // FIRST, then emits a synthetic *snapshot* frame for each
    // already-terminal distribution (dist1 FAILED) — chained ahead of
    // any live frame. Non-terminal distributions are NOT snapshotted;
    // they surface via the live stream as they progress.
    let mut stream = handlers::subscribe_attachment_bundle(
        &a.node,
        pb::SubscribeAttachmentBundleRequest {
            bundle_id: bundle_id.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("subscribe_attachment_bundle must succeed");

    // Release dist2's hold AFTER subscribing. A stably-held IN_PROGRESS
    // distribution emits no further progress events, so a late
    // subscriber would legitimately never get a live frame for it —
    // the contract delivers in-flight distributions via the live
    // stream, which only fires on a state change. Releasing lets the
    // receiver fetch+complete dist2, producing the post-subscribe live
    // frame(s) the contract is about. dist1 is already FAILED + handled
    // by the receiver, so clearing directives doesn't disturb it.
    clear_receive_test_directives();

    let mut dist1_failed_idx: Option<usize> = None;
    let mut first_dist2_idx: Option<usize> = None;
    let mut idx = 0usize;
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline && (dist1_failed_idx.is_none() || first_dist2_idx.is_none()) {
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(frame))) => {
                if frame.distribution_id == dist1
                    && distribution_status(&frame)
                        == Some(pb::DistributionStatus::DISTRIBUTION_STATUS_FAILED)
                    && dist1_failed_idx.is_none()
                {
                    dist1_failed_idx = Some(idx);
                }
                // Any live frame for dist2 (it progresses
                // IN_PROGRESS→COMPLETED once the hold is released);
                // the contract point is that it arrives *after* the
                // terminal snapshot, not its specific status.
                if frame.distribution_id == dist2 && first_dist2_idx.is_none() {
                    first_dist2_idx = Some(idx);
                }
                idx += 1;
            }
            Ok(Some(Err(e))) => panic!("subscribe stream error frame: {e:?}"),
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    let f1 = dist1_failed_idx.expect(
        "PRD §test 29: expected a snapshot frame for the already-terminal \
         distribution (file 1, FAILED)",
    );
    let f2 = first_dist2_idx.expect(
        "PRD §test 29: expected a live frame for the in-flight \
         distribution (file 2) after releasing its hold",
    );
    assert!(
        f1 < f2,
        "PRD §test 29: terminal snapshot frame (file 1 FAILED, idx={f1}) \
         must precede the first live frame for the in-flight \
         distribution (file 2, idx={f2}) per the late-subscribe contract"
    );

    drop((a, b));
}
