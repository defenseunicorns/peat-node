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
            node_id: format!("d23-{label}"),
            app_id: "d23".into(),
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
#[tokio::test]
#[ignore = "needs a bandwidth-controlled receiver fixture (peat-node-only design decision)"]
async fn cancel_in_flight_stops_transfer() {}

/// PRD test 25 — `unknown_node_id_marked_failed_after_grace`.
///
/// `NodeList{[nonexistent]}`; assert that after `discovery_grace_secs`,
/// per-node status is FAILED.
///
/// **Gap:** the grace-period mechanism is not yet implemented. v1
/// records `--attachment-discovery-grace-secs` as a config knob but
/// there is no background task that scans pending NodeList targets for
/// unresolved IDs and promotes them to FAILED. Currently a
/// `NodeList{[nonexistent]}` ingest succeeds and the resulting
/// distribution sits idle with empty node_statuses indefinitely.
///
/// Implementation outline for the follow-up: spawn a per-bundle grace
/// timer on send; when it fires, walk `IrohFileDistribution::status`,
/// compute the set of declared-but-unconnected targets, and synthesise
/// FAILED entries into the runtime via `apply_progress`. The watcher's
/// terminal counter then drives `maybe_finalize_bundle` as today.
#[tokio::test]
#[ignore = "needs the --attachment-discovery-grace-secs background task (not yet implemented in v1)"]
async fn unknown_node_id_marked_failed_after_grace() {}

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
#[tokio::test]
#[ignore = "needs a fault-injection hook to drive a single distribution to FAILED"]
async fn subscribe_mixed_state_emits_snapshot_for_terminal_then_live_for_inflight() {}
