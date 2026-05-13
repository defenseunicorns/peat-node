//! PRD-006 SubscribeAttachmentBundle coverage (Step 7b).
//!
//! Drives the subscribe handler in-process against a real `SidecarNode`,
//! same as `subscribe_test.rs` does for the document-change stream —
//! avoiding the need to build a Connect server-streaming wire client. The
//! HTTP-path coverage stays with the non-streaming RPCs in
//! `attachments_smoke_test.rs`; subscribe's contract is exercised here at
//! the Rust API boundary where the streaming semantics are easiest to
//! assert.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use peat_node::attachments::config::{AttachmentConfig, AttachmentPriorityCli};
use peat_node::attachments::handlers;
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb;
use sha2::{Digest, Sha256};

fn sha256_of(bytes: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().to_vec()
}

async fn boot_with_attachments() -> (Arc<SidecarNode>, std::path::PathBuf, tempfile::TempDir) {
    let data_dir = tempfile::tempdir().unwrap();
    let root_dir = tempfile::tempdir().unwrap();
    let root_path = root_dir.path().to_path_buf();

    let attachment_config = AttachmentConfig::from_raw(
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

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: "subscribe-test".into(),
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

    (node, root_path, root_dir)
}

fn build_send_request(
    root: &std::path::Path,
    name: &str,
    payload: &[u8],
) -> pb::SendAttachmentsRequest {
    std::fs::write(root.join(name), payload).unwrap();
    let hash = sha256_of(payload);
    pb::SendAttachmentsRequest {
        files: vec![pb::FileSpec {
            root_name: "outbox".into(),
            relative_path: name.into(),
            size_bytes: payload.len() as u64,
            sha256: hash,
            ..Default::default()
        }],
        scope: buffa::MessageField::some(pb::DistributionScopeSpec {
            scope: Some(pb::distribution_scope_spec::Scope::AllNodes(Box::default())),
            ..Default::default()
        }),
        ..Default::default()
    }
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

/// Zero-peer scope: the watcher's initial status check sees
/// `total_targets == 0` (peat-protocol returns is_complete=true
/// vacuously) and emits a Completed terminal frame. Subscribe right
/// after Send should observe that terminal frame either via snapshot
/// (if the watcher beat us) or via live stream (if we beat the watcher).
///
/// PRD §SubscribeAttachmentBundle late-subscribe contract: stream emits
/// at most one frame per distribution and closes cleanly.
#[tokio::test]
async fn subscribe_zero_peer_distribution_closes_after_terminal_frame() {
    let (node, root, _root_guard) = boot_with_attachments().await;
    let send_req = build_send_request(&root, "a.bin", b"hello");

    let resp = handlers::send_attachments(&node, send_req).await.unwrap();
    assert_eq!(resp.handles.len(), 1);
    let bundle_id = resp.bundle_id.clone();

    // Brief settle for the watcher's initial-status-check spawn. The
    // PRD contract still works without this sleep — late subscribers
    // get the snapshot frame — but a short pause exercises both paths
    // (snapshot if watcher beat us, live if we beat the watcher).
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut stream = handlers::subscribe_attachment_bundle(
        &node,
        pb::SubscribeAttachmentBundleRequest {
            bundle_id,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // The stream must close after one terminal frame.
    let frame = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("subscribe must yield a frame within 2s")
        .expect("stream must not end before the terminal frame")
        .expect("frame must not be an Err");
    assert!(
        is_terminal(&frame),
        "zero-peer distribution must emit a terminal frame, got status={:?}",
        distribution_status(&frame)
    );

    // After the single terminal frame, the stream closes.
    let next = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
    assert!(
        matches!(next, Ok(None)),
        "stream must close after the single terminal frame; got {next:?}"
    );
}

/// Subscribing to an unknown bundle_id returns NotFound, not an empty
/// stream that hangs.
#[tokio::test]
async fn subscribe_unknown_bundle_returns_not_found() {
    let (node, _root, _root_guard) = boot_with_attachments().await;
    // The Ok side is a boxed Stream that doesn't impl Debug, so
    // `unwrap_err` won't compile — match explicitly.
    let result = handlers::subscribe_attachment_bundle(
        &node,
        pb::SubscribeAttachmentBundleRequest {
            bundle_id: "does-not-exist".into(),
            ..Default::default()
        },
    )
    .await;
    match result {
        Ok(_) => panic!("unknown bundle_id must not return Ok"),
        Err(e) => assert_eq!(e.code, connectrpc::ErrorCode::NotFound),
    }
}

/// Two concurrent subscribers on the same bundle each receive the
/// terminal frame. The live broadcast fans out; the snapshot phase
/// catches whichever subscriber attached after the watcher fired.
#[tokio::test]
async fn two_subscribers_each_receive_terminal_frame() {
    let (node, root, _root_guard) = boot_with_attachments().await;
    let send_req = build_send_request(&root, "b.bin", b"world");
    let resp = handlers::send_attachments(&node, send_req).await.unwrap();
    let bundle_id = resp.bundle_id;

    // Drive both subscribers — one immediately (likely catches live
    // frame), one after a short pause (likely catches snapshot).
    let mut a = handlers::subscribe_attachment_bundle(
        &node,
        pb::SubscribeAttachmentBundleRequest {
            bundle_id: bundle_id.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut b = handlers::subscribe_attachment_bundle(
        &node,
        pb::SubscribeAttachmentBundleRequest {
            bundle_id,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let frame_a = tokio::time::timeout(Duration::from_secs(2), a.next())
        .await
        .expect("subscriber A must yield within 2s")
        .expect("subscriber A stream must not end early")
        .expect("subscriber A frame must not be Err");
    let frame_b = tokio::time::timeout(Duration::from_secs(2), b.next())
        .await
        .expect("subscriber B must yield within 2s")
        .expect("subscriber B stream must not end early")
        .expect("subscriber B frame must not be Err");

    assert!(is_terminal(&frame_a));
    assert!(is_terminal(&frame_b));
    assert_eq!(frame_a.distribution_id, frame_b.distribution_id);
}
