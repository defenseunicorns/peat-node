// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! Deployer — Phase 3 receiver-side loops.
//!
//! Two independent poll-driven cycles run on every tick of `run`:
//!
//! 1. **Deployment observer** (CRDT-02 + CRDT-03 + BLOB-03 + BLOB-04):
//!    Scan `deployment_requests`, filter to docs targeting this node with
//!    `receiver_status == "pending"`, wire the blob peer index, fetch the blob.
//!
//! 2. **Discovery loop** (SYNC-03):
//!    Scan `available_packages`, skip blobs already staged locally, otherwise
//!    fetch and copy to `{blob_work_dir}/catalog/{pkg_ref}/package.zarf.tar.zst`.
//!
//! Per RECV-01, this module is explicitly SEPARATE from watcher.rs so RA
//! health polling is not blocked by multi-minute blob downloads.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use peat_mesh::storage::{BlobHash, BlobMetadata, BlobProgress, BlobToken};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::node::SidecarNode;
use crate::types::{AvailablePackage, DeploymentRequest, DeploymentStatus};

/// Configuration for the deployer loop. Mirrors WatcherConfig shape.
#[derive(Debug, Clone)]
pub struct DeployerConfig {
    pub poll_interval: Duration,
    pub blob_work_dir: PathBuf,
}

/// Run the deployer loop. Ticks on `poll_interval` and drives both cycles.
///
/// Spawned in a separate tokio task from the watcher so that multi-minute
/// blob downloads do not block the agent health poll cycle (RECV-01).
pub async fn run(config: DeployerConfig, node: Arc<SidecarNode>) {
    let mut interval = tokio::time::interval(config.poll_interval);
    // Pitfall 4 (T-03-02-03): prevents double-fetch on overlapping poll cycles.
    let in_progress: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    info!(
        node_id = node.node_id(),
        poll_interval = ?config.poll_interval,
        blob_work_dir = %config.blob_work_dir.display(),
        "deployer started"
    );

    loop {
        interval.tick().await;
        if let Err(e) =
            poll_deployment_requests_with_guard(&node, &config, &in_progress).await
        {
            warn!("deployer poll (deployments) failed: {e}");
        }
        if let Err(e) = poll_available_packages(&node, &config).await {
            warn!("deployer poll (discovery) failed: {e}");
        }
    }
}

/// Public test-friendly wrapper — runs one deployment poll cycle with a fresh guard.
///
/// Calling this directly from tests avoids the need to drive the full `run` loop
/// and lets tests complete in milliseconds rather than waiting for poll ticks.
pub async fn poll_deployment_requests(
    node: &SidecarNode,
    config: &DeployerConfig,
) -> anyhow::Result<()> {
    let guard: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    poll_deployment_requests_with_guard(node, config, &guard).await
}

async fn poll_deployment_requests_with_guard(
    node: &SidecarNode,
    _config: &DeployerConfig,
    in_progress: &Arc<Mutex<HashSet<String>>>,
) -> anyhow::Result<()> {
    let doc_ids = node.list_documents("deployment_requests").await?;
    for id in doc_ids {
        let Some(json) = node.get_document("deployment_requests", &id).await? else {
            continue;
        };
        let mut req: DeploymentRequest = match serde_json::from_str(&json) {
            Ok(r) => r,
            Err(e) => {
                warn!(doc_id = %id, "deployer: malformed deployment_requests doc: {e}");
                continue;
            }
        };

        // CRDT-02: target filter — only process docs addressed to this node
        if req.target_agent_id != node.node_id() {
            continue;
        }
        // CRDT-03: idempotency guard — skip any doc whose receiver_status is not Pending
        if req.receiver_status != DeploymentStatus::Pending {
            debug!(
                doc_id = %id,
                status = ?req.receiver_status,
                "deployer: skipping non-pending doc"
            );
            continue;
        }

        // Pitfall 4 guard (T-03-02-03): prevent double-fetch across overlapping poll cycles
        {
            let mut g = in_progress.lock().await;
            if !g.insert(req.id.clone()) {
                debug!(doc_id = %req.id, "deployer: already in progress, skipping");
                continue;
            }
        }

        let result = fetch_for_deployment(node, &req).await;

        // Record status transition — write back to CRDT store regardless of success/failure
        match result {
            Ok(handle) => {
                req.receiver_status = DeploymentStatus::Fetching;
                info!(
                    doc_id = %req.id,
                    path = %handle.path.display(),
                    "deployer: blob fetched, receiver_status -> fetching (Phase 4 will deploy)"
                );
            }
            Err(e) => {
                req.receiver_status = DeploymentStatus::Failed;
                warn!(doc_id = %req.id, "deployer: fetch failed: {e}");
            }
        }
        let updated = serde_json::to_string(&req)?;
        if let Err(e) = node
            .put_document("deployment_requests", &req.id, &updated)
            .await
        {
            warn!(doc_id = %req.id, "deployer: failed to write status update: {e}");
        }

        // Release in-progress guard after status update is committed
        {
            let mut g = in_progress.lock().await;
            g.remove(&req.id);
        }
    }
    Ok(())
}

/// BLOB-03 + BLOB-04: three-step peer wiring followed by a progress-emitting fetch.
///
/// Step sequence (per must_haves key_links):
///   1. add_blob_peer    — register the sender's blob endpoint in the local index
///   2. advertise_blob   — record that the sender holds this specific blob hash
///   3. fetch_blob       — QUIC download with BlobProgress callback (BLOB-04)
///
/// Returns the BlobHandle on success. On any step failure, the error propagates
/// to poll_deployment_requests_with_guard which writes receiver_status = Failed.
async fn fetch_for_deployment(
    node: &SidecarNode,
    req: &DeploymentRequest,
) -> anyhow::Result<peat_mesh::storage::BlobHandle> {
    // T-03-02-01: parse blob_ticket JSON with fallback to typed fields
    let ticket: serde_json::Value = serde_json::from_str(&req.blob_ticket)
        .unwrap_or_else(|_| serde_json::json!({}));
    let hash_hex = ticket
        .get("hash")
        .and_then(|v| v.as_str())
        .unwrap_or(req.iroh_blob_hash.as_str())
        .to_string();
    let size_bytes = ticket
        .get("size_bytes")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let sender_endpoint_id = ticket
        .get("sender_endpoint_id")
        .and_then(|v| v.as_str())
        .unwrap_or(req.sender_endpoint_id.as_str())
        .to_string();

    // Step 1: BLOB-03 — register the sender's blob endpoint
    info!(
        endpoint_id_hex = %sender_endpoint_id,
        doc_id = %req.id,
        "deployer: add_blob_peer"
    );
    node.add_blob_peer(&sender_endpoint_id).await?;

    // Step 2: BLOB-03 — record that the sender holds this specific blob
    info!(
        endpoint_id_hex = %sender_endpoint_id,
        hash_hex = %hash_hex,
        doc_id = %req.id,
        "deployer: advertise_blob"
    );
    node.advertise_blob_for_hash(&sender_endpoint_id, &hash_hex)
        .await?;

    // Step 3: BLOB-04 — fetch with structured progress logging
    info!(hash_hex = %hash_hex, doc_id = %req.id, "deployer: fetch_blob start");
    let token = BlobToken {
        hash: BlobHash::from_hex(&hash_hex),
        size_bytes,
        metadata: BlobMetadata::with_name(&req.package_name),
    };
    let doc_id_for_log = req.id.clone();
    let handle = node
        .fetch_blob_from_peer(&token, move |p| {
            match &p {
                BlobProgress::Started { total_bytes } => {
                    info!(
                        total_bytes = *total_bytes,
                        doc_id = %doc_id_for_log,
                        "deployer: fetch progress: started"
                    );
                }
                BlobProgress::Downloading {
                    downloaded_bytes,
                    total_bytes,
                } => {
                    let pct = if *total_bytes > 0 {
                        (*downloaded_bytes * 100) / *total_bytes
                    } else {
                        0
                    };
                    info!(
                        pct,
                        downloaded_bytes = *downloaded_bytes,
                        total_bytes = *total_bytes,
                        doc_id = %doc_id_for_log,
                        "deployer: fetch progress: downloading"
                    );
                }
                BlobProgress::Completed { local_path } => {
                    info!(
                        path = %local_path.display(),
                        doc_id = %doc_id_for_log,
                        "deployer: fetch progress: completed"
                    );
                }
                BlobProgress::Failed { error } => {
                    warn!(%error, doc_id = %doc_id_for_log, "deployer: fetch progress: failed");
                }
            }
        })
        .await?;
    Ok(handle)
}

/// SYNC-03: Discovery loop — scan available_packages and pull unknown blobs
/// to the local catalog directory.
///
/// For each available_packages doc:
///   1. Skip if blob already staged locally (idempotency)
///   2. Sanitize pkg_ref against path traversal (T-03-02-02)
///   3. Three-step wiring: add_peer → advertise_blob → fetch_blob
///   4. Copy (NOT rename) the fetched blob to {blob_work_dir}/catalog/{pkg_ref}/package.zarf.tar.zst
///      The canonical {blob_work_dir}/{hash_hex} must stay in place for Phase 4.
pub async fn poll_available_packages(
    node: &SidecarNode,
    config: &DeployerConfig,
) -> anyhow::Result<()> {
    let doc_ids = node.list_documents("available_packages").await?;
    for pkg_ref in doc_ids {
        let Some(json) = node.get_document("available_packages", &pkg_ref).await? else {
            continue;
        };
        let pkg: AvailablePackage = match serde_json::from_str(&json) {
            Ok(p) => p,
            Err(e) => {
                warn!(pkg_ref = %pkg_ref, "discovery: malformed available_packages doc: {e}");
                continue;
            }
        };

        let hash = BlobHash::from_hex(&pkg.iroh_blob_hash);
        if node.blob_exists_locally(&hash) {
            debug!(pkg_ref = %pkg_ref, "discovery: blob already local, skipping");
            continue;
        }

        // T-03-02-02: sanitize pkg_ref before using as a filesystem path segment
        let safe_ref = sanitize_pkg_ref(&pkg_ref);
        if safe_ref != pkg_ref {
            warn!(
                original = %pkg_ref,
                sanitized = %safe_ref,
                "discovery: pkg_ref contained unsafe chars; using sanitized form"
            );
        }

        // Step 1: register sender's blob endpoint
        if let Err(e) = node.add_blob_peer(&pkg.sender_endpoint_id).await {
            warn!(pkg_ref = %pkg_ref, "discovery: add_blob_peer failed: {e}");
            continue;
        }
        // Step 2: record that sender has this blob
        if let Err(e) = node
            .advertise_blob_for_hash(&pkg.sender_endpoint_id, &pkg.iroh_blob_hash)
            .await
        {
            warn!(pkg_ref = %pkg_ref, "discovery: advertise_blob failed: {e}");
            continue;
        }

        let token = BlobToken {
            hash: hash.clone(),
            // size unknown at discovery time; the downloader uses the hash to verify
            size_bytes: 0,
            metadata: BlobMetadata::with_name(&pkg.name),
        };

        // No-op progress closure for discovery cycle (no specific doc_id to annotate)
        let fetch_result = node.fetch_blob_from_peer(&token, |_| {}).await;

        match fetch_result {
            Ok(handle) => {
                let catalog_dir = config.blob_work_dir.join("catalog").join(&safe_ref);
                if let Err(e) = tokio::fs::create_dir_all(&catalog_dir).await {
                    warn!(pkg_ref = %pkg_ref, "discovery: failed to create catalog dir: {e}");
                    continue;
                }
                let dest = catalog_dir.join("package.zarf.tar.zst");
                // Pitfall 6: COPY not RENAME — canonical blob stays at blob_work_dir/{hash_hex}
                // so Phase 4 can locate it for `uds zarf package deploy`.
                if let Err(e) = tokio::fs::copy(&handle.path, &dest).await {
                    warn!(pkg_ref = %pkg_ref, "discovery: failed to copy to catalog: {e}");
                    continue;
                }
                info!(
                    pkg_ref = %pkg_ref,
                    dest = %dest.display(),
                    "discovery: staged to catalog"
                );
            }
            Err(e) => {
                warn!(pkg_ref = %pkg_ref, "discovery: fetch failed: {e}");
            }
        }
    }
    Ok(())
}

/// Sanitize a pkg_ref for use as a single filesystem path segment.
///
/// Replaces path separators, null bytes, control characters, and `..`
/// sequences with underscores. This prevents path traversal attacks
/// when the pkg_ref is used to construct catalog directory paths (T-03-02-02).
fn sanitize_pkg_ref(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            '/' | '\\' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect::<String>()
        .replace("..", "__")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_pkg_ref_blocks_path_traversal() {
        // Char map fires first: '/' → '_', then ".." → "__"
        // "../etc/passwd": '.'→'.', '.'→'.', '/'→'_', … = ".._etc_passwd"
        //   → replace ".." with "__": "___etc_passwd" (3 underscores: __ + _etc_passwd)
        assert_eq!(sanitize_pkg_ref("../etc/passwd"), "___etc_passwd");
        // "foo/../bar": 'f','o','o','/'→'_','.'→'.','.'→'.','/'→'_','b','a','r' = "foo_.._bar"
        //   → replace ".." with "__": "foo____bar" (4 underscores: foo_ + __ + _bar)
        assert_eq!(sanitize_pkg_ref("foo/../bar"), "foo____bar");
        // '/' → '_' only (no ".." to replace)
        assert_eq!(sanitize_pkg_ref("foo/bar"), "foo_bar");
        // '\\' → '_'
        assert_eq!(sanitize_pkg_ref("foo\\bar"), "foo_bar");
        // Normal pkg_ref unchanged
        assert_eq!(sanitize_pkg_ref("normal-pkg-0.1.0-arm64"), "normal-pkg-0.1.0-arm64");
    }

    #[test]
    fn sanitize_pkg_ref_replaces_null_bytes() {
        let input = "foo\0bar";
        assert_eq!(sanitize_pkg_ref(input), "foo_bar");
    }

    #[test]
    fn sanitize_pkg_ref_replaces_control_chars() {
        let input = "foo\x01bar\x1fend";
        assert_eq!(sanitize_pkg_ref(input), "foo_bar_end");
    }
}
