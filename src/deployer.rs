// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! Deployer — Phase 3 + Phase 4 receiver-side loops.
//!
//! Three independent poll-driven cycles run on every tick of `run`:
//!
//! 1. **Deployment observer** (CRDT-02 + CRDT-03 + BLOB-03 + BLOB-04 + RECV-03):
//!    Scan `deployment_requests`, filter to docs targeting this node with
//!    `receiver_status == "pending"`, validate architecture, wire the blob peer
//!    index, fetch the blob.
//!
//! 2. **Deploy loop** (RECV-02 + RECV-04):
//!    Scan `deployment_requests` for docs with `receiver_status == "fetching"`,
//!    shell out to `uds zarf package deploy`, transition through
//!    Deploying → Deployed | Failed with exponential backoff.
//!
//! 3. **Discovery loop** (SYNC-03):
//!    Scan `available_packages`, skip blobs already staged locally, otherwise
//!    fetch and copy to `{blob_work_dir}/catalog/{pkg_ref}/package.zarf.tar.zst`.
//!
//! Per RECV-01, this module is explicitly SEPARATE from watcher.rs so RA
//! health polling is not blocked by multi-minute blob downloads.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use peat_mesh::storage::{BlobHash, BlobMetadata, BlobProgress, BlobToken};
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::node::SidecarNode;
use crate::types::{AvailablePackage, DeploymentRequest, DeploymentStatus};

/// Configuration for the deployer loop. Mirrors WatcherConfig shape.
#[derive(Debug, Clone)]
pub struct DeployerConfig {
    pub poll_interval: Duration,
    pub blob_work_dir: PathBuf,
    /// Path to kubeconfig file. When Some(path), exported as KUBECONFIG env
    /// to the `uds zarf` subprocess. When None, the subprocess inherits the
    /// parent process's KUBECONFIG (production K8s pods always have one;
    /// dev machines fall back to ~/.kube/config automatically).
    pub kubeconfig: Option<PathBuf>,
    /// Maximum deploy retries before writing receiver_status = Failed (RECV-04).
    pub max_deploy_retries: u32,
    /// Initial backoff seconds; each retry doubles up to a 300-second cap.
    /// Production default 2; tests set to 0 for in-millisecond test runs.
    pub initial_backoff_secs: u64,
    /// Command to invoke for deployment. Production = "uds"; tests inject a
    /// mock script path so the test harness can exercise shell-out behavior
    /// without requiring the real UDS CLI or a k3s cluster.
    pub deploy_command: String,
}

/// Run the deployer loop. Ticks on `poll_interval` and drives all three cycles.
///
/// Spawned in a separate tokio task from the watcher so that multi-minute
/// blob downloads do not block the agent health poll cycle (RECV-01).
pub async fn run(config: DeployerConfig, node: Arc<SidecarNode>) {
    let mut interval = tokio::time::interval(config.poll_interval);
    // Pitfall 4 (T-03-02-03): prevents double-fetch on overlapping poll cycles.
    let in_progress: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    // RECV-04: in-memory retry counter keyed by request_id. Lost on process
    // restart (intentional — restart is a natural recovery event).
    let retry_counts: Arc<Mutex<HashMap<String, u32>>> =
        Arc::new(Mutex::new(HashMap::new()));

    info!(
        node_id = node.node_id(),
        poll_interval = ?config.poll_interval,
        blob_work_dir = %config.blob_work_dir.display(),
        kubeconfig = ?config.kubeconfig,
        max_deploy_retries = config.max_deploy_retries,
        deploy_command = %config.deploy_command,
        "deployer started"
    );

    loop {
        interval.tick().await;
        if let Err(e) =
            poll_deployment_requests_with_guard(&node, &config, &in_progress, &retry_counts).await
        {
            warn!("deployer poll (deployments) failed: {e}");
        }
        if let Err(e) = poll_deploying_requests(&node, &config, &retry_counts).await {
            warn!("deployer poll (deploying) failed: {e}");
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
    let counts: Arc<Mutex<HashMap<String, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    poll_deployment_requests_with_guard(node, config, &guard, &counts).await
}

/// Test helper — expose the retry_counts map so tests can inspect counter state
/// after poll_deploying_requests completes.
pub async fn poll_deploying_requests_with_counts(
    node: &SidecarNode,
    config: &DeployerConfig,
    retry_counts: &Arc<Mutex<HashMap<String, u32>>>,
) -> anyhow::Result<()> {
    poll_deploying_requests(node, config, retry_counts).await
}

/// Test helper — run one Pending-handler poll cycle with a caller-supplied
/// retry_counts map so tests can verify the counter is cleared on Pending → Fetching.
pub async fn poll_deployment_requests_with_counts(
    node: &SidecarNode,
    config: &DeployerConfig,
    retry_counts: &Arc<Mutex<HashMap<String, u32>>>,
) -> anyhow::Result<()> {
    let guard: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    poll_deployment_requests_with_guard(node, config, &guard, retry_counts).await
}

async fn poll_deployment_requests_with_guard(
    node: &SidecarNode,
    _config: &DeployerConfig,
    in_progress: &Arc<Mutex<HashSet<String>>>,
    retry_counts: &Arc<Mutex<HashMap<String, u32>>>,
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

        // RECV-03: arch validation BEFORE blob fetch.
        // Must release the in-progress guard on every code path that `continue`s.
        let arch_check = match validate_architecture(node, &req).await {
            Ok(c) => c,
            Err(e) => {
                warn!(doc_id = %req.id, "deployer: arch validation read failed: {e}");
                // Release guard, skip this doc this cycle — next tick will retry
                let mut g = in_progress.lock().await;
                g.remove(&req.id);
                continue;
            }
        };

        if let ArchCheck::Mismatch { local, requested } = arch_check {
            // Pitfall 7: release the in-progress guard on arch-failure path
            req.receiver_status = DeploymentStatus::Failed;
            warn!(
                doc_id = %req.id,
                local_arch = %local,
                requested_arch = %requested,
                "deployer: RECV-03 arch mismatch, writing receiver_status = Failed"
            );
            let updated = serde_json::to_string(&req)?;
            if let Err(e) = node
                .put_document("deployment_requests", &req.id, &updated)
                .await
            {
                warn!(doc_id = %req.id, "deployer: failed to write arch-mismatch Failed: {e}");
            }
            let mut g = in_progress.lock().await;
            g.remove(&req.id);
            continue;
        }

        let result = fetch_for_deployment(node, &req).await;

        // Record status transition — write back to CRDT store regardless of success/failure
        match result {
            Ok(handle) => {
                req.receiver_status = DeploymentStatus::Fetching;
                // RECV-04: fresh Pending → Fetching transition clears any prior retry counter
                // so ResetDeployment gives the operator a full retry budget (not the old count).
                retry_counts.lock().await.remove(&req.id);
                info!(
                    doc_id = %req.id,
                    path = %handle.path.display(),
                    "deployer: blob fetched, receiver_status -> fetching (Phase 4 will deploy)"
                );
            }
            Err(e) => {
                req.receiver_status = DeploymentStatus::Failed;
                // Also clear the deploy-stage retry counter on fetch failure: the doc goes
                // to Failed and will not re-enter the deploying stage without a ResetDeployment,
                // which re-promotes it to Pending → Fetching and gets a fresh budget anyway.
                retry_counts.lock().await.remove(&req.id);
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

/// Result of the RECV-03 architecture compatibility check.
#[derive(Debug)]
enum ArchCheck {
    /// Arches agree, or req.architecture is empty, or platforms doc is missing/has no arch.
    Match,
    /// Both arches are non-empty strings AND they differ.
    Mismatch { local: String, requested: String },
}

/// RECV-03: Read the receiver's own `platforms/{node_id}` doc and compare the
/// `architecture` field against `req.architecture`. Returns:
///
/// - `Ok(Match)` — arches agree OR req.architecture is empty (Pitfall 5)
///   OR platforms doc is missing / has no arch (Pitfall 1)
/// - `Ok(Mismatch)` — both arches are non-empty strings AND they differ
/// - `Err(e)` — CRDT read failure (bubble up; caller writes Failed)
///
/// The deployer treats Mismatch as a terminal Failed state and does NOT fetch.
async fn validate_architecture(
    node: &SidecarNode,
    req: &DeploymentRequest,
) -> anyhow::Result<ArchCheck> {
    // Pitfall 5: empty arch on the request means the sender did not claim an arch.
    // Proceed to fetch — the package file itself carries arch info.
    if req.architecture.is_empty() {
        return Ok(ArchCheck::Match);
    }

    // Pitfall 1: platforms/{node_id} may not yet be written on the first poll
    // after process start. Treat missing as "arch unknown" and proceed.
    let Some(json) = node.get_document("platforms", node.node_id()).await? else {
        debug!(
            doc_id = %req.id,
            "deployer: platforms doc missing, skipping arch validation"
        );
        return Ok(ArchCheck::Match);
    };

    let platform: serde_json::Value =
        serde_json::from_str(&json).unwrap_or(serde_json::Value::Null);
    let local_arch = platform.get("architecture").and_then(|v| v.as_str());

    match local_arch {
        None => {
            debug!(
                doc_id = %req.id,
                "deployer: platforms doc has no architecture field, skipping validation"
            );
            Ok(ArchCheck::Match)
        }
        Some(arch) if arch == req.architecture => Ok(ArchCheck::Match),
        Some(arch) => {
            warn!(
                doc_id = %req.id,
                local_arch = %arch,
                request_arch = %req.architecture,
                "deployer: RECV-03 arch mismatch — will mark Failed without fetch"
            );
            Ok(ArchCheck::Mismatch {
                local: arch.to_string(),
                requested: req.architecture.clone(),
            })
        }
    }
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

/// RECV-02 + RECV-04: scan deployment_requests for docs in Fetching state that
/// target this node, shell out to `uds zarf package deploy`, transition to
/// Deployed or Failed. Applies exponential backoff around transient failures.
///
/// Design note: retries loop INSIDE a single poll tick (not one attempt per tick)
/// so the doc stays in Deploying state and does not re-enter the Fetching filter
/// on the next tick (which would re-fetch the blob unnecessarily).
async fn poll_deploying_requests(
    node: &SidecarNode,
    config: &DeployerConfig,
    retry_counts: &Arc<Mutex<HashMap<String, u32>>>,
) -> anyhow::Result<()> {
    let doc_ids = node.list_documents("deployment_requests").await?;
    for id in doc_ids {
        let Some(json) = node.get_document("deployment_requests", &id).await? else {
            continue;
        };
        let mut req: DeploymentRequest = match serde_json::from_str(&json) {
            Ok(r) => r,
            Err(e) => {
                warn!(doc_id = %id, "deployer: malformed deployment_requests doc in deploying scan: {e}");
                continue;
            }
        };
        if req.target_agent_id != node.node_id() {
            continue;
        }
        if req.receiver_status != DeploymentStatus::Fetching {
            continue;
        }

        // Pitfall 2: the canonical blob file must exist before shelling out.
        // T-04-03-02: validate hash is ^[0-9a-fA-F]{64}$ BEFORE path join to
        // prevent directory traversal from a malicious iroh_blob_hash value.
        let hash = &req.iroh_blob_hash;
        if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            warn!(
                doc_id = %req.id,
                hash = %hash,
                "deployer: iroh_blob_hash is not a valid 64-char hex string — marking Failed"
            );
            req.receiver_status = DeploymentStatus::Failed;
            let updated = serde_json::to_string(&req)?;
            let _ = node.put_document("deployment_requests", &req.id, &updated).await;
            retry_counts.lock().await.remove(&req.id);
            continue;
        }
        let blob_path = config.blob_work_dir.join(hash.as_str());
        if !blob_path.exists() {
            warn!(
                doc_id = %req.id,
                path = %blob_path.display(),
                "deployer: canonical blob missing — marking Failed"
            );
            req.receiver_status = DeploymentStatus::Failed;
            let updated = serde_json::to_string(&req)?;
            let _ = node.put_document("deployment_requests", &req.id, &updated).await;
            retry_counts.lock().await.remove(&req.id);
            continue;
        }

        // Pitfall 3: write Deploying to CRDT BEFORE invoking subprocess so that
        // a crashed peat-node leaves a recoverable Deploying doc rather than a
        // re-deployable Fetching doc (which would cause a duplicate deploy).
        req.receiver_status = DeploymentStatus::Deploying;
        let deploying_json = serde_json::to_string(&req)?;
        if let Err(e) = node
            .put_document("deployment_requests", &req.id, &deploying_json)
            .await
        {
            warn!(doc_id = %req.id, "deployer: failed to write Deploying status: {e}");
            continue;
        }

        // RECV-04: in-poll-tick retry loop with exponential backoff.
        let outcome = try_deploy_with_backoff(&blob_path, &req, config, retry_counts).await;

        match outcome {
            Ok(()) => {
                req.receiver_status = DeploymentStatus::Deployed;
                retry_counts.lock().await.remove(&req.id);
                info!(doc_id = %req.id, "deployer: uds zarf deploy succeeded");
            }
            Err(e) => {
                req.receiver_status = DeploymentStatus::Failed;
                retry_counts.lock().await.remove(&req.id);
                warn!(doc_id = %req.id, "deployer: uds zarf deploy exhausted retries: {e}");
            }
        }
        let final_json = serde_json::to_string(&req)?;
        if let Err(e) = node
            .put_document("deployment_requests", &req.id, &final_json)
            .await
        {
            warn!(doc_id = %req.id, "deployer: failed to write final deploy status: {e}");
        }
    }
    Ok(())
}

/// Run `<deploy_command> zarf package deploy <blob_path> --confirm [--set-variables=K=V…]`
/// with retries and exponential backoff. Returns Ok(()) on a zero-exit run,
/// Err on exhausted retries.
///
/// T-04-03-01: tokio::process::Command::arg does NOT invoke a shell — each arg is
/// a separate argv[n] entry with no field-splitting or glob expansion.
/// T-04-03-05: deploy_command defaults to "uds"; only tests set a mock path
/// via direct struct construction (no CLI flag exposes this field).
/// T-04-03-07: worst-case hold time = max_retries × initial_backoff × 2^n, capped
/// at 300s per sleep. With defaults (5 retries, 2s initial): (2+4+8+16+32) = 62s.
async fn try_deploy_with_backoff(
    blob_path: &std::path::Path,
    req: &DeploymentRequest,
    config: &DeployerConfig,
    retry_counts: &Arc<Mutex<HashMap<String, u32>>>,
) -> anyhow::Result<()> {
    loop {
        // Build the command fresh each attempt (Command is not Clone/reusable).
        let mut cmd = Command::new(&config.deploy_command);
        cmd.arg("zarf")
            .arg("package")
            .arg("deploy")
            .arg(blob_path)
            .arg("--confirm");
        // Pitfall 4: emit one --set-variables per entry (stringToString format).
        for (k, v) in &req.zarf_vars {
            cmd.arg(format!("--set-variables={}={}", k, v));
        }
        // Pitfall 6: only set KUBECONFIG when configured; inherit otherwise so
        // dev machines with ~/.kube/config work without explicit configuration.
        if let Some(kubeconfig) = &config.kubeconfig {
            cmd.env("KUBECONFIG", kubeconfig);
        }

        let result = cmd.output().await;
        let exit_ok = match &result {
            Ok(out) => out.status.success(),
            Err(_) => false, // ErrorKind::NotFound (uds missing in PATH) etc.
        };

        if exit_ok {
            if let Ok(out) = &result {
                let stdout = String::from_utf8_lossy(&out.stdout);
                if !stdout.is_empty() {
                    debug!(doc_id = %req.id, stdout = %stdout, "deployer: uds zarf stdout");
                }
            }
            return Ok(());
        }

        // Log the failure details for operator visibility.
        match &result {
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                warn!(
                    doc_id = %req.id,
                    status = ?out.status,
                    stderr = %stderr,
                    "deployer: uds zarf exited non-zero"
                );
            }
            Err(e) => {
                warn!(
                    doc_id = %req.id,
                    error = %e,
                    "deployer: uds zarf spawn failed (binary missing in PATH?)"
                );
            }
        }

        // Increment the per-request retry counter.
        let attempt = {
            let mut g = retry_counts.lock().await;
            let counter = g.entry(req.id.clone()).or_insert(0);
            *counter += 1;
            *counter
        };

        if attempt > config.max_deploy_retries {
            anyhow::bail!(
                "uds zarf deploy failed {} times (max {})",
                attempt,
                config.max_deploy_retries
            );
        }

        // Exponential backoff: initial_backoff_secs * 2^attempt, capped at 300s.
        // Use attempt.min(8) in the exponent so the shift never overflows u64.
        let backoff = std::cmp::min(
            config
                .initial_backoff_secs
                .saturating_mul(1u64 << attempt.min(8) as u64),
            300,
        );
        info!(
            doc_id = %req.id,
            attempt,
            max = config.max_deploy_retries,
            backoff_secs = backoff,
            "deployer: retrying deploy after backoff"
        );
        if backoff > 0 {
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
    }
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
