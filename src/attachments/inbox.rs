//! Receive-side attachment watcher (PRD-006 v1.1).
//!
//! The v1 PRD-006 surface as it shipped in #64 only proved sender-side
//! correctness. Distribution documents synced to peers via Automerge but
//! nothing on the receive side observed them or pulled the referenced
//! blob — peat-protocol's `file_distribution.rs:617-621` flags that
//! receive-side observer pattern as v2 work. Result: a "successful"
//! `SendAttachments → COMPLETED` round-trip delivered nothing to anyone
//! outside the sender.
//!
//! This module closes that gap *in peat-node* without touching
//! peat-protocol. A background task polls the synced
//! `file_distributions` collection, identifies documents that target
//! this node (by short-form iroh endpoint id in `target_nodes`), pulls
//! the referenced blob via `NetworkedIrohBlobStore::fetch_blob` (which
//! iterates known iroh peers internally), and writes the bytes to an
//! operator-configured inbox directory.
//!
//! # Self-skip
//!
//! Distribution documents the local node *sent* (originated through
//! `handlers::send_attachments`) are skipped via a registry lookup:
//! `bundle_registry.lookup_distribution(distribution_id).is_some()`
//! returns true only for distributions this node originated. Receivers
//! never have an entry there because the registry is populated
//! exclusively by `SendAttachments`.
//!
//! # Targeting
//!
//! peat-protocol's `IrohFileDistribution::resolve_targets` produces
//! `target_nodes` from the sender's `known_peers` at distribute time
//! (formatted as `endpoint_id.fmt_short()`). The watcher matches this
//! node's own short-form endpoint id against that list. Edge case: a
//! peer that joined the sender's mesh *after* `distribute()` is not in
//! `target_nodes` and will not auto-receive. The v1 sender-side
//! targeting model. Acceptable for the immediate use case; a v2
//! "open subscription" mode (receive any distribution matching a
//! local filter) is the natural follow-up.
//!
//! # Idempotency + retry
//!
//! Two layers of "already handled" tracking:
//!
//! 1. An in-memory `HashSet<String>` per process scoped to one watcher
//!    instance. Records distribution_ids that succeeded, were malformed,
//!    or didn't target this node, so subsequent sweeps don't re-parse
//!    them. Fetch / write failures are NOT recorded — they retry on the
//!    next tick.
//! 2. A filesystem check before every fetch: if
//!    `{inbox}/{distribution_id}/` already contains a file matching the
//!    declared blob_size, treat the distribution as already-delivered
//!    and short-circuit. This is the durable source of truth — the
//!    in-memory set gets cleared on restart, the filesystem doesn't.
//!    Without this, a peat-node restart would re-fetch and re-write
//!    every historical delivery (idempotent under atomic rename, but
//!    wasteful disk I/O linear in lifetime delivery count).
//!
//! The in-memory set grows with the number of unique distribution_ids
//! observed in `file_distributions` over a single process's lifetime
//! — bounded practically by the formation's distribution count. For
//! very long-running sidecars on busy formations a future commit may
//! add explicit LRU bounding; today the per-entry cost (one
//! `String`) keeps the memory footprint negligible.
//!
//! # Inbox layout
//!
//! `{inbox_root}/{distribution_id}/{filename}` where `filename` comes
//! from `BlobMetadata.name` (set by the sender's `build_blob_metadata`
//! from `display_name` or the basename of `relative_path`), sanitized
//! to remove path separators. Distribution-ID namespacing avoids
//! collisions when two different distributions target the same logical
//! filename. Applications watching the inbox can correlate the
//! distribution_id back to the sender via
//! `GetAttachmentDistribution(distribution_id)`.

#![allow(clippy::result_large_err)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use chrono::Utc;
use peat_mesh::storage::blob_traits::{BlobHash, BlobMetadata, BlobStore, BlobToken};
use peat_mesh::storage::{AutomergeStore, NetworkedIrohBlobStore};
use peat_protocol::storage::{
    read_distribution_document, scan_distribution_documents, write_receiver_node_status,
    DistributionDocument, NodeTransferStatus, TransferState,
};
use tracing::{debug, info, warn};

use crate::attachments::registry::BundleRegistry;

/// Spawn the inbox watcher task. Returns immediately. The task runs for
/// the lifetime of the process (or until `document_store` / `blob_store`
/// are dropped, but those live in `SidecarNode` for the same lifetime).
pub fn spawn_inbox_watcher(
    document_store: Arc<AutomergeStore>,
    blob_store: Arc<NetworkedIrohBlobStore>,
    registry: Arc<BundleRegistry>,
    inbox_root: PathBuf,
    own_endpoint_short: String,
    poll_interval: Duration,
) {
    tokio::spawn(async move {
        info!(
            inbox = %inbox_root.display(),
            endpoint = %own_endpoint_short,
            interval_secs = poll_interval.as_secs_f64(),
            "attachment inbox watcher started"
        );
        let mut handled: HashSet<String> = HashSet::new();
        let mut ticker = tokio::time::interval(poll_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Discard immediate-first tick — file_distributions is empty
        // at startup; the first useful sweep is after at least one
        // tick of upstream sync.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(e) = scan_once(
                &document_store,
                &blob_store,
                &registry,
                &inbox_root,
                &own_endpoint_short,
                &mut handled,
            )
            .await
            {
                warn!(error = %e, "inbox sweep failed; will retry next tick");
            }
        }
    });
}

async fn scan_once(
    document_store: &Arc<AutomergeStore>,
    blob_store: &Arc<NetworkedIrohBlobStore>,
    registry: &Arc<BundleRegistry>,
    inbox_root: &Path,
    own_endpoint_short: &str,
    handled: &mut HashSet<String>,
) -> anyhow::Result<()> {
    // rc.9+: use the typed peat-protocol API which reads the structured
    // Automerge document (metadata byte-scalar + node_statuses typed
    // Map) and reconstructs the in-memory `DistributionDocument`.
    // Malformed entries are logged and skipped inside the scan helper.
    let docs = scan_distribution_documents(document_store.as_ref())?;
    debug!(
        doc_count = docs.len(),
        already_handled = handled.len(),
        "inbox sweep"
    );
    for (doc_id, doc) in docs {
        if handled.contains(&doc_id) {
            continue;
        }

        // Self-skip: distributions this node originated have a
        // registry entry; receivers never do.
        if registry.lookup_distribution(&doc.distribution_id).is_some() {
            handled.insert(doc_id);
            continue;
        }

        debug!(
            distribution_id = %doc.distribution_id,
            blob_hash = %doc.blob_hash,
            target_nodes = ?doc.target_nodes,
            own = %own_endpoint_short,
            "inbox: seen distribution doc"
        );

        // Targeting check: my short endpoint id must be in the
        // sender's resolved target_nodes list.
        if !doc.target_nodes.contains(&own_endpoint_short.to_string()) {
            debug!(
                distribution_id = %doc.distribution_id,
                "inbox: not a target, skipping"
            );
            handled.insert(doc_id);
            continue;
        }

        // Filesystem-based "already delivered" gate. Distinct from the
        // in-memory `handled` set: this survives process restart, so
        // a long-running receiver that restarts doesn't re-fetch and
        // re-write every historical delivery (caught by the PRD-006
        // v1.1 QA review on PR #65).
        if already_delivered(inbox_root, &doc.distribution_id, doc.blob_size).await {
            debug!(
                distribution_id = %doc.distribution_id,
                "inbox: filesystem already has the delivered file, skipping fetch"
            );
            handled.insert(doc_id);
            continue;
        }

        // Write a Transferring status into the distribution doc's
        // node_statuses map before fetching. The sender's progress
        // watcher (peat#864 / peat-protocol 0.9.0-rc.7) re-reads the doc
        // on each observer event and emits an IN_PROGRESS frame to
        // `subscribe_progress` subscribers. Best-effort: a failure here
        // does not block the fetch itself — the worst case is the sender
        // never observes our in-flight state.
        if let Err(e) = write_node_status(
            document_store,
            &doc,
            own_endpoint_short,
            TransferStateWrite::Transferring,
        ) {
            warn!(
                distribution_id = %doc.distribution_id,
                error = %e,
                "failed to write Transferring node status; sender will see no in-progress frame"
            );
        }

        // Test fault/throttle seam (no-op in production — see
        // `ReceiveTestDirective`). Consulted after the Transferring
        // write so the sender has already observed IN_PROGRESS.
        match peek_receive_directive(&doc.blob_hash) {
            Some(ReceiveTestDirective::FailFetch(msg)) => {
                if let Err(e) = write_node_status(
                    document_store,
                    &doc,
                    own_endpoint_short,
                    TransferStateWrite::Failed(msg),
                ) {
                    warn!(
                        distribution_id = %doc.distribution_id,
                        error = %e,
                        "test seam: failed to write injected Failed node status"
                    );
                }
                handled.insert(doc_id);
                continue;
            }
            Some(ReceiveTestDirective::HoldInFlight) => {
                // Re-read (cheap): if the sender cancelled while we
                // were holding, stop — a receiver must not deliver a
                // cancelled distribution (basis of PRD test 24's
                // deterministic mid-flight cancel).
                match read_distribution_document(document_store.as_ref(), &doc.distribution_id) {
                    Ok(Some(fresh)) if fresh.status != "distributing" => {
                        debug!(
                            distribution_id = %doc.distribution_id,
                            status = %fresh.status,
                            "test seam: distribution no longer distributing; releasing hold"
                        );
                        handled.insert(doc_id);
                        continue;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        warn!(
                            distribution_id = %doc.distribution_id,
                            error = %e,
                            "test seam: hold re-read failed; will retry next tick"
                        );
                    }
                }
                // Skip fetch this tick; NOT marked handled → revisited
                // next sweep, staying IN_PROGRESS. Non-blocking: other
                // distributions in this sweep proceed normally.
                continue;
            }
            None => {}
        }

        // Fetch the blob. `NetworkedIrohBlobStore::fetch_blob` iterates
        // known iroh peers internally and tries each via the
        // iroh-blobs downloader. If the sender isn't yet reachable
        // (handshake still settling, transient network), the call
        // returns Err and we retry on the next tick.
        let token = BlobToken {
            hash: BlobHash(doc.blob_hash.clone()),
            size_bytes: doc.blob_size,
            metadata: doc.blob_metadata.clone(),
        };
        let handle = match blob_store.fetch_blob(&token, |_| {}).await {
            Ok(h) => h,
            Err(e) => {
                debug!(
                    distribution_id = %doc.distribution_id,
                    error = %e,
                    "fetch_blob not yet succeeding; will retry next tick"
                );
                continue;
            }
        };

        // Write the bytes to the inbox.
        match write_to_inbox(inbox_root, &doc, &handle.path).await {
            Ok(target) => {
                info!(
                    distribution_id = %doc.distribution_id,
                    blob_hash = %doc.blob_hash,
                    size_bytes = doc.blob_size,
                    target = %target.display(),
                    "attachment received and written to inbox"
                );
                // Write Completed terminal status — the sender's watcher
                // observes this, emits one final DistributionStatus frame
                // with completed=total_targets, and drops the broadcast
                // sender so subscribers see RecvError::Closed.
                if let Err(e) = write_node_status(
                    document_store,
                    &doc,
                    own_endpoint_short,
                    TransferStateWrite::Completed,
                ) {
                    warn!(
                        distribution_id = %doc.distribution_id,
                        error = %e,
                        "failed to write Completed node status; sender will see no terminal frame for this node"
                    );
                }
                handled.insert(doc_id);
            }
            Err(e) => {
                warn!(
                    distribution_id = %doc.distribution_id,
                    error = %e,
                    "inbox write failed; will retry next tick"
                );
                // No `handled.insert` — retry on next tick. No Failed
                // node-status write either: retries are intentional and
                // a Failed flip would prematurely close the sender's
                // broadcast channel for this distribution.
            }
        }
    }
    Ok(())
}

// ===========================================================================
// Test fault/throttle seam (PRD §Testing Plan tests 24 & 29)
// ===========================================================================
//
// Tests 24 (`cancel_in_flight_stops_transfer`) and 29
// (`subscribe_mixed_state_emits_snapshot_for_terminal_then_live_for_inflight`)
// both need to control a receiver's blob fetch deterministically: 24
// needs a measurable in-flight window to cancel into, 29 needs one
// distribution driven to FAILED while another stays IN_PROGRESS.
//
// This is a process-global, default-empty registry consulted once per
// distribution per scan tick. **Not** a Cargo feature or `#[cfg(test)]`:
// integration tests are a separate crate (so `#[cfg(test)]` lib gates
// are inert for them), and a feature flag would exclude these PRD
// acceptance tests from the default `cargo test` CI run — the entire
// point of un-ignoring them is that CI exercises them. The cost when
// unpopulated (production) is one `RwLock` read returning `None` per
// distribution per 1s scan tick: negligible, and a complete behavioral
// no-op. Keyed by **blob_hash** (hex), not distribution_id, so a test
// can arm a directive *before* `SendAttachments` mints the
// distribution_id — race-free against the receiver's first scan.
// `#[doc(hidden)]`: the seam must be a non-`cfg(test)` `pub` symbol so
// the separate integration-test crate can reach it under the default
// `cargo test` (see the section comment above), but it is NOT a
// supported library API for external peat-node consumers. Hidden from
// rustdoc + signalled as internal; renaming into a `__test_seam`
// module was considered and deferred (it would churn the test imports
// for marginal additional signal over doc(hidden)).
#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum ReceiveTestDirective {
    /// Hold this distribution in-flight: after the `Transferring`
    /// write, skip the fetch *this tick* and move on (do NOT block the
    /// scan loop, do NOT mark handled) so the distribution stays
    /// IN_PROGRESS and is revisited next tick. Each revisit re-reads
    /// the doc: once the sender cancels (status != "distributing") the
    /// receiver stops — it must not deliver a cancelled distribution
    /// (a correctness property, and the basis of PRD test 24's
    /// deterministic mid-flight cancel).
    ///
    /// Non-blocking by design: an earlier `PauseBeforeFetch(Duration)`
    /// did an inline `sleep` inside the sequential per-distribution
    /// scan loop, which starved every *other* distribution in the
    /// same sweep for the pause duration (order-dependent flake in
    /// PRD test 29's two-distribution bundle).
    HoldInFlight,
    /// Skip the fetch entirely and write a `Failed` node_status with
    /// this error string. Drives one distribution to FAILED
    /// deterministically for PRD test 29.
    FailFetch(String),
}

static RECEIVE_TEST_HOOK: OnceLock<RwLock<HashMap<String, ReceiveTestDirective>>> = OnceLock::new();

fn receive_test_hook() -> &'static RwLock<HashMap<String, ReceiveTestDirective>> {
    RECEIVE_TEST_HOOK.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Test-only: arm a receive-path directive for blobs whose hex
/// `blob_hash` equals `blob_hash`. Production never calls this; an
/// unarmed hash is a no-op. See [`ReceiveTestDirective`].
#[doc(hidden)]
pub fn set_receive_test_directive(blob_hash: &str, directive: ReceiveTestDirective) {
    receive_test_hook()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(blob_hash.to_string(), directive);
}

/// Test-only: clear all armed receive-path directives.
#[doc(hidden)]
pub fn clear_receive_test_directives() {
    receive_test_hook()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
}

fn peek_receive_directive(blob_hash: &str) -> Option<ReceiveTestDirective> {
    let guard = receive_test_hook()
        .read()
        .unwrap_or_else(|e| e.into_inner());
    guard.get(blob_hash).cloned()
}

/// Receiver-side node-status writes the watcher emits via
/// [`peat_protocol::storage::write_receiver_node_status`].
///
/// `Transferring` once fetch begins; `Completed` once the inbox write
/// lands atomically. `Failed` is normally NOT written by the v1 watcher
/// — fetch/write failures retry on the next tick rather than being
/// treated as permanent — and is reachable only through the test
/// fault seam ([`ReceiveTestDirective::FailFetch`]), which deterministically
/// drives a single distribution to FAILED for PRD §Testing Plan
/// test 29. A production retry-budget-exhaustion give-up would also
/// use this arm.
enum TransferStateWrite {
    Transferring,
    Completed,
    /// error string carried into the written `NodeTransferStatus`.
    Failed(String),
}

/// Write a receiver's `NodeTransferStatus` into the distribution doc
/// via the typed peat-protocol API. Each receiver writes only to its
/// own keyed entry in `node_statuses` (a typed Automerge Map on rc.9+),
/// so concurrent receivers don't collide and a receiver's sequential
/// writes (Transferring → Completed) are causally ordered against
/// themselves on the same key.
///
/// Replaces the pre-rc.9 inline read-modify-write of the wholesale-
/// scalar `data` field, which was the substrate-side root of
/// [defenseunicorns/peat#864](https://github.com/defenseunicorns/peat/issues/864).
fn write_node_status(
    document_store: &Arc<AutomergeStore>,
    doc: &DistributionDocument,
    own_endpoint_short: &str,
    state: TransferStateWrite,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let ns = match state {
        TransferStateWrite::Transferring => NodeTransferStatus {
            node_id: own_endpoint_short.to_string(),
            status: TransferState::Transferring,
            progress_bytes: 0,
            total_bytes: doc.blob_size,
            started_at: Some(now),
            completed_at: None,
            error: None,
        },
        TransferStateWrite::Completed => {
            // Preserve started_at if the scan-tick snapshot saw our
            // own Transferring write; otherwise stamp now so the doc
            // has some timing signal at all.
            let started_at = doc
                .node_statuses
                .get(own_endpoint_short)
                .and_then(|s| s.started_at)
                .or(Some(now));
            NodeTransferStatus {
                node_id: own_endpoint_short.to_string(),
                status: TransferState::Completed,
                progress_bytes: doc.blob_size,
                total_bytes: doc.blob_size,
                started_at,
                completed_at: Some(now),
                error: None,
            }
        }
        TransferStateWrite::Failed(ref msg) => {
            let started_at = doc
                .node_statuses
                .get(own_endpoint_short)
                .and_then(|s| s.started_at)
                .or(Some(now));
            NodeTransferStatus {
                node_id: own_endpoint_short.to_string(),
                status: TransferState::Failed,
                progress_bytes: 0,
                total_bytes: doc.blob_size,
                started_at,
                completed_at: None,
                error: Some(msg.clone()),
            }
        }
    };

    write_receiver_node_status(
        document_store.as_ref(),
        &doc.distribution_id,
        own_endpoint_short,
        &ns,
    )?;

    debug!(
        distribution_id = %doc.distribution_id,
        node = %own_endpoint_short,
        new_status = ?match state {
            TransferStateWrite::Transferring => "Transferring",
            TransferStateWrite::Completed => "Completed",
            TransferStateWrite::Failed(_) => "Failed",
        },
        "wrote receiver node_status into distribution doc"
    );
    Ok(())
}

/// Check whether the inbox already contains a delivered file for this
/// distribution. The "matching-size + non-hidden" rule treats the
/// existence of any regular file with the declared blob_size in
/// `{inbox}/{distribution_id}/` as proof of prior delivery — the
/// filesystem layout's distribution-id-namespaced directory is the
/// durable source of truth, while the in-memory `handled` set is just
/// a per-process parse-cost optimisation.
///
/// Returns false on any I/O error so the caller falls through to the
/// fetch path and retries — better to re-deliver than to silently
/// skip a file that ought to land.
async fn already_delivered(inbox_root: &Path, distribution_id: &str, expected_size: u64) -> bool {
    let dir = inbox_root.join(distribution_id);
    if !dir.is_dir() {
        return false;
    }
    let mut iter = match tokio::fs::read_dir(&dir).await {
        Ok(i) => i,
        Err(_) => return false,
    };
    while let Ok(Some(entry)) = iter.next_entry().await {
        let path = entry.path();
        // Skip our own in-flight `.{name}.partial` markers.
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|s| s.starts_with('.'))
        {
            continue;
        }
        if let Ok(md) = entry.metadata().await {
            if md.is_file() && md.len() == expected_size {
                return true;
            }
        }
    }
    false
}

/// Compute the final inbox path for a distribution and copy the blob
/// bytes there via a tmp-file + rename pair so readers never see a
/// partial file.
async fn write_to_inbox(
    inbox_root: &Path,
    doc: &DistributionDocument,
    blob_local_path: &Path,
) -> std::io::Result<PathBuf> {
    let dir = inbox_root.join(&doc.distribution_id);
    tokio::fs::create_dir_all(&dir).await?;

    let filename = inbox_filename(&doc.blob_metadata, &doc.distribution_id);
    let target = dir.join(&filename);
    let tmp = dir.join(format!(".{filename}.partial"));

    // tokio::fs::copy reads + writes asynchronously; for v1 sizes
    // (256 MiB cap on max_file_bytes) the buffered copy is fine.
    tokio::fs::copy(blob_local_path, &tmp).await?;
    tokio::fs::rename(&tmp, &target).await?;
    Ok(target)
}

/// Derive a safe inbox filename from the blob metadata. Strips path
/// separators; falls back to `<distribution_id>.bin` if metadata has
/// no name or the name sanitises to empty.
fn inbox_filename(metadata: &BlobMetadata, distribution_id: &str) -> String {
    if let Some(raw) = metadata.name.as_ref() {
        // Take only the last path component; strip leading dots so
        // a malicious sender can't smuggle hidden files past an
        // operator scanning ls -l on the inbox.
        let last = std::path::Path::new(raw)
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.trim_start_matches('.'))
            .filter(|s| !s.is_empty());
        if let Some(name) = last {
            return name.to_string();
        }
    }
    format!("{distribution_id}.bin")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn inbox_filename_uses_metadata_name() {
        let m = BlobMetadata {
            name: Some("report.pdf".into()),
            content_type: None,
            custom: HashMap::new(),
        };
        assert_eq!(inbox_filename(&m, "dist-X"), "report.pdf");
    }

    #[test]
    fn inbox_filename_strips_path_components() {
        let m = BlobMetadata {
            name: Some("/etc/passwd".into()),
            content_type: None,
            custom: HashMap::new(),
        };
        // Path::file_name on "/etc/passwd" returns "passwd" — the leading
        // segments are stripped so a sender cannot use the metadata name
        // to redirect writes outside the inbox subdirectory.
        assert_eq!(inbox_filename(&m, "dist-X"), "passwd");
    }

    #[test]
    fn inbox_filename_strips_leading_dot() {
        let m = BlobMetadata {
            name: Some(".bashrc".into()),
            content_type: None,
            custom: HashMap::new(),
        };
        assert_eq!(inbox_filename(&m, "dist-X"), "bashrc");
    }

    #[test]
    fn inbox_filename_falls_back_on_empty_metadata() {
        let m = BlobMetadata {
            name: None,
            content_type: None,
            custom: HashMap::new(),
        };
        assert_eq!(inbox_filename(&m, "dist-X"), "dist-X.bin");
    }

    #[test]
    fn inbox_filename_falls_back_on_dotfile_that_strips_to_empty() {
        let m = BlobMetadata {
            name: Some("...".into()),
            content_type: None,
            custom: HashMap::new(),
        };
        // "..." has file_name "..." → strip leading dots → empty →
        // fallback to distribution_id-based name.
        assert_eq!(inbox_filename(&m, "dist-X"), "dist-X.bin");
    }

    #[tokio::test]
    async fn already_delivered_false_when_dir_absent() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!already_delivered(tmp.path(), "never-delivered", 100).await);
    }

    #[tokio::test]
    async fn already_delivered_true_when_matching_size_present() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dist-X");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let payload = vec![0u8; 1024];
        tokio::fs::write(dir.join("got.bin"), &payload)
            .await
            .unwrap();
        assert!(already_delivered(tmp.path(), "dist-X", 1024).await);
    }

    #[tokio::test]
    async fn already_delivered_false_when_size_differs() {
        // PRD §Validation Rule 9 guarantees content+size match before
        // ingest; this is the conservative case where the previous
        // delivery exists but was for a different blob (e.g.
        // distribution_id collision across formations or a manual
        // file drop). Re-fetching with the new size is the safe move.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dist-X");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join("got.bin"), b"shorter")
            .await
            .unwrap();
        assert!(!already_delivered(tmp.path(), "dist-X", 1024).await);
    }

    #[tokio::test]
    async fn already_delivered_ignores_partial_marker() {
        // `.{filename}.partial` is the in-flight tmp file the watcher
        // writes before atomic rename. If a crash leaves one behind
        // it must NOT count as a successful delivery.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dist-X");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join(".got.bin.partial"), vec![0u8; 1024])
            .await
            .unwrap();
        assert!(!already_delivered(tmp.path(), "dist-X", 1024).await);
    }
}
