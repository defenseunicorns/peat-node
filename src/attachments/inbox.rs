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

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use peat_mesh::storage::blob_traits::{BlobHash, BlobMetadata, BlobStore, BlobToken};
use peat_mesh::storage::{AutomergeStore, NetworkedIrohBlobStore};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::attachments::registry::BundleRegistry;

const DISTRIBUTION_COLLECTION: &str = "file_distributions";

/// Shape of the distribution document peat-protocol writes. Mirrors
/// `IrohFileDistribution::store_distribution_document`'s JSON layout.
/// Only the fields the watcher needs are extracted.
#[derive(Debug, Deserialize)]
struct DistributionDoc {
    distribution_id: String,
    blob_hash: String,
    blob_size: u64,
    #[serde(default)]
    blob_metadata: BlobMetadata,
    target_nodes: Vec<String>,
}

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
    let collection = document_store.collection(DISTRIBUTION_COLLECTION);
    let docs = collection.scan()?;
    debug!(
        doc_count = docs.len(),
        already_handled = handled.len(),
        "inbox sweep"
    );
    for (doc_id, bytes) in docs {
        if handled.contains(&doc_id) {
            continue;
        }

        let doc: DistributionDoc = match serde_json::from_slice(&bytes) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    doc_id = %doc_id,
                    error = %e,
                    "skipping malformed distribution document (will not retry)"
                );
                handled.insert(doc_id);
                continue;
            }
        };

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
                handled.insert(doc_id);
            }
            Err(e) => {
                warn!(
                    distribution_id = %doc.distribution_id,
                    error = %e,
                    "inbox write failed; will retry next tick"
                );
                // No `handled.insert` — retry on next tick.
            }
        }
    }
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
    doc: &DistributionDoc,
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
        tokio::fs::write(dir.join("got.bin"), &payload).await.unwrap();
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
        tokio::fs::write(dir.join("got.bin"), b"shorter").await.unwrap();
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
        tokio::fs::write(dir.join(".got.bin.partial"), vec![0u8; 1024]).await.unwrap();
        assert!(!already_delivered(tmp.path(), "dist-X", 1024).await);
    }
}
