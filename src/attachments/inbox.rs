//! Filesystem inbox sink (PRD-006 v1.1).
//!
//! The receive-side distribution lifecycle — observe synced
//! distribution documents that target this node, fetch the referenced
//! blob, write per-receiver `node_statuses` so the sender's progress
//! watcher emits cross-peer frames, the test fault seam, dedup and
//! retry — is owned by peat-protocol
//! (`IrohFileDistribution::start_receive_watcher`, issue #68). This
//! module is the thin peat-node tail: a [`ReceiveSink`] that decides
//! *where the bytes land* (an operator-configured inbox directory) and
//! *whether a prior delivery already satisfied a distribution* (so a
//! restarted long-running receiver doesn't re-fetch every historical
//! delivery).
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

use std::path::{Path, PathBuf};

use peat_mesh::storage::blob_traits::BlobMetadata;
use peat_protocol::storage::{DistributionDocument, ReceiveSink};
use tracing::info;

// Re-export the test fault seam from its new home in peat-protocol so
// existing integration tests (`peat_node::attachments::inbox::{...}`)
// keep compiling. The seam moved upstream with the receive lifecycle
// (#68); peat-node no longer owns it.
#[doc(hidden)]
pub use peat_protocol::storage::{
    clear_receive_test_directives, set_receive_test_directive, ReceiveTestDirective,
};

/// [`ReceiveSink`] that writes received blobs to an operator-configured
/// inbox directory. One sink instance per peat-node process; the
/// receive watcher (owned by peat-protocol) drives it.
pub struct FilesystemInboxSink {
    inbox_root: PathBuf,
}

impl FilesystemInboxSink {
    pub fn new(inbox_root: PathBuf) -> Self {
        Self { inbox_root }
    }
}

#[async_trait::async_trait]
impl ReceiveSink for FilesystemInboxSink {
    /// Filesystem-based "already delivered" gate. The
    /// distribution-id-namespaced directory is the durable source of
    /// truth: a long-running receiver that restarts doesn't re-fetch
    /// and re-write every historical delivery. The "matching-size +
    /// non-hidden" rule treats the existence of any regular file with
    /// the declared blob_size in `{inbox}/{distribution_id}/` as proof
    /// of prior delivery. Returns false on any I/O error so the caller
    /// falls through to the fetch path and retries — better to
    /// re-deliver than to silently skip a file that ought to land.
    async fn already_delivered(&self, doc: &DistributionDocument) -> bool {
        let dir = self.inbox_root.join(&doc.distribution_id);
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
                if md.is_file() && md.len() == doc.blob_size {
                    return true;
                }
            }
        }
        false
    }

    /// Copy the blob bytes into `{inbox}/{distribution_id}/{filename}`
    /// via a tmp-file + rename pair so readers never see a partial
    /// file.
    async fn deliver(&self, doc: &DistributionDocument, blob_path: &Path) -> anyhow::Result<()> {
        let dir = self.inbox_root.join(&doc.distribution_id);
        tokio::fs::create_dir_all(&dir).await?;

        let filename = inbox_filename(&doc.blob_metadata, &doc.distribution_id);
        let target = dir.join(&filename);
        let tmp = dir.join(format!(".{filename}.partial"));

        // tokio::fs::copy reads + writes asynchronously; for v1 sizes
        // (256 MiB cap on max_file_bytes) the buffered copy is fine.
        tokio::fs::copy(blob_path, &tmp).await?;

        // Post-write validation: the on-disk copy must match the distribution's
        // declared size before we publish it (the tmp→target rename). Content
        // integrity is already guaranteed upstream — iroh verifies the blob
        // against its content hash on fetch — so a size match confirms the
        // local write is complete and untruncated. On mismatch, drop the tmp
        // and return Err so the receive watcher retries on the next sweep
        // rather than publishing a short file.
        let written = tokio::fs::metadata(&tmp).await?.len();
        if written != doc.blob_size {
            let _ = tokio::fs::remove_file(&tmp).await;
            anyhow::bail!(
                "inbox write size mismatch for {filename} (dist {}): wrote {written} bytes, \
                 expected {} — leaving for retry",
                doc.distribution_id,
                doc.blob_size
            );
        }
        tokio::fs::rename(&tmp, &target).await?;
        info!(
            distribution_id = %doc.distribution_id,
            filename = %filename,
            bytes = written,
            blob_hash = %doc.blob_hash,
            target = %target.display(),
            "attachment received, validated, and written to inbox"
        );
        Ok(())
    }
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
    use peat_mesh::storage::blob_traits::BlobMetadata;
    use peat_protocol::storage::{DistributionScope, TransferPriority};
    use std::collections::HashMap;

    fn doc_with(distribution_id: &str, blob_size: u64, name: Option<&str>) -> DistributionDocument {
        DistributionDocument {
            distribution_id: distribution_id.to_string(),
            blob_hash: "deadbeef".to_string(),
            blob_size,
            blob_metadata: BlobMetadata {
                name: name.map(|s| s.to_string()),
                content_type: None,
                custom: HashMap::new(),
            },
            scope: DistributionScope::AllNodes,
            priority: TransferPriority::Normal,
            target_nodes: vec![],
            started_at: chrono::Utc::now(),
            status: "distributing".to_string(),
            cancelled_at: None,
            collection: None,
            node_statuses: HashMap::new(),
        }
    }

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
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        assert!(
            !sink
                .already_delivered(&doc_with("never-delivered", 100, None))
                .await
        );
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
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        assert!(
            sink.already_delivered(&doc_with("dist-X", 1024, None))
                .await
        );
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
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        assert!(
            !sink
                .already_delivered(&doc_with("dist-X", 1024, None))
                .await
        );
    }

    #[tokio::test]
    async fn already_delivered_ignores_partial_marker() {
        // `.{filename}.partial` is the in-flight tmp file the sink
        // writes before atomic rename. If a crash leaves one behind
        // it must NOT count as a successful delivery.
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("dist-X");
        tokio::fs::create_dir_all(&dir).await.unwrap();
        tokio::fs::write(dir.join(".got.bin.partial"), vec![0u8; 1024])
            .await
            .unwrap();
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        assert!(
            !sink
                .already_delivered(&doc_with("dist-X", 1024, None))
                .await
        );
    }

    #[tokio::test]
    async fn deliver_writes_file_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), b"hello world").unwrap();
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        let doc = doc_with("dist-Y", 11, Some("greeting.txt"));

        sink.deliver(&doc, src.path()).await.unwrap();
        let landed = tmp.path().join("dist-Y").join("greeting.txt");
        assert_eq!(std::fs::read(&landed).unwrap(), b"hello world");
        assert!(sink.already_delivered(&doc).await);

        // Re-deliver: atomic rename overwrites, no partial left behind.
        sink.deliver(&doc, src.path()).await.unwrap();
        assert_eq!(std::fs::read(&landed).unwrap(), b"hello world");
    }
}
