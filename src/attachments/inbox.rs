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
//! `{inbox_root}/{relative_path}` — the inbox mirrors the sender's outbox
//! layout, so a file dropped at `outbox/sub/report.pdf` lands at
//! `inbox/sub/report.pdf` with its original name and subdirectories intact.
//! The relative path comes from `BlobMetadata.name` (set by the sender's
//! `build_blob_metadata` from `display_name` or the full `relative_path`).
//! Re-delivery of the same path overwrites (latest-wins).
//!
//! Because the sender controls `BlobMetadata.name`, it is re-sanitised on
//! arrival ([`inbox_relpath`]): only `Normal` path components are accepted, so
//! an absolute path or one containing `..` can never escape the inbox — such a
//! name falls back to a flat `{distribution_id}.bin` at the inbox root.
//! Applications watching the inbox can still correlate a delivery back to the
//! sender via `GetAttachmentDistribution(distribution_id)`.

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
    /// Filesystem-based "already delivered" gate, keyed on the file's mirrored
    /// path (`{inbox}/{relative_path}`). Restart idempotency: a long-running
    /// receiver that restarts doesn't re-fetch a delivery whose target already
    /// holds a regular file of the declared `blob_size`. Size-only — the bytes
    /// are content-verified by iroh on fetch. Returns false on any I/O error
    /// (and on the rare same-path/same-size-but-different-content case) so the
    /// caller re-delivers rather than silently skipping a file that should land.
    async fn already_delivered(&self, doc: &DistributionDocument) -> bool {
        let rel = inbox_relpath(&doc.blob_metadata)
            .unwrap_or_else(|| PathBuf::from(format!("{}.bin", doc.distribution_id)));
        match tokio::fs::metadata(self.inbox_root.join(rel)).await {
            Ok(md) => md.is_file() && md.len() == doc.blob_size,
            Err(_) => false,
        }
    }

    /// Write the blob to `{inbox}/{relative_path}`, mirroring the sender's
    /// layout (so `outbox/sub/demo.txt` lands at `inbox/sub/demo.txt`), via a
    /// tmp-file + rename so readers never see a partial file. Re-delivery of the
    /// same path overwrites (latest-wins). The relative path is re-sanitised
    /// here ([`inbox_relpath`]) — the sender controls `blob_metadata.name`, so a
    /// name that is absolute or contains `..` is rejected and the file lands at
    /// a flat `<distribution_id>.bin` at the inbox root instead of escaping it.
    async fn deliver(&self, doc: &DistributionDocument, blob_path: &Path) -> anyhow::Result<()> {
        let rel = inbox_relpath(&doc.blob_metadata)
            .unwrap_or_else(|| PathBuf::from(format!("{}.bin", doc.distribution_id)));
        let target = self.inbox_root.join(&rel);

        // Create the (possibly nested) parent dir, and stage the tmp file in it
        // so the publishing rename is atomic on the same filesystem.
        let parent = target
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.inbox_root.clone());
        tokio::fs::create_dir_all(&parent).await?;
        let fname = target
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("blob");
        let tmp = parent.join(format!(".{fname}.partial"));

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
                "inbox write size mismatch for {} (dist {}): wrote {written} bytes, \
                 expected {} — leaving for retry",
                rel.display(),
                doc.distribution_id,
                doc.blob_size
            );
        }
        tokio::fs::rename(&tmp, &target).await?;
        info!(
            distribution_id = %doc.distribution_id,
            filename = %rel.display(),
            bytes = written,
            blob_hash = %doc.blob_hash,
            target = %target.display(),
            "attachment received, validated, and written to inbox"
        );
        Ok(())
    }
}

/// Resolve the sender-provided `blob_metadata.name` into a safe path **relative
/// to the inbox root**, preserving subdirectories so the inbox mirrors the
/// sender's layout (`outbox/sub/demo.txt` → `inbox/sub/demo.txt`).
///
/// Path-traversal guard: the sender controls `name`, so only `Normal` path
/// components are accepted. Any absolute path, `..`, root, or drive-prefix
/// component makes this return `None`, and the caller falls back to a flat
/// `<distribution_id>.bin` at the inbox root — a malicious or malformed name
/// can never write outside the inbox. Returns `None` for a missing/empty name
/// or one that sanitises to nothing.
fn inbox_relpath(metadata: &BlobMetadata) -> Option<PathBuf> {
    use std::path::Component;

    let raw = metadata.name.as_deref()?;
    if raw.is_empty() {
        return None;
    }
    let mut safe = PathBuf::new();
    for comp in std::path::Path::new(raw).components() {
        match comp {
            Component::Normal(c) => safe.push(c),
            Component::CurDir => {} // "." — ignore
            // ".." (ParentDir), "/" (RootDir), or a Windows prefix — reject the
            // whole name rather than try to repair it.
            _ => return None,
        }
    }
    if safe.as_os_str().is_empty() {
        None
    } else {
        Some(safe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peat_mesh::storage::blob_traits::BlobMetadata;
    use peat_protocol::storage::{DistributionScope, TransferPriority};
    use std::collections::HashMap;

    fn meta(name: Option<&str>) -> BlobMetadata {
        BlobMetadata {
            name: name.map(String::from),
            content_type: None,
            custom: HashMap::new(),
        }
    }

    fn doc_with(distribution_id: &str, blob_size: u64, name: Option<&str>) -> DistributionDocument {
        DistributionDocument {
            distribution_id: distribution_id.to_string(),
            blob_hash: "deadbeef".to_string(),
            blob_size,
            blob_metadata: meta(name),
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
    fn relpath_preserves_subdirs() {
        assert_eq!(
            inbox_relpath(&meta(Some("report.pdf"))),
            Some(PathBuf::from("report.pdf"))
        );
        assert_eq!(
            inbox_relpath(&meta(Some("sub/dir/report.pdf"))),
            Some(PathBuf::from("sub/dir/report.pdf"))
        );
    }

    #[test]
    fn relpath_rejects_traversal_and_absolute() {
        // The sender controls `name`; these must never resolve to a path that
        // could escape the inbox — reject them so the caller uses the fallback.
        assert_eq!(inbox_relpath(&meta(Some("../../etc/passwd"))), None);
        assert_eq!(inbox_relpath(&meta(Some("/etc/passwd"))), None);
        assert_eq!(inbox_relpath(&meta(Some("a/../../b"))), None);
    }

    #[test]
    fn relpath_none_for_missing_or_empty() {
        assert_eq!(inbox_relpath(&meta(None)), None);
        assert_eq!(inbox_relpath(&meta(Some(""))), None);
        assert_eq!(inbox_relpath(&meta(Some("./"))), None);
    }

    #[tokio::test]
    async fn already_delivered_false_when_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        assert!(
            !sink
                .already_delivered(&doc_with("d", 100, Some("a.txt")))
                .await
        );
    }

    #[tokio::test]
    async fn already_delivered_true_when_matching_size() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("a.txt"), vec![0u8; 1024])
            .await
            .unwrap();
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        assert!(
            sink.already_delivered(&doc_with("d", 1024, Some("a.txt")))
                .await
        );
    }

    #[tokio::test]
    async fn already_delivered_false_when_size_differs() {
        let tmp = tempfile::tempdir().unwrap();
        tokio::fs::write(tmp.path().join("a.txt"), b"short")
            .await
            .unwrap();
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        assert!(
            !sink
                .already_delivered(&doc_with("d", 1024, Some("a.txt")))
                .await
        );
    }

    #[tokio::test]
    async fn deliver_mirrors_relative_path_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), b"hello world").unwrap();
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        let doc = doc_with("dist-Y", 11, Some("sub/dir/greeting.txt"));

        sink.deliver(&doc, src.path()).await.unwrap();
        // Mirrors the sender's relative path — NOT a {distribution_id} dir.
        let landed = tmp.path().join("sub").join("dir").join("greeting.txt");
        assert_eq!(std::fs::read(&landed).unwrap(), b"hello world");
        assert!(sink.already_delivered(&doc).await);

        // Re-deliver overwrites (latest-wins); no partial left behind.
        sink.deliver(&doc, src.path()).await.unwrap();
        assert_eq!(std::fs::read(&landed).unwrap(), b"hello world");
        assert!(!tmp
            .path()
            .join("sub")
            .join("dir")
            .join(".greeting.txt.partial")
            .exists());
    }

    #[tokio::test]
    async fn deliver_traversal_name_stays_inside_inbox() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), b"x").unwrap();
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        // A hostile name resolves to the flat fallback inside the inbox, never
        // outside it.
        let doc = doc_with("dist-evil", 1, Some("../../../../tmp/pwned"));
        sink.deliver(&doc, src.path()).await.unwrap();
        assert!(tmp.path().join("dist-evil.bin").is_file());
    }

    #[tokio::test]
    async fn deliver_size_mismatch_bails_without_publishing() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), b"hello world").unwrap(); // 11 bytes
        let sink = FilesystemInboxSink::new(tmp.path().to_path_buf());
        let doc = doc_with("dist-Z", 99, Some("x.txt")); // declares 99
        assert!(sink.deliver(&doc, src.path()).await.is_err());
        assert!(
            !tmp.path().join("x.txt").exists(),
            "a short file must not be published"
        );
    }
}
