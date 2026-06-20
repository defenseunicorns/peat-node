//! Single-pass hash + blob-store ingest with content-address rollback
//! safety (PRD-006 Step 2 ingest sub-module).
//!
//! Per file:
//!   1. Open the canonicalised absolute path with `O_NOFOLLOW` on Linux to
//!      defeat the TOCTOU symlink swap noted in PRD §Validation Rule 5.
//!   2. Stream the file into [`BlobStore::create_blob_from_stream`] through
//!      a tee-style [`AsyncRead`] adapter that also feeds every byte into a
//!      `Sha256` hasher — no double-read.
//!   3. After the stream completes, verify the finalised sha256 equals the
//!      caller's asserted value (PRD Rule 9 streaming match). On mismatch,
//!      best-effort `delete_blob` and bail.
//!
//! After every file is ingested, distributions are started via
//! [`FileDistribution::distribute`]. If any per-file step fails, every blob
//! this request *newly created* is rolled back. Blobs that already existed
//! in the local store before the request are never deleted — iroh-blobs is
//! content-addressed and the same token may be referenced by other live
//! distributions. The "pre-existing" set is captured by snapshotting
//! [`BlobStore::list_local_blobs`] at the start of the call.
//!
//! `AsyncRead` wrappers don't require `pin-project` here because the inner
//! reader ([`tokio::fs::File`]) is `Unpin`.

#![allow(clippy::result_large_err)]

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use connectrpc::ConnectError;
use peat_mesh::storage::blob_traits::{BlobHash, BlobMetadata, BlobStore, BlobToken};
use peat_protocol::storage::file_distribution::{
    DistributionHandle, DistributionScope, FileDistribution, TransferPriority,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, ReadBuf};
use tracing::warn;

use crate::attachments::validate::{ValidatedBundle, ValidatedFile, ValidatedScope};

/// One file's ingest result. Maps directly onto the `AttachmentHandle` wire
/// message returned in [`pb::SendAttachmentsResponse`].
///
/// Carries the full [`DistributionHandle`] (not just the distribution_id)
/// so subsequent calls to [`FileDistribution::status`] /
/// [`FileDistribution::cancel`] can pass the handle peat-protocol gave us
/// rather than reconstructing one with dummy fields.
///
/// [`pb::SendAttachmentsResponse`]: crate::pb::SendAttachmentsResponse
#[derive(Debug, Clone)]
pub struct IngestedBlob {
    /// Position in the caller's `SendAttachmentsRequest::files`.
    pub file_index: usize,
    pub blob_token: BlobToken,
    pub distribution_handle: DistributionHandle,
}

/// Ingest every file in a validated bundle, then start a distribution per
/// file. Atomic on failure — see module docs for the rollback contract.
pub async fn ingest_bundle<B, F>(
    validated: ValidatedBundle,
    blob_store: &B,
    file_distribution: &F,
    priority: TransferPriority,
) -> Result<Vec<IngestedBlob>, ConnectError>
where
    B: BlobStore + ?Sized,
    F: FileDistribution + ?Sized,
{
    // Snapshot pre-existing blob hashes for rollback safety. A blob that
    // existed *before* this request must not be deleted on rollback — some
    // other live distribution may reference it.
    let pre_existing: HashSet<BlobHash> = blob_store
        .list_local_blobs()
        .into_iter()
        .map(|t| t.hash)
        .collect();

    let scope = scope_to_protocol(&validated.scope);

    // Phase 1: ingest every file. Track tokens this request produced so we
    // can roll them back if any later step fails.
    let mut created: Vec<BlobToken> = Vec::with_capacity(validated.files.len());
    for file in &validated.files {
        match ingest_file(file, blob_store).await {
            Ok(token) => created.push(token),
            Err(e) => {
                rollback(blob_store, &created, &pre_existing).await;
                return Err(e);
            }
        }
    }

    // Phase 2: start a distribution per file. If any distribute() call
    // fails, roll back every newly-created blob (the already-started
    // distributions are best-effort cancelled by the service layer — we
    // don't have a clean cross-distribution cancel API here in v1).
    let mut handles = Vec::with_capacity(validated.files.len());
    for (i, file) in validated.files.iter().enumerate() {
        let token = &created[i];
        match file_distribution
            .distribute(token, scope.clone(), priority)
            .await
        {
            Ok(h) => handles.push(IngestedBlob {
                file_index: file.file_index,
                blob_token: token.clone(),
                distribution_handle: h,
            }),
            Err(e) => {
                rollback(blob_store, &created, &pre_existing).await;
                return Err(ConnectError::internal(format!(
                    "files[{}]: distribute failed: {e}",
                    file.file_index
                )));
            }
        }
    }

    Ok(handles)
}

fn scope_to_protocol(scope: &ValidatedScope) -> DistributionScope {
    match scope {
        ValidatedScope::AllNodes => DistributionScope::AllNodes,
        ValidatedScope::NodeList(node_ids) => DistributionScope::Nodes {
            node_ids: node_ids.clone(),
        },
    }
}

async fn ingest_file<B: BlobStore + ?Sized>(
    file: &ValidatedFile,
    blob_store: &B,
) -> Result<BlobToken, ConnectError> {
    let std_file = open_nofollow(&file.absolute_path).map_err(|e| {
        ConnectError::invalid_argument(format!(
            "files[{}]: open `{}` failed: {}",
            file.file_index,
            file.absolute_path.display(),
            e
        ))
    })?;
    let tokio_file = tokio::fs::File::from_std(std_file);

    let hasher = Arc::new(Mutex::new(Sha256::new()));
    let mut tee = TeeReader::new(tokio_file, hasher.clone());

    let metadata = build_blob_metadata(file);
    let token = blob_store
        .create_blob_from_stream(&mut tee, Some(file.size_bytes), metadata)
        .await
        .map_err(|e| {
            ConnectError::internal(format!(
                "files[{}]: blob ingest failed: {e}",
                file.file_index
            ))
        })?;

    // Streaming hash match (PRD Rule 9). On mismatch, delete the just-created
    // blob — never leave an orphan that maps to wrong content.
    let computed = hasher
        .lock()
        .expect("sha256 hasher poisoned (no thread panicked while holding the lock)")
        .clone()
        .finalize();
    if computed.as_slice() != file.sha256.as_slice() {
        let _ = blob_store.delete_blob(&token.hash).await;
        return Err(ConnectError::invalid_argument(format!(
            "files[{}].sha256: streamed hash does not match declared",
            file.file_index
        )));
    }

    Ok(token)
}

fn build_blob_metadata(file: &ValidatedFile) -> BlobMetadata {
    // Carry the FULL relative path (forward-slashed), not just the basename, so
    // the receiver can mirror the sender's layout (inbox/<relative_path>)
    // instead of flattening every file to its basename. The receiver
    // re-sanitises this against path traversal before use. `display_name` still
    // overrides when a caller set one explicitly.
    let name = file
        .display_name
        .clone()
        .or_else(|| Some(file.relative_path.clone()));

    BlobMetadata {
        name,
        content_type: file.content_type.clone(),
        custom: HashMap::new(),
    }
}

/// Open `path` for read with the OS's "do not traverse a final symlink"
/// flag where supported. On Linux this is `O_NOFOLLOW` via `OpenOptionsExt`;
/// on other Unixes most kernels honour `O_NOFOLLOW` the same way. On
/// platforms without `OpenOptionsExt::custom_flags` we open normally — PRD
/// §Validation Rule 5's TOCTOU mitigation degrades; the descendant check
/// in `validate.rs` is still in place.
fn open_nofollow(path: &Path) -> io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    opts.open(path)
}

async fn rollback<B: BlobStore + ?Sized>(
    blob_store: &B,
    created: &[BlobToken],
    pre_existing: &HashSet<BlobHash>,
) {
    for token in created {
        if pre_existing.contains(&token.hash) {
            // The token's content existed before this request. A live
            // distribution somewhere else may reference it — never delete.
            continue;
        }
        if let Err(e) = blob_store.delete_blob(&token.hash).await {
            // Rollback is best-effort. A failure to clean up an orphaned
            // blob is not a hard error — the operator can prune via the
            // blob store's GC path. Log so it's visible.
            warn!(
                hash = %token.hash,
                error = %e,
                "rollback: failed to delete newly-created blob"
            );
        }
    }
}

// ---- Tee reader -------------------------------------------------------------

/// AsyncRead adapter that mirrors every byte read into a shared `Sha256`
/// hasher. The inner reader is `Unpin` here ([`tokio::fs::File`]); a
/// `pin-project` crate would be needed to wrap non-`Unpin` readers safely.
struct TeeReader<R> {
    inner: R,
    hasher: Arc<Mutex<Sha256>>,
}

impl<R> TeeReader<R> {
    fn new(inner: R, hasher: Arc<Mutex<Sha256>>) -> Self {
        Self { inner, hasher }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TeeReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let pre = buf.filled().len();
        let inner = Pin::new(&mut self.inner);
        match inner.poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let new_data = &buf.filled()[pre..];
                if !new_data.is_empty() {
                    self.hasher
                        .lock()
                        .expect("sha256 hasher poisoned (no thread panicked while holding it)")
                        .update(new_data);
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::validate::{ValidatedBundle, ValidatedFile, ValidatedScope};
    use async_trait::async_trait;
    use peat_mesh::storage::blob_traits::{BlobHandle, BlobProgress};
    use peat_protocol::storage::file_distribution::{
        DistributionHandle, DistributionStatus, NodeTransferStatus, TransferState,
    };
    use std::path::PathBuf;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::broadcast;

    /// Hex-encoded sha256, used as a stand-in for the content-addressed
    /// token in tests. Production uses BLAKE3 via iroh-blobs; the mock only
    /// needs *some* content-derived token to exercise rollback semantics.
    fn content_hash(data: &[u8]) -> String {
        let out = Sha256::digest(data);
        hex::encode(out)
    }

    fn sha256_of(data: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(data);
        let out = h.finalize();
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&out);
        arr
    }

    /// In-test BlobStore that keeps content in a HashMap keyed by a
    /// content-derived token. Only the methods exercised by ingest are
    /// implemented; the rest panic.
    #[derive(Default)]
    struct MockBlobStore {
        blobs: StdMutex<HashMap<String, Vec<u8>>>,
    }

    impl MockBlobStore {
        fn insert_raw(&self, data: &[u8]) -> BlobToken {
            let hash = content_hash(data);
            self.blobs
                .lock()
                .unwrap()
                .insert(hash.clone(), data.to_vec());
            BlobToken {
                hash: BlobHash(hash),
                size_bytes: data.len() as u64,
                metadata: BlobMetadata::default(),
            }
        }
    }

    #[async_trait]
    impl BlobStore for MockBlobStore {
        async fn create_blob(
            &self,
            _path: &Path,
            _metadata: BlobMetadata,
        ) -> anyhow::Result<BlobToken> {
            unimplemented!("MockBlobStore::create_blob not used by ingest tests")
        }

        async fn create_blob_from_bytes(
            &self,
            data: &[u8],
            metadata: BlobMetadata,
        ) -> anyhow::Result<BlobToken> {
            let hash = content_hash(data);
            self.blobs
                .lock()
                .unwrap()
                .insert(hash.clone(), data.to_vec());
            Ok(BlobToken {
                hash: BlobHash(hash),
                size_bytes: data.len() as u64,
                metadata,
            })
        }

        async fn fetch_blob<F>(
            &self,
            _token: &BlobToken,
            _progress: F,
        ) -> anyhow::Result<BlobHandle>
        where
            F: FnMut(BlobProgress) + Send + 'static,
        {
            unimplemented!("MockBlobStore::fetch_blob not used by ingest tests")
        }

        fn blob_exists_locally(&self, hash: &BlobHash) -> bool {
            self.blobs.lock().unwrap().contains_key(&hash.0)
        }

        fn blob_info(&self, hash: &BlobHash) -> Option<BlobToken> {
            let map = self.blobs.lock().unwrap();
            let bytes = map.get(&hash.0)?;
            Some(BlobToken {
                hash: hash.clone(),
                size_bytes: bytes.len() as u64,
                metadata: BlobMetadata::default(),
            })
        }

        async fn delete_blob(&self, hash: &BlobHash) -> anyhow::Result<()> {
            self.blobs.lock().unwrap().remove(&hash.0);
            Ok(())
        }

        fn list_local_blobs(&self) -> Vec<BlobToken> {
            self.blobs
                .lock()
                .unwrap()
                .iter()
                .map(|(h, v)| BlobToken {
                    hash: BlobHash(h.clone()),
                    size_bytes: v.len() as u64,
                    metadata: BlobMetadata::default(),
                })
                .collect()
        }

        fn local_storage_bytes(&self) -> u64 {
            self.blobs
                .lock()
                .unwrap()
                .values()
                .map(|v| v.len() as u64)
                .sum()
        }
    }

    /// In-test FileDistribution that returns a synthetic handle on every
    /// distribute(). The other methods panic — ingest only calls distribute.
    struct MockFileDistribution;

    #[async_trait]
    impl FileDistribution for MockFileDistribution {
        async fn distribute(
            &self,
            blob_token: &BlobToken,
            scope: DistributionScope,
            priority: TransferPriority,
        ) -> anyhow::Result<DistributionHandle> {
            Ok(DistributionHandle::new(
                blob_token.hash.clone(),
                scope,
                priority,
            ))
        }

        async fn status(&self, _handle: &DistributionHandle) -> anyhow::Result<DistributionStatus> {
            unimplemented!("MockFileDistribution::status not used by ingest tests")
        }

        async fn cancel(&self, _handle: &DistributionHandle) -> anyhow::Result<()> {
            unimplemented!("MockFileDistribution::cancel not used by ingest tests")
        }

        async fn wait_for_completion(
            &self,
            _handle: &DistributionHandle,
            _timeout: Duration,
        ) -> anyhow::Result<DistributionStatus> {
            unimplemented!("MockFileDistribution::wait_for_completion not used by ingest tests")
        }

        async fn subscribe_progress(
            &self,
            _handle: &DistributionHandle,
        ) -> anyhow::Result<broadcast::Receiver<DistributionStatus>> {
            unimplemented!("MockFileDistribution::subscribe_progress not used by ingest tests")
        }
    }

    /// Write `bytes` to `root/rel` and return a `ValidatedFile` carrying
    /// the absolute path + the *declared* sha256 (caller supplies this so
    /// the test can deliberately pass the wrong hash).
    fn validated_file(
        index: usize,
        root: &Path,
        rel: &str,
        bytes: &[u8],
        declared_sha256: [u8; 32],
    ) -> ValidatedFile {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, bytes).unwrap();
        let canonical = std::fs::canonicalize(&path).unwrap();
        ValidatedFile {
            file_index: index,
            absolute_path: canonical,
            root_name: "outbox".to_string(),
            relative_path: rel.to_string(),
            size_bytes: bytes.len() as u64,
            sha256: declared_sha256,
            content_type: None,
            display_name: None,
        }
    }

    fn bundle_with(files: Vec<ValidatedFile>) -> ValidatedBundle {
        ValidatedBundle {
            files,
            scope: ValidatedScope::AllNodes,
            bundle_id: None,
        }
    }

    #[tokio::test]
    async fn ingest_hash_mismatch_cleans_up_blob() {
        let dir = TempDir::new().unwrap();
        let store = MockBlobStore::default();
        let fd = MockFileDistribution;

        let bytes = b"hello world";
        // Declared sha256 is intentionally wrong — actual hash differs.
        let wrong = [0xAAu8; 32];
        let file = validated_file(0, dir.path(), "a.bin", bytes, wrong);

        let err = ingest_bundle(
            bundle_with(vec![file]),
            &store,
            &fd,
            TransferPriority::Normal,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument);
        assert!(err.to_string().contains("sha256"));

        // The blob that ingest created mid-stream must be gone — the store's
        // list_local_blobs reports zero local blobs.
        assert!(
            store.blobs.lock().unwrap().is_empty(),
            "post-mismatch blob store should be empty (hash-mismatch cleanup)"
        );
    }

    #[tokio::test]
    async fn ingest_atomic_on_partial_failure() {
        let dir = TempDir::new().unwrap();
        let store = MockBlobStore::default();
        let fd = MockFileDistribution;

        let b1 = b"file one content";
        let b2 = b"file two content";
        let b3 = b"file three content";

        // File 2 has a deliberately-wrong sha256 so its streaming hash
        // check fails. Files 1 and 3 have correct hashes.
        let f1 = validated_file(0, dir.path(), "f1.bin", b1, sha256_of(b1));
        let f2 = validated_file(1, dir.path(), "f2.bin", b2, [0xBBu8; 32]);
        let f3 = validated_file(2, dir.path(), "f3.bin", b3, sha256_of(b3));

        let err = ingest_bundle(
            bundle_with(vec![f1, f2, f3]),
            &store,
            &fd,
            TransferPriority::Normal,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument);

        // File 1 was ingested before file 2 failed — its blob must have
        // been rolled back. File 3 was never reached. Store is empty.
        assert!(
            store.blobs.lock().unwrap().is_empty(),
            "partial-failure rollback must delete files-already-ingested blobs"
        );
    }

    #[tokio::test]
    async fn rollback_preserves_pre_existing_blob_tokens() {
        let dir = TempDir::new().unwrap();
        let store = MockBlobStore::default();
        let fd = MockFileDistribution;

        let content_c = b"shared content C";
        // Pre-populate the store with content C. The content-addressed
        // token T is whatever the mock derives from the bytes — the
        // assertion below uses it directly.
        let pre_token = store.insert_raw(content_c);

        // File 1 has the SAME content C — ingest will produce the same
        // token T (the mock's content_hash is deterministic). The
        // pre-existing snapshot must record T, so a later rollback skips it.
        // File 2 has a wrong-sha256 to force a rollback during ingest.
        let f1 = validated_file(0, dir.path(), "f1.bin", content_c, sha256_of(content_c));
        let bad = b"different content";
        let f2 = validated_file(1, dir.path(), "f2.bin", bad, [0xCCu8; 32]);

        let err = ingest_bundle(
            bundle_with(vec![f1, f2]),
            &store,
            &fd,
            TransferPriority::Normal,
        )
        .await
        .unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::InvalidArgument);

        // T must STILL be in the store — rollback recognised that it was
        // pre-existing and skipped the delete. This is the content-address
        // safety invariant: another live distribution may reference T.
        assert!(
            store.blob_exists_locally(&pre_token.hash),
            "pre-existing blob token must survive rollback"
        );
        // Nothing else stuck around (the mock's content-addressing means
        // file 2 either created a token mid-stream which was then rolled
        // back, or never created one because its hash mismatched first;
        // either way it must not be in the store).
        let remaining: Vec<String> = store.blobs.lock().unwrap().keys().cloned().collect();
        assert_eq!(remaining, vec![pre_token.hash.0]);
    }

    // Suppress the unused-import warning on imports that are only present
    // for the trait-method signatures (NodeTransferStatus, TransferState).
    #[allow(dead_code)]
    fn _imports_for_trait_signatures(_: NodeTransferStatus, _: TransferState, _: PathBuf) {}
}
