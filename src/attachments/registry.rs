//! Bundle handle table — the in-memory store that backs PRD-006's
//! bundle-id idempotency, conflict detection, late-subscribe lookup, and
//! the retention / LRU-eviction lifecycle.
//!
//! # Lookup semantics (PRD §Validation Rule 12)
//!
//! [`BundleRegistry::check_resubmit`] maps `(bundle_id, identity)` onto one
//! of three outcomes:
//!
//! - [`BundleLookup::Idempotent`] — a bundle with this `bundle_id` exists
//!   AND its identity-set (`root_name`, `relative_path`, `size_bytes`,
//!   `sha256`, in order) matches the request's. The handler must return
//!   the *existing* handles unchanged. Optional metadata
//!   (`content_type`, `display_name`) is **not** part of identity equality
//!   — a resubmit that adds, removes, or changes either is still treated
//!   as identical, and the original ingest's metadata is retained.
//!
//! - [`BundleLookup::Conflict`] — a bundle with this `bundle_id` exists in
//!   a *non-terminal* state (`Pending` / `InProgress` / `Completed`), but
//!   the identity-set doesn't match. The handler must reject
//!   `ALREADY_EXISTS` with the existing bundle's `created_at` in the error
//!   detail. The existing bundle is **not** touched.
//!
//! - [`BundleLookup::NotFound`] — either the bundle was never seen, or it
//!   was evicted (retention timeout / LRU pressure), or it is in a
//!   *terminal-reusable* state (`Failed` / `Cancelled`). The handler
//!   proceeds with a fresh ingest and calls [`insert`]; an existing
//!   terminal-reusable record is replaced and its prior distribution IDs
//!   are removed from the lookup index, so a `GetAttachmentDistribution`
//!   against the old IDs returns `NotFound` (PRD test 17 acceptance).
//!
//! # Lifecycle
//!
//! - [`insert`] adds a new record; if `--attachment-max-known-bundles` is
//!   reached, an LRU pass evicts the least-recently-touched bundle (O(N)
//!   scan over a bounded N; default 4096).
//! - [`evict_expired`] sweeps every bundle and drops those whose
//!   `last_touched_at` is older than the retention window AND whose status
//!   is terminal. Non-terminal bundles never expire on retention alone —
//!   only via LRU pressure. PRD §Configuration's
//!   `--attachment-handle-retention-secs=0` disables retention entirely.
//!
//! # Concurrency
//!
//! Wrapped in `std::sync::RwLock`. Reads (`check_resubmit`, `get`,
//! `lookup_distribution`) are short and don't take the write lock unless
//! they update `last_touched_at`. Writes (`insert`, `update_status`,
//! `evict_expired`) take exclusive access briefly. The registry is
//! `Send + Sync` so the service layer can hold an `Arc<BundleRegistry>`
//! on `SidecarNode`.
//!
//! # In-memory only (PRD §Configuration "Handle-table durability")
//!
//! No persistence. A peat-node restart drops every `bundle_id`; subscribers
//! re-attaching to pre-restart IDs receive `NotFound`. v2 may add durable
//! handle tables; v1 is explicit about the limitation so the surprise
//! doesn't show up in production.
//!
//! [`insert`]: BundleRegistry::insert
//! [`evict_expired`]: BundleRegistry::evict_expired

#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};

use peat_protocol::storage::file_distribution::DistributionHandle;

use crate::attachments::validate::ValidatedFile;

/// Subset of `ValidatedFile` that participates in the bundle-id identity
/// equality check. Optional metadata (`content_type`, `display_name`) is
/// intentionally excluded — PRD §Validation Rule 12 retains the original
/// ingest's optional fields across resubmits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileIdentity {
    pub root_name: String,
    pub relative_path: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
}

impl FileIdentity {
    pub fn from_validated(file: &ValidatedFile) -> Self {
        Self {
            root_name: file.root_name.clone(),
            relative_path: file.relative_path.clone(),
            size_bytes: file.size_bytes,
            sha256: file.sha256,
        }
    }
}

/// Identity-set for a bundle. Ordered: file-position matters per PRD
/// ("for every file, in the same order").
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BundleIdentity {
    pub files: Vec<FileIdentity>,
}

impl BundleIdentity {
    pub fn from_validated(files: &[ValidatedFile]) -> Self {
        Self {
            files: files.iter().map(FileIdentity::from_validated).collect(),
        }
    }
}

/// One AttachmentHandle row stored on the bundle. Mirrors the wire
/// `AttachmentHandle` and is what `check_resubmit` returns for the
/// idempotent path.
#[derive(Clone, Debug)]
pub struct AttachmentHandleRecord {
    pub file_index: usize,
    pub blob_token_hash: String,
    /// The full handle returned by `FileDistribution::distribute`. Stored
    /// so subsequent `file_distribution.status / cancel` calls receive
    /// the exact handle peat-protocol issued, not a partial reconstruction.
    pub distribution_handle: DistributionHandle,
    /// Original metadata. Resubmits never overwrite — PRD Rule 12
    /// "original values retained".
    pub content_type: Option<String>,
    pub display_name: Option<String>,
}

impl AttachmentHandleRecord {
    /// Convenience accessor — the distribution_id key used by the registry
    /// reverse index.
    pub fn distribution_id(&self) -> &str {
        &self.distribution_handle.distribution_id
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BundleStatus {
    Pending,
    InProgress,
    Completed,
    /// Reserved for v2 (receive-side observer hooks). v1 senders never emit.
    Partial,
    Failed,
    Cancelled,
}

impl BundleStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Partial | Self::Failed | Self::Cancelled
        )
    }

    /// PRD §Validation Rule 12 terminal-reuse branch: `Failed` and
    /// `Cancelled` bundles can be resubmitted with a different identity
    /// set. `Completed` and `Partial` lock the identity.
    pub fn allows_reuse_with_different_files(self) -> bool {
        matches!(self, Self::Failed | Self::Cancelled)
    }
}

/// One bundle's full state. Cloned on every read — the registry holds the
/// canonical copy under the inner RwLock.
#[derive(Clone, Debug)]
pub struct BundleRecord {
    pub bundle_id: String,
    pub identity: BundleIdentity,
    pub handles: Vec<AttachmentHandleRecord>,
    pub status: BundleStatus,
    pub created_at: SystemTime,
    pub last_touched_at: SystemTime,
}

impl BundleRecord {
    pub fn new(
        bundle_id: String,
        identity: BundleIdentity,
        handles: Vec<AttachmentHandleRecord>,
    ) -> Self {
        let now = SystemTime::now();
        Self {
            bundle_id,
            identity,
            handles,
            status: BundleStatus::Pending,
            created_at: now,
            last_touched_at: now,
        }
    }
}

/// Outcome of `check_resubmit`. See module docs for the three branches.
#[derive(Clone, Debug)]
pub enum BundleLookup {
    Idempotent(BundleRecord),
    Conflict { created_at: SystemTime },
    NotFound,
}

#[derive(Clone, Copy, Debug)]
pub struct RegistryConfig {
    /// Seconds. `0` disables retention entirely (no idempotency,
    /// no late-subscribe — discouraged).
    pub handle_retention_secs: u32,
    /// Hard cap on resident bundle count; LRU eviction fires when over.
    pub max_known_bundles: u32,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            handle_retention_secs: crate::attachments::config::DEFAULT_HANDLE_RETENTION_SECS,
            max_known_bundles: crate::attachments::config::DEFAULT_MAX_KNOWN_BUNDLES,
        }
    }
}

/// Thread-safe bundle handle table. See module docs.
pub struct BundleRegistry {
    inner: RwLock<RegistryInner>,
    config: RegistryConfig,
}

struct RegistryInner {
    bundles: HashMap<String, BundleRecord>,
    /// distribution_id → bundle_id. Maintained on insert / update / evict.
    distribution_index: HashMap<String, String>,
}

impl BundleRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            inner: RwLock::new(RegistryInner {
                bundles: HashMap::new(),
                distribution_index: HashMap::new(),
            }),
            config,
        }
    }

    /// PRD Rule 12 lookup. See [`BundleLookup`] variants.
    ///
    /// Two-phase locking: a read lock for the common case (bundle absent,
    /// or present-and-non-terminal-and-conflicting — both pure reads),
    /// then upgrade to a write lock only when state actually mutates
    /// (idempotent `last_touched_at` bump, terminal-reuse drop). The
    /// read→write upgrade re-fetches the bundle entry in case it changed
    /// between the lock releases.
    pub fn check_resubmit(&self, bundle_id: &str, identity: &BundleIdentity) -> BundleLookup {
        // Phase 1: read-only probe.
        {
            let inner = self
                .inner
                .read()
                .expect("BundleRegistry RwLock poisoned (no thread panicked while holding it)");
            match inner.bundles.get(bundle_id) {
                None => return BundleLookup::NotFound,
                Some(existing) => {
                    if !existing.status.allows_reuse_with_different_files()
                        && existing.identity != *identity
                    {
                        // Pure read path: a non-terminal-reusable bundle
                        // with a conflicting identity returns Conflict
                        // without touching state. No write lock needed.
                        return BundleLookup::Conflict {
                            created_at: existing.created_at,
                        };
                    }
                    // Falls through to the write-lock path for the
                    // mutating branches (terminal-reuse drop, idempotent
                    // `last_touched_at` bump).
                }
            }
        }

        // Phase 2: write lock for the mutating branches. Re-fetch in case
        // a concurrent caller mutated state between phases. Race: if the
        // bundle was evicted between phases, treat as NotFound (the
        // caller will run a fresh ingest, which is the correct semantic).
        let mut inner = self.inner.write().expect("BundleRegistry RwLock poisoned");
        match inner.bundles.get(bundle_id) {
            None => BundleLookup::NotFound,
            Some(existing) => {
                if existing.status.allows_reuse_with_different_files() {
                    // Terminal-reusable (Failed / Cancelled). Clear the
                    // stale entry's distribution_ids from the lookup
                    // index NOW so `GetAttachmentDistribution` against
                    // the old IDs returns NotFound the instant we
                    // return (PRD test 17 acceptance).
                    let stale = existing.clone();
                    drop_record(&mut inner, &stale);
                    BundleLookup::NotFound
                } else if existing.identity == *identity {
                    let mut rec = existing.clone();
                    rec.last_touched_at = SystemTime::now();
                    if let Some(live) = inner.bundles.get_mut(bundle_id) {
                        live.last_touched_at = rec.last_touched_at;
                    }
                    BundleLookup::Idempotent(rec)
                } else {
                    // Conflict observed under the write lock — phase 1's
                    // read may have raced an idempotent-bump under
                    // another caller; the answer is still Conflict.
                    BundleLookup::Conflict {
                        created_at: existing.created_at,
                    }
                }
            }
        }
    }

    /// Store a new bundle record. If the registry is at capacity, evicts
    /// the LRU resident first. The caller has already cleared any prior
    /// record with this bundle_id via `check_resubmit`.
    pub fn insert(&self, record: BundleRecord) {
        let mut inner = self.inner.write().expect("BundleRegistry RwLock poisoned");

        // If a prior record exists at this bundle_id (terminal-reusable
        // that check_resubmit cleared, or a re-insert race), drop it first
        // so the distribution_index doesn't leak.
        if let Some(prior) = inner.bundles.remove(&record.bundle_id) {
            drop_record_indexes_only(&mut inner, &prior);
        }

        // LRU eviction when at capacity. handle_retention=0 means
        // "no retention" but max_known_bundles still bounds the table.
        let cap = self.config.max_known_bundles as usize;
        if cap > 0 && inner.bundles.len() >= cap {
            evict_lru(&mut inner);
        }

        for h in &record.handles {
            inner
                .distribution_index
                .insert(h.distribution_id().to_string(), record.bundle_id.clone());
        }
        inner.bundles.insert(record.bundle_id.clone(), record);
    }

    /// Read a bundle by ID. Updates `last_touched_at` so LRU ordering
    /// reflects access, not just write.
    pub fn get(&self, bundle_id: &str) -> Option<BundleRecord> {
        let mut inner = self.inner.write().expect("BundleRegistry RwLock poisoned");
        let now = SystemTime::now();
        if let Some(rec) = inner.bundles.get_mut(bundle_id) {
            rec.last_touched_at = now;
            Some(rec.clone())
        } else {
            None
        }
    }

    /// Map `distribution_id → (bundle_id, BundleRecord)` for the
    /// `GetAttachmentDistribution` and `CancelAttachmentDistribution`
    /// handlers' bundle lookups.
    pub fn lookup_distribution(&self, distribution_id: &str) -> Option<(String, BundleRecord)> {
        let inner = self.inner.read().expect("BundleRegistry RwLock poisoned");
        let bundle_id = inner.distribution_index.get(distribution_id)?.clone();
        let record = inner.bundles.get(&bundle_id)?.clone();
        Some((bundle_id, record))
    }

    /// Set a bundle's status and bump `last_touched_at`. No-op if absent.
    pub fn update_status(&self, bundle_id: &str, status: BundleStatus) {
        let mut inner = self.inner.write().expect("BundleRegistry RwLock poisoned");
        if let Some(rec) = inner.bundles.get_mut(bundle_id) {
            rec.status = status;
            rec.last_touched_at = SystemTime::now();
        }
    }

    /// Drop bundles whose terminal status has aged past
    /// `handle_retention_secs`. `0` disables retention entirely.
    pub fn evict_expired(&self) {
        if self.config.handle_retention_secs == 0 {
            return;
        }
        let mut inner = self.inner.write().expect("BundleRegistry RwLock poisoned");
        let cutoff =
            SystemTime::now() - Duration::from_secs(self.config.handle_retention_secs as u64);
        let mut to_drop: Vec<BundleRecord> = Vec::new();
        for rec in inner.bundles.values() {
            if rec.status.is_terminal() && rec.last_touched_at < cutoff {
                to_drop.push(rec.clone());
            }
        }
        for rec in to_drop {
            drop_record(&mut inner, &rec);
        }
    }

    /// Number of resident bundles. For tests and metrics.
    pub fn len(&self) -> usize {
        let inner = self.inner.read().expect("BundleRegistry RwLock poisoned");
        inner.bundles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of resident bundles whose status is non-terminal — the
    /// honest "in flight" count for PRD §Validation Rule 11. Distinct
    /// from `len()` (which includes terminal bundles still within the
    /// retention window). O(N) over a bounded N (default 4096 from
    /// `max_known_bundles`), called only on `SendAttachments`.
    pub fn non_terminal_count(&self) -> usize {
        let inner = self.inner.read().expect("BundleRegistry RwLock poisoned");
        inner
            .bundles
            .values()
            .filter(|r| !r.status.is_terminal())
            .count()
    }
}

fn evict_lru(inner: &mut RegistryInner) {
    // O(N) scan; N is bounded by max_known_bundles (default 4096) so this
    // is fine for the v1 in-memory table. If profiling shows N getting
    // large enough to matter, swap in an ordered LRU index (linked_hash_map
    // or indexmap with insertion-order metadata).
    let oldest = inner
        .bundles
        .values()
        .min_by_key(|r| r.last_touched_at)
        .cloned();
    if let Some(rec) = oldest {
        drop_record(inner, &rec);
    }
}

/// Remove the bundle AND its distribution_index entries.
fn drop_record(inner: &mut RegistryInner, rec: &BundleRecord) {
    inner.bundles.remove(&rec.bundle_id);
    drop_record_indexes_only(inner, rec);
}

/// Remove only the distribution_index entries (used when the bundle entry
/// itself has already been removed via `bundles.remove`).
fn drop_record_indexes_only(inner: &mut RegistryInner, rec: &BundleRecord) {
    for h in &rec.handles {
        inner.distribution_index.remove(h.distribution_id());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fid(rel: &str, size: u64, sha: u8) -> FileIdentity {
        FileIdentity {
            root_name: "outbox".into(),
            relative_path: rel.into(),
            size_bytes: size,
            sha256: [sha; 32],
        }
    }

    fn identity(files: Vec<FileIdentity>) -> BundleIdentity {
        BundleIdentity { files }
    }

    fn record(bundle_id: &str, identity: BundleIdentity, dist_ids: &[&str]) -> BundleRecord {
        use peat_mesh::storage::blob_traits::BlobHash;
        use peat_protocol::storage::file_distribution::{DistributionScope, TransferPriority};
        let handles = dist_ids
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let mut handle = DistributionHandle::new(
                    BlobHash(format!("token-{i}")),
                    DistributionScope::AllNodes,
                    TransferPriority::Normal,
                );
                handle.distribution_id = (*d).to_string();
                AttachmentHandleRecord {
                    file_index: i,
                    blob_token_hash: format!("token-{i}"),
                    distribution_handle: handle,
                    content_type: None,
                    display_name: None,
                }
            })
            .collect();
        BundleRecord::new(bundle_id.to_string(), identity, handles)
    }

    fn cfg(retention: u32, max: u32) -> RegistryConfig {
        RegistryConfig {
            handle_retention_secs: retention,
            max_known_bundles: max,
        }
    }

    /// PRD test 12 — idempotent_resubmit_same_bundle.
    #[test]
    fn idempotent_resubmit_returns_existing_handles() {
        let reg = BundleRegistry::new(cfg(86_400, 16));
        let ident = identity(vec![fid("a.bin", 5, 1)]);
        reg.insert(record("X", ident.clone(), &["dist-a"]));

        match reg.check_resubmit("X", &ident) {
            BundleLookup::Idempotent(rec) => {
                assert_eq!(rec.bundle_id, "X");
                assert_eq!(rec.handles.len(), 1);
                assert_eq!(rec.handles[0].distribution_id(), "dist-a");
            }
            other => panic!("expected Idempotent, got {other:?}"),
        }
    }

    /// PRD test 19 — idempotent_resubmit_ignores_optional_metadata_changes.
    ///
    /// Identity equality covers only root_name, relative_path, size_bytes,
    /// sha256. content_type / display_name on the wire FileSpec are not
    /// part of the registry identity check, so a resubmit that adds them
    /// is treated as identical at the registry layer. The service handler
    /// is responsible for preserving the original metadata when returning
    /// the existing handles.
    #[test]
    fn idempotent_resubmit_ignores_optional_metadata_changes() {
        let reg = BundleRegistry::new(cfg(86_400, 16));
        // The FileIdentity type literally cannot carry content_type /
        // display_name — its existence is the assertion. Construct two
        // identities with the same identity-fields and assert PartialEq.
        let a = identity(vec![fid("a.bin", 11, 2)]);
        let b = identity(vec![fid("a.bin", 11, 2)]);
        assert_eq!(a, b);

        reg.insert(record("X", a, &["dist-x"]));
        match reg.check_resubmit("X", &b) {
            BundleLookup::Idempotent(rec) => {
                // The stored handles are unchanged — original metadata
                // (which would live on the handle record) is preserved.
                assert_eq!(rec.handles[0].distribution_id(), "dist-x");
            }
            other => panic!("expected Idempotent, got {other:?}"),
        }
    }

    /// PRD test 13 — bundle_id_reuse_with_different_files_rejected.
    #[test]
    fn bundle_id_reuse_with_different_files_returns_conflict() {
        let reg = BundleRegistry::new(cfg(86_400, 16));
        let original = identity(vec![fid("a.bin", 5, 1)]);
        let conflicting = identity(vec![fid("a.bin", 99, 1)]); // different size_bytes
        let mut rec = record("X", original, &["dist-a"]);
        rec.status = BundleStatus::Completed;
        reg.insert(rec);

        match reg.check_resubmit("X", &conflicting) {
            BundleLookup::Conflict { created_at } => {
                // created_at is the original's — the existing bundle
                // is preserved, not overwritten.
                let got = reg.get("X").expect("bundle X must remain resident");
                assert_eq!(got.created_at, created_at);
                assert_eq!(got.handles[0].distribution_id(), "dist-a");
            }
            other => panic!("expected Conflict, got {other:?}"),
        }
    }

    /// PRD test 17 — bundle_id_terminal_state_allows_reuse_with_different_files.
    #[test]
    fn failed_or_cancelled_bundle_allows_reuse_and_drops_prior_distribution_ids() {
        for terminal in [BundleStatus::Failed, BundleStatus::Cancelled] {
            let reg = BundleRegistry::new(cfg(86_400, 16));
            let original = identity(vec![fid("a.bin", 5, 1)]);
            let fresh = identity(vec![fid("a.bin", 99, 1)]);
            let mut rec = record("X", original, &["old-dist"]);
            rec.status = terminal;
            reg.insert(rec);

            // The prior distribution_id resolves before resubmit.
            assert!(reg.lookup_distribution("old-dist").is_some());

            // check_resubmit on a terminal-reusable bundle returns
            // NotFound (signalling: fresh ingest path), and clears the
            // distribution_index so the prior IDs are no longer resolvable
            // — PRD test 17 acceptance.
            match reg.check_resubmit("X", &fresh) {
                BundleLookup::NotFound => {}
                other => panic!("expected NotFound for terminal-reuse, got {other:?}"),
            }
            assert!(
                reg.lookup_distribution("old-dist").is_none(),
                "prior distribution_id must not be resolvable after terminal-reuse"
            );

            // The caller now does a fresh insert; lookup_distribution
            // resolves the new dist id, not the old.
            reg.insert(record("X", fresh, &["new-dist"]));
            assert!(reg.lookup_distribution("new-dist").is_some());
            assert!(reg.lookup_distribution("old-dist").is_none());
        }
    }

    /// PRD test 30 — evicted_bundle_id_treated_as_fresh_request.
    #[test]
    fn lru_eviction_treats_evicted_bundle_id_as_fresh() {
        let reg = BundleRegistry::new(cfg(86_400, 1));
        let ident_x = identity(vec![fid("a.bin", 5, 1)]);
        let ident_y = identity(vec![fid("b.bin", 7, 2)]);
        let mut rec_x = record("X", ident_x.clone(), &["dist-x"]);
        rec_x.status = BundleStatus::Completed;
        reg.insert(rec_x);

        // A subsequent insert at capacity evicts X (LRU). Sleep briefly so
        // last_touched_at differs by more than the SystemTime resolution
        // on hosts where it's coarse.
        std::thread::sleep(Duration::from_millis(2));
        let mut rec_y = record("Y", ident_y, &["dist-y"]);
        rec_y.status = BundleStatus::Completed;
        reg.insert(rec_y);

        assert_eq!(reg.len(), 1, "max_known_bundles=1 must enforce capacity");
        assert!(reg.get("X").is_none(), "X must have been LRU-evicted");
        assert!(reg.get("Y").is_some());
        assert!(
            reg.lookup_distribution("dist-x").is_none(),
            "evicted bundle's distribution_ids must be cleared from the index"
        );

        // Resubmit X with a different identity. Because X was evicted,
        // check_resubmit returns NotFound — fresh ingest path.
        let ident_x_fresh = identity(vec![fid("a.bin", 999, 1)]);
        match reg.check_resubmit("X", &ident_x_fresh) {
            BundleLookup::NotFound => {}
            other => panic!("expected NotFound after eviction, got {other:?}"),
        }
    }

    /// Retention sweep drops only terminal bundles whose last_touched_at
    /// is older than the retention window. Non-terminal bundles survive.
    #[test]
    fn evict_expired_drops_only_aged_terminal_bundles() {
        // 1-second retention so the sweep meaningfully triggers.
        let reg = BundleRegistry::new(cfg(1, 16));
        let ident = identity(vec![fid("a.bin", 5, 1)]);
        let mut rec_t = record("terminal-old", ident.clone(), &["dist-t"]);
        rec_t.status = BundleStatus::Completed;
        // Forge an older last_touched_at without sleeping.
        rec_t.last_touched_at = SystemTime::now() - Duration::from_secs(10);
        reg.insert(rec_t);

        let mut rec_p = record("pending-old", ident.clone(), &["dist-p"]);
        rec_p.status = BundleStatus::Pending;
        rec_p.last_touched_at = SystemTime::now() - Duration::from_secs(10);
        reg.insert(rec_p);

        let mut rec_fresh = record("terminal-fresh", ident, &["dist-f"]);
        rec_fresh.status = BundleStatus::Completed;
        reg.insert(rec_fresh);

        reg.evict_expired();
        assert!(
            reg.get("terminal-old").is_none(),
            "aged terminal must evict"
        );
        assert!(
            reg.get("pending-old").is_some(),
            "non-terminal must survive retention sweep regardless of age"
        );
        assert!(
            reg.get("terminal-fresh").is_some(),
            "fresh terminal within retention window must survive"
        );
    }

    /// Retention disabled (`handle_retention_secs = 0`) means `evict_expired`
    /// is a no-op. LRU eviction is still in force.
    #[test]
    fn retention_zero_disables_evict_expired() {
        let reg = BundleRegistry::new(cfg(0, 16));
        let ident = identity(vec![fid("a.bin", 5, 1)]);
        let mut rec = record("X", ident, &["dist-x"]);
        rec.status = BundleStatus::Completed;
        rec.last_touched_at = SystemTime::now() - Duration::from_secs(86_400 * 365);
        reg.insert(rec);
        reg.evict_expired();
        assert!(reg.get("X").is_some(), "retention=0 must disable expiry");
    }

    #[test]
    fn update_status_bumps_last_touched_and_changes_status() {
        let reg = BundleRegistry::new(cfg(86_400, 16));
        let ident = identity(vec![fid("a.bin", 5, 1)]);
        reg.insert(record("X", ident, &["dist-x"]));
        let before = reg.get("X").unwrap();
        std::thread::sleep(Duration::from_millis(2));
        reg.update_status("X", BundleStatus::Completed);
        let after = reg.get("X").unwrap();
        assert_eq!(after.status, BundleStatus::Completed);
        assert!(after.last_touched_at > before.last_touched_at);
    }

    #[test]
    fn check_resubmit_unknown_id_returns_not_found() {
        let reg = BundleRegistry::new(cfg(86_400, 16));
        let ident = identity(vec![fid("a.bin", 5, 1)]);
        match reg.check_resubmit("ghost", &ident) {
            BundleLookup::NotFound => {}
            other => panic!("expected NotFound for unknown, got {other:?}"),
        }
    }
}
