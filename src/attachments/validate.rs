//! Validation for `SendAttachmentsRequest` (PRD-006 §Validation Rules).
//!
//! [`validate_request`] runs the rules that can be checked without ingesting
//! file bytes: bundle size/count caps (rules 1, 2), root allowlist (rule 3),
//! relative-path safety (rule 4), descendant resolution (rule 5 — except the
//! `O_NOFOLLOW` open, which is rule 5's TOCTOU mitigation that lives with
//! ingest in `attachments::ingest`), file metadata (rule 6), on-disk size
//! match (rule 7), per-file size cap (rule 8), sha256 *length* check (rule 9
//! — streaming hash match also belongs to ingest), and scope sanity
//! (rule 10).
//!
//! Out of scope for this module — handled where the prerequisite state lives:
//!   * Rule 11 (concurrency cap) — needs the in-flight registry; lives in
//!     the service handler.
//!   * Rule 12 (bundle-ID idempotency / conflict) — needs the bundle
//!     registry's handle table.
//!   * Rule 5 `O_NOFOLLOW` open + rule 9 streaming hash — live in `ingest`,
//!     where the file is opened and streamed.
//!
//! Validation MUST pass for every file in the bundle before any blob is
//! created. The request fails atomically — partial ingestion is not allowed.
//! [`ValidatedBundle`] carries the resolved absolute paths so ingest does
//! not re-resolve.

// `connectrpc::ConnectError` is 248 bytes — over clippy's default
// `result_large_err` threshold. Boxing it would just push friction onto the
// service layer (which has to return `Result<_, ConnectError>` against the
// generated trait); the existing RPC handlers already accept this shape.
#![allow(clippy::result_large_err)]

use std::path::{Component, Path, PathBuf};

use connectrpc::ConnectError;

use crate::attachments::config::AttachmentConfig;
use crate::pb;

/// A request that has passed every non-ingest validation rule. Carries the
/// resolved absolute paths (descendant-of-root verified) so ingest does not
/// re-resolve.
#[derive(Debug)]
pub struct ValidatedBundle {
    pub files: Vec<ValidatedFile>,
    pub scope: ValidatedScope,
    /// Caller-supplied bundle ID, if any. Idempotency is enforced by the
    /// registry, not here.
    pub bundle_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ValidatedFile {
    /// Index into the original `SendAttachmentsRequest::files` — surfaced on
    /// the per-file `AttachmentHandle` so the caller can correlate.
    pub file_index: usize,
    /// Canonicalised, descendant-of-root verified absolute path. Re-opened
    /// with `O_NOFOLLOW` at ingest time to defeat post-canonicalisation
    /// symlink swaps (PRD §Validation Rule 5 TOCTOU mitigation).
    pub absolute_path: PathBuf,
    pub root_name: String,
    pub relative_path: String,
    pub size_bytes: u64,
    /// Exactly 32 raw bytes — the wire field's `bytes` length was already
    /// length-checked at rule 9.
    pub sha256: [u8; 32],
    pub content_type: Option<String>,
    pub display_name: Option<String>,
}

/// Scope variants the v1 sender side actually accepts. `Formation` and
/// `Capable` are rejected at validation time (FAILED_PRECONDITION) so they
/// never reach this enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedScope {
    AllNodes,
    NodeList(Vec<String>),
}

/// Run all non-ingest validation rules. Returns `Err` on the first rule
/// that fails — the request is atomic so there is no partial success.
pub fn validate_request(
    req: &pb::SendAttachmentsRequest,
    cfg: &AttachmentConfig,
) -> Result<ValidatedBundle, ConnectError> {
    // Rule 1: bundle file count.
    if req.files.is_empty() {
        return Err(ConnectError::invalid_argument(
            "files: bundle must contain at least one file",
        ));
    }
    if req.files.len() > cfg.max_files_per_bundle as usize {
        return Err(ConnectError::resource_exhausted(format!(
            "files: bundle has {} entries, exceeds max_files_per_bundle={}",
            req.files.len(),
            cfg.max_files_per_bundle
        )));
    }

    // Rule 2: bundle total bytes. Sum first; catch overflow as resource_exhausted
    // rather than silently wrapping.
    let total: u64 = req
        .files
        .iter()
        .try_fold(0u64, |acc, f| acc.checked_add(f.size_bytes))
        .ok_or_else(|| {
            ConnectError::resource_exhausted("files: aggregate size_bytes overflows u64")
        })?;
    if total > cfg.max_bundle_bytes {
        return Err(ConnectError::resource_exhausted(format!(
            "files: aggregate size {} exceeds max_bundle_bytes={}",
            total, cfg.max_bundle_bytes
        )));
    }

    // Rule 10: scope sanity. Run before per-file checks so a caller bug
    // that drops `scope` fails fast — and so an explicit Capable / Formation
    // rejection surfaces FAILED_PRECONDITION without first incurring N file
    // stats.
    let scope = validate_scope(req.scope.as_option(), cfg)?;

    // Rules 3-9 (length only) per file.
    let mut files = Vec::with_capacity(req.files.len());
    for (idx, raw) in req.files.iter().enumerate() {
        files.push(validate_file(idx, raw, cfg)?);
    }

    Ok(ValidatedBundle {
        files,
        scope,
        bundle_id: req.bundle_id.clone(),
    })
}

fn validate_scope(
    scope: Option<&pb::DistributionScopeSpec>,
    cfg: &AttachmentConfig,
) -> Result<ValidatedScope, ConnectError> {
    // PRD Rule 10: unset scope (either the field omitted entirely or a
    // default-constructed DistributionScopeSpec with no oneof variant set)
    // rejects with INVALID_ARGUMENT — no silent fallback to AllNodes.
    let spec = scope.ok_or_else(|| {
        ConnectError::invalid_argument(
            "scope: required (no silent fallback to AllNodes — a caller bug \
             that drops scope must not fan a bundle out to every reachable peer)",
        )
    })?;
    let inner = spec.scope.as_ref().ok_or_else(|| {
        ConnectError::invalid_argument(
            "scope: oneof variant required — got DistributionScopeSpec with unset variant",
        )
    })?;

    use crate::pb::distribution_scope_spec::Scope as S;
    match inner {
        S::AllNodes(_) => Ok(ValidatedScope::AllNodes),
        S::NodeList(nl) => {
            if nl.node_ids.is_empty() {
                return Err(ConnectError::invalid_argument(
                    "scope.node_list.node_ids: must contain at least one entry",
                ));
            }
            if nl.node_ids.len() > cfg.max_node_list_len as usize {
                return Err(ConnectError::resource_exhausted(format!(
                    "scope.node_list.node_ids: {} entries exceeds max_node_list_len={}",
                    nl.node_ids.len(),
                    cfg.max_node_list_len
                )));
            }
            Ok(ValidatedScope::NodeList(nl.node_ids.clone()))
        }
        S::Formation(_) => Err(ConnectError::failed_precondition(
            "scope.formation: formation resolution is not implemented in v1 — \
             back FormationScope with a live formation-membership lookup before re-enabling",
        )),
        S::Capable(_) => Err(ConnectError::failed_precondition(
            "scope.capable: capability vocabulary is deferred to a follow-on ADR — \
             CapableScope is a reserved-but-rejected v1 variant",
        )),
    }
}

fn validate_file(
    index: usize,
    f: &pb::FileSpec,
    cfg: &AttachmentConfig,
) -> Result<ValidatedFile, ConnectError> {
    let field_path = |suffix: &str| format!("files[{index}].{suffix}");

    // Rule 9 (length only): sha256 must be exactly 32 raw bytes. Done before
    // any file is opened — a hex-encoded sha256 (64 ASCII bytes) is a common
    // bug and must reject without I/O.
    if f.sha256.len() != 32 {
        return Err(ConnectError::invalid_argument(format!(
            "{}: sha256 must be exactly 32 raw bytes (NOT hex-encoded), got {} bytes",
            field_path("sha256"),
            f.sha256.len()
        )));
    }
    let mut sha256 = [0u8; 32];
    sha256.copy_from_slice(&f.sha256);

    // Rule 3: root_name in allowlist.
    let root = cfg.lookup_root(&f.root_name).ok_or_else(|| {
        ConnectError::invalid_argument(format!(
            "{}: unknown root `{}` — not in --attachment-root allowlist",
            field_path("root_name"),
            f.root_name
        ))
    })?;

    // Rule 4: relative_path safety. Reject empty, absolute, and `..` /
    // root-component traversal as a string check *before* canonicalisation.
    // The canonicalisation step (rule 5) re-checks descendant-of-root, but
    // catching the obvious cases here keeps the error close to the bug.
    if f.relative_path.is_empty() {
        return Err(ConnectError::invalid_argument(format!(
            "{}: must not be empty",
            field_path("relative_path")
        )));
    }
    if f.relative_path.starts_with('/') {
        return Err(ConnectError::invalid_argument(format!(
            "{}: must not start with `/` (paths are relative to the root)",
            field_path("relative_path")
        )));
    }
    let rel = Path::new(&f.relative_path);
    for comp in rel.components() {
        match comp {
            Component::ParentDir => {
                return Err(ConnectError::invalid_argument(format!(
                    "{}: must not contain `..` path component",
                    field_path("relative_path")
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(ConnectError::invalid_argument(format!(
                    "{}: must be a plain relative path inside the root",
                    field_path("relative_path")
                )));
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }

    // Rule 5 (descendant check): canonicalise root.join(rel) and verify the
    // result is still a descendant of `root`. This catches in-root symlinks
    // that point outside the root — std::fs::canonicalize resolves every
    // symlink in the path.
    let joined = root.join(rel);
    let canonical = std::fs::canonicalize(&joined).map_err(|e| {
        ConnectError::invalid_argument(format!(
            "{}: cannot resolve `{}`: {}",
            field_path("relative_path"),
            joined.display(),
            e
        ))
    })?;
    if !canonical.starts_with(root) {
        return Err(ConnectError::invalid_argument(format!(
            "{}: resolved path `{}` escapes root `{}` (symlink escape)",
            field_path("relative_path"),
            canonical.display(),
            root.display()
        )));
    }

    // Rule 6: regular file, exists, readable. After canonicalisation,
    // `metadata` is the target's metadata — symlinks-to-non-regular files
    // are caught here.
    let metadata = std::fs::metadata(&canonical).map_err(|e| {
        ConnectError::invalid_argument(format!(
            "{}: stat failed for `{}`: {}",
            field_path("relative_path"),
            canonical.display(),
            e
        ))
    })?;
    if !metadata.is_file() {
        return Err(ConnectError::invalid_argument(format!(
            "{}: `{}` is not a regular file",
            field_path("relative_path"),
            canonical.display()
        )));
    }

    // Rule 7: on-disk size must match exactly.
    if metadata.len() != f.size_bytes {
        return Err(ConnectError::invalid_argument(format!(
            "{}: on-disk size {} ≠ declared size_bytes {}",
            field_path("size_bytes"),
            metadata.len(),
            f.size_bytes
        )));
    }

    // Rule 8: per-file size cap.
    if f.size_bytes > cfg.max_file_bytes {
        return Err(ConnectError::resource_exhausted(format!(
            "{}: {} bytes exceeds max_file_bytes={}",
            field_path("size_bytes"),
            f.size_bytes,
            cfg.max_file_bytes
        )));
    }

    Ok(ValidatedFile {
        file_index: index,
        absolute_path: canonical,
        root_name: f.root_name.clone(),
        relative_path: f.relative_path.clone(),
        size_bytes: f.size_bytes,
        sha256,
        content_type: f.content_type.clone(),
        display_name: f.display_name.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attachments::config::{
        AttachmentConfig, AttachmentPriorityCli, DEFAULT_HANDLE_RETENTION_SECS,
        DEFAULT_MAX_BUNDLE_BYTES, DEFAULT_MAX_CONCURRENT_DISTRIBUTIONS,
        DEFAULT_MAX_FILES_PER_BUNDLE, DEFAULT_MAX_FILE_BYTES, DEFAULT_MAX_KNOWN_BUNDLES,
        DEFAULT_MAX_NODE_LIST_LEN, DEFAULT_QUEUE_WHEN_FULL,
    };
    use crate::pb;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn cfg_with(roots: HashMap<String, PathBuf>) -> AttachmentConfig {
        AttachmentConfig {
            roots,
            inbox_path: None,
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_bundle_bytes: DEFAULT_MAX_BUNDLE_BYTES,
            max_files_per_bundle: DEFAULT_MAX_FILES_PER_BUNDLE,
            max_node_list_len: DEFAULT_MAX_NODE_LIST_LEN,
            max_concurrent_distributions: DEFAULT_MAX_CONCURRENT_DISTRIBUTIONS,
            queue_when_full: DEFAULT_QUEUE_WHEN_FULL,
            default_priority: AttachmentPriorityCli::Routine,
            discovery_grace_secs: 0,
            handle_retention_secs: DEFAULT_HANDLE_RETENTION_SECS,
            max_known_bundles: DEFAULT_MAX_KNOWN_BUNDLES,
            inbox_poll_secs: crate::attachments::config::DEFAULT_INBOX_POLL_SECS,
            outbox_watch: false,
            outbox_poll_secs: crate::attachments::config::DEFAULT_OUTBOX_POLL_SECS,
        }
    }

    fn one_root(name: &str) -> (TempDir, AttachmentConfig) {
        let dir = TempDir::new().unwrap();
        let canonical = std::fs::canonicalize(dir.path()).unwrap();
        let mut roots = HashMap::new();
        roots.insert(name.to_string(), canonical);
        (dir, cfg_with(roots))
    }

    fn write_file(root: &Path, rel: &str, bytes: &[u8]) -> [u8; 32] {
        let path = root.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, bytes).unwrap();
        sha256_of(bytes)
    }

    fn sha256_of(bytes: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(bytes);
        let out = h.finalize();
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&out);
        arr
    }

    fn file_spec(
        root_name: &str,
        relative_path: &str,
        size_bytes: u64,
        sha256: Vec<u8>,
    ) -> pb::FileSpec {
        pb::FileSpec {
            root_name: root_name.to_string(),
            relative_path: relative_path.to_string(),
            size_bytes,
            sha256,
            content_type: None,
            display_name: None,
            ..Default::default()
        }
    }

    fn scope_all_nodes() -> buffa::MessageField<pb::DistributionScopeSpec> {
        buffa::MessageField::some(pb::DistributionScopeSpec {
            scope: Some(pb::distribution_scope_spec::Scope::AllNodes(Box::default())),
            ..Default::default()
        })
    }

    fn scope_capable() -> buffa::MessageField<pb::DistributionScopeSpec> {
        buffa::MessageField::some(pb::DistributionScopeSpec {
            scope: Some(pb::distribution_scope_spec::Scope::Capable(Box::default())),
            ..Default::default()
        })
    }

    fn req_with(
        files: Vec<pb::FileSpec>,
        scope: buffa::MessageField<pb::DistributionScopeSpec>,
    ) -> pb::SendAttachmentsRequest {
        pb::SendAttachmentsRequest {
            files,
            scope,
            ..Default::default()
        }
    }

    fn assert_invalid_argument(err: &ConnectError, expected_path_fragment: &str) {
        // ConnectError exposes its message via `Display`. Field paths are
        // embedded in the message text (the wire ErrorDetail carries the
        // structured form; this assertion is a tight enough proxy for the
        // PRD acceptance criterion).
        let msg = err.to_string();
        assert_eq!(
            err.code,
            connectrpc::ErrorCode::InvalidArgument,
            "expected InvalidArgument, got `{msg}`"
        );
        assert!(
            msg.contains(expected_path_fragment),
            "expected field path `{expected_path_fragment}` in error message: {msg}"
        );
    }

    fn assert_code(err: &ConnectError, expected: connectrpc::ErrorCode) {
        assert_eq!(err.code, expected, "expected {expected:?}, got `{}`", err);
    }

    #[test]
    fn validate_rejects_unknown_root() {
        let (root, mut cfg) = one_root("outbox");
        // Drop the only root so lookup fails.
        cfg.roots.clear();
        let hash = write_file(root.path(), "a.bin", b"hello");
        let req = req_with(
            vec![file_spec("outbox", "a.bin", 5, hash.to_vec())],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files[0].root_name");
    }

    #[test]
    fn validate_rejects_absolute_relative_path() {
        let (_root, cfg) = one_root("outbox");
        let req = req_with(
            vec![file_spec("outbox", "/etc/passwd", 1, vec![0u8; 32])],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files[0].relative_path");
    }

    #[test]
    fn validate_rejects_parent_traversal() {
        let (_root, cfg) = one_root("outbox");
        let req = req_with(
            vec![file_spec("outbox", "../escape.bin", 1, vec![0u8; 32])],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files[0].relative_path");
    }

    #[cfg(unix)]
    #[test]
    fn validate_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;
        let (root, cfg) = one_root("outbox");
        let outside = TempDir::new().unwrap();
        let outside_file = outside.path().join("secret");
        std::fs::write(&outside_file, b"secret").unwrap();
        // In-root symlink pointing outside the root.
        symlink(&outside_file, root.path().join("link.bin")).unwrap();
        let req = req_with(
            vec![file_spec("outbox", "link.bin", 6, vec![0u8; 32])],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files[0].relative_path");
    }

    #[test]
    fn validate_rejects_size_mismatch() {
        let (root, cfg) = one_root("outbox");
        let hash = write_file(root.path(), "a.bin", b"hello"); // 5 bytes on disk
        let req = req_with(
            vec![file_spec("outbox", "a.bin", 99, hash.to_vec())], // declared 99
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files[0].size_bytes");
    }

    #[test]
    fn validate_rejects_size_cap() {
        let (root, mut cfg) = one_root("outbox");
        cfg.max_file_bytes = 4;
        let hash = write_file(root.path(), "a.bin", b"hello"); // 5 bytes
        let req = req_with(
            vec![file_spec("outbox", "a.bin", 5, hash.to_vec())],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_code(&err, connectrpc::ErrorCode::ResourceExhausted);
    }

    #[test]
    fn validate_rejects_bundle_cap() {
        let (root, mut cfg) = one_root("outbox");
        cfg.max_bundle_bytes = 6; // Two 5-byte files = 10, over cap.
        let h_a = write_file(root.path(), "a.bin", b"hello");
        let h_b = write_file(root.path(), "b.bin", b"world");
        let req = req_with(
            vec![
                file_spec("outbox", "a.bin", 5, h_a.to_vec()),
                file_spec("outbox", "b.bin", 5, h_b.to_vec()),
            ],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_code(&err, connectrpc::ErrorCode::ResourceExhausted);
        assert!(err.to_string().contains("max_bundle_bytes"));
    }

    #[test]
    fn validate_rejects_too_many_files() {
        let (root, mut cfg) = one_root("outbox");
        cfg.max_files_per_bundle = 1;
        let h_a = write_file(root.path(), "a.bin", b"hello");
        let h_b = write_file(root.path(), "b.bin", b"world");
        let req = req_with(
            vec![
                file_spec("outbox", "a.bin", 5, h_a.to_vec()),
                file_spec("outbox", "b.bin", 5, h_b.to_vec()),
            ],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_code(&err, connectrpc::ErrorCode::ResourceExhausted);
        assert!(err.to_string().contains("max_files_per_bundle"));
    }

    #[test]
    fn validate_rejects_capable_scope_v1() {
        let (root, cfg) = one_root("outbox");
        let hash = write_file(root.path(), "a.bin", b"hello");
        let req = req_with(
            vec![file_spec("outbox", "a.bin", 5, hash.to_vec())],
            scope_capable(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_code(&err, connectrpc::ErrorCode::FailedPrecondition);
        assert!(err.to_string().contains("scope.capable"));
    }

    #[test]
    fn validate_rejects_wrong_length_sha256() {
        let (root, cfg) = one_root("outbox");
        let _ = write_file(root.path(), "a.bin", b"hello");
        // 16 bytes: short.
        let req = req_with(
            vec![file_spec("outbox", "a.bin", 5, vec![0u8; 16])],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files[0].sha256");

        // 64 bytes: a hex-encoded sha256 is 64 ASCII bytes. The PRD calls
        // this out as a particularly common bug.
        let req = req_with(
            vec![file_spec("outbox", "a.bin", 5, vec![0u8; 64])],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files[0].sha256");
    }

    #[test]
    fn validate_rejects_unset_scope_field() {
        let (root, cfg) = one_root("outbox");
        let hash = write_file(root.path(), "a.bin", b"hello");
        // scope MessageField unset entirely (omitted).
        let req = req_with(
            vec![file_spec("outbox", "a.bin", 5, hash.to_vec())],
            buffa::MessageField::none(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "scope");
    }

    #[test]
    fn validate_rejects_unset_scope_oneof() {
        let (root, cfg) = one_root("outbox");
        let hash = write_file(root.path(), "a.bin", b"hello");
        // scope present but oneof variant unset.
        let req = req_with(
            vec![file_spec("outbox", "a.bin", 5, hash.to_vec())],
            buffa::MessageField::some(pb::DistributionScopeSpec::default()),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "scope");
    }

    #[test]
    fn validate_accepts_well_formed_request() {
        let (root, cfg) = one_root("outbox");
        let bytes = b"hello world";
        let hash = write_file(root.path(), "sub/a.bin", bytes);
        let req = req_with(
            vec![file_spec(
                "outbox",
                "sub/a.bin",
                bytes.len() as u64,
                hash.to_vec(),
            )],
            scope_all_nodes(),
        );
        let validated = validate_request(&req, &cfg).expect("validation should pass");
        assert_eq!(validated.files.len(), 1);
        assert_eq!(validated.files[0].size_bytes, bytes.len() as u64);
        assert_eq!(validated.files[0].sha256, hash);
        assert_eq!(validated.scope, ValidatedScope::AllNodes);
        // absolute_path is canonical and descendant of the canonical root.
        let canonical_root = std::fs::canonicalize(root.path()).unwrap();
        assert!(validated.files[0]
            .absolute_path
            .starts_with(&canonical_root));
    }

    #[test]
    fn validate_rejects_empty_files() {
        let (_root, cfg) = one_root("outbox");
        let req = req_with(vec![], scope_all_nodes());
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files");
    }

    #[test]
    fn validate_rejects_directory_as_file() {
        let (root, cfg) = one_root("outbox");
        let sub = root.path().join("subdir");
        std::fs::create_dir_all(&sub).unwrap();
        // Target a directory rather than a regular file.
        let req = req_with(
            vec![file_spec("outbox", "subdir", 0, vec![0u8; 32])],
            scope_all_nodes(),
        );
        let err = validate_request(&req, &cfg).unwrap_err();
        assert_invalid_argument(&err, "files[0].relative_path");
        assert!(err.to_string().contains("not a regular file"));
    }
}
