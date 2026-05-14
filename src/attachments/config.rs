//! Operator-facing configuration for the attachment surface (PRD-006).
//!
//! All knobs are exposed as `--attachment-*` CLI flags / `PEAT_NODE_ATTACHMENT_*`
//! env vars on `peat-node`. [`AttachmentConfig::from_raw`] is the construction
//! seam: it takes the raw clap-parsed strings and produces a validated config
//! with canonicalised root paths.
//!
//! Safety default: with no `--attachment-root` configured, [`has_roots`]
//! returns false and all four attachment RPCs return `Unimplemented` at the
//! service layer. This keeps "RPC exposed but unsafe" impossible by default —
//! operators must consciously opt in by naming the roots that may be read.
//!
//! [`has_roots`]: AttachmentConfig::has_roots

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use thiserror::Error;

/// PRD §Configuration defaults — kept as named constants so the values appear
/// in one place and the field initialisers stay readable.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 256 * 1024 * 1024; // 256 MiB
pub const DEFAULT_MAX_BUNDLE_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB
pub const DEFAULT_MAX_FILES_PER_BUNDLE: u32 = 64;
pub const DEFAULT_MAX_NODE_LIST_LEN: u32 = 256;
pub const DEFAULT_MAX_CONCURRENT_DISTRIBUTIONS: u32 = 4;
pub const DEFAULT_QUEUE_WHEN_FULL: bool = false;
pub const DEFAULT_DISCOVERY_GRACE_SECS: u32 = 30;
pub const DEFAULT_HANDLE_RETENTION_SECS: u32 = 86_400; // 24h
pub const DEFAULT_MAX_KNOWN_BUNDLES: u32 = 4096;

/// CLI-facing priority enum. Maps 1:1 onto `peat_protocol::storage::file_distribution::TransferPriority`
/// at the service layer (Step 7). Kept local so the proto / CLI surface does
/// not leak the peat-protocol type into operator-facing config.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum AttachmentPriorityCli {
    Bulk,
    Low,
    Routine,
    Priority,
    Critical,
}

impl AttachmentPriorityCli {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bulk => "bulk",
            Self::Low => "low",
            Self::Routine => "routine",
            Self::Priority => "priority",
            Self::Critical => "critical",
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("--attachment-root entry `{0}` missing `=` separator (expected `name=path`)")]
    RootMissingSeparator(String),

    #[error("--attachment-root name must be non-empty and match [A-Za-z0-9_-]+ — got `{0}`")]
    RootInvalidName(String),

    #[error("--attachment-root duplicate name `{0}`")]
    RootDuplicateName(String),

    #[error("--attachment-root `{name}` path `{path}`: {source}")]
    RootCanonicalise {
        name: String,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("--attachment-root `{name}` resolved to `{path}` which is not a directory")]
    RootNotDirectory { name: String, path: PathBuf },
}

/// Parsed, canonicalised attachment configuration.
///
/// Build via [`AttachmentConfig::from_raw`] in `main.rs` after `Args::parse`.
/// Once constructed, root paths are absolute and verified to be readable
/// directories at startup time — validators (Step 3) can rely on the
/// `roots[name]` lookup never pointing at a non-existent or non-directory
/// path *as of startup* (TOCTOU mitigation against post-startup root
/// removal is the validator's job per PRD §Validation Rule 5).
#[derive(Clone, Debug)]
pub struct AttachmentConfig {
    /// `name → canonicalised absolute root path`. Empty map → RPC disabled.
    pub roots: HashMap<String, PathBuf>,
    pub max_file_bytes: u64,
    pub max_bundle_bytes: u64,
    pub max_files_per_bundle: u32,
    pub max_node_list_len: u32,
    pub max_concurrent_distributions: u32,
    pub queue_when_full: bool,
    pub default_priority: AttachmentPriorityCli,
    pub discovery_grace_secs: u32,
    pub handle_retention_secs: u32,
    pub max_known_bundles: u32,
}

impl Default for AttachmentConfig {
    /// Defaults match PRD §Configuration. Empty `roots` → RPC disabled.
    fn default() -> Self {
        Self {
            roots: HashMap::new(),
            max_file_bytes: DEFAULT_MAX_FILE_BYTES,
            max_bundle_bytes: DEFAULT_MAX_BUNDLE_BYTES,
            max_files_per_bundle: DEFAULT_MAX_FILES_PER_BUNDLE,
            max_node_list_len: DEFAULT_MAX_NODE_LIST_LEN,
            max_concurrent_distributions: DEFAULT_MAX_CONCURRENT_DISTRIBUTIONS,
            queue_when_full: DEFAULT_QUEUE_WHEN_FULL,
            default_priority: AttachmentPriorityCli::Routine,
            discovery_grace_secs: DEFAULT_DISCOVERY_GRACE_SECS,
            handle_retention_secs: DEFAULT_HANDLE_RETENTION_SECS,
            max_known_bundles: DEFAULT_MAX_KNOWN_BUNDLES,
        }
    }
}

#[allow(clippy::too_many_arguments)]
impl AttachmentConfig {
    /// Build from raw clap-parsed values. Canonicalises every root path and
    /// fails fast on bad inputs (missing dir, duplicate name, bad name format).
    pub fn from_raw(
        raw_roots: &[String],
        max_file_bytes: u64,
        max_bundle_bytes: u64,
        max_files_per_bundle: u32,
        max_node_list_len: u32,
        max_concurrent_distributions: u32,
        queue_when_full: bool,
        default_priority: AttachmentPriorityCli,
        discovery_grace_secs: u32,
        handle_retention_secs: u32,
        max_known_bundles: u32,
    ) -> Result<Self, ConfigError> {
        let mut roots = HashMap::new();
        for spec in raw_roots {
            // clap's `value_delimiter = ','` plus repeated `--attachment-root`
            // both feed into Vec<String>. An empty entry (e.g. `--attachment-root=`)
            // is silently dropped — operators rarely intend an empty token.
            if spec.is_empty() {
                continue;
            }
            let (name, path) = parse_root_spec(spec)?;
            if roots.contains_key(&name) {
                return Err(ConfigError::RootDuplicateName(name));
            }
            roots.insert(name, path);
        }

        Ok(Self {
            roots,
            max_file_bytes,
            max_bundle_bytes,
            max_files_per_bundle,
            max_node_list_len,
            max_concurrent_distributions,
            queue_when_full,
            default_priority,
            discovery_grace_secs,
            handle_retention_secs,
            max_known_bundles,
        })
    }

    /// True iff at least one `--attachment-root` was configured. The four
    /// attachment RPCs short-circuit to `Unimplemented` when this is false.
    pub fn has_roots(&self) -> bool {
        !self.roots.is_empty()
    }

    /// Lookup the canonicalised root path by name. Returns `None` for any
    /// name not in the allowlist — validators surface this as
    /// `INVALID_ARGUMENT` per PRD §Validation Rule 3.
    pub fn lookup_root(&self, name: &str) -> Option<&Path> {
        self.roots.get(name).map(PathBuf::as_path)
    }
}

/// Parse a single `name=path` spec, canonicalise the path, and verify it
/// resolves to an existing directory.
fn parse_root_spec(spec: &str) -> Result<(String, PathBuf), ConfigError> {
    let (name, raw_path) = spec
        .split_once('=')
        .ok_or_else(|| ConfigError::RootMissingSeparator(spec.to_string()))?;

    if !is_valid_root_name(name) {
        return Err(ConfigError::RootInvalidName(name.to_string()));
    }

    let raw_path = PathBuf::from(raw_path);
    let canonical =
        std::fs::canonicalize(&raw_path).map_err(|source| ConfigError::RootCanonicalise {
            name: name.to_string(),
            path: raw_path.clone(),
            source,
        })?;

    if !canonical.is_dir() {
        return Err(ConfigError::RootNotDirectory {
            name: name.to_string(),
            path: canonical,
        });
    }

    Ok((name.to_string(), canonical))
}

fn is_valid_root_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn cfg_with_roots(specs: &[&str]) -> Result<AttachmentConfig, ConfigError> {
        let raw: Vec<String> = specs.iter().map(|s| s.to_string()).collect();
        AttachmentConfig::from_raw(
            &raw,
            DEFAULT_MAX_FILE_BYTES,
            DEFAULT_MAX_BUNDLE_BYTES,
            DEFAULT_MAX_FILES_PER_BUNDLE,
            DEFAULT_MAX_NODE_LIST_LEN,
            DEFAULT_MAX_CONCURRENT_DISTRIBUTIONS,
            DEFAULT_QUEUE_WHEN_FULL,
            AttachmentPriorityCli::Routine,
            DEFAULT_DISCOVERY_GRACE_SECS,
            DEFAULT_HANDLE_RETENTION_SECS,
            DEFAULT_MAX_KNOWN_BUNDLES,
        )
    }

    #[test]
    fn default_has_no_roots_so_rpc_is_disabled() {
        let cfg = AttachmentConfig::default();
        assert!(!cfg.has_roots());
        assert!(cfg.lookup_root("outbox").is_none());
    }

    #[test]
    fn from_raw_empty_input_disables_rpc() {
        let cfg = cfg_with_roots(&[]).unwrap();
        assert!(!cfg.has_roots());
    }

    #[test]
    fn from_raw_canonicalises_root_path() {
        let dir = TempDir::new().unwrap();
        let spec = format!("outbox={}", dir.path().display());
        let cfg = cfg_with_roots(&[&spec]).unwrap();
        assert!(cfg.has_roots());
        let resolved = cfg.lookup_root("outbox").unwrap();
        assert!(resolved.is_absolute());
        assert_eq!(
            resolved,
            std::fs::canonicalize(dir.path()).unwrap().as_path()
        );
    }

    #[test]
    fn from_raw_rejects_missing_separator() {
        let dir = TempDir::new().unwrap();
        // No `=`
        let spec = format!("{}", dir.path().display());
        let err = cfg_with_roots(&[&spec]).unwrap_err();
        assert!(matches!(err, ConfigError::RootMissingSeparator(_)));
    }

    #[test]
    fn from_raw_rejects_invalid_name() {
        let dir = TempDir::new().unwrap();
        let spec = format!("out box={}", dir.path().display()); // space
        let err = cfg_with_roots(&[&spec]).unwrap_err();
        assert!(matches!(err, ConfigError::RootInvalidName(_)));
    }

    #[test]
    fn from_raw_rejects_empty_name() {
        let dir = TempDir::new().unwrap();
        let spec = format!("={}", dir.path().display());
        let err = cfg_with_roots(&[&spec]).unwrap_err();
        assert!(matches!(err, ConfigError::RootInvalidName(_)));
    }

    #[test]
    fn from_raw_rejects_nonexistent_path() {
        let spec = "outbox=/definitely/does/not/exist/peat-node-attachment-cfg-test";
        let err = cfg_with_roots(&[spec]).unwrap_err();
        assert!(matches!(err, ConfigError::RootCanonicalise { .. }));
    }

    #[test]
    fn from_raw_rejects_path_that_is_not_a_directory() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("a-file");
        std::fs::write(&file_path, b"not a dir").unwrap();
        let spec = format!("outbox={}", file_path.display());
        let err = cfg_with_roots(&[&spec]).unwrap_err();
        assert!(matches!(err, ConfigError::RootNotDirectory { .. }));
    }

    #[test]
    fn from_raw_rejects_duplicate_name() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        let err = cfg_with_roots(&[
            &format!("outbox={}", a.path().display()),
            &format!("outbox={}", b.path().display()),
        ])
        .unwrap_err();
        assert!(matches!(err, ConfigError::RootDuplicateName(_)));
    }

    #[test]
    fn from_raw_accepts_multiple_distinct_roots() {
        let a = TempDir::new().unwrap();
        let b = TempDir::new().unwrap();
        let cfg = cfg_with_roots(&[
            &format!("outbox={}", a.path().display()),
            &format!("media={}", b.path().display()),
        ])
        .unwrap();
        assert!(cfg.lookup_root("outbox").is_some());
        assert!(cfg.lookup_root("media").is_some());
        assert!(cfg.lookup_root("missing").is_none());
    }

    #[test]
    fn from_raw_drops_empty_spec_entries() {
        // clap's value_delimiter on `a,,b` produces an empty middle token —
        // tolerate without failing the whole config.
        let a = TempDir::new().unwrap();
        let cfg = cfg_with_roots(&["", &format!("outbox={}", a.path().display()), ""]).unwrap();
        assert!(cfg.has_roots());
    }
}
