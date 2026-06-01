//! Credential loading for `peat`.
//!
//! ADR-001 §Credentials specifies a YAML credential bundle resolved in this
//! order:
//!   1. `--creds <PATH>` argument
//!   2. `PEAT_CREDS` environment variable (path to the YAML file)
//!   3. `$XDG_CONFIG_HOME/peat/credentials.yaml` (if `$XDG_CONFIG_HOME` is set)
//!   4. Platform-native config dir (macOS: `~/Library/Application Support`,
//!      Linux: `~/.config` respecting `$XDG_CONFIG_HOME`)
//!   5. `~/.config/peat/credentials.yaml` (XDG fallback; useful on macOS)
//!
//! Steps 3–5 are tried in order; the first path that exists on disk wins.
//! If none exist, step 4 (platform-native) is used as the reported default
//! so error messages point somewhere sensible.
//!
//! The on-disk schema is intentionally narrow today — it mirrors the fields of
//! `peat-node`'s `SidecarConfig` that are relevant to mesh participation. The
//! full credential-bundle shape is pending formalization in an ADR-006
//! amendment (tracked at <https://github.com/defenseunicorns/peat/issues/940>).
//! Until then, this format is the source of truth and the CLI rejects unknown
//! fields strictly so migrations are explicit.
//!
//! Failure to resolve credentials is a fatal error; the CLI does not silently
//! fall back to an anonymous join.
//!
//! Example `credentials.yaml`:
//!
//! ```yaml
//! app_id: my-app
//! shared_key: <base64-formation-key>
//! # Optional: persist the local Automerge store across invocations.
//! # Without this the CLI uses a TempDir that is deleted on exit, so
//! # documents only survive if they sync to a connected peer.
//! # ~/  prefix is expanded to the user's home directory.
//! data_dir: ~/.local/share/peat/my-app
//! # Optional initial peers in `endpoint_id@host:port` form.
//! peers:
//!   - <endpoint_id>@10.0.0.5:4242
//! ```
//!
//! `encryption_key` is accepted by the schema for forward compatibility with
//! the bundle shape pinned in <https://github.com/defenseunicorns/peat/issues/940>,
//! but [`load`] rejects bundles that set it. The byte-level cipher path it
//! would plug into operates at a different layer than peat-node's
//! application-level `StoreCipher` envelope; until the layer question is
//! settled, the CLI fails fast rather than silently bypassing encryption
//! an operator may believe is on.

use serde::Deserialize;
use std::path::{Path, PathBuf};

use crate::cli::CliError;

/// On-disk credential bundle. `deny_unknown_fields` so typos and stale fields
/// surface loudly rather than being silently ignored.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeatCredentials {
    pub app_id: String,
    pub shared_key: String,
    #[serde(default)]
    pub peers: Vec<String>,
    /// Optional persistent store directory. `~/` prefix is expanded to the
    /// user's home directory. If absent the CLI uses a TempDir per invocation.
    #[serde(default)]
    pub data_dir: Option<String>,
    /// Disable mDNS peer discovery. mDNS is on by default so that CLI
    /// invocations on the same host or LAN find each other without explicit
    /// `peers:` configuration. Set to `true` in container deployments where
    /// mDNS multicast is unavailable or undesired.
    #[serde(default)]
    pub disable_mdns: bool,
    #[serde(default)]
    pub encryption_key: Option<String>,
}

/// Resolve credentials per the ADR-001 chain.
///
/// `explicit` is the `--creds <PATH>` argument, if supplied.
pub fn load(explicit: Option<&Path>) -> Result<PeatCredentials, CliError> {
    let path = resolve_path(explicit)?;
    let raw = std::fs::read_to_string(&path).map_err(|e| {
        CliError::Auth(format!(
            "could not read credentials file {}: {e}",
            path.display()
        ))
    })?;
    let creds = parse(&raw).map_err(|e| {
        CliError::Auth(format!(
            "could not parse credentials file {}: {e}",
            path.display()
        ))
    })?;
    // `encryption_key` is accepted by the schema for forward compatibility
    // with peat#940, but the byte-level cipher path it would plug into
    // (`AutomergeBackendConfig.cipher`) operates at a different layer
    // than peat-node's application-level `StoreCipher` envelope. Wiring
    // peat-cli for at-rest encryption that interoperates with peat-node
    // requires the layer question to be settled. Until then we reject
    // the field with a clear error rather than silently bypassing
    // encryption an operator may believe is on.
    if creds.encryption_key.is_some() {
        return Err(CliError::Auth(format!(
            "credentials at {} set `encryption_key`, but peat-cli does not yet \
             apply it (at-rest cipher layering is being resolved in peat#940). \
             Remove the field, or pass an unencrypted bundle, to proceed.",
            path.display()
        )));
    }
    Ok(creds)
}

/// Resolve which path to read, without reading it. Exposed for `peat
/// doctor`-style introspection.
pub fn resolve_path(explicit: Option<&Path>) -> Result<PathBuf, CliError> {
    // `PEAT_CREDS` is also exposed to clap as the `--creds` env fallback, but
    // we re-check here so direct callers (tests, future internal use) follow
    // the same chain without depending on clap.
    let env = std::env::var("PEAT_CREDS").ok();
    resolve_path_with(explicit, env.as_deref(), &config_candidates())
}

/// Build the ordered list of candidate credential paths for the platform.
///
/// On macOS this includes both the native `~/Library/Application Support` path
/// and the XDG `~/.config` fallback so that users coming from a Linux
/// background can place the file at the familiar location.
fn config_candidates() -> Vec<PathBuf> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    // Explicit XDG override — honoured on all platforms.
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
    {
        candidates.push(xdg.join("peat").join("credentials.yaml"));
    }

    // Platform-native default (macOS: ~/Library/Application Support;
    // Linux: ~/.config, already respecting $XDG_CONFIG_HOME via dirs).
    if let Some(dir) = dirs::config_dir() {
        let p = dir.join("peat").join("credentials.yaml");
        if !candidates.contains(&p) {
            candidates.push(p);
        }
    }

    // XDG ~/.config fallback — only meaningful on macOS where dirs::config_dir
    // returns ~/Library/Application Support instead.
    if let Some(home) = dirs::home_dir() {
        let p = home.join(".config").join("peat").join("credentials.yaml");
        if !candidates.contains(&p) {
            candidates.push(p);
        }
    }

    candidates
}

/// Pure resolution helper. Tests drive this directly so they never have to
/// mutate process-global state.
///
/// Walks `candidates` and returns the first path that exists on disk.
/// If none exist, returns the first candidate (the platform-preferred default)
/// so that the caller's error message points somewhere sensible.
/// Returns `Err` only when the candidates list is empty.
fn resolve_path_with(
    explicit: Option<&Path>,
    env: Option<&str>,
    candidates: &[PathBuf],
) -> Result<PathBuf, CliError> {
    if let Some(p) = explicit {
        return Ok(p.to_path_buf());
    }
    if let Some(env) = env.filter(|s| !s.is_empty()) {
        return Ok(PathBuf::from(env));
    }
    if let Some(first) = candidates.first() {
        for candidate in candidates {
            if candidate.exists() {
                return Ok(candidate.clone());
            }
        }
        return Ok(first.clone());
    }
    Err(CliError::Auth(
        "no credentials path resolved — pass --creds, set PEAT_CREDS, or place a file at the platform config dir".into(),
    ))
}

fn parse(raw: &str) -> Result<PeatCredentials, serde_yaml::Error> {
    serde_yaml::from_str(raw)
}

/// Expand a `data_dir` string from the credential bundle into a `PathBuf`.
///
/// A leading `~/` is replaced with the user's home directory. All other paths
/// are returned as-is (relative paths are accepted; callers resolve them
/// against the process working directory).
pub fn expand_data_dir(raw: &str) -> Result<PathBuf, CliError> {
    if let Some(suffix) = raw.strip_prefix("~/") {
        let home = dirs::home_dir().ok_or_else(|| {
            CliError::Auth("cannot expand `~/` in data_dir: home directory unknown".into())
        })?;
        Ok(home.join(suffix))
    } else {
        Ok(PathBuf::from(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_bundle() {
        let creds = parse(
            r#"
app_id: my-app
shared_key: abc123
"#,
        )
        .unwrap();
        assert_eq!(creds.app_id, "my-app");
        assert_eq!(creds.shared_key, "abc123");
        assert!(creds.peers.is_empty());
        assert!(creds.encryption_key.is_none());
        // mDNS on by default — disable_mdns absent → false.
        assert!(!creds.disable_mdns);
    }

    #[test]
    fn disable_mdns_parses_and_defaults_false() {
        let with_flag = parse("app_id: x\nshared_key: y\ndisable_mdns: true\n").unwrap();
        assert!(with_flag.disable_mdns);
        let without_flag = parse("app_id: x\nshared_key: y\n").unwrap();
        assert!(!without_flag.disable_mdns);
    }

    #[test]
    fn parses_full_bundle() {
        let creds = parse(
            r#"
app_id: my-app
shared_key: abc123
peers:
  - id1@10.0.0.5:4242
  - id2@10.0.0.6:4242
encryption_key: zzz
"#,
        )
        .unwrap();
        assert_eq!(creds.peers.len(), 2);
        assert_eq!(creds.encryption_key.as_deref(), Some("zzz"));
    }

    #[test]
    fn rejects_unknown_fields() {
        let err = parse(
            r#"
app_id: my-app
shared_key: abc123
not_a_field: oops
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn rejects_missing_required_fields() {
        assert!(parse("app_id: only").is_err());
        assert!(parse("shared_key: only").is_err());
    }

    fn fake_candidates() -> Vec<PathBuf> {
        vec![
            PathBuf::from("/fake/config/peat/credentials.yaml"),
            PathBuf::from("/fake/fallback/peat/credentials.yaml"),
        ]
    }

    #[test]
    fn explicit_path_wins_over_env() {
        let explicit = PathBuf::from("/explicit/path");
        let resolved = resolve_path_with(
            Some(&explicit),
            Some("/should/not/be/used"),
            &fake_candidates(),
        )
        .unwrap();
        assert_eq!(resolved, explicit);
    }

    #[test]
    fn env_used_when_no_explicit() {
        let resolved = resolve_path_with(None, Some("/env/path"), &fake_candidates()).unwrap();
        assert_eq!(resolved, PathBuf::from("/env/path"));
    }

    #[test]
    fn empty_env_falls_through_to_first_candidate() {
        let resolved = resolve_path_with(None, Some(""), &fake_candidates()).unwrap();
        // Neither fake path exists on disk, so the first candidate is returned.
        assert_eq!(
            resolved,
            PathBuf::from("/fake/config/peat/credentials.yaml")
        );
    }

    #[test]
    fn second_candidate_used_when_first_absent() {
        let dir = tempfile::tempdir().unwrap();
        let present = dir.path().join("peat").join("credentials.yaml");
        std::fs::create_dir_all(present.parent().unwrap()).unwrap();
        std::fs::write(&present, "app_id: x\nshared_key: y\n").unwrap();

        let candidates = vec![
            PathBuf::from("/does/not/exist/peat/credentials.yaml"),
            present.clone(),
        ];
        let resolved = resolve_path_with(None, None, &candidates).unwrap();
        assert_eq!(resolved, present);
    }

    #[test]
    fn no_sources_returns_auth_error() {
        let err = resolve_path_with(None, None, &[]).unwrap_err();
        assert_eq!(err.exit_code(), 2);
    }

    #[test]
    fn load_returns_auth_error_on_missing_file() {
        let missing = PathBuf::from("/definitely/does/not/exist/peat-creds.yaml");
        let err = load(Some(&missing)).unwrap_err();
        assert_eq!(err.exit_code(), 2, "{err}");
    }

    #[test]
    fn load_round_trips_real_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.yaml");
        std::fs::write(&path, "app_id: my-app\nshared_key: abc123\n").unwrap();
        let creds = load(Some(&path)).unwrap();
        assert_eq!(creds.app_id, "my-app");
    }

    #[test]
    fn load_rejects_encryption_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.yaml");
        std::fs::write(
            &path,
            "app_id: my-app\nshared_key: abc123\nencryption_key: zzz\n",
        )
        .unwrap();
        let err = load(Some(&path)).unwrap_err();
        assert_eq!(err.exit_code(), 2);
        assert!(err.to_string().contains("encryption_key"));
        assert!(err.to_string().contains("peat#940"));
    }

    // --- expand_data_dir tests ---

    #[test]
    fn expand_data_dir_absolute_passthrough() {
        let p = expand_data_dir("/tmp/peat/myapp").unwrap();
        assert_eq!(p, PathBuf::from("/tmp/peat/myapp"));
    }

    #[test]
    fn expand_data_dir_relative_passthrough() {
        let p = expand_data_dir("relative/path").unwrap();
        assert_eq!(p, PathBuf::from("relative/path"));
    }

    #[test]
    fn expand_data_dir_tilde_slash_expands() {
        // Only test that `~/` is expanded; exact home dir is env-dependent.
        let p = expand_data_dir("~/peat/myapp").unwrap();
        let home = dirs::home_dir().expect("home dir required for this test");
        assert_eq!(p, home.join("peat/myapp"));
        assert!(p.is_absolute(), "expanded path must be absolute");
    }

    #[test]
    fn expand_data_dir_tilde_alone_is_absolute() {
        // "~" with no trailing slash is treated as a literal path segment, not
        // home expansion — only `~/` prefix is special.
        let p = expand_data_dir("~").unwrap();
        assert_eq!(p, PathBuf::from("~"));
    }

    #[test]
    fn parses_data_dir_field() {
        let creds = parse("app_id: x\nshared_key: y\ndata_dir: /tmp/peat\n").unwrap();
        assert_eq!(creds.data_dir.as_deref(), Some("/tmp/peat"));
    }

    #[test]
    fn data_dir_defaults_to_none() {
        let creds = parse("app_id: x\nshared_key: y\n").unwrap();
        assert!(creds.data_dir.is_none());
    }
}
