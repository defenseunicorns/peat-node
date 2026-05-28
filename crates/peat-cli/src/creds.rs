//! Credential loading for `peat`.
//!
//! ADR-001 §Credentials specifies a YAML credential bundle resolved in this
//! order:
//!   1. `--creds <PATH>` argument
//!   2. `PEAT_CREDS` environment variable (path to the YAML file)
//!   3. `$XDG_CONFIG_HOME/peat/credentials.yaml` (platform default)
//!
//! The on-disk schema is intentionally narrow today — it mirrors the fields of
//! `peat-node`'s `SidecarConfig` that are relevant to mesh participation. The
//! full credential-bundle shape is pending formalization in an ADR-006
//! amendment; see the cross-repo tracking issue. Until then, this format is
//! the source of truth and the CLI rejects unknown fields strictly so
//! migrations are explicit.
//!
//! Failure to resolve credentials is a fatal error; the CLI does not silently
//! fall back to an anonymous join.
//!
//! Example `credentials.yaml`:
//!
//! ```yaml
//! app_id: my-app
//! shared_key: <base64-formation-key>
//! # Optional initial peers in `endpoint_id@host:port` form.
//! peers:
//!   - <endpoint_id>@10.0.0.5:4242
//! # Optional base64 32-byte AES-256-GCM key for at-rest encryption.
//! encryption_key: <base64-32-byte-key>
//! ```

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
    parse(&raw).map_err(|e| {
        CliError::Auth(format!(
            "could not parse credentials file {}: {e}",
            path.display()
        ))
    })
}

/// Resolve which path to read, without reading it. Exposed for `peat
/// doctor`-style introspection.
pub fn resolve_path(explicit: Option<&Path>) -> Result<PathBuf, CliError> {
    // `PEAT_CREDS` is also exposed to clap as the `--creds` env fallback, but
    // we re-check here so direct callers (tests, future internal use) follow
    // the same chain without depending on clap.
    let env = std::env::var("PEAT_CREDS").ok();
    resolve_path_with(explicit, env.as_deref(), dirs::config_dir())
}

/// Pure resolution helper. Tests drive this directly so they never have to
/// mutate process-global state.
fn resolve_path_with(
    explicit: Option<&Path>,
    env: Option<&str>,
    config_dir: Option<PathBuf>,
) -> Result<PathBuf, CliError> {
    if let Some(p) = explicit {
        return Ok(p.to_path_buf());
    }
    if let Some(env) = env.filter(|s| !s.is_empty()) {
        return Ok(PathBuf::from(env));
    }
    if let Some(dir) = config_dir {
        return Ok(dir.join("peat").join("credentials.yaml"));
    }
    Err(CliError::Auth(
        "no credentials path resolved — pass --creds, set PEAT_CREDS, or place a file at the platform config dir".into(),
    ))
}

fn parse(raw: &str) -> Result<PeatCredentials, serde_yaml::Error> {
    serde_yaml::from_str(raw)
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

    fn fake_config_dir() -> Option<PathBuf> {
        Some(PathBuf::from("/fake/config"))
    }

    #[test]
    fn explicit_path_wins_over_env() {
        let explicit = PathBuf::from("/explicit/path");
        let resolved = resolve_path_with(
            Some(&explicit),
            Some("/should/not/be/used"),
            fake_config_dir(),
        )
        .unwrap();
        assert_eq!(resolved, explicit);
    }

    #[test]
    fn env_used_when_no_explicit() {
        let resolved = resolve_path_with(None, Some("/env/path"), fake_config_dir()).unwrap();
        assert_eq!(resolved, PathBuf::from("/env/path"));
    }

    #[test]
    fn empty_env_falls_through_to_config_dir() {
        let resolved = resolve_path_with(None, Some(""), fake_config_dir()).unwrap();
        assert_eq!(
            resolved,
            PathBuf::from("/fake/config")
                .join("peat")
                .join("credentials.yaml")
        );
    }

    #[test]
    fn no_sources_returns_auth_error() {
        let err = resolve_path_with(None, None, None).unwrap_err();
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
}
