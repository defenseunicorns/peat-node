pub mod create;
pub mod delete;
pub mod observe;
pub mod output;
pub mod query;
pub mod update;
pub mod writes;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use thiserror::Error;

use self::output::OutputFormat;

/// `peat` — operator CLI for a Peat mesh deployment.
///
/// `peat` joins the mesh as a real Peat node (peat-node ADR-001) and exposes
/// CRUD-shaped operator commands. Read commands (`query`, `observe`) inspect
/// mesh state; write commands (`create`, `update`, `delete`) modify it.
#[derive(Debug, Parser)]
#[command(name = "peat", version, about, long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub common: CommonArgs,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Parser)]
pub struct CommonArgs {
    /// Path to credentials file. Falls back to PEAT_CREDS env, then platform config dir.
    #[arg(long, global = true, env = "PEAT_CREDS")]
    pub creds: Option<PathBuf>,

    /// Identity this CLI joins as. Default: ephemeral identity derived from credentials.
    #[arg(long = "as", global = true, value_name = "ID")]
    pub as_id: Option<String>,

    /// Optional target peer to bias view toward.
    #[arg(long, global = true, value_name = "ID")]
    pub target: Option<String>,

    /// Transport hint (e.g. quic, btle). Default: auto.
    #[arg(long, global = true, value_name = "NAME")]
    pub transport: Option<String>,

    /// Join / sync timeout (e.g. 10s, 1m). Default: 10s.
    #[arg(long, global = true, value_name = "DURATION", default_value = "10s")]
    pub timeout: String,

    /// Output format.
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Text)]
    pub output: OutputFormat,

    /// Increase log verbosity. Repeat for more detail.
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Fetch current state of a target and exit.
    Query(query::QueryArgs),

    /// Subscribe and stream updates until interrupted.
    Observe(observe::ObserveArgs),

    /// Create a new document in a collection.
    Create(create::CreateArgs),

    /// Apply a delta to an existing document (or create if missing).
    Update(update::UpdateArgs),

    /// Tombstone a document.
    Delete(delete::DeleteArgs),
}

/// Exit-code-bearing CLI error. Codes match the table in peat-node ADR-001.
#[derive(Debug, Error)]
pub enum CliError {
    #[error("{0}")]
    Generic(String),
    #[error("authentication failure: {0}")]
    Auth(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("malformed request: {0}")]
    Malformed(String),
    #[error("not yet implemented: {0}")]
    NotImplemented(&'static str),
    /// SIGINT received while a streaming subcommand was running. Maps to
    /// exit 130 (128 + SIGINT) by Unix convention.
    #[error("interrupted")]
    Interrupted,
    /// Downstream pipe consumer closed its read end. ADR-001 §"Shell
    /// integration discipline" says exit silently with status 0 — the
    /// caller (main.rs) treats this variant as a clean exit, not as
    /// failure. Distinguishes pipe-close from a real write error so the
    /// observe loop doesn't have to string-match an error message.
    #[error("broken pipe")]
    BrokenPipe,
}

impl CliError {
    /// Exit code per ADR-001's "Shell integration discipline" table.
    pub fn exit_code(&self) -> u8 {
        match self {
            CliError::Generic(_) | CliError::NotImplemented(_) => 1,
            CliError::Auth(_) => 2,
            CliError::PermissionDenied(_) => 3,
            CliError::Malformed(_) => 4,
            CliError::Interrupted => 130,
            CliError::BrokenPipe => 0,
        }
    }
}

impl Cli {
    /// Dispatch into the chosen subcommand.
    pub async fn run(self) -> Result<(), CliError> {
        match self.command {
            Command::Query(args) => query::run(args, self.common).await,
            Command::Observe(args) => observe::run(args, self.common).await,
            Command::Create(args) => create::run(args, self.common).await,
            Command::Update(args) => update::run(args, self.common).await,
            Command::Delete(args) => delete::run(args, self.common).await,
        }
    }
}

/// Parse a `--timeout` value like `10s`, `1m`, or `500ms`. Sufficient for v1;
/// `humantime`-style parsing can come later if operators want richer syntax.
pub(crate) fn parse_timeout(s: &str) -> Result<std::time::Duration, CliError> {
    use std::time::Duration;
    let map_err = |e: std::num::ParseIntError| CliError::Malformed(format!("timeout `{s}`: {e}"));
    if let Some(num) = s.strip_suffix("ms") {
        return num
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(map_err);
    }
    if let Some(num) = s.strip_suffix('s') {
        return num.parse::<u64>().map(Duration::from_secs).map_err(map_err);
    }
    if let Some(num) = s.strip_suffix('m') {
        return num
            .parse::<u64>()
            .map(|n| Duration::from_secs(n * 60))
            .map_err(map_err);
    }
    Err(CliError::Malformed(format!(
        "timeout `{s}`: expected suffix s, m, or ms (e.g. 10s, 1m, 500ms)"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parses_seconds() {
        assert_eq!(parse_timeout("10s").unwrap(), Duration::from_secs(10));
    }
    #[test]
    fn parses_minutes() {
        assert_eq!(parse_timeout("2m").unwrap(), Duration::from_secs(120));
    }
    #[test]
    fn parses_milliseconds() {
        assert_eq!(parse_timeout("500ms").unwrap(), Duration::from_millis(500));
    }
    #[test]
    fn rejects_missing_suffix() {
        let err = parse_timeout("10").unwrap_err();
        assert_eq!(err.exit_code(), 4);
    }
    #[test]
    fn rejects_bad_number() {
        let err = parse_timeout("abc s").unwrap_err();
        assert_eq!(err.exit_code(), 4);
    }
}
