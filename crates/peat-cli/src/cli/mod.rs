pub mod create;
pub mod delete;
pub mod observe;
pub mod output;
pub mod query;
pub mod update;

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
}

impl CliError {
    /// Exit code per ADR-001's "Shell integration discipline" table.
    pub fn exit_code(&self) -> u8 {
        match self {
            CliError::Generic(_) | CliError::NotImplemented(_) => 1,
            CliError::Auth(_) => 2,
            CliError::PermissionDenied(_) => 3,
            CliError::Malformed(_) => 4,
        }
    }
}

impl Cli {
    /// Dispatch into the chosen subcommand. All handlers are stubbed in Phase 1.
    pub fn run(self) -> Result<(), CliError> {
        match self.command {
            Command::Query(_) => Err(CliError::NotImplemented("query")),
            Command::Observe(_) => Err(CliError::NotImplemented("observe")),
            Command::Create(_) => Err(CliError::NotImplemented("create")),
            Command::Update(_) => Err(CliError::NotImplemented("update")),
            Command::Delete(_) => Err(CliError::NotImplemented("delete")),
        }
    }
}
