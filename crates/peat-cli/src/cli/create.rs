use clap::Args;
use std::path::PathBuf;

#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("input").required(true).args(["from", "set"]))]
pub struct CreateArgs {
    /// Target collection.
    pub collection: String,

    /// Explicit document id. Default: generated.
    #[arg(long, value_name = "DOC_ID")]
    pub id: Option<String>,

    /// Read document content from file. Use `-` for stdin.
    #[arg(long, value_name = "PATH", conflicts_with = "set")]
    pub from: Option<PathBuf>,

    /// Build document from `path=value` pairs (repeatable).
    #[arg(long, value_name = "PATH=VALUE")]
    pub set: Vec<String>,

    /// Validate and prepare the operation; do not submit.
    #[arg(long)]
    pub dry_run: bool,

    /// Block until at least one peer has acknowledged.
    #[arg(long)]
    pub wait_for_sync: bool,

    /// Skip schema validation (emits warning to stderr).
    #[arg(long)]
    pub no_validate: bool,
}
