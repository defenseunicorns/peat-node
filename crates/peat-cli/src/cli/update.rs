use clap::Args;
use std::path::PathBuf;

#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("input").required(true).args(["from", "set"]))]
pub struct UpdateArgs {
    /// Target as `<COLLECTION>/<DOC_ID>`.
    pub target: String,

    /// Read full document content (delta computed from current). Use `-` for stdin.
    #[arg(long, value_name = "PATH", conflicts_with = "set")]
    pub from: Option<PathBuf>,

    /// Surgical field updates as `path=value` (repeatable).
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
