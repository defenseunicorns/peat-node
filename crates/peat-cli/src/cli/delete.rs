use clap::Args;

#[derive(Debug, Args)]
pub struct DeleteArgs {
    /// Target as `<COLLECTION>/<DOC_ID>`.
    pub target: String,

    /// Block until at least one peer has acknowledged.
    #[arg(long)]
    pub wait_for_sync: bool,
}
