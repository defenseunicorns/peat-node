use clap::Args;

#[derive(Debug, Args)]
pub struct QueryArgs {
    /// Target as `<COLLECTION>` or `<COLLECTION>/<DOC_ID>`.
    pub target: String,

    /// Cap the number of records emitted.
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,
}
