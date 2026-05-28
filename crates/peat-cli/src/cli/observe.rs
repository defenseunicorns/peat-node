use clap::{Args, ValueEnum};

#[derive(Debug, Args)]
pub struct ObserveArgs {
    /// Target as `<COLLECTION>` or `<COLLECTION>/<DOC_ID>`.
    pub target: String,

    /// Sync mode (maps to ADR-019 sync modes).
    #[arg(long, value_enum, default_value_t = SyncMode::LatestOnly)]
    pub mode: SyncMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SyncMode {
    /// Stream current-state updates only.
    LatestOnly,
    /// Tail recent history then live updates.
    Windowed,
    /// Every delta — forensics, debugging, CDC.
    FullHistory,
}
