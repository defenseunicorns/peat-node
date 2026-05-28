use clap::ValueEnum;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable. Columnar for collections, pretty-printed for docs.
    Text,
    /// Single canonical JSON value (for `query`).
    Json,
    /// One JSON record per line (natural for `observe`).
    Ndjson,
}
