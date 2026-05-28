use clap::Parser;
use peat_cli::Cli;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    // Tracing goes to stderr by default — keeps stdout clean for piping per
    // ADR-001 §"Shell integration discipline." `RUST_LOG=info` etc. opt in.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "peat_cli=warn".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("peat: {e}");
            ExitCode::from(e.exit_code())
        }
    }
}
