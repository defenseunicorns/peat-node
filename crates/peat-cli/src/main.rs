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

    // ADR-001 §"Shell integration discipline": pipe-close exits silently
    // with status 0 (not a SIGPIPE-kill at the OS level). Ignore SIGPIPE so
    // stdout writes surface a BrokenPipe error we can map to a clean exit
    // in the streaming handlers (currently `observe`).
    #[cfg(unix)]
    // SAFETY: signal() with SIG_IGN has no preconditions and no async-signal
    // safety hazard; we touch no shared state inside a handler because there
    // is no handler.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }

    let cli = Cli::parse();
    match cli.run().await {
        Ok(()) | Err(peat_cli::CliError::BrokenPipe) => {
            // Pipe-close is a clean exit per ADR-001 §"Shell integration
            // discipline" — no stderr line, status 0.
            ExitCode::SUCCESS
        }
        Err(peat_cli::CliError::Interrupted) => {
            // SIGINT: convention is silent exit with status 130.
            ExitCode::from(130)
        }
        Err(e) => {
            eprintln!("peat: {e}");
            ExitCode::from(e.exit_code())
        }
    }
}
