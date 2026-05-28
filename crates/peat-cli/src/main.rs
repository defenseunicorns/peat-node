use clap::Parser;
use peat_cli::Cli;
use std::process::ExitCode;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("peat: {e}");
            ExitCode::from(e.exit_code())
        }
    }
}
