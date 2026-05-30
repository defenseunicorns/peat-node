//! Parser-construction tests for the Phase 1 skeleton.
//!
//! These tests exercise the clap surface declared in peat-node ADR-001.
//! They do not run the CLI — they verify the parser accepts the documented
//! flags, rejects the documented conflicts, and applies the documented
//! defaults. Behavioral tests follow in later phases.

use clap::Parser;
use peat_cli::cli::{output::OutputFormat, Cli, Command};

fn parse(args: &[&str]) -> Cli {
    let mut full = vec!["peat"];
    full.extend_from_slice(args);
    Cli::try_parse_from(full).expect("parse")
}

fn parse_err(args: &[&str]) -> clap::Error {
    let mut full = vec!["peat"];
    full.extend_from_slice(args);
    Cli::try_parse_from(full).expect_err("expected parse error")
}

#[test]
fn query_minimal() {
    let cli = parse(&["query", "contacts"]);
    match cli.command {
        Command::Query(q) => {
            assert_eq!(q.target.as_deref(), Some("contacts"));
            assert!(!q.all_collections);
            assert_eq!(q.limit, None);
        }
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn query_all_collections_via_long_flag() {
    let cli = parse(&["query", "--all-collections"]);
    match cli.command {
        Command::Query(q) => {
            assert_eq!(q.target, None);
            assert!(q.all_collections);
        }
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn query_all_collections_via_alias() {
    let cli = parse(&["query", "--all"]);
    match cli.command {
        Command::Query(q) => {
            assert_eq!(q.target, None);
            assert!(q.all_collections);
        }
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn query_requires_target_or_all() {
    let err = parse_err(&["query"]);
    assert_eq!(
        err.kind(),
        clap::error::ErrorKind::MissingRequiredArgument,
        "{err}"
    );
}

#[test]
fn query_rejects_target_combined_with_all() {
    let err = parse_err(&["query", "contacts", "--all"]);
    assert_eq!(
        err.kind(),
        clap::error::ErrorKind::ArgumentConflict,
        "{err}"
    );
}

#[test]
fn observe_all_collections_via_long_flag() {
    let cli = parse(&["observe", "--all-collections"]);
    match cli.command {
        Command::Observe(o) => {
            assert_eq!(o.target, None);
            assert!(o.all_collections);
        }
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn observe_requires_target_or_all() {
    let err = parse_err(&["observe"]);
    assert_eq!(
        err.kind(),
        clap::error::ErrorKind::MissingRequiredArgument,
        "{err}"
    );
}

#[test]
fn observe_rejects_target_combined_with_all() {
    let err = parse_err(&["observe", "contacts", "--all"]);
    assert_eq!(
        err.kind(),
        clap::error::ErrorKind::ArgumentConflict,
        "{err}"
    );
}

#[test]
fn query_with_limit_and_output() {
    let cli = parse(&["query", "contacts", "--limit", "10", "--output", "json"]);
    assert_eq!(cli.common.output, OutputFormat::Json);
    match cli.command {
        Command::Query(q) => assert_eq!(q.limit, Some(10)),
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn observe_defaults_to_latest_only() {
    use peat_cli::cli::observe::SyncMode;
    let cli = parse(&["observe", "contacts/c-1"]);
    match cli.command {
        Command::Observe(o) => assert_eq!(o.mode, SyncMode::LatestOnly),
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn observe_accepts_all_sync_modes() {
    use peat_cli::cli::observe::SyncMode;
    for (flag, expected) in [
        ("latest-only", SyncMode::LatestOnly),
        ("windowed", SyncMode::Windowed),
        ("full-history", SyncMode::FullHistory),
    ] {
        let cli = parse(&["observe", "contacts", "--mode", flag]);
        match cli.command {
            Command::Observe(o) => assert_eq!(o.mode, expected),
            _ => panic!("wrong subcommand"),
        }
    }
}

#[test]
fn create_requires_from_or_set() {
    let err = parse_err(&["create", "contacts"]);
    assert_eq!(
        err.kind(),
        clap::error::ErrorKind::MissingRequiredArgument,
        "{err}"
    );
}

#[test]
fn create_rejects_from_and_set_together() {
    let err = parse_err(&["create", "contacts", "--from", "doc.json", "--set", "x=1"]);
    assert_eq!(
        err.kind(),
        clap::error::ErrorKind::ArgumentConflict,
        "{err}"
    );
}

#[test]
fn create_with_from() {
    let cli = parse(&["create", "contacts", "--from", "doc.json", "--dry-run"]);
    match cli.command {
        Command::Create(c) => {
            assert_eq!(c.collection, "contacts");
            assert_eq!(c.from.as_deref().unwrap().to_str().unwrap(), "doc.json");
            assert!(c.dry_run);
            assert!(!c.wait_for_sync);
            assert!(!c.no_validate);
        }
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn create_with_repeated_set() {
    let cli = parse(&[
        "create",
        "contacts",
        "--set",
        "name=alice",
        "--set",
        "rank=1",
    ]);
    match cli.command {
        Command::Create(c) => assert_eq!(c.set, vec!["name=alice", "rank=1"]),
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn update_requires_from_or_set() {
    let err = parse_err(&["update", "contacts/c-1"]);
    assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
}

#[test]
fn update_with_set_and_wait_for_sync() {
    let cli = parse(&[
        "update",
        "contacts/c-1",
        "--set",
        "name=alice",
        "--wait-for-sync",
    ]);
    match cli.command {
        Command::Update(u) => {
            assert_eq!(u.target, "contacts/c-1");
            assert_eq!(u.set, vec!["name=alice"]);
            assert!(u.wait_for_sync);
        }
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn delete_minimal() {
    let cli = parse(&["delete", "contacts/c-1"]);
    match cli.command {
        Command::Delete(d) => {
            assert_eq!(d.target, "contacts/c-1");
            assert!(!d.wait_for_sync);
        }
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn schema_list_parses() {
    use peat_cli::cli::schema::SchemaSubcommand;
    let cli = parse(&["schema", "list"]);
    match cli.command {
        Command::Schema(s) => assert!(matches!(s.sub, SchemaSubcommand::List)),
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn schema_describe_parses_with_target() {
    use peat_cli::cli::schema::SchemaSubcommand;
    let cli = parse(&["schema", "describe", "capabilities"]);
    match cli.command {
        Command::Schema(s) => match s.sub {
            SchemaSubcommand::Describe { target } => assert_eq!(target, "capabilities"),
            _ => panic!("wrong schema sub"),
        },
        _ => panic!("wrong subcommand"),
    }
}

#[test]
fn schema_describe_requires_target() {
    let err = parse_err(&["schema", "describe"]);
    assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
}

#[test]
fn schema_requires_sub() {
    let err = parse_err(&["schema"]);
    assert!(
        matches!(
            err.kind(),
            clap::error::ErrorKind::MissingSubcommand
                | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        ),
        "unexpected error kind: {err}"
    );
}

#[test]
fn common_args_defaults() {
    let cli = parse(&["query", "contacts"]);
    assert_eq!(cli.common.timeout, "10s");
    assert_eq!(cli.common.output, OutputFormat::Text);
    assert_eq!(cli.common.verbose, 0);
    assert!(cli.common.creds.is_none());
}

#[test]
fn verbosity_counts() {
    let cli = parse(&["query", "contacts", "-vvv"]);
    assert_eq!(cli.common.verbose, 3);
}

#[test]
fn global_args_after_subcommand() {
    let cli = parse(&[
        "query",
        "contacts",
        "--output",
        "ndjson",
        "--timeout",
        "30s",
    ]);
    assert_eq!(cli.common.output, OutputFormat::Ndjson);
    assert_eq!(cli.common.timeout, "30s");
}

#[test]
fn help_renders() {
    let err = parse_err(&["--help"]);
    assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);
}
