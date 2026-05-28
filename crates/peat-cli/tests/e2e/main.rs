//! End-to-end functional tests for the `peat` binary.
//!
//! Per peat-node ADR-001: this suite spawns the real `peat` binary as a
//! subprocess and asserts on its stdout / stderr / exit code. It is the
//! infrastructure on which Phase 2+ behavioral tests (multi-node sync,
//! credential resolution, observe streaming, etc.) will be built.
//!
//! Phase 1 only exercises the binary's own surface: `--help`, `--version`,
//! subcommand `--help`, and the documented stub exit code + stderr line.

use assert_cmd::Command;
use predicates::prelude::*;

/// Locate the `peat` binary built by cargo for this test run.
///
/// `assert_cmd` picks up the binary cargo builds when running `cargo test`,
/// so no path wiring is needed in CI or locally.
fn peat() -> Command {
    Command::cargo_bin("peat").expect("cargo built the `peat` binary")
}

#[test]
fn help_renders_with_all_subcommands() {
    peat()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Usage: peat"))
        .stdout(predicate::str::contains("query"))
        .stdout(predicate::str::contains("observe"))
        .stdout(predicate::str::contains("create"))
        .stdout(predicate::str::contains("update"))
        .stdout(predicate::str::contains("delete"));
}

#[test]
fn version_renders() {
    peat()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("peat "));
}

#[test]
fn subcommand_help_renders() {
    for sub in ["query", "observe", "create", "update", "delete"] {
        peat()
            .args([sub, "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Usage:"));
    }
}

#[test]
fn query_without_creds_exits_auth_error() {
    // ADR-001 "Shell integration discipline": auth failure → exit 2, empty
    // stdout, explanation on stderr. Passing a path that doesn't exist
    // bypasses any platform-default config that may be present on the
    // developer's machine.
    peat()
        .args([
            "query",
            "contacts",
            "--creds",
            "/definitely/does/not/exist.yaml",
        ])
        .assert()
        .code(2)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("authentication failure"));
}

#[test]
fn observe_without_creds_exits_auth_error() {
    // Same shape as query: missing creds → exit 2 with the auth message on
    // stderr, no stdout. Confirms the streaming subcommand reaches the join
    // prelude before any subscription work.
    peat()
        .args([
            "observe",
            "contacts",
            "--creds",
            "/definitely/does/not/exist.yaml",
        ])
        .assert()
        .code(2)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("authentication failure"));
}

#[test]
fn create_without_creds_exits_auth_error() {
    peat()
        .args([
            "create",
            "contacts",
            "--set",
            "name=alice",
            "--creds",
            "/definitely/does/not/exist.yaml",
        ])
        .assert()
        .code(2)
        .stdout(predicate::str::is_empty())
        .stderr(predicate::str::contains("authentication failure"));
}

#[test]
fn create_dry_run_renders_op_without_join() {
    // --dry-run skips the join prelude entirely, so missing creds don't
    // matter and we get the would-be op on stdout. Confirms the
    // ArgGroup wiring and the JSON shape of the dry-run output.
    peat()
        .args([
            "create",
            "contacts",
            "--id",
            "c-1",
            "--set",
            "name=alice",
            "--set",
            "position.lat=40.7128",
            "--dry-run",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"op\": \"create\""))
        .stdout(predicate::str::contains("\"key\": \"contacts:c-1\""))
        .stdout(predicate::str::contains("\"name\": \"alice\""))
        .stdout(predicate::str::contains("\"lat\": 40.7128"));
}

#[test]
fn update_from_returns_not_implemented_with_upstream_link() {
    // --from is gated on peat-mesh#187; the error names the issue so
    // operators know where to look. Note: clap parses --from before the
    // handler runs, so we need a valid --from arg shape even though we
    // never read the file.
    peat()
        .args(["update", "contacts/c-1", "--from", "doc.json"])
        .assert()
        .code(1)
        .stderr(predicate::str::contains("update --from"))
        .stderr(predicate::str::contains("peat-mesh#187"));
}

#[test]
fn update_without_target_doc_id_is_malformed() {
    peat()
        .args(["update", "contacts", "--set", "name=alice"])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("update requires"));
}

#[test]
fn delete_without_target_doc_id_is_malformed() {
    peat()
        .args(["delete", "contacts"])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("delete requires"));
}

#[test]
fn missing_subcommand_is_a_parse_error() {
    // clap prints usage to stderr and exits non-zero when no subcommand is given.
    peat()
        .assert()
        .failure()
        .stderr(predicate::str::contains("Usage:"));
}

#[test]
fn unknown_subcommand_is_a_parse_error() {
    peat()
        .arg("nonexistent")
        .assert()
        .failure()
        .stderr(predicate::str::contains("unrecognized"));
}

#[test]
fn create_requires_from_or_set_at_binary_level() {
    // Mirrors the in-process parser test but exercises the real binary path,
    // proving the ArgGroup constraint survives compilation + linking.
    peat()
        .args(["create", "contacts"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("required"));
}
