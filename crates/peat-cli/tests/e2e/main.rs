//! End-to-end functional tests for the `peat` binary.
//!
//! Per peat-node ADR-001: this suite spawns the real `peat` binary as a
//! subprocess and asserts on its stdout / stderr / exit code.
//!
//! Two layers:
//!   - **Surface** tests (this file): exercise the binary's own surface
//!     — `--help`, exit codes, parser errors, dry-run paths. Don't need
//!     a live mesh peer.
//!   - **Scenario** tests (this file via `mod scenarios`): stand up a
//!     real `AutomergeBackend` peer via `topology::TestPeer` and verify
//!     end-to-end behavior across the wire. Slower (real Iroh
//!     handshakes) but exercise the same code paths as production.

mod scenarios;
mod topology;

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
        .stdout(predicate::str::contains("delete"))
        .stdout(predicate::str::contains("schema"));
}

#[test]
fn schema_list_runs_offline_without_creds() {
    // `schema list` is a local registry inspector — no creds, no
    // mesh handshake. Confirms an operator can discover registered
    // types before they have a credential bundle in hand.
    peat()
        .args(["schema", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("COLLECTION"))
        .stdout(predicate::str::contains("capabilities"))
        .stdout(predicate::str::contains("Capability"));
}

#[test]
fn schema_describe_renders_field_shape() {
    // Field-level table for one type. Asserts on the format strings
    // exercised by Capability so the renderer's contract is pinned.
    peat()
        .args(["schema", "describe", "capabilities"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Capability (v1)"))
        .stdout(predicate::str::contains("collection: capabilities"))
        .stdout(predicate::str::contains("confidence"))
        .stdout(predicate::str::contains("percentage"))
        .stdout(predicate::str::contains("Sensor"));
}

#[test]
fn schema_describe_resolves_by_canonical_id() {
    peat()
        .args(["schema", "describe", "peat.capability.v1.Capability"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Capability"));
}

#[test]
fn schema_describe_unknown_target_is_malformed() {
    peat()
        .args(["schema", "describe", "no-such-collection"])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("no registered type matches"));
}

#[test]
fn schema_list_json_output_is_array() {
    // `--output json` contract: a JSON array, one element per type,
    // each element carrying the documented keys. Scripts depend on this.
    let output = peat()
        .args(["--output", "json", "schema", "list"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let parsed: serde_json::Value = serde_json::from_slice(&output).expect("stdout is JSON");
    let arr = parsed.as_array().expect("schema list json is an array");
    assert!(!arr.is_empty(), "expected non-empty registered-types array");
    for entry in arr {
        for key in ["id", "name", "version", "collection", "fields"] {
            assert!(
                entry.get(key).is_some(),
                "missing key `{key}` in schema list entry: {entry}"
            );
        }
    }
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
fn create_for_unknown_collection_skips_validation() {
    // "contacts" is not a peat-schema-registered collection, so the
    // schema gate accepts the document structurally and dry-run prints.
    peat()
        .args([
            "create",
            "contacts",
            "--id",
            "c-1",
            "--set",
            "name=alice",
            "--dry-run",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"op\": \"create\""));
}

#[test]
fn create_for_known_collection_validates_and_rejects_missing_required_fields() {
    // "capabilities" IS a registered collection (peat-schema). Building
    // only `name` leaves `id` and other proto3 fields at their defaults.
    // validate_capability rejects an empty id with MissingField, which
    // surfaces as CliError::Malformed → exit 4.
    peat()
        .args([
            "create",
            "capabilities",
            "--set",
            "name=thermal",
            "--dry-run",
        ])
        .assert()
        .code(4)
        .stderr(predicate::str::contains("schema validation failed"))
        .stderr(predicate::str::contains("Capability"));
}

#[test]
fn create_with_no_validate_skips_schema_gate_for_known_collection() {
    // --no-validate bypasses the gate so a Capability missing `id`
    // succeeds at the dry-run stage. Warning goes to stderr.
    peat()
        .args([
            "create",
            "capabilities",
            "--set",
            "name=thermal",
            "--dry-run",
            "--no-validate",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"op\": \"create\""));
}

#[test]
fn update_from_missing_file_is_malformed() {
    // `--from` is parsed *before* the join prelude (peat-mesh#187 landed
    // the delta API, so this is real now). A bad path surfaces as
    // CliError::Malformed → exit 4 before we attempt any mesh handshake,
    // so passing a path that doesn't exist exercises the eager-read path.
    peat()
        .args([
            "update",
            "contacts/c-1",
            "--from",
            "/definitely/does/not/exist.json",
        ])
        .assert()
        .code(4);
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
