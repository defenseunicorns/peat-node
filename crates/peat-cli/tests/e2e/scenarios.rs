//! Scenario tests: real mesh peer ↔ spawned `peat` binary.
//!
//! Each scenario boots a `TestPeer` (a real `AutomergeBackend` bound to a
//! loopback Iroh endpoint), writes a `credentials.yaml` pointing at it,
//! and exercises a peat-cli command as a subprocess. The harness exercises
//! the full join handshake + sync transport — there are no mocks.

use assert_cmd::Command;
use peat_mesh::storage::json_convert::{automerge_to_json, json_to_automerge};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

use super::topology::{self, TestPeer};

/// Spawn `peat <args>` against `creds`, asserting it exits 0, and return
/// `(stdout, stderr)` as UTF-8. Centralises the subprocess plumbing so each
/// scenario stays focused on its actual assertions.
async fn run_peat(creds: &Path, args: &[&str]) -> (String, String) {
    let mut owned: Vec<String> = vec![
        "--creds".into(),
        creds.to_string_lossy().into_owned(),
        "--timeout".into(),
        "15s".into(),
    ];
    owned.extend(args.iter().map(|s| (*s).to_string()));
    let args_for_display = owned.clone();

    let output = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("peat")
            .unwrap()
            .env("RUST_LOG", "peat_cli=warn")
            .args(owned.iter().map(|s| s.as_str()))
            .timeout(SCENARIO_TIMEOUT)
            .output()
            .expect("spawn peat")
    })
    .await
    .expect("join blocking");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    assert!(
        output.status.success(),
        "peat {args_for_display:?} failed (exit {:?})\nstdout={stdout}\nstderr={stderr}",
        output.status.code(),
    );
    (stdout, stderr)
}

/// Poll the peer's store until the key appears or `deadline` elapses.
async fn await_key(peer: &TestPeer, key: &str, deadline: Duration) -> Value {
    let start = std::time::Instant::now();
    loop {
        if let Ok(Some(doc)) = peer.backend.store().get(key) {
            return automerge_to_json(&doc);
        }
        if start.elapsed() >= deadline {
            panic!("key `{key}` did not appear on peer within {deadline:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll the peer's store until the doc at `key` has converged to a value
/// different from `baseline`, then return the post-change value.
///
/// Use this for "after second write" assertions where `await_key` (existence
/// semantics) is unsafe — when the doc already exists from an earlier
/// create/seed, `await_key` returns immediately on the first poll regardless
/// of whether the awaited update has propagated yet. That stale-read is a
/// real source of test flakiness whenever the merge takes longer than the
/// CLI's `--wait-for-sync` heuristic (currently a 750ms post-write sleep,
/// not a true ack — peat-cli plan §"--wait-for-sync"). `await_key_change`
/// holds the loop until the peer's CRDT actually advances past `baseline`,
/// or fires a self-diagnostic panic on timeout.
///
/// Caller captures `baseline` immediately before issuing the change — the
/// "before" snapshot of the key as the peer sees it. The helper compares
/// structural JSON equality; field reordering inside maps is normalized by
/// `serde_json::Value`'s PartialEq.
async fn await_key_change(
    peer: &TestPeer,
    key: &str,
    baseline: &Value,
    deadline: Duration,
) -> Value {
    let start = std::time::Instant::now();
    let mut last = Value::Null;
    loop {
        if let Ok(Some(doc)) = peer.backend.store().get(key) {
            last = automerge_to_json(&doc);
            if &last != baseline {
                return last;
            }
        }
        if start.elapsed() >= deadline {
            panic!(
                "key `{key}` did not advance past baseline within {deadline:?}; \
                 baseline={baseline}, last={last}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll the peer's store until the key is gone (tombstoned) or deadline.
async fn await_key_gone(peer: &TestPeer, key: &str, deadline: Duration) {
    let start = std::time::Instant::now();
    loop {
        if matches!(peer.backend.store().get(key), Ok(None)) {
            return;
        }
        if start.elapsed() >= deadline {
            panic!("key `{key}` was not deleted on peer within {deadline:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Per-scenario timeout. Tuned to be slack enough for an Iroh handshake +
/// initial sync on a loaded CI runner but tight enough to catch hangs.
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to poll the peer's store for an expected state transition
/// (`await_key`, `await_key_change`, `await_key_gone`) before declaring
/// a sync hang. CLI writes ride a 750 ms heuristic post-write sleep
/// (`POST_WRITE_SYNC_WAIT` in writes.rs) rather than a real ack — when
/// the e2e binary's `#[serial_test::serial(peat_cli_two_party)]` block
/// runs alongside other workspace binaries on a loaded CI runner, the
/// receive-side merge can land seconds after the CLI exits. A 10 s
/// deadline was tight enough to flake intermittently; 30 s is generous
/// without competing with `SCENARIO_TIMEOUT` (which still bounds CLI
/// subprocess wall time, not test polling).
const POLL_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn query_returns_doc_written_on_peer() {
    let peer = TestPeer::start().await;
    let doc = json_to_automerge(&json!({"name": "alice", "rank": 1}), None).unwrap();
    peer.backend.store().put("contacts:c-1", &doc).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);
    let (stdout, _) = run_peat(&creds, &["--output", "json", "query", "contacts/c-1"]).await;

    let parsed: Value = serde_json::from_str(&stdout).expect("stdout is JSON");
    assert_eq!(parsed["name"], json!("alice"));
    assert_eq!(parsed["rank"], json!(1));
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn query_collection_returns_all_docs_keyed() {
    let peer = TestPeer::start().await;
    for (id, name) in [("c-1", "alice"), ("c-2", "bob"), ("c-3", "carol")] {
        let doc = json_to_automerge(&json!({"name": name}), None).unwrap();
        peer.backend
            .store()
            .put(&format!("contacts:{id}"), &doc)
            .unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);
    let (stdout, _) = run_peat(&creds, &["--output", "json", "query", "contacts"]).await;
    let parsed: Value = serde_json::from_str(&stdout).expect("stdout is JSON");
    let obj = parsed.as_object().expect("query collection emits object");
    assert_eq!(obj.len(), 3, "expected 3 keyed entries; got {obj:?}");
    assert_eq!(obj["contacts:c-2"]["name"], json!("bob"));
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn create_propagates_to_peer() {
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    let (stdout, _) = run_peat(
        &creds,
        &[
            "create",
            "contacts",
            "--id",
            "c-new",
            "--set",
            "name=dave",
            "--wait-for-sync",
        ],
    )
    .await;
    assert!(stdout.trim().ends_with("contacts:c-new"));

    let observed = await_key(&peer, "contacts:c-new", POLL_DEADLINE).await;
    assert_eq!(observed["name"], json!("dave"));
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn update_set_modifies_existing_doc() {
    let peer = TestPeer::start().await;
    let doc = json_to_automerge(&json!({"name": "alice", "rank": 1}), None).unwrap();
    peer.backend.store().put("contacts:c-1", &doc).unwrap();
    let baseline = automerge_to_json(&peer.backend.store().get("contacts:c-1").unwrap().unwrap());

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    run_peat(
        &creds,
        &[
            "update",
            "contacts/c-1",
            "--set",
            "rank=2",
            "--wait-for-sync",
        ],
    )
    .await;

    let updated = await_key_change(&peer, "contacts:c-1", &baseline, POLL_DEADLINE).await;
    assert_eq!(updated["rank"], json!(2), "rank should be updated");
    assert_eq!(
        updated["name"],
        json!("alice"),
        "other fields should be preserved"
    );
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn update_set_against_missing_doc_creates_it() {
    // ADR-021 + ADR-001: update is upsert-shaped — initial update on a
    // missing doc is initial creation, not recreation.
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    run_peat(
        &creds,
        &[
            "update",
            "contacts/c-fresh",
            "--set",
            "name=erin",
            "--wait-for-sync",
        ],
    )
    .await;

    let created = await_key(&peer, "contacts:c-fresh", POLL_DEADLINE).await;
    assert_eq!(created["name"], json!("erin"));
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn update_from_applies_delta_to_existing_doc() {
    // Round-trip-edit (ADR-001 Phase 4b, peat-mesh#187): write a doc on
    // the peer, then `update --from` against the proposed shape. The
    // peer should observe the merged state — both the prior fields and
    // the edited field — proving the delta path applied changes rather
    // than recreating the doc.
    let peer = TestPeer::start().await;
    let doc = json_to_automerge(&json!({"name": "alice", "rank": 1}), None).unwrap();
    peer.backend.store().put("contacts:c-1", &doc).unwrap();
    let baseline = automerge_to_json(&peer.backend.store().get("contacts:c-1").unwrap().unwrap());

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    // Write the proposed full doc to a tempfile; the CLI reads it and
    // diffs against current.
    let proposed_path = dir.path().join("proposed.json");
    std::fs::write(
        &proposed_path,
        serde_json::to_string(&json!({"name": "alice", "rank": 5, "tag": "lead"})).unwrap(),
    )
    .unwrap();

    run_peat(
        &creds,
        &[
            "update",
            "contacts/c-1",
            "--from",
            proposed_path.to_str().unwrap(),
            "--wait-for-sync",
        ],
    )
    .await;

    let merged = await_key_change(&peer, "contacts:c-1", &baseline, POLL_DEADLINE).await;
    assert_eq!(merged["name"], json!("alice"));
    assert_eq!(merged["rank"], json!(5), "rank should be updated to 5");
    assert_eq!(merged["tag"], json!("lead"), "new field should be present");
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn update_rejects_known_collection_with_invalid_post_merge_shape() {
    // `update`'s validation surface is meaningfully distinct from
    // `create`'s: it gates the *post-merge* JSON, so a `--set` that
    // leaves a known-typed collection missing a required field surfaces
    // as `CliError::Malformed` (exit 4) only here. Drives the
    // `validate_against_schema` rejection arm in `cli/update.rs`.
    //
    // "capabilities" is a registered collection (peat-schema); building
    // only `name` against a missing doc upserts into `{name: "thermal"}`,
    // which fails `validate_capability` with `MissingField(id)`.
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    let creds_path = creds.to_string_lossy().into_owned();
    let output = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("peat")
            .unwrap()
            .args([
                "--creds",
                &creds_path,
                "--timeout",
                "15s",
                "update",
                "capabilities/cap-fresh",
                "--set",
                "name=thermal",
            ])
            .timeout(SCENARIO_TIMEOUT)
            .output()
            .expect("spawn peat")
    })
    .await
    .unwrap();

    assert_eq!(
        output.status.code(),
        Some(4),
        "expected exit 4 (Malformed)\nstdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("schema validation failed"),
        "stderr missing `schema validation failed`: {stderr}"
    );
    assert!(
        stderr.contains("Capability"),
        "stderr missing type name `Capability`: {stderr}"
    );
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn update_from_against_missing_doc_creates_it() {
    // Upsert semantics: missing doc → initial creation via `put` (no
    // delta to compute against).
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    let proposed_path = dir.path().join("proposed.json");
    std::fs::write(
        &proposed_path,
        serde_json::to_string(&json!({"name": "frank", "rank": 9})).unwrap(),
    )
    .unwrap();

    run_peat(
        &creds,
        &[
            "update",
            "contacts/c-new",
            "--from",
            proposed_path.to_str().unwrap(),
            "--wait-for-sync",
        ],
    )
    .await;

    let created = await_key(&peer, "contacts:c-new", POLL_DEADLINE).await;
    assert_eq!(created["name"], json!("frank"));
    assert_eq!(created["rank"], json!(9));
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn delete_tombstones_doc_on_peer() {
    let peer = TestPeer::start().await;
    let doc = json_to_automerge(&json!({"name": "alice"}), None).unwrap();
    peer.backend.store().put("contacts:c-1", &doc).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    let (stdout, _) = run_peat(&creds, &["delete", "contacts/c-1", "--wait-for-sync"]).await;
    assert!(stdout.contains("tombstone:contacts/c-1"));

    await_key_gone(&peer, "contacts:c-1", POLL_DEADLINE).await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn create_rejects_duplicate_id() {
    let peer = TestPeer::start().await;
    let doc = json_to_automerge(&json!({"name": "alice"}), None).unwrap();
    peer.backend
        .store()
        .put("contacts:c-existing", &doc)
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    // First create succeeded (preseed); now try create again with same id.
    let creds_path = creds.to_string_lossy().into_owned();
    let output = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("peat")
            .unwrap()
            .args([
                "--creds",
                &creds_path,
                "--timeout",
                "15s",
                "create",
                "contacts",
                "--id",
                "c-existing",
                "--set",
                "name=ignored",
            ])
            .timeout(SCENARIO_TIMEOUT)
            .output()
            .expect("spawn peat")
    })
    .await
    .unwrap();

    assert_eq!(
        output.status.code(),
        Some(4),
        "duplicate create should exit 4 (Malformed); stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("already exists"));
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn query_limit_caps_result_count() {
    let peer = TestPeer::start().await;
    for i in 0..10 {
        let doc = json_to_automerge(&json!({"i": i}), None).unwrap();
        peer.backend
            .store()
            .put(&format!("contacts:c-{i:02}"), &doc)
            .unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);
    let (stdout, _) = run_peat(
        &creds,
        &["--output", "json", "query", "contacts", "--limit", "3"],
    )
    .await;
    let parsed: Value = serde_json::from_str(&stdout).expect("stdout is JSON");
    assert_eq!(
        parsed.as_object().unwrap().len(),
        3,
        "--limit 3 should cap to 3 docs"
    );
}

// ---------------------------------------------------------------------
// Multi-binary scenarios: two real `peat` binary instances running
// concurrently or sequentially against the same in-process [`TestPeer`]
// rendezvous. Validates that data flows end-to-end across separate
// process boundaries — not just CLI ↔ in-process backend.
// ---------------------------------------------------------------------

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn observe_subprocess_streams_create_from_second_subprocess() {
    // Two real `peat` binaries: one running `observe`, one running
    // `create`. The observer must see the writer's record stream past on
    // stdout, proving the CLI's receive-side stream pump works when the
    // traffic is authored by another CLI binary (not by a direct
    // `TestPeer.store.put` shortcut).
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    let mut observer =
        topology::spawn_peat_streaming(&creds, &["observe", "contacts", "--output", "ndjson"]);

    // Give the observer's join handshake + subscription registration a
    // moment to complete before the writer subprocess starts. Without
    // this, the writer races the subscribe and the observer can miss
    // the event.
    tokio::time::sleep(Duration::from_secs(2)).await;

    run_peat(
        &creds,
        &[
            "create",
            "contacts",
            "--id",
            "c-bridge",
            "--set",
            "name=alice",
            "--wait-for-sync",
        ],
    )
    .await;

    topology::await_stdout_contains(&mut observer, "c-bridge", Duration::from_secs(15)).await;
    // observer is killed on Drop (kill_on_drop=true).
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn update_from_round_trip_across_two_subprocesses() {
    // Sequential cross-CLI round-trip-edit: CLI #1 seeds via `create`,
    // CLI #2 fetches the current state via `query --output json`, edits
    // it, and feeds it back via `update --from -`. Mirrors the operator
    // workflow documented in `crates/peat-cli/README.md` — every step is
    // a real subprocess invocation, no `TestPeer.store.put` shortcuts.
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    // CLI #1: seed.
    run_peat(
        &creds,
        &[
            "create",
            "contacts",
            "--id",
            "c-round",
            "--set",
            "name=alice",
            "--set",
            "rank=1",
            "--wait-for-sync",
        ],
    )
    .await;
    // Wait for the seed to materialise on the peer before reading it back.
    let baseline = await_key(&peer, "contacts:c-round", POLL_DEADLINE).await;

    // CLI #2: fetch current state as canonical JSON.
    let (fetched_stdout, _) =
        run_peat(&creds, &["--output", "json", "query", "contacts/c-round"]).await;
    let mut fetched: Value = serde_json::from_str(&fetched_stdout).expect("query stdout is JSON");
    fetched["rank"] = json!(7);
    fetched["tag"] = json!("lead");

    let proposed_path = dir.path().join("edited.json");
    std::fs::write(&proposed_path, serde_json::to_string(&fetched).unwrap()).unwrap();

    // CLI #3: apply the edit via the delta path.
    run_peat(
        &creds,
        &[
            "update",
            "contacts/c-round",
            "--from",
            proposed_path.to_str().unwrap(),
            "--wait-for-sync",
        ],
    )
    .await;

    let merged = await_key_change(&peer, "contacts:c-round", &baseline, POLL_DEADLINE).await;
    assert_eq!(merged["name"], json!("alice"), "unedited field preserved");
    assert_eq!(merged["rank"], json!(7), "edited field updated");
    assert_eq!(merged["tag"], json!("lead"), "new field appended");
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn observe_subprocess_streams_delete_from_second_subprocess() {
    // CDC delete-visibility, end-to-end (peat-mesh#202, rc.29). Two
    // real `peat` binaries: one running `observe`, one running
    // `delete`. The observer's stdout carries a
    // `{"key": "...", "deleted": true}` ndjson line for the tombstoned
    // doc — driving the `render_observe_deleted` path from a remote
    // tombstone arrival, not just a locally-observed race.
    //
    // The fix is upstream: peat-mesh `AutomergeStore::delete` now
    // fires `observer_tx`, so the tombstone-receive path
    // (`apply_tombstone` → `self.remove` → `store.delete`) wakes the
    // observer channel — matching the CDC contract documented on
    // `subscribe_to_observer_changes` ("fires for ALL document
    // changes"). ADR-001 Open Question §7 resolved.
    let peer = TestPeer::start().await;
    let seed = json_to_automerge(&json!({"name": "alice"}), None).unwrap();
    peer.backend.store().put("contacts:c-tomb", &seed).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    let mut observer =
        topology::spawn_peat_streaming(&creds, &["observe", "contacts", "--output", "ndjson"]);

    // Let the observer's join handshake + subscription settle before
    // the delete fires, otherwise the delete races the subscribe.
    tokio::time::sleep(Duration::from_secs(2)).await;

    run_peat(&creds, &["delete", "contacts/c-tomb", "--wait-for-sync"]).await;

    let seen =
        topology::await_stdout_contains(&mut observer, "\"deleted\":true", Duration::from_secs(15))
            .await;
    assert!(
        seen.contains("contacts:c-tomb"),
        "expected tombstone for c-tomb in observer stdout; saw:\n{seen}"
    );
}

// ---------------------------------------------------------------------
// peat-schema registered-type lifecycles. One scenario per builtin
// type (Capability / NodeConfig / NodeState / CellConfig / CellState):
// create → query (typed text render) → update --set → query (json
// verify) → delete → verify gone. Drives the
// `apply_proto3_defaults` + `validate_against_schema` accept arms +
// `render_typed_doc` dispatch + the full sync round-trip for each
// registered descriptor.
//
// Both create and update use `--set <field>=<value>` for the
// operator-ergonomic path. peat-node#112 lands the proto3 zero-
// defaulting that makes `--set` work on registered types — without it,
// prost's strict deserialize rejects partial payloads. These tests
// double as the integration-side proof that the defaults wire-up holds
// across the full sync round trip.
// ---------------------------------------------------------------------

/// Shared lifecycle driver for a registered Peat type.
///
/// Each call performs an independent six-step lifecycle on a fresh
/// `TestPeer`:
///   1. `create --set k=v…` supplying only the validator-required
///      fields. proto3 zero-defaults for siblings are populated by
///      peat-cli (peat-node#112).
///   2. `query --output text` — assert the typed renderer emits the
///      type-name header + the expected label/value substrings,
///   3. `query --output json` — assert structural parse,
///   4. `update --set k=v` to mutate one field (delta path),
///   5. `query --output json` confirms the merge landed,
///   6. `delete` and confirm the peer tombstones the doc.
async fn run_typed_lifecycle(
    collection: &str,
    doc_id: &str,
    type_name: &str,
    create_sets: &[&str],
    expect_text_contains: &[&str],
    update_set: &str,
    expect_json_after_update: impl Fn(&Value),
) {
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);
    let key = format!("{collection}:{doc_id}");
    let target = format!("{collection}/{doc_id}");

    // 1. create — only the validator-required fields, defaults fill the rest.
    let mut create_args: Vec<&str> = vec!["create", collection, "--id", doc_id];
    for s in create_sets {
        create_args.push("--set");
        create_args.push(s);
    }
    create_args.push("--wait-for-sync");
    run_peat(&creds, &create_args).await;
    let baseline = await_key(&peer, &key, POLL_DEADLINE).await;

    // 2. query --output text (typed render dispatch).
    let (text_stdout, _) = run_peat(&creds, &["--output", "text", "query", &target]).await;
    assert!(
        text_stdout.contains(type_name),
        "expected type-name header `{type_name}` in text output for {collection}; got:\n{text_stdout}"
    );
    for needle in expect_text_contains {
        assert!(
            text_stdout.contains(needle),
            "expected `{needle}` in text output for {collection}; got:\n{text_stdout}"
        );
    }

    // 3. query --output json (structural shape).
    let (json_stdout, _) = run_peat(&creds, &["--output", "json", "query", &target]).await;
    let _: Value = serde_json::from_str(&json_stdout).expect("query json stdout parses");

    // 4. update --set (delta path via AutomergeStore::diff).
    run_peat(
        &creds,
        &["update", &target, "--set", update_set, "--wait-for-sync"],
    )
    .await;

    // 5. verify the merge. Use change-detection rather than existence —
    // the key already exists from step 1, so plain `await_key` would
    // return the pre-update state on the first poll.
    let merged = await_key_change(&peer, &key, &baseline, POLL_DEADLINE).await;
    expect_json_after_update(&merged);

    // 6. delete + verify tombstoned.
    run_peat(&creds, &["delete", &target, "--wait-for-sync"]).await;
    await_key_gone(&peer, &key, POLL_DEADLINE).await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_capability_registered_type() {
    run_typed_lifecycle(
        "capabilities",
        "cap-1",
        "Capability",
        &["id=cap-1", "name=thermal-sensor", "confidence=0.9"],
        &["Capability", "thermal-sensor"],
        "name=thermal-sensor-v2",
        |merged| {
            assert_eq!(merged["id"], json!("cap-1"));
            assert_eq!(merged["name"], json!("thermal-sensor-v2"));
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_node_config_registered_type() {
    run_typed_lifecycle(
        "node-configs",
        "node-1",
        "NodeConfig",
        &[
            "id=node-1",
            "platform_type=platform-a",
            "comm_range_m=1500.0",
            "max_speed_mps=12.5",
        ],
        &["NodeConfig", "platform-a"],
        "platform_type=platform-b",
        |merged| {
            assert_eq!(merged["id"], json!("node-1"));
            assert_eq!(merged["platform_type"], json!("platform-b"));
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_node_state_registered_type() {
    // NodeState has no required scalar fields; defaults alone are a
    // valid document. Validator only constrains position lat/lon if
    // position is set — keep it null.
    run_typed_lifecycle(
        "node-states",
        "ns-1",
        "NodeState",
        &["fuel_minutes=30"],
        &["NodeState"],
        "fuel_minutes=45",
        |merged| {
            assert_eq!(merged["fuel_minutes"], json!(45));
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_cell_config_registered_type() {
    run_typed_lifecycle(
        "cell-configs",
        "cc-1",
        "CellConfig",
        &["id=cc-1", "min_size=2", "max_size=8"],
        &["CellConfig"],
        "max_size=12",
        |merged| {
            assert_eq!(merged["id"], json!("cc-1"));
            assert_eq!(merged["max_size"], json!(12));
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn query_all_collections_returns_keys_from_every_collection() {
    // `--all-collections` scans the full mesh store, returning docs
    // keyed by `<collection>:<doc_id>` across every reachable
    // collection. Authorization gating is formation-key-only today
    // (peat#941 deferred), so the bundle's read scope = the full store.
    let peer = TestPeer::start().await;
    peer.backend
        .store()
        .put(
            "contacts:c-1",
            &json_to_automerge(&json!({"name": "alice"}), None).unwrap(),
        )
        .unwrap();
    peer.backend
        .store()
        .put(
            "things:t-1",
            &json_to_automerge(&json!({"label": "widget"}), None).unwrap(),
        )
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    let (stdout, _) = run_peat(&creds, &["--output", "json", "query", "--all-collections"]).await;
    let parsed: Value = serde_json::from_str(&stdout).expect("stdout is JSON");
    let obj = parsed
        .as_object()
        .expect("--all-collections emits keyed object");
    assert!(
        obj.contains_key("contacts:c-1"),
        "expected contacts entry; got keys {:?}",
        obj.keys().collect::<Vec<_>>()
    );
    assert!(
        obj.contains_key("things:t-1"),
        "expected things entry; got keys {:?}",
        obj.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn observe_all_collections_streams_events_from_every_collection() {
    // Cross-collection observer: one `peat observe --all` subprocess
    // should see ndjson events for writes against multiple
    // collections, not just one.
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);

    let mut observer = topology::spawn_peat_streaming(
        &creds,
        &["observe", "--all-collections", "--output", "ndjson"],
    );

    // Allow the observer's join handshake + subscription to settle
    // before the writers fire.
    tokio::time::sleep(Duration::from_secs(2)).await;

    run_peat(
        &creds,
        &[
            "create",
            "contacts",
            "--id",
            "c-2",
            "--set",
            "name=carol",
            "--wait-for-sync",
        ],
    )
    .await;
    run_peat(
        &creds,
        &[
            "create",
            "things",
            "--id",
            "t-2",
            "--set",
            "label=gadget",
            "--wait-for-sync",
        ],
    )
    .await;

    // Observer must see BOTH collections in its stdout — the
    // `--all-collections` flag turns off the prefix filter so every
    // event reaches the renderer.
    let seen =
        topology::await_stdout_contains(&mut observer, "things:t-2", Duration::from_secs(20)).await;
    assert!(
        seen.contains("contacts:c-2"),
        "expected contacts event in observer stdout; saw:\n{seen}"
    );
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_cell_state_registered_type() {
    // CellState has no required scalar fields; defaults alone are valid.
    // `leader_id` would force a paired `members` mutation (the validator
    // demands leader_id ∈ members), so anchor on `platoon_id` instead —
    // it's an `optional string` with no cross-field constraint, suitable
    // for round-tripping an arbitrary opaque identifier.
    run_typed_lifecycle(
        "cell-states",
        "cs-1",
        "CellState",
        &["platoon_id=g-1"],
        &["CellState"],
        "platoon_id=g-2",
        |merged| {
            assert_eq!(merged["platoon_id"], json!("g-2"));
        },
    )
    .await;
}
