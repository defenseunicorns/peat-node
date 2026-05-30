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

    let observed = await_key(&peer, "contacts:c-new", Duration::from_secs(10)).await;
    assert_eq!(observed["name"], json!("dave"));
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn update_set_modifies_existing_doc() {
    let peer = TestPeer::start().await;
    let doc = json_to_automerge(&json!({"name": "alice", "rank": 1}), None).unwrap();
    peer.backend.store().put("contacts:c-1", &doc).unwrap();

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

    let updated = await_key(&peer, "contacts:c-1", Duration::from_secs(10)).await;
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

    let created = await_key(&peer, "contacts:c-fresh", Duration::from_secs(10)).await;
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

    let merged = await_key(&peer, "contacts:c-1", Duration::from_secs(10)).await;
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

    let created = await_key(&peer, "contacts:c-new", Duration::from_secs(10)).await;
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

    await_key_gone(&peer, "contacts:c-1", Duration::from_secs(10)).await;
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
    await_key(&peer, "contacts:c-round", Duration::from_secs(10)).await;

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

    let merged = await_key(&peer, "contacts:c-round", Duration::from_secs(10)).await;
    assert_eq!(merged["name"], json!("alice"), "unedited field preserved");
    assert_eq!(merged["rank"], json!(7), "edited field updated");
    assert_eq!(merged["tag"], json!("lead"), "new field appended");
}

// Note: observer-side reception of a remote tombstone is intentionally NOT
// exercised by an e2e scenario. peat-mesh's `apply_tombstone` receive path
// calls `store.delete()`, which does not fire `observer_tx` — the channel
// `peat observe` subscribes to. The `render_observe_deleted` arm in
// `cli/observe.rs` is therefore unreachable from a remote tombstone
// arrival today; the same code does fire correctly on a locally-observed
// concurrent put-then-tombstone race, but that's not reproducible
// deterministically from an e2e harness. Tracked as an upstream
// peat-mesh API gap in ADR-001 Open Questions §7.

// ---------------------------------------------------------------------
// peat-schema registered-type lifecycles. One scenario per builtin
// type (Capability / NodeConfig / NodeState / CellConfig / CellState):
// create → query (typed text render) → update --from → query (json
// verify) → delete → verify gone. Drives the
// `validate_against_schema` accept arm + `render_typed_doc` dispatch
// + the full sync round-trip for each registered descriptor.
//
// All registered types route through prost's serde derive, which
// rejects missing fields (no `#[serde(default)]`). So both create and
// update must supply a complete proto3 JSON value — `--set` partial
// payloads fail. The helper writes the supplied JSON to a tempfile and
// uses `--from` for that reason. peat-cli's UX for these types would
// be improved by zero-defaulting on `--set`; tracked as a follow-up.
// ---------------------------------------------------------------------

/// Shared lifecycle driver for a registered Peat type.
///
/// Each call performs an independent six-step lifecycle on a fresh
/// `TestPeer`:
///   1. `create --from <path>` with a complete valid proto3 JSON value,
///   2. `query --output text` — assert the typed renderer emits the
///      type-name header + the expected label/value substrings,
///   3. `query --output json` — assert structural parse,
///   4. `update --from <path>` with the mutated value (real delta path),
///   5. `query --output json` confirms the merge landed,
///   6. `delete` and confirm the peer tombstones the doc.
async fn run_typed_lifecycle(
    collection: &str,
    doc_id: &str,
    type_name: &str,
    initial: Value,
    mutated: Value,
    expect_text_contains: &[&str],
    expect_json_after_update: impl Fn(&Value),
) {
    let peer = TestPeer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let creds = peer.creds_tempfile(&dir);
    let key = format!("{collection}:{doc_id}");
    let target = format!("{collection}/{doc_id}");

    let initial_path = dir.path().join(format!("{doc_id}-initial.json"));
    let mutated_path = dir.path().join(format!("{doc_id}-mutated.json"));
    std::fs::write(&initial_path, serde_json::to_string(&initial).unwrap()).unwrap();
    std::fs::write(&mutated_path, serde_json::to_string(&mutated).unwrap()).unwrap();

    // 1. create.
    run_peat(
        &creds,
        &[
            "create",
            collection,
            "--id",
            doc_id,
            "--from",
            initial_path.to_str().unwrap(),
            "--wait-for-sync",
        ],
    )
    .await;
    await_key(&peer, &key, Duration::from_secs(10)).await;

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

    // 4. update --from (real delta path via AutomergeStore::diff).
    run_peat(
        &creds,
        &[
            "update",
            &target,
            "--from",
            mutated_path.to_str().unwrap(),
            "--wait-for-sync",
        ],
    )
    .await;

    // 5. verify the merge.
    let merged = await_key(&peer, &key, Duration::from_secs(10)).await;
    expect_json_after_update(&merged);

    // 6. delete + verify tombstoned.
    run_peat(&creds, &["delete", &target, "--wait-for-sync"]).await;
    await_key_gone(&peer, &key, Duration::from_secs(10)).await;
}

/// Minimal valid Capability — `id`, `name` required; `confidence` in
/// [0, 1]; sibling proto3 fields zero-defaulted to satisfy prost's
/// strict deserialiser.
fn capability_value(id: &str, name: &str, confidence: f64) -> Value {
    json!({
        "id": id,
        "name": name,
        "capability_type": 0,
        "confidence": confidence,
        "metadata_json": "",
        "registered_at": null,
    })
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_capability_registered_type() {
    run_typed_lifecycle(
        "capabilities",
        "cap-1",
        "Capability",
        capability_value("cap-1", "thermal-sensor", 0.9),
        capability_value("cap-1", "thermal-sensor-v2", 0.95),
        &["Capability", "thermal-sensor"],
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
    // NodeConfig fields per proto: id, platform_type, capabilities[],
    // comm_range_m (must be > 0), max_speed_mps (must be > 0),
    // operator_binding (optional), created_at.
    let initial = json!({
        "id": "node-1",
        "platform_type": "ground-rover",
        "capabilities": [],
        "comm_range_m": 1500.0,
        "max_speed_mps": 12.5,
        "operator_binding": null,
        "created_at": null,
    });
    let mut mutated = initial.clone();
    mutated["platform_type"] = json!("aerial-quad");
    mutated["max_speed_mps"] = json!(30.0);
    run_typed_lifecycle(
        "node-configs",
        "node-1",
        "NodeConfig",
        initial,
        mutated,
        &["NodeConfig", "ground-rover"],
        |merged| {
            assert_eq!(merged["id"], json!("node-1"));
            assert_eq!(merged["platform_type"], json!("aerial-quad"));
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_node_state_registered_type() {
    // NodeState has no `id` field — the doc-id (`ns-1`) lives in the
    // store key. Validation only kicks in for position lat/lon range;
    // every proto3 sibling (including enum-typed fields like `health`
    // and `phase`, which prost requires) is populated with its zero
    // value to satisfy the strict deserialiser.
    let initial = json!({
        "position": null,
        "fuel_minutes": 0,
        "health": 0,
        "phase": 0,
        "cell_id": null,
        "zone_id": null,
        "timestamp": null,
    });
    let mut mutated = initial.clone();
    mutated["fuel_minutes"] = json!(45);
    run_typed_lifecycle(
        "node-states",
        "ns-1",
        "NodeState",
        initial,
        mutated,
        &["NodeState"],
        |merged| {
            assert_eq!(merged["fuel_minutes"], json!(45));
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_cell_config_registered_type() {
    // CellConfig fields per proto: id, max_size, min_size, created_at.
    // Validation: id non-empty, max_size >= min_size, min_size >= 2.
    let initial = json!({
        "id": "cc-1",
        "max_size": 8,
        "min_size": 2,
        "created_at": null,
    });
    let mut mutated = initial.clone();
    mutated["max_size"] = json!(12);
    run_typed_lifecycle(
        "cell-configs",
        "cc-1",
        "CellConfig",
        initial,
        mutated,
        &["CellConfig"],
        |merged| {
            assert_eq!(merged["id"], json!("cc-1"));
            assert_eq!(merged["max_size"], json!(12));
        },
    )
    .await;
}

#[tokio::test]
#[serial_test::serial(peat_cli_two_party)]
async fn lifecycle_cell_state_registered_type() {
    // CellState fields per proto: config, leader_id (optional),
    // members[], capabilities[], platoon_id (optional), timestamp.
    // No `id` field — doc-id (`cs-1`) lives in the store key.
    // Validation kicks in only when `config`, `capabilities`, or
    // `leader_id` are populated; keeping them empty exercises the
    // wiring without entangling validation logic from sibling types.
    let initial = json!({
        "config": null,
        "leader_id": null,
        "members": [],
        "capabilities": [],
        "platoon_id": null,
        "timestamp": null,
    });
    let mut mutated = initial.clone();
    mutated["members"] = json!(["node-1", "node-2"]);
    run_typed_lifecycle(
        "cell-states",
        "cs-1",
        "CellState",
        initial,
        mutated,
        &["CellState"],
        |merged| {
            assert_eq!(merged["members"], json!(["node-1", "node-2"]));
        },
    )
    .await;
}
