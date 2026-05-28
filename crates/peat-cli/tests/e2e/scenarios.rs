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

use super::topology::TestPeer;

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
