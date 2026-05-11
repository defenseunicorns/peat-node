//! Subprocess-driven two-node sync test — closes #44.
//!
//! Spawns two real `peat-node` binaries (via `CARGO_BIN_EXE_peat-node`),
//! peers them over direct UDP, drives Platform + Document writes both
//! directions, then asserts `GetSyncStats.bytes_sent` / `bytes_received`
//! are non-zero on both sides.
//!
//! This is the test path the deleted Go `test/go/cmd/synctest` used to
//! cover. The in-process `tests/sync_test.rs` covers the same byte-
//! counter assertion using two in-process SidecarNodes — the apparent
//! "in-process counters stay zero" claim that earlier comments here
//! made turned out to be a JSON-parsing bug in the test, not a
//! limitation of the in-process path. This subprocess variant exists
//! independently because driving the real binary catches a class of
//! bugs (CLI parsing, process lifecycle, real Iroh QUIC over a real
//! UDP socket bound to a real port) that the in-process test can't.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};

const A_GRPC: u16 = 50095;
const A_IROH: u16 = 51195;
const B_GRPC: u16 = 50096;
const B_IROH: u16 = 51196;

/// Kills the wrapped child on drop so a panicking test doesn't orphan binaries.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

fn spawn_node(
    bin: &str,
    grpc_port: u16,
    iroh_port: u16,
    node_id: &str,
    data_dir: &Path,
) -> ChildGuard {
    let child = Command::new(bin)
        .arg("--listen")
        .arg(format!("tcp://127.0.0.1:{grpc_port}"))
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--node-id")
        .arg(node_id)
        .arg("--iroh-udp-port")
        .arg(iroh_port.to_string())
        .arg("--auto-sync")
        .env("RUST_LOG", "peat_node=warn,peat_mesh=warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn peat-node");
    ChildGuard(child)
}

async fn call(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/{method}");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "{method} returned {status}: {text}");
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

#[tokio::test]
async fn two_subprocess_sync_increments_byte_counters() {
    let bin = env!("CARGO_BIN_EXE_peat-node");

    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    let _node_a = spawn_node(bin, A_GRPC, A_IROH, "node-a", dir_a.path());
    let _node_b = spawn_node(bin, B_GRPC, B_IROH, "node-b", dir_b.path());

    // Static settle, mirroring the previously-working Go synctest's
    // 2-second sleep after process spawn. An aggressive `wait_ready`
    // poll loop races with the Iroh endpoint's address-publish step in
    // ways that observably suppress per-peer counter increments later;
    // the calmer fixed sleep avoids that interaction.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let base_a = format!("http://127.0.0.1:{A_GRPC}");
    let base_b = format!("http://127.0.0.1:{B_GRPC}");

    // Two explicit Status calls, also mirroring the Go reference.
    let status_a = call(&client, &base_a, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"]
        .as_str()
        .expect("GetStatus.endpointAddr missing")
        .to_string();
    let _status_b = call(&client, &base_b, "GetStatus", serde_json::json!({})).await;

    // Peer B -> A via direct UDP. No relay.
    call(
        &client,
        &base_b,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_a,
            "addresses": [format!("127.0.0.1:{A_IROH}")],
        }),
    )
    .await;

    // Settle for handshake.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ListPeers verification (the Go test asserts >= 1 here).
    let peers_b = call(&client, &base_b, "ListPeers", serde_json::json!({})).await;
    let peer_count = peers_b["peers"].as_array().map(|a| a.len()).unwrap_or(0);
    assert!(
        peer_count >= 1,
        "node-b should report >= 1 peer, got {peer_count}"
    );

    // Explicit StartSync — Go test calls these even with --auto-sync.
    call(&client, &base_a, "StartSync", serde_json::json!({})).await;
    call(&client, &base_b, "StartSync", serde_json::json!({})).await;

    // Drive writes on A (Platform + Document), mirroring the deleted
    // Go synctest's pattern that empirically produced non-zero counter
    // exchanges on every CI run. A single tiny document write isn't
    // always enough to land bytes through the cooldown-protected sync
    // path; a typed Platform plus a generic doc reliably is.
    call(
        &client,
        &base_a,
        "PutPlatform",
        serde_json::json!({
            "platform": {
                "id": "cluster-alpha-agent",
                "platformType": "uds-remote-agent",
                "name": "UDS Remote Agent @ cluster-alpha",
                "status": "PLATFORM_STATUS_READY",
                "latitude": 38.8977,
                "longitude": -77.0365,
                "capabilities": ["package-management", "registry-sync"]
            }
        }),
    )
    .await;
    call(
        &client,
        &base_a,
        "PutDocument",
        serde_json::json!({
            "collection": "deployments",
            "docId": "app-v2",
            "jsonData": r#"{"name":"mission-app","version":"2.0.0","status":"deployed"}"#,
        }),
    )
    .await;

    // Confirm B sees A's platform.
    poll_for_platforms(&client, &base_b, "cluster-alpha-agent").await;

    // Also fetch the deployment document on B — Go reference does this
    // and the extra GetDocument shapes the in-flight sync state.
    let _ = call(
        &client,
        &base_b,
        "GetDocument",
        serde_json::json!({"collection":"deployments","docId":"app-v2"}),
    )
    .await;

    // Bidirectional: write on B, poll A. The byte counters in the
    // deleted Go synctest only became non-zero after both directions
    // had moved data.
    call(
        &client,
        &base_b,
        "PutPlatform",
        serde_json::json!({
            "platform": {
                "id": "cluster-bravo-agent",
                "platformType": "uds-remote-agent",
                "name": "UDS Remote Agent @ cluster-bravo",
                "status": "PLATFORM_STATUS_READY",
                "latitude": 34.0522,
                "longitude": -118.2437,
                "capabilities": ["package-management"]
            }
        }),
    )
    .await;
    poll_for_platforms(&client, &base_a, "cluster-bravo-agent").await;

    // The core assertion #44 exists to recover: counters must show real
    // traffic moved.
    //
    // All `total_bytes_sent.fetch_add` call sites in peat-mesh
    // v0.9.0-rc.7 live inside Negentropy (ADR-040 set-reconciliation)
    // paths — `send_negentropy_init`, `sync_with_peer_negentropy`, and
    // their handlers. The per-document sync push triggered by writes
    // does NOT touch the counters; Negentropy's periodic rounds do.
    // Wait long enough for at least one Negentropy round to complete
    // before sampling. The deleted Go synctest routinely saw ~8 KB. Poll
    // briefly to give the atomic increments observation time after the
    // doc surfaces — `Ordering::Relaxed` is monotonic but not strictly
    // ordered against the GetDocument response.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut bytes_sent_a;
    let mut bytes_received_a;
    let mut bytes_sent_b;
    let mut bytes_received_b;
    loop {
        let stats_a = call(&client, &base_a, "GetSyncStats", serde_json::json!({})).await;
        let stats_b = call(&client, &base_b, "GetSyncStats", serde_json::json!({})).await;
        bytes_sent_a = json_u64(&stats_a["bytesSent"]);
        bytes_received_a = json_u64(&stats_a["bytesReceived"]);
        bytes_sent_b = json_u64(&stats_b["bytesSent"]);
        bytes_received_b = json_u64(&stats_b["bytesReceived"]);
        let all_nonzero =
            bytes_sent_a > 0 && bytes_received_a > 0 && bytes_sent_b > 0 && bytes_received_b > 0;
        if all_nonzero || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        bytes_sent_a > 0,
        "node-a bytes_sent == 0 after observed sync"
    );
    assert!(
        bytes_received_a > 0,
        "node-a bytes_received == 0 after observed sync"
    );
    assert!(
        bytes_sent_b > 0,
        "node-b bytes_sent == 0 after observed sync"
    );
    assert!(
        bytes_received_b > 0,
        "node-b bytes_received == 0 after observed sync"
    );
}

/// Proto3 JSON encodes `uint64` as a *string* (preserves precision past
/// JSON's 53-bit double mantissa). Some encoders emit numbers for small
/// values; handle both forms.
fn json_u64(v: &serde_json::Value) -> u64 {
    if let Some(n) = v.as_u64() {
        return n;
    }
    if let Some(s) = v.as_str() {
        return s.parse().unwrap_or(0);
    }
    0
}

async fn poll_for_platforms(client: &reqwest::Client, base: &str, want_id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let resp = call(client, base, "GetPlatforms", serde_json::json!({})).await;
        if let Some(arr) = resp["platforms"].as_array() {
            if arr.iter().any(|p| p["id"].as_str() == Some(want_id)) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("platform {want_id} did not sync to {base} within 30s");
}
