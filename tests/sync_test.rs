//! Two-node CRDT sync — replaces the former Go `test/go/cmd/synctest`.
//!
//! Boots two `SidecarNode`s in-process on different gRPC and Iroh UDP
//! ports, peers them via direct UDP addressing (no relay), writes a
//! document on node-a, polls node-b until it appears, then asserts:
//!
//! - the document content matches end-to-end,
//! - bidirectional sync works (node-b -> node-a too),
//! - `GetSyncStats.bytes_sent` and `.bytes_received` are non-zero on
//!   both nodes after sync has actually moved data.
//!
//! The test drives the public Connect-RPC surface via plain HTTP+JSON
//! (Connect protocol) using `reqwest` — same shape as `grpc_test.rs`.

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

async fn boot_server(grpc_port: u16, iroh_udp_port: u16) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("test-{grpc_port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: Some(iroh_udp_port),
            attachment_config: Default::default(),
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
        })
        .await
        .unwrap(),
    );

    let service = Arc::new(PeatSidecarService::new(node));
    let router = service.register(connectrpc::Router::new());
    let addr: std::net::SocketAddr = format!("127.0.0.1:{grpc_port}").parse().unwrap();

    tokio::spawn(async move {
        connectrpc::Server::new(router).serve(addr).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    (client, format!("http://127.0.0.1:{grpc_port}"))
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

    assert!(
        resp.status().is_success(),
        "{method} returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    resp.json().await.unwrap()
}

#[tokio::test]
async fn two_node_direct_udp_sync() {
    // Distinct port pairs so this test can run alongside `grpc_test.rs`.
    const A_GRPC: u16 = 50091;
    const A_IROH: u16 = 51191;
    const B_GRPC: u16 = 50092;
    const B_IROH: u16 = 51192;

    let (client_a, base_a) = boot_server(A_GRPC, A_IROH).await;
    let (client_b, base_b) = boot_server(B_GRPC, B_IROH).await;

    // Both endpoints up — fetch A's endpoint id.
    let status_a = call(&client_a, &base_a, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"]
        .as_str()
        .expect("endpointAddr missing on GetStatus")
        .to_string();

    // Peer B -> A via direct UDP. No relay.
    call(
        &client_b,
        &base_b,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_a,
            "addresses": [format!("127.0.0.1:{A_IROH}")],
        }),
    )
    .await;

    // Brief settle for handshake.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // StartSync on both. (auto-sync isn't on here since we constructed
    // the nodes directly, not via the binary.)
    call(&client_a, &base_a, "StartSync", serde_json::json!({})).await;
    call(&client_b, &base_b, "StartSync", serde_json::json!({})).await;

    // Write on A.
    call(
        &client_a,
        &base_a,
        "PutDocument",
        serde_json::json!({
            "collection": "sync-test",
            "docId": "from-a",
            "jsonData": r#"{"origin":"a"}"#,
        }),
    )
    .await;

    // Poll B for the doc.
    let want_a = r#"{"origin":"a"}"#;
    let got_b = poll_for_document(&client_b, &base_b, "sync-test", "from-a", want_a).await;
    assert_eq!(got_b, want_a, "doc content mismatch on node-b after sync");

    // Bidirectional: write on B, poll A.
    call(
        &client_b,
        &base_b,
        "PutDocument",
        serde_json::json!({
            "collection": "sync-test",
            "docId": "from-b",
            "jsonData": r#"{"origin":"b"}"#,
        }),
    )
    .await;
    let want_b = r#"{"origin":"b"}"#;
    let got_a = poll_for_document(&client_a, &base_a, "sync-test", "from-b", want_b).await;
    assert_eq!(got_a, want_b, "doc content mismatch on node-a after sync");

    // Connection liveness + byte counters. Proto3 JSON encodes `uint64`
    // as a string to preserve precision past JSON's 53-bit mantissa, so
    // a naive `as_u64()` returns None and silently degrades to zero —
    // exactly the trap that made these counters look like they didn't
    // increment in-process. Use `json_u64` instead. The wiring works
    // fine; the bug was in how the test parsed the response.
    let stats_a = call(&client_a, &base_a, "GetSyncStats", serde_json::json!({})).await;
    let stats_b = call(&client_b, &base_b, "GetSyncStats", serde_json::json!({})).await;
    let peers_a = json_u64(&stats_a["connectedPeers"]);
    let peers_b = json_u64(&stats_b["connectedPeers"]);
    assert!(peers_a >= 1, "node-a should see >= 1 peer, got {peers_a}");
    assert!(peers_b >= 1, "node-b should see >= 1 peer, got {peers_b}");

    let bytes_sent_a = json_u64(&stats_a["bytesSent"]);
    let bytes_received_a = json_u64(&stats_a["bytesReceived"]);
    let bytes_sent_b = json_u64(&stats_b["bytesSent"]);
    let bytes_received_b = json_u64(&stats_b["bytesReceived"]);
    assert!(
        bytes_sent_a > 0 && bytes_received_a > 0,
        "node-a sync stats zero after sync: sent={bytes_sent_a} received={bytes_received_a}"
    );
    assert!(
        bytes_sent_b > 0 && bytes_received_b > 0,
        "node-b sync stats zero after sync: sent={bytes_sent_b} received={bytes_received_b}"
    );
}

/// Proto3 JSON encodes `uint64` as a string. Handle both forms (some
/// encoders emit small values as numbers).
fn json_u64(v: &serde_json::Value) -> u64 {
    if let Some(n) = v.as_u64() {
        return n;
    }
    if let Some(s) = v.as_str() {
        return s.parse().unwrap_or(0);
    }
    0
}

async fn poll_for_document(
    client: &reqwest::Client,
    base: &str,
    collection: &str,
    doc_id: &str,
    expected_substr: &str,
) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let resp = call(
            client,
            base,
            "GetDocument",
            serde_json::json!({ "collection": collection, "docId": doc_id }),
        )
        .await;
        if let Some(data) = resp["jsonData"].as_str() {
            if data.contains(expected_substr) {
                return data.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("document {collection}/{doc_id} did not sync within 30s");
}
