//! DDIL partition behavior — two peers disconnect, then reconnect,
//! then sync resumes for newly-issued writes.
//!
//! This is the minimum claim of `docs/DESIGN.md`'s partition-tolerance
//! story: connectivity drops, returns, sync resumes. The stronger
//! claim — that writes made *during* the partition catch up on
//! reconnect — depends on `sync_all_documents_with_peer` /
//! Negentropy reconciliation behavior that doesn't fire reliably in
//! the same-process Iroh reconnect path observed here; that scenario
//! is tracked separately as a real-network exercise (likely belongs
//! in `test/cross-cluster-sync.sh` or its successor).

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

async fn boot(grpc_port: u16, iroh_port: u16) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: format!("test-{grpc_port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: Some(iroh_port),
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
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "{method} returned {status}: {text}");
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

async fn poll_for_doc(
    client: &reqwest::Client,
    base: &str,
    collection: &str,
    doc_id: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let resp = call(
            client,
            base,
            "GetDocument",
            serde_json::json!({"collection":collection,"docId":doc_id}),
        )
        .await;
        if let Some(d) = resp["jsonData"].as_str() {
            return Some(d.to_string());
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    None
}

#[tokio::test]
async fn sync_resumes_after_disconnect_and_reconnect() {
    let (client_a, base_a) = boot(50141, 51241).await;
    let (client_b, base_b) = boot(50142, 51242).await;

    let status_a = call(&client_a, &base_a, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"].as_str().unwrap().to_string();
    call(
        &client_b,
        &base_b,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_a,
            "addresses": [format!("127.0.0.1:{}", 51241)],
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    call(&client_a, &base_a, "StartSync", serde_json::json!({})).await;
    call(&client_b, &base_b, "StartSync", serde_json::json!({})).await;

    // Pre-disconnect: a write on A reaches B.
    call(
        &client_a,
        &base_a,
        "PutDocument",
        serde_json::json!({
            "collection": "ddil",
            "docId": "pre-disconnect",
            "jsonData": r#"{"phase":"pre"}"#,
        }),
    )
    .await;
    poll_for_doc(
        &client_b,
        &base_b,
        "ddil",
        "pre-disconnect",
        Duration::from_secs(15),
    )
    .await
    .expect("pre-disconnect doc did not reach B");

    // Disconnect B's view of A. The "did the disconnect stick"
    // assertion is racy — background sync tasks can re-establish via
    // `transport.get_or_connect`; it's exercised in
    // `sync_control_test.rs::disconnect_peer_empties_peer_list` in a
    // setting without ongoing sync activity. Here we only need the
    // disconnect to perturb the connection enough that the reconnect
    // step is meaningful.
    call(
        &client_b,
        &base_b,
        "DisconnectPeer",
        serde_json::json!({"endpointId": endpoint_a}),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Reconnect.
    call(
        &client_b,
        &base_b,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_a,
            "addresses": [format!("127.0.0.1:{}", 51241)],
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    call(&client_a, &base_a, "StartSync", serde_json::json!({})).await;
    call(&client_b, &base_b, "StartSync", serde_json::json!({})).await;

    // Post-reconnect: a NEW write on A reaches B. This is the minimum
    // post-heal sync claim — catch-up of writes made *during* the
    // partition is a stronger claim and isn't exercised here (see
    // file-level comment).
    call(
        &client_a,
        &base_a,
        "PutDocument",
        serde_json::json!({
            "collection": "ddil",
            "docId": "post-reconnect",
            "jsonData": r#"{"phase":"post"}"#,
        }),
    )
    .await;
    poll_for_doc(
        &client_b,
        &base_b,
        "ddil",
        "post-reconnect",
        Duration::from_secs(30),
    )
    .await
    .expect("post-reconnect doc did not reach B within 30s");
}
