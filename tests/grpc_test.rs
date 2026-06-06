//! End-to-end functional tests: boots a full Connect RPC server and exercises
//! the API through HTTP/JSON (Connect protocol), validating both plaintext
//! and encrypted modes.
//!
//! Uses plain reqwest as the client — this tests the actual Connect protocol
//! (HTTP + JSON) rather than depending on a specific RPC client library.

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

/// Boot a Connect RPC server on the given port and return a reqwest client + base URL.
async fn boot_server(port: u16, encryption_key: Option<String>) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("test-{port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key,
            iroh_udp_port: None,
            attachment_config: Default::default(),
        })
        .await
        .unwrap(),
    );

    let service = Arc::new(PeatSidecarService::new(node));
    let router = service.register(connectrpc::Router::new());
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    tokio::spawn(async move {
        connectrpc::Server::new(router).serve(addr).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    (client, format!("http://127.0.0.1:{port}"))
}

/// Call a Connect RPC unary method with JSON encoding.
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
        "{method} returned {}",
        resp.status()
    );
    resp.json().await.unwrap()
}

#[tokio::test]
async fn connect_protocol_full_crud_plaintext() {
    let (client, base) = boot_server(50081, None).await;

    // GetStatus
    let status = call(&client, &base, "GetStatus", serde_json::json!({})).await;
    assert_eq!(status["nodeId"], "test-50081");
    assert!(!status["endpointAddr"].as_str().unwrap().is_empty());

    // PutDocument
    call(
        &client,
        &base,
        "PutDocument",
        serde_json::json!({
            "collection": "test",
            "docId": "doc-1",
            "jsonData": r#"{"hello":"world"}"#
        }),
    )
    .await;

    // GetDocument
    let doc = call(
        &client,
        &base,
        "GetDocument",
        serde_json::json!({"collection": "test", "docId": "doc-1"}),
    )
    .await;
    assert_eq!(doc["jsonData"], r#"{"hello":"world"}"#);

    // ListDocuments
    let list = call(
        &client,
        &base,
        "ListDocuments",
        serde_json::json!({"collection": "test"}),
    )
    .await;
    assert_eq!(list["docIds"], serde_json::json!(["doc-1"]));

    // DeleteDocument
    call(
        &client,
        &base,
        "DeleteDocument",
        serde_json::json!({"collection": "test", "docId": "doc-1"}),
    )
    .await;

    let doc = call(
        &client,
        &base,
        "GetDocument",
        serde_json::json!({"collection": "test", "docId": "doc-1"}),
    )
    .await;
    assert!(doc.get("jsonData").is_none() || doc["jsonData"].is_null());

    // GetSyncStats
    let stats = call(&client, &base, "GetSyncStats", serde_json::json!({})).await;
    assert_eq!(stats.get("connectedPeers"), None); // 0 is omitted by proto3 JSON

    // PutNode (typed collection)
    call(
        &client,
        &base,
        "PutNode",
        serde_json::json!({
            "node": {
                "id": "plat-1",
                "nodeType": "uds-remote-agent",
                "name": "test-agent",
                "status": "NODE_STATUS_READY",
                "latitude": 38.89,
                "longitude": -77.03,
                "capabilities": ["deploy", "monitor"]
            }
        }),
    )
    .await;

    // GetNodes
    let nodes = call(&client, &base, "GetNodes", serde_json::json!({})).await;
    let plats = nodes["nodes"].as_array().unwrap();
    assert_eq!(plats.len(), 1);
    assert_eq!(plats[0]["id"], "plat-1");
    assert_eq!(plats[0]["name"], "test-agent");
}

#[tokio::test]
async fn connect_protocol_full_crud_encrypted() {
    use base64::Engine;
    let key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    let (client, base) = boot_server(50082, Some(key)).await;

    // PutDocument (encrypted at rest)
    call(
        &client,
        &base,
        "PutDocument",
        serde_json::json!({
            "collection": "secure",
            "docId": "secret-1",
            "jsonData": r#"{"classified":"top-secret"}"#
        }),
    )
    .await;

    // GetDocument (decrypted transparently)
    let doc = call(
        &client,
        &base,
        "GetDocument",
        serde_json::json!({"collection": "secure", "docId": "secret-1"}),
    )
    .await;
    assert_eq!(doc["jsonData"], r#"{"classified":"top-secret"}"#);

    // Overwrite
    call(
        &client,
        &base,
        "PutDocument",
        serde_json::json!({
            "collection": "secure",
            "docId": "secret-1",
            "jsonData": r#"{"classified":"updated"}"#
        }),
    )
    .await;

    let doc = call(
        &client,
        &base,
        "GetDocument",
        serde_json::json!({"collection": "secure", "docId": "secret-1"}),
    )
    .await;
    assert_eq!(doc["jsonData"], r#"{"classified":"updated"}"#);

    // List
    let list = call(
        &client,
        &base,
        "ListDocuments",
        serde_json::json!({"collection": "secure"}),
    )
    .await;
    assert_eq!(list["docIds"], serde_json::json!(["secret-1"]));

    // Delete
    call(
        &client,
        &base,
        "DeleteDocument",
        serde_json::json!({"collection": "secure", "docId": "secret-1"}),
    )
    .await;

    let doc = call(
        &client,
        &base,
        "GetDocument",
        serde_json::json!({"collection": "secure", "docId": "secret-1"}),
    )
    .await;
    assert!(doc.get("jsonData").is_none() || doc["jsonData"].is_null());
}

#[tokio::test]
async fn connect_protocol_peer_and_sync_ops() {
    let (client, base) = boot_server(50083, None).await;

    // ListPeers (should be empty)
    let peers = call(&client, &base, "ListPeers", serde_json::json!({})).await;
    assert!(peers.get("peers").is_none() || peers["peers"].as_array().unwrap().is_empty());

    // StartSync / StopSync
    call(&client, &base, "StartSync", serde_json::json!({})).await;
    let stats = call(&client, &base, "GetSyncStats", serde_json::json!({})).await;
    assert_eq!(stats["syncActive"], true);

    call(&client, &base, "StopSync", serde_json::json!({})).await;
    let stats = call(&client, &base, "GetSyncStats", serde_json::json!({})).await;
    // syncActive=false is omitted by proto3 JSON (default value)
    assert!(stats.get("syncActive").is_none() || stats["syncActive"] == false);
}

#[tokio::test]
async fn connect_peer_rejects_empty_addressing() {
    // ConnectPeer with neither `addresses` nor `relay_url` must error
    // explicitly — the previous behavior (silent 10-second wait for a
    // relay URL, then opaque 500) was the original peer-reported bug.
    let (client, base) = boot_server(50084, None).await;

    // A real-enough endpoint id (32 bytes hex). We never actually try to
    // connect; the empty-addressing check fails before the handshake.
    let dummy_endpoint_id = "0".repeat(64);

    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/ConnectPeer");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .json(&serde_json::json!({ "endpointId": dummy_endpoint_id }))
        .send()
        .await
        .unwrap();

    assert!(
        !resp.status().is_success(),
        "expected ConnectPeer with no addresses + no relay_url to fail, got HTTP {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let message = body["message"].as_str().unwrap_or_default().to_lowercase();
    assert!(
        message.contains("addresses") || message.contains("relay"),
        "error message should mention addresses or relay_url, got: {body}"
    );
}
