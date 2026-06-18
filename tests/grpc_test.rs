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
            iroh_secret_key: None,
            attachment_config: Default::default(),
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
            ..Default::default()
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

#[tokio::test]
async fn connect_protocol_structured_doc_with_value_field() {
    // Regression for peat-node#7 (blocker): a document whose JSON has a
    // top-level "value" string field must round-trip intact through the Connect
    // surface — the is_encrypted() gate in get_document must not confuse user
    // data with the encrypted-wrapper sentinel at the HTTP layer.
    let (client, base) = boot_server(50085, None).await;

    // Payload with a "value" key (the former encrypted-wrapper field name)
    call(
        &client,
        &base,
        "PutDocument",
        serde_json::json!({
            "collection": "test",
            "docId": "doc-value-field",
            "jsonData": r#"{"value":"hello","name":"alice"}"#
        }),
    )
    .await;

    let doc = call(
        &client,
        &base,
        "GetDocument",
        serde_json::json!({"collection": "test", "docId": "doc-value-field"}),
    )
    .await;

    let json_data = doc["jsonData"].as_str().expect("jsonData must be present");
    let v: serde_json::Value = serde_json::from_str(json_data).unwrap();
    assert_eq!(v["value"], "hello", "value field must be preserved");
    assert_eq!(v["name"], "alice", "name field must not be dropped");

    // Nested object with a "value" key at depth — also must round-trip correctly
    call(
        &client,
        &base,
        "PutDocument",
        serde_json::json!({
            "collection": "test",
            "docId": "doc-nested",
            "jsonData": r#"{"outer":{"value":"nested"},"count":3}"#
        }),
    )
    .await;

    let doc2 = call(
        &client,
        &base,
        "GetDocument",
        serde_json::json!({"collection": "test", "docId": "doc-nested"}),
    )
    .await;

    let json_data2 = doc2["jsonData"].as_str().expect("jsonData must be present");
    let v2: serde_json::Value = serde_json::from_str(json_data2).unwrap();
    assert_eq!(v2["outer"]["value"], "nested");
    assert_eq!(v2["count"], 3);
}

#[tokio::test]
async fn connect_protocol_collection_config_rpcs() {
    // Blocker from peat-node#55 QA review: SetCollectionConfig, GetCollectionConfig,
    // and ListCollectionConfigs must be exercised at the Connect HTTP+JSON layer
    // to verify wire encoding of CollectionConfig (DeletionPolicy enum, optional TTL
    // fields) end-to-end.
    let (client, base) = boot_server(50086, None).await;

    // GetCollectionConfig on a collection that has no config → empty response
    let not_found = call(
        &client,
        &base,
        "GetCollectionConfig",
        serde_json::json!({"collection": "logs"}),
    )
    .await;
    assert!(
        not_found.get("config").is_none() || not_found["config"].is_null(),
        "expected no config for unconfigured collection, got: {not_found}"
    );

    // SetCollectionConfig — tombstone policy with a TTL
    call(
        &client,
        &base,
        "SetCollectionConfig",
        serde_json::json!({
            "config": {
                "collection": "logs",
                "deletionPolicy": "DELETION_POLICY_TOMBSTONE",
                "tombstoneTtlSecs": 86400
            }
        }),
    )
    .await;

    // GetCollectionConfig — round-trip
    let resp = call(
        &client,
        &base,
        "GetCollectionConfig",
        serde_json::json!({"collection": "logs"}),
    )
    .await;
    let cfg = &resp["config"];
    assert_eq!(cfg["collection"], "logs");
    assert_eq!(cfg["deletionPolicy"], "DELETION_POLICY_TOMBSTONE");
    // proto3 JSON encodes uint64 as a decimal string
    assert_eq!(cfg["tombstoneTtlSecs"], "86400");

    // Set a second collection
    call(
        &client,
        &base,
        "SetCollectionConfig",
        serde_json::json!({
            "config": {
                "collection": "events",
                "deletionPolicy": "DELETION_POLICY_IMPLICIT_TTL",
                "softDeleteTtlSecs": 3600
            }
        }),
    )
    .await;

    // ListCollectionConfigs — both configs present
    let list_resp = call(
        &client,
        &base,
        "ListCollectionConfigs",
        serde_json::json!({}),
    )
    .await;
    let configs = list_resp["configs"].as_array().expect("configs array");
    assert_eq!(configs.len(), 2, "expected 2 configs, got {configs:?}");
    let names: Vec<&str> = configs
        .iter()
        .map(|c| c["collection"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"logs"), "logs must be in list");
    assert!(names.contains(&"events"), "events must be in list");

    // Empty-name validation — must fail
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/SetCollectionConfig");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .json(&serde_json::json!({"config": {"collection": "", "deletionPolicy": "DELETION_POLICY_SOFT_DELETE"}}))
        .send()
        .await
        .unwrap();
    assert!(
        !resp.status().is_success(),
        "SetCollectionConfig with empty collection name must fail, got HTTP {}",
        resp.status()
    );
}

/// Read all Connect streaming frames from the response body.
/// Connect stream frame: [flags:u8][length:u32_be][payload:length]
/// flags=0x00 → message; flags=0x02 → end-of-stream trailer (stop reading).
fn read_connect_stream(body: &[u8]) -> Vec<serde_json::Value> {
    let mut messages = vec![];
    let mut pos = 0usize;
    while pos + 5 <= body.len() {
        let flags = body[pos];
        let len = u32::from_be_bytes([body[pos + 1], body[pos + 2], body[pos + 3], body[pos + 4]])
            as usize;
        pos += 5;
        if pos + len > body.len() {
            break;
        }
        let payload = &body[pos..pos + len];
        pos += len;
        if flags & 0x02 != 0 {
            break;
        }
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(payload) {
            messages.push(v);
        }
    }
    messages
}

#[tokio::test]
async fn connect_protocol_subscribe_initial_snapshot() {
    // Warning from peat-node#55 QA review: Subscribe snapshot interleaving with
    // live events is verified at the trait surface in subscribe_query_test.rs but
    // should also be exercised at the Connect HTTP+JSON wire layer.
    let (client, base) = boot_server(50087, None).await;

    // Write 2 documents before subscribing
    for (id, payload) in [("snap-1", r#"{"k":1}"#), ("snap-2", r#"{"k":2}"#)] {
        call(
            &client,
            &base,
            "PutDocument",
            serde_json::json!({"collection": "snap", "docId": id, "jsonData": payload}),
        )
        .await;
    }

    // Subscribe with a short timeout — we only need the snapshot, not live events.
    // Server-streaming Connect RPCs require application/connect+json and a
    // length-prefixed request frame: [flags:u8=0][len:u32_be][json_payload].
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/Subscribe");
    let payload = serde_json::to_vec(&serde_json::json!({"collections": ["snap"]})).unwrap();
    let mut frame = vec![0u8]; // flags = 0 (message)
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    let mut resp = client
        .post(&url)
        .header("content-type", "application/connect+json")
        .body(frame)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "Subscribe returned {}",
        resp.status()
    );

    // Subscribe is long-lived — read chunks until we have ≥2 UPSERT events or timeout.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut buf: Vec<u8> = vec![];
    let mut messages: Vec<serde_json::Value> = vec![];
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        match tokio::time::timeout(std::time::Duration::from_millis(500), resp.chunk()).await {
            Ok(Ok(Some(chunk))) => {
                buf.extend_from_slice(&chunk);
                let parsed = read_connect_stream(&buf);
                messages = parsed;
                if messages.len() >= 2 {
                    break;
                }
            }
            _ => break,
        }
    }

    assert!(
        messages.len() >= 2,
        "expected ≥2 snapshot events, got {}",
        messages.len()
    );
    let doc_ids: Vec<&str> = messages
        .iter()
        .filter_map(|m| m["docId"].as_str())
        .collect();
    assert!(doc_ids.contains(&"snap-1"), "snap-1 must be in snapshot");
    assert!(doc_ids.contains(&"snap-2"), "snap-2 must be in snapshot");
    for m in &messages {
        assert_eq!(
            m["changeType"], "CHANGE_TYPE_UPSERT",
            "snapshot events must be UPSERT"
        );
    }
}
