// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! Sender RPC integration tests: PublishDeployment, GetDeploymentRequests (SEND-01, SEND-02).
//!
//! Follows the boot_server + call helper pattern from grpc_test.rs. Uses ports
//! 50084-50087 (one per test) to avoid conflicts with grpc_test.rs (50081-50083).

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

async fn boot_server(port: u16, encryption_key: Option<String>) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.keep();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: format!("test-{port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir_path.clone(),
            peers: vec![],
            encryption_key,
            enable_deployer: false,
            blob_work_dir: dir_path.join("blobs"),
            download_timeout_secs: 30,
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

/// Create a dummy package file for PublishDeployment calls.
fn fake_package() -> (tempfile::NamedTempFile, String) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), b"fake zarf package bytes for test").unwrap();
    let p = tmp.path().to_str().unwrap().to_string();
    (tmp, p)
}

#[tokio::test]
async fn test_publish_deployment_returns_request_id() {
    let (client, base) = boot_server(50084, None).await;
    let (_tmp, pkg_path) = fake_package();

    let resp = call(
        &client,
        &base,
        "PublishDeployment",
        serde_json::json!({
            "packagePath": pkg_path,
            "targetAgentId": "receiver-node-1",
            "zarfVars": {"K": "V"},
        }),
    )
    .await;

    let request_id = resp["requestId"].as_str().expect("requestId present");
    assert!(!request_id.is_empty(), "request_id must be non-empty");
    assert!(
        request_id.len() >= 32,
        "request_id must be a UUID-shaped string: {request_id}"
    );

    // Verify the doc is visible on the local node
    let list = call(&client, &base, "GetDeploymentRequests", serde_json::json!({})).await;
    let reqs = list["requests"].as_array().expect("requests array");
    assert_eq!(
        reqs.len(),
        1,
        "expected exactly one request after one PublishDeployment"
    );
    assert_eq!(reqs[0]["id"].as_str(), Some(request_id));
}

#[tokio::test]
async fn test_publish_deployment_crdt_doc_shape() {
    let (client, base) = boot_server(50085, None).await;
    let (_tmp, pkg_path) = fake_package();

    call(
        &client,
        &base,
        "PublishDeployment",
        serde_json::json!({
            "packagePath": pkg_path,
            "targetAgentId": "receiver-node-2",
            "zarfVars": {},
        }),
    )
    .await;

    let list = call(&client, &base, "GetDeploymentRequests", serde_json::json!({})).await;
    let doc = &list["requests"][0];

    // Required fields
    assert!(
        doc["id"].as_str().map(|s| !s.is_empty()).unwrap_or(false),
        "id"
    );
    assert_eq!(doc["targetAgentId"].as_str(), Some("receiver-node-2"));
    assert!(
        doc["packageName"]
            .as_str()
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "package_name"
    );
    let hash = doc["irohBlobHash"].as_str().expect("iroh_blob_hash");
    assert_eq!(hash.len(), 64, "BLAKE3 hex is 64 chars: {hash}");
    assert!(
        hash.chars().all(|c| c.is_ascii_hexdigit()),
        "hex only: {hash}"
    );
    let endpoint = doc["senderEndpointId"].as_str().expect("sender_endpoint_id");
    assert!(!endpoint.is_empty(), "sender_endpoint_id non-empty");

    // Status fields
    assert_eq!(doc["senderStatus"].as_str(), Some("pending"));
    assert_eq!(doc["receiverStatus"].as_str(), Some("pending"));

    // blob_ticket is a JSON string; parse and verify keys
    let ticket_str = doc["blobTicket"].as_str().expect("blob_ticket");
    let ticket: serde_json::Value =
        serde_json::from_str(ticket_str).expect("blob_ticket parses as JSON");
    assert!(
        ticket.get("hash").and_then(|v| v.as_str()).is_some(),
        "blob_ticket.hash"
    );
    assert!(
        ticket
            .get("size_bytes")
            .and_then(|v| v.as_u64())
            .is_some(),
        "blob_ticket.size_bytes"
    );
    assert!(
        ticket
            .get("sender_endpoint_id")
            .and_then(|v| v.as_str())
            .is_some(),
        "blob_ticket.sender_endpoint_id"
    );
}

#[tokio::test]
async fn test_publish_deployment_twice_produces_two_ids() {
    let (client, base) = boot_server(50086, None).await;
    let (_tmp, pkg_path) = fake_package();

    let body = serde_json::json!({
        "packagePath": pkg_path,
        "targetAgentId": "same-receiver",
        "zarfVars": {},
    });

    let r1 = call(&client, &base, "PublishDeployment", body.clone()).await;
    let r2 = call(&client, &base, "PublishDeployment", body).await;

    let id1 = r1["requestId"].as_str().unwrap();
    let id2 = r2["requestId"].as_str().unwrap();
    assert_ne!(
        id1, id2,
        "two PublishDeployment calls must produce different request_ids (no sender-side dedup)"
    );

    let list = call(&client, &base, "GetDeploymentRequests", serde_json::json!({})).await;
    let reqs = list["requests"].as_array().unwrap();
    assert_eq!(
        reqs.len(),
        2,
        "expected two docs after two PublishDeployment calls"
    );
}

#[tokio::test]
async fn test_get_deployment_requests_includes_status_fields() {
    let (client, base) = boot_server(50087, None).await;

    // Empty list on a fresh node
    let empty = call(&client, &base, "GetDeploymentRequests", serde_json::json!({})).await;
    assert_eq!(
        empty["requests"].as_array().map(|a| a.len()).unwrap_or(0),
        0
    );

    // After one publish, exactly one entry with both status fields
    let (_tmp, pkg_path) = fake_package();
    call(
        &client,
        &base,
        "PublishDeployment",
        serde_json::json!({
            "packagePath": pkg_path,
            "targetAgentId": "r",
            "zarfVars": {},
        }),
    )
    .await;

    let list = call(&client, &base, "GetDeploymentRequests", serde_json::json!({})).await;
    let reqs = list["requests"].as_array().unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(
        reqs[0]["senderStatus"].is_string(),
        "sender_status field present"
    );
    assert!(
        reqs[0]["receiverStatus"].is_string(),
        "receiver_status field present"
    );
}
