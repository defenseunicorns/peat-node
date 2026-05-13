// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! CRDT-04 integration tests: ResetDeployment RPC.
//!
//! Verifies that an operator can reset a stuck deployment's receiver_status to
//! Pending via the ResetDeployment RPC, and that error cases (unknown ID, empty
//! ID) return the correct Connect error codes.
//!
//! Uses ports 50090-50092 to avoid conflicts with other integration test files.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;
use peat_node::types::{DeploymentRequest, DeploymentStatus};

// ─── Test server helpers ────────────────────────────────────────────────────

async fn boot_server(port: u16) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.keep();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: format!("test-reset-{port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir_path.clone(),
            peers: vec![],
            encryption_key: None,
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

/// Call a Connect RPC method and assert success; return parsed JSON body.
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

/// Call a Connect RPC method and return the HTTP status + JSON body regardless of
/// success/failure. Used to assert on error responses.
async fn call_raw(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/{method}");
    let resp = client
        .post(&url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap();

    let status = resp.status();
    let json: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Write a DeploymentRequest doc directly to the CRDT store via PutDocument.
async fn put_deployment_request(
    client: &reqwest::Client,
    base: &str,
    request_id: &str,
    receiver_status: &str,
) {
    let doc = serde_json::json!({
        "id": request_id,
        "target_agent_id": "test-receiver",
        "package_name": "test-package.zarf.tar.zst",
        "package_version": "1.0.0",
        "architecture": "arm64",
        "iroh_blob_hash": "a".repeat(64),
        "sender_endpoint_id": "test-sender",
        "zarf_vars": {},
        "sender_status": "deployed",
        "receiver_status": receiver_status,
        "created_at": 1_700_000_000i64,
        "blob_ticket": "{\"hash\":\"aabbcc\",\"size_bytes\":1234,\"sender_endpoint_id\":\"test\"}",
    });

    call(
        client,
        base,
        "PutDocument",
        serde_json::json!({
            "collection": "deployment_requests",
            "docId": request_id,
            "jsonData": doc.to_string(),
        }),
    )
    .await;
}

// ─── CRDT-04 Tests ──────────────────────────────────────────────────────────

/// CRDT-04 happy path: reset a Failed deployment back to Pending.
/// Verifies that receiver_status transitions to "pending" and all other fields
/// are preserved byte-for-byte through the serde round-trip.
#[tokio::test]
async fn test_crdt_04_reset_deployment_to_pending() {
    let (client, base) = boot_server(50090).await;
    let request_id = "11111111-1111-1111-1111-111111111111";

    // Write a deployment_requests doc with receiver_status = "failed" (common reset case).
    put_deployment_request(&client, &base, request_id, "failed").await;

    // Verify the doc is written with the expected initial state.
    let list = call(
        &client,
        &base,
        "GetDeploymentRequests",
        serde_json::json!({}),
    )
    .await;
    let reqs = list["requests"].as_array().expect("requests array");
    assert_eq!(reqs.len(), 1, "one request written");
    assert_eq!(reqs[0]["receiverStatus"].as_str(), Some("failed"));
    let original_package_name = reqs[0]["packageName"].as_str().unwrap().to_string();
    let original_architecture = reqs[0]["architecture"].as_str().unwrap().to_string();
    let original_blob_hash = reqs[0]["irohBlobHash"].as_str().unwrap().to_string();
    let original_sender_status = reqs[0]["senderStatus"].as_str().unwrap().to_string();

    // Invoke ResetDeployment.
    call(
        &client,
        &base,
        "ResetDeployment",
        serde_json::json!({ "requestId": request_id }),
    )
    .await;

    // Re-read the doc and assert receiver_status is now "pending".
    let list2 = call(
        &client,
        &base,
        "GetDeploymentRequests",
        serde_json::json!({}),
    )
    .await;
    let reqs2 = list2["requests"].as_array().expect("requests array after reset");
    assert_eq!(reqs2.len(), 1, "still exactly one request after reset");
    assert_eq!(
        reqs2[0]["receiverStatus"].as_str(),
        Some("pending"),
        "receiver_status must be reset to pending"
    );

    // Assert all other fields are preserved byte-for-byte.
    assert_eq!(
        reqs2[0]["packageName"].as_str(),
        Some(original_package_name.as_str()),
        "package_name preserved"
    );
    assert_eq!(
        reqs2[0]["architecture"].as_str(),
        Some(original_architecture.as_str()),
        "architecture preserved"
    );
    assert_eq!(
        reqs2[0]["irohBlobHash"].as_str(),
        Some(original_blob_hash.as_str()),
        "iroh_blob_hash preserved"
    );
    assert_eq!(
        reqs2[0]["senderStatus"].as_str(),
        Some(original_sender_status.as_str()),
        "sender_status preserved (ResetDeployment only touches receiver_status)"
    );
    assert_eq!(
        reqs2[0]["id"].as_str(),
        Some(request_id),
        "id preserved"
    );
}

/// CRDT-04: ResetDeployment with an unknown request_id returns NotFound.
#[tokio::test]
async fn test_crdt_04_reset_unknown_request() {
    let (client, base) = boot_server(50091).await;

    // No docs written — any request_id is unknown.
    let (status, body) = call_raw(
        &client,
        &base,
        "ResetDeployment",
        serde_json::json!({ "requestId": "nonexistent-uuid" }),
    )
    .await;

    // Connect protocol maps NotFound to HTTP 404.
    assert_eq!(
        status,
        reqwest::StatusCode::NOT_FOUND,
        "unknown request_id must return 404 Not Found, body: {body}"
    );
    // Connect JSON error body contains a "code" field.
    let code = body["code"].as_str().unwrap_or("");
    assert!(
        code.contains("not_found") || code.contains("NOT_FOUND"),
        "error code must be not_found, got: {code}"
    );
}

/// CRDT-04: ResetDeployment with an empty request_id returns InvalidArgument.
#[tokio::test]
async fn test_crdt_04_reset_empty_request_id() {
    let (client, base) = boot_server(50092).await;

    // Empty string request_id — must be rejected before any CRDT lookup.
    let (status, body) = call_raw(
        &client,
        &base,
        "ResetDeployment",
        serde_json::json!({ "requestId": "" }),
    )
    .await;

    // Connect protocol maps InvalidArgument to HTTP 400.
    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "empty request_id must return 400 Bad Request, body: {body}"
    );
    let code = body["code"].as_str().unwrap_or("");
    assert!(
        code.contains("invalid_argument") || code.contains("INVALID_ARGUMENT"),
        "error code must be invalid_argument, got: {code}"
    );
}
