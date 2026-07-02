//! StopSync / StartSync flip the sync_active flag; DisconnectPeer
//! removes the peer from ListPeers. Mirrors the deleted Go `functest`
//! Phase 4 sync-control + peer-disconnect sections.

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

async fn boot(grpc_port: u16, iroh_port: u16) -> (reqwest::Client, String) {
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
            iroh_udp_port: Some(iroh_port),
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

#[tokio::test]
async fn stop_sync_then_resume_flips_active_flag() {
    let (client, base) = boot(50131, 51231).await;

    // StartSync (boot defaults sync_active=false until called explicitly).
    call(&client, &base, "StartSync", serde_json::json!({})).await;
    let stats = call(&client, &base, "GetSyncStats", serde_json::json!({})).await;
    assert_eq!(stats["syncActive"], true);

    call(&client, &base, "StopSync", serde_json::json!({})).await;
    let stats = call(&client, &base, "GetSyncStats", serde_json::json!({})).await;
    // proto3 JSON elides default-false; absent or explicit false both OK.
    assert!(
        stats.get("syncActive").is_none() || stats["syncActive"] == serde_json::Value::Bool(false),
        "expected syncActive=false after StopSync, got {stats}"
    );

    call(&client, &base, "StartSync", serde_json::json!({})).await;
    let stats = call(&client, &base, "GetSyncStats", serde_json::json!({})).await;
    assert_eq!(stats["syncActive"], true);
}

#[tokio::test]
async fn disconnect_peer_empties_peer_list() {
    let (client_a, base_a) = boot(50132, 51232).await;
    let (client_b, base_b) = boot(50133, 51233).await;

    let status_a = call(&client_a, &base_a, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"].as_str().unwrap().to_string();

    call(
        &client_b,
        &base_b,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_a,
            "addresses": [format!("127.0.0.1:{}", 51232)],
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let peers = call(&client_b, &base_b, "ListPeers", serde_json::json!({})).await;
    let count_before = peers["peers"].as_array().map(|a| a.len()).unwrap_or(0);
    assert!(count_before >= 1, "expected >=1 peer before disconnect");

    call(
        &client_b,
        &base_b,
        "DisconnectPeer",
        serde_json::json!({"endpointId": endpoint_a}),
    )
    .await;

    let peers = call(&client_b, &base_b, "ListPeers", serde_json::json!({})).await;
    let count_after = peers["peers"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(
        count_after, 0,
        "expected 0 peers after DisconnectPeer, got {count_after}"
    );
}
