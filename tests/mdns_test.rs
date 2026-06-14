//! mDNS peer discovery — two SidecarNodes on the same host, no --peer wiring.
//!
//! Boots two nodes with `disable_mdns: false` and a shared `app_id` but no
//! explicit peers. Asserts that mDNS advertisement + browsing bridge the two
//! nodes and that a document written on node-A appears on node-B.
//!
//! Marked `#[ignore]` because loopback multicast is required and is
//! unavailable in most containerised CI environments. Run with
//! `--include-ignored` on bare metal or the self-hosted arm64 runner.

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

const A_GRPC: u16 = 51291;
const A_IROH: u16 = 51292;
const B_GRPC: u16 = 51293;
const B_IROH: u16 = 51294;

async fn boot_mdns_node(
    grpc_port: u16,
    iroh_udp_port: u16,
    app_id: &str,
) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: format!("mdns-test-{grpc_port}"),
            app_id: app_id.to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: Some(iroh_udp_port),
            disable_mdns: false,
            blob_stall_timeout: None,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
            attachment_config: Default::default(),
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
    assert!(
        resp.status().is_success(),
        "{method} returned {}: {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
    resp.json().await.unwrap()
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

#[tokio::test]
#[ignore = "requires loopback multicast; fails in containerised CI — run with --include-ignored on bare metal or the self-hosted runner"]
async fn mdns_two_nodes_sync_without_explicit_peers() {
    // Unique app_id per run so concurrent test suites don't cross-discover.
    let app_id = format!("peat-mdns-test-{}", uuid::Uuid::new_v4());

    let (client_a, base_a) = boot_mdns_node(A_GRPC, A_IROH, &app_id).await;
    let (client_b, base_b) = boot_mdns_node(B_GRPC, B_IROH, &app_id).await;

    // Give mDNS time to advertise and browse before writing.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Write a document on node-A.
    call(
        &client_a,
        &base_a,
        "PutDocument",
        serde_json::json!({
            "collection": "mdns-sync",
            "docId": "probe-1",
            "jsonData": r#"{"sensor":"mdns-probe"}"#,
        }),
    )
    .await;

    // Node-B must see it via the mDNS-established connection.
    let data = poll_for_document(&client_b, &base_b, "mdns-sync", "probe-1", "mdns-probe").await;
    assert!(data.contains("mdns-probe"), "unexpected: {data}");
}
