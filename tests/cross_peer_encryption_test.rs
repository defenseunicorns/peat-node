//! Encryption-at-rest cross-peer. Encrypted node syncs to plaintext
//! peer; the peer sees the `ENC:v1:` envelope opaque on its store.
//!
//! Equivalent to the deleted Go `functest` Phase 3.

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

async fn boot(
    grpc_port: u16,
    iroh_port: u16,
    encryption_key: Option<String>,
) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("test-{grpc_port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key,
            iroh_udp_port: Some(iroh_port),
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
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    assert!(status.is_success(), "{method} returned {status}: {text}");
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

#[tokio::test]
async fn encrypted_node_syncs_opaque_envelope_to_plain_peer() {
    use base64::Engine;
    let key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);

    // Encrypted node-A; plaintext (no key) node-B.
    let (client_a, base_a) = boot(50111, 51211, Some(key)).await;
    let (client_b, base_b) = boot(50112, 51212, None).await;

    let status_a = call(&client_a, &base_a, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"].as_str().unwrap().to_string();

    call(
        &client_b,
        &base_b,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_a,
            "addresses": [format!("127.0.0.1:{}", 51211)],
        }),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    call(&client_a, &base_a, "StartSync", serde_json::json!({})).await;
    call(&client_b, &base_b, "StartSync", serde_json::json!({})).await;

    let plaintext = r#"{"classified":"top-secret"}"#;
    call(
        &client_a,
        &base_a,
        "PutDocument",
        serde_json::json!({
            "collection": "secure",
            "docId": "doc-1",
            "jsonData": plaintext,
        }),
    )
    .await;

    // On node-A the read is transparently decrypted.
    let got_a = call(
        &client_a,
        &base_a,
        "GetDocument",
        serde_json::json!({"collection":"secure","docId":"doc-1"}),
    )
    .await;
    assert_eq!(
        got_a["jsonData"].as_str(),
        Some(plaintext),
        "node-a should see plaintext via transparent decrypt"
    );

    // Poll node-B for the doc to land via sync.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut got_b_data: Option<String> = None;
    while tokio::time::Instant::now() < deadline {
        let resp = call(
            &client_b,
            &base_b,
            "GetDocument",
            serde_json::json!({"collection":"secure","docId":"doc-1"}),
        )
        .await;
        if let Some(d) = resp["jsonData"].as_str() {
            got_b_data = Some(d.to_string());
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let data_on_b = got_b_data.expect("doc did not sync to plaintext peer in 30s");
    assert!(
        data_on_b.starts_with("ENC:v1:"),
        "plaintext peer must see opaque ENC:v1: envelope, got: {data_on_b}"
    );
    assert!(
        !data_on_b.contains("classified"),
        "plaintext must not leak across the wire: {data_on_b}"
    );
}
