//! Formation isolation — two nodes with different `app_id` + `shared_key`
//! must not exchange documents even if peered at the transport level.
//!
//! Equivalent to the deleted Go `functest` Phase 5.
//!
//! Ports are allocated dynamically (a free TCP port for the gRPC server, a
//! free UDP port for the iroh endpoint) and the spawned servers are aborted
//! at the end of the test. An earlier version hard-coded `50101/50102`
//! (gRPC) and `51201/51202` (iroh) and never shut the servers down: a
//! crashed or re-run test left listeners bound on those fixed ports, so the
//! next run's servers failed to bind (`serve().unwrap()` panic) and its
//! clients silently talked to the *stale* servers — manufacturing a
//! cross-formation "leak" that was a harness artifact, not a product bug
//! (peat-mesh isolates correctly; verified at the library level).

use std::net::{TcpListener, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;
use tokio::task::JoinHandle;

/// Grab a currently-free TCP port by binding to `:0` and reading it back.
/// There's a small race between drop and the server's rebind; acceptable for
/// a test and far less flaky than fixed ports.
fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Grab a currently-free UDP port (for the iroh endpoint).
fn free_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Boot a sidecar on dynamically-chosen ports. Returns the HTTP client, the
/// base URL, the gRPC server task handle (abort it to shut the server down),
/// and the iroh UDP port (so a peer can be given a concrete dial address).
async fn boot(app_id: &str, shared_key: &str) -> (reqwest::Client, String, JoinHandle<()>, u16) {
    let grpc_port = free_tcp_port();
    let iroh_port = free_udp_port();
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("test-{grpc_port}"),
            app_id: app_id.to_string(),
            shared_key: shared_key.to_string(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key: None,
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
    let handle = tokio::spawn(async move {
        // On a clean dynamic port this serves until aborted; we don't unwrap
        // a teardown error into a panic.
        let _ = connectrpc::Server::new(router).serve(addr).await;
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    (
        client,
        format!("http://127.0.0.1:{grpc_port}"),
        handle,
        iroh_port,
    )
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
    // For this test we allow non-2xx from ConnectPeer (the formation
    // mismatch may be detected at handshake time or quietly afterward);
    // the assertion is on whether docs sync, not on the connect call.
    if !status.is_success() && !method.eq_ignore_ascii_case("ConnectPeer") {
        panic!("{method} returned {status}: {text}");
    }
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

const ALPHA_KEY: &str = "qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqo="; // 0xAA * 32
const BRAVO_KEY: &str = "u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7u7s="; // 0xBB * 32

#[tokio::test]
async fn different_formations_do_not_exchange_documents() {
    let (client_a, base_a, handle_a, iroh_a) = boot("alpha", ALPHA_KEY).await;
    let (client_b, base_b, handle_b, _iroh_b) = boot("bravo", BRAVO_KEY).await;

    let status_a = call(&client_a, &base_a, "GetStatus", serde_json::json!({})).await;
    let endpoint_a = status_a["endpointAddr"].as_str().unwrap().to_string();

    // Attempt to peer. May succeed or fail at the transport layer depending
    // on when the formation key handshake rejects — either is fine; the
    // assertion is that nothing leaks across formations.
    let _ = call(
        &client_b,
        &base_b,
        "ConnectPeer",
        serde_json::json!({
            "endpointId": endpoint_a,
            "addresses": [format!("127.0.0.1:{iroh_a}")],
        }),
    )
    .await;

    call(&client_a, &base_a, "StartSync", serde_json::json!({})).await;
    call(&client_b, &base_b, "StartSync", serde_json::json!({})).await;

    call(
        &client_a,
        &base_a,
        "PutDocument",
        serde_json::json!({
            "collection": "isolation-test",
            "docId": "from-alpha",
            "jsonData": r#"{"formation":"alpha"}"#,
        }),
    )
    .await;

    // Wait long enough that any cross-formation leak would surface.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let resp = call(
        &client_b,
        &base_b,
        "GetDocument",
        serde_json::json!({"collection":"isolation-test","docId":"from-alpha"}),
    )
    .await;

    let leaked = resp["jsonData"].as_str();
    handle_a.abort();
    handle_b.abort();

    assert!(
        leaked.is_none(),
        "document leaked across formations: {resp}"
    );
}
