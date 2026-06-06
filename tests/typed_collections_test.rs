//! Full-field round-trips for typed collections (Cell, Track, Command).
//! The existing grpc_test.rs covers Platform minimally; this covers
//! the remaining typed surfaces with all optional fields populated,
//! mirroring the deleted Go `functest` Phase 1.

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

async fn boot(port: u16) -> (reqwest::Client, String) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: format!("test-{port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key: None,
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
async fn cell_round_trip_with_all_fields() {
    let (client, base) = boot(50121).await;
    call(
        &client,
        &base,
        "PutCell",
        serde_json::json!({
            "cell": {
                "id": "cell-001",
                "name": "Alpha Cell",
                "status": "CELL_STATUS_ACTIVE",
                "platformCount": 5,
                "centerLatitude": 34.0522,
                "centerLongitude": -118.2437,
                "capabilities": ["recon", "relay"],
                "formationId": "form-001",
                "leaderId": "plat-001"
            }
        }),
    )
    .await;
    let resp = call(&client, &base, "GetCells", serde_json::json!({})).await;
    let cells = resp["cells"].as_array().expect("cells array");
    assert_eq!(cells.len(), 1);
    let c = &cells[0];
    assert_eq!(c["id"], "cell-001");
    assert_eq!(c["name"], "Alpha Cell");
    assert_eq!(c["status"], "CELL_STATUS_ACTIVE");
    assert_eq!(c["platformCount"], 5);
    assert!((c["centerLatitude"].as_f64().unwrap() - 34.0522).abs() < 0.0001);
    assert!((c["centerLongitude"].as_f64().unwrap() + 118.2437).abs() < 0.0001);
    let caps: Vec<&str> = c["capabilities"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(caps, vec!["recon", "relay"]);
    assert_eq!(c["formationId"], "form-001");
    assert_eq!(c["leaderId"], "plat-001");
}

#[tokio::test]
async fn track_round_trip_with_all_optional_fields() {
    let (client, base) = boot(50122).await;
    call(
        &client,
        &base,
        "PutTrack",
        serde_json::json!({
            "track": {
                "id": "trk-001",
                "sourcePlatform": "plat-001",
                "cellId": "cell-001",
                "formationId": "form-001",
                "latitude": 35.0,
                "longitude": -120.0,
                "altitudeM": 3000.0,
                "cepM": 15.0,
                "headingDeg": 270.0,
                "speedMps": 250.0,
                "classification": "UNCLASSIFIED",
                "confidence": 0.92,
                "category": "TRACK_CATEGORY_AIR"
            }
        }),
    )
    .await;
    let resp = call(&client, &base, "GetTracks", serde_json::json!({})).await;
    let tracks = resp["tracks"].as_array().expect("tracks array");
    assert_eq!(tracks.len(), 1);
    let t = &tracks[0];
    assert_eq!(t["id"], "trk-001");
    assert_eq!(t["sourcePlatform"], "plat-001");
    assert_eq!(t["cellId"], "cell-001");
    assert_eq!(t["formationId"], "form-001");
    assert!((t["latitude"].as_f64().unwrap() - 35.0).abs() < 0.0001);
    assert!((t["longitude"].as_f64().unwrap() + 120.0).abs() < 0.0001);
    assert!((t["altitudeM"].as_f64().unwrap() - 3000.0).abs() < 0.0001);
    assert!((t["cepM"].as_f64().unwrap() - 15.0).abs() < 0.0001);
    assert!((t["headingDeg"].as_f64().unwrap() - 270.0).abs() < 0.0001);
    assert!((t["speedMps"].as_f64().unwrap() - 250.0).abs() < 0.0001);
    assert_eq!(t["classification"], "UNCLASSIFIED");
    assert!((t["confidence"].as_f64().unwrap() - 0.92).abs() < 0.0001);
    assert_eq!(t["category"], "TRACK_CATEGORY_AIR");
}

#[tokio::test]
async fn command_round_trip_with_all_fields() {
    let (client, base) = boot(50123).await;
    call(
        &client,
        &base,
        "PutCommand",
        serde_json::json!({
            "command": {
                "id": "cmd-001",
                "targetId": "plat-001",
                "commandType": "deploy-package",
                "status": "COMMAND_STATUS_PENDING",
                "createdAt": "1700000000",
                "expiresAt": "1700003600",
                "payloadJson": r#"{"package":"nginx","version":"1.25"}"#
            }
        }),
    )
    .await;
    let resp = call(&client, &base, "GetCommands", serde_json::json!({})).await;
    let commands = resp["commands"].as_array().expect("commands array");
    assert_eq!(commands.len(), 1);
    let c = &commands[0];
    assert_eq!(c["id"], "cmd-001");
    assert_eq!(c["targetId"], "plat-001");
    assert_eq!(c["commandType"], "deploy-package");
    assert_eq!(c["status"], "COMMAND_STATUS_PENDING");
    // proto3 JSON int64 → string
    assert_eq!(c["createdAt"], "1700000000");
    assert_eq!(c["expiresAt"], "1700003600");
    assert_eq!(c["payloadJson"], r#"{"package":"nginx","version":"1.25"}"#);
}
