//! Subprocess regression for peat-node#185 / peat-mesh#306.
//!
//! A sends bytes to B, A goes offline, B restarts from persistent storage,
//! then B creates a fresh NodeList distribution to C. The test uses stable
//! identities, static direct peers, no mDNS, and no hosted relay. It asserts
//! byte-identical custody at both hops and a non-vacuous terminal sender
//! outcome after the restart.

use std::net::{TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use base64::Engine;
use sha2::{Digest, Sha256};
use tokio::process::{Child, Command};

const SHARED_KEY: &str = "KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio=";

struct ChildGuard(Option<Child>);

impl ChildGuard {
    async fn terminate(&mut self) {
        let Some(mut child) = self.0.take() else {
            return;
        };
        let _ = child.start_kill();
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("peat-node subprocess must exit within 10s")
            .expect("wait for peat-node subprocess");
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.start_kill();
        }
    }
}

fn reserve_ports() -> ([u16; 3], [u16; 3]) {
    let tcp = [
        TcpListener::bind("127.0.0.1:0").unwrap(),
        TcpListener::bind("127.0.0.1:0").unwrap(),
        TcpListener::bind("127.0.0.1:0").unwrap(),
    ];
    let udp = [
        UdpSocket::bind("127.0.0.1:0").unwrap(),
        UdpSocket::bind("127.0.0.1:0").unwrap(),
        UdpSocket::bind("127.0.0.1:0").unwrap(),
    ];
    (
        tcp.map(|socket| socket.local_addr().unwrap().port()),
        udp.map(|socket| socket.local_addr().unwrap().port()),
    )
}

struct SpawnConfig<'a> {
    grpc_port: u16,
    iroh_port: u16,
    node_id: &'a str,
    data_dir: &'a Path,
    peers: &'a [String],
    attachment_root: Option<(&'a str, &'a Path)>,
    attachment_inbox: Option<&'a Path>,
}

fn spawn_node(bin: &str, cfg: SpawnConfig<'_>) -> ChildGuard {
    let mut command = Command::new(bin);
    command
        .arg("--listen")
        .arg(format!("tcp://127.0.0.1:{}", cfg.grpc_port))
        .arg("--data-dir")
        .arg(cfg.data_dir)
        .arg("--node-id")
        .arg(cfg.node_id)
        .arg("--shared-key")
        .arg(SHARED_KEY)
        .arg("--iroh-udp-port")
        .arg(cfg.iroh_port.to_string())
        .arg("--auto-sync")
        .arg("--disable-mdns")
        .arg("--attachment-inbox-poll-secs")
        .arg("1");

    for peer in cfg.peers {
        command.arg("--peer").arg(peer);
    }
    if let Some((name, path)) = cfg.attachment_root {
        command
            .arg("--attachment-root")
            .arg(format!("{name}={}", path.display()));
    }
    if let Some(path) = cfg.attachment_inbox {
        command.arg("--attachment-inbox").arg(path);
    }

    if std::env::var_os("PEAT_TEST_LOGS").is_some() {
        command
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| {
                    "peat_node=info,peat_mesh::storage::file_distribution=debug".to_string()
                }),
            )
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
    } else {
        command
            .env("RUST_LOG", "peat_node=warn,peat_mesh=warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
    }

    let child = command.spawn().expect("spawn peat-node subprocess");
    ChildGuard(Some(child))
}

async fn wait_grpc_ready(client: &reqwest::Client, base: &str) {
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/GetStatus");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = client
            .post(&url)
            .header("content-type", "application/json")
            .json(&serde_json::json!({}))
            .send()
            .await
        {
            if response.status().is_success() {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "peat-node at {base} did not become ready within 30s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn call(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let response = client
        .post(format!("{base}/peat.sidecar.v1.PeatSidecar/{method}"))
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|error| panic!("{method} request failed: {error}"));
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    assert!(status.is_success(), "{method} returned {status}: {text}");
    serde_json::from_str(&text).unwrap_or(serde_json::Value::Null)
}

async fn wait_connected(client: &reqwest::Client, base: &str, minimum: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let last = call(client, base, "GetStatus", serde_json::json!({})).await;
        if last["connectedPeers"].as_u64().unwrap_or(0) >= minimum {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "node at {base} did not reach {minimum} connected peer(s); last status: {last}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn find_matching_file(root: &Path, expected: &[u8]) -> Option<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with('.'))
            {
                continue;
            }
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                pending.push(path);
            } else if kind.is_file() && std::fs::read(&path).is_ok_and(|bytes| bytes == expected) {
                return Some(path);
            }
        }
    }
    None
}

async fn wait_for_file(stage: &str, root: &Path, expected: &[u8], timeout: Duration) -> PathBuf {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(path) = find_matching_file(root, expected) {
            return path;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{stage}: byte-identical attachment did not appear under {} within {timeout:?}",
            root.display()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn send_body(
    root_name: &str,
    relative_path: &str,
    payload: &[u8],
    target: &str,
) -> serde_json::Value {
    let digest = Sha256::digest(payload);
    serde_json::json!({
        "files": [{
            "rootName": root_name,
            "relativePath": relative_path,
            "sizeBytes": payload.len(),
            "sha256": base64::engine::general_purpose::STANDARD.encode(digest),
        }],
        "scope": { "nodeList": { "nodeIds": [target] } }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(iroh_two_node)]
async fn persistent_sender_restart_redistributes_to_c_with_a_offline() {
    let bin = env!("CARGO_BIN_EXE_peat-node");
    let (grpc, iroh) = reserve_ports();
    let data_a = tempfile::tempdir().unwrap();
    let data_b = tempfile::tempdir().unwrap();
    let data_c = tempfile::tempdir().unwrap();
    let root_a = tempfile::tempdir().unwrap();
    let custody_b = tempfile::tempdir().unwrap();
    let inbox_c = tempfile::tempdir().unwrap();

    let endpoint_a = peat_node::identity::derive_endpoint_id(SHARED_KEY, "restart-a").unwrap();
    let endpoint_b = peat_node::identity::derive_endpoint_id(SHARED_KEY, "restart-b").unwrap();
    let endpoint_c = peat_node::identity::derive_endpoint_id(SHARED_KEY, "restart-c").unwrap();
    let peer_a = format!("{endpoint_a}@127.0.0.1:{}", iroh[0]);
    let peer_c = format!("{endpoint_c}@127.0.0.1:{}", iroh[2]);
    let base_a = format!("http://127.0.0.1:{}", grpc[0]);
    let base_b = format!("http://127.0.0.1:{}", grpc[1]);
    let base_c = format!("http://127.0.0.1:{}", grpc[2]);

    // A and C start first. B's static peer list then establishes A-B and B-C;
    // A and C never receive one another's address.
    let mut node_a = spawn_node(
        bin,
        SpawnConfig {
            grpc_port: grpc[0],
            iroh_port: iroh[0],
            node_id: "restart-a",
            data_dir: data_a.path(),
            peers: &[],
            attachment_root: Some(("outbox", root_a.path())),
            attachment_inbox: None,
        },
    );
    let _node_c = spawn_node(
        bin,
        SpawnConfig {
            grpc_port: grpc[2],
            iroh_port: iroh[2],
            node_id: "restart-c",
            data_dir: data_c.path(),
            peers: &[],
            attachment_root: None,
            attachment_inbox: Some(inbox_c.path()),
        },
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    wait_grpc_ready(&client, &base_a).await;
    wait_grpc_ready(&client, &base_c).await;

    let mut node_b = spawn_node(
        bin,
        SpawnConfig {
            grpc_port: grpc[1],
            iroh_port: iroh[1],
            node_id: "restart-b",
            data_dir: data_b.path(),
            peers: &[peer_a.clone(), peer_c.clone()],
            attachment_root: Some(("custody", custody_b.path())),
            attachment_inbox: Some(custody_b.path()),
        },
    );
    wait_grpc_ready(&client, &base_b).await;
    wait_connected(&client, &base_b, 2).await;

    // Valid deterministic 1x1 PNG; small size makes the regression about
    // convergence and restart semantics rather than transfer duration.
    let payload = base64::engine::general_purpose::STANDARD
        .decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")
        .unwrap();
    let relative_path = "restart-custody.png";
    std::fs::write(root_a.path().join(relative_path), &payload).unwrap();
    call(
        &client,
        &base_a,
        "SendAttachments",
        send_body("outbox", relative_path, &payload, &endpoint_b[..10]),
    )
    .await;
    let b_path = wait_for_file(
        "A-to-B custody",
        custody_b.path(),
        &payload,
        Duration::from_secs(60),
    )
    .await;
    let b_bytes = std::fs::read(&b_path).unwrap();
    assert_eq!(Sha256::digest(&b_bytes), Sha256::digest(&payload));

    // The original source remains offline for the entire B->C distribution.
    node_a.terminate().await;
    node_b.terminate().await;

    // Reopen B's exact data directory, attachment root, inbox, identity, and
    // direct C peer. This is process-level persistence, not an in-memory reset.
    node_b = spawn_node(
        bin,
        SpawnConfig {
            grpc_port: grpc[1],
            iroh_port: iroh[1],
            node_id: "restart-b",
            data_dir: data_b.path(),
            peers: std::slice::from_ref(&peer_c),
            attachment_root: Some(("custody", custody_b.path())),
            attachment_inbox: Some(custody_b.path()),
        },
    );
    wait_grpc_ready(&client, &base_b).await;
    let restarted_status = call(&client, &base_b, "GetStatus", serde_json::json!({})).await;
    assert_eq!(
        restarted_status["endpointAddr"], endpoint_b,
        "B's endpoint identity must remain stable across restart"
    );
    wait_connected(&client, &base_b, 1).await;

    let response = call(
        &client,
        &base_b,
        "SendAttachments",
        send_body("custody", relative_path, &payload, &endpoint_c[..10]),
    )
    .await;
    let distribution_id = response["handles"][0]["distributionId"]
        .as_str()
        .expect("B's post-restart SendAttachments must return a distribution id")
        .to_string();

    let c_path = wait_for_file(
        "post-restart B-to-C delivery",
        inbox_c.path(),
        &payload,
        Duration::from_secs(60),
    )
    .await;
    let c_bytes = std::fs::read(c_path).unwrap();
    assert_eq!(
        c_bytes, payload,
        "C must materialize byte-identical content"
    );
    assert_eq!(Sha256::digest(&c_bytes), Sha256::digest(&b_bytes));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let expected_bytes = payload.len().to_string();
    loop {
        let last = call(
            &client,
            &base_b,
            "GetAttachmentDistribution",
            serde_json::json!({ "distributionId": distribution_id }),
        )
        .await;
        let completed_target = last["perNode"].as_array().is_some_and(|nodes| {
            nodes.iter().any(|node| {
                node["nodeId"] == endpoint_c[..10]
                    && node["status"] == "DISTRIBUTION_STATUS_COMPLETED"
                    && node["bytesTransferred"].as_str() == Some(expected_bytes.as_str())
            })
        });
        let aggregate_terminal = matches!(
            last["status"].as_str(),
            Some(
                "DISTRIBUTION_STATUS_COMPLETED"
                    | "DISTRIBUTION_STATUS_PARTIAL"
                    | "DISTRIBUTION_STATUS_FAILED"
                    | "DISTRIBUTION_STATUS_CANCELLED"
            )
        );
        if completed_target && aggregate_terminal {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "B never exposed C's completed {expected_bytes}/{expected_bytes} receiver status and a terminal aggregate after C materialized bytes; last: {last}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    node_b.terminate().await;
}
