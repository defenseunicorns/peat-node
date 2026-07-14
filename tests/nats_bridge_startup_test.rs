//! Real-process evidence that an unavailable local NATS broker does not block
//! the existing Peat Connect RPC service or leak URL credentials.

use std::net::TcpListener;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.start_kill();
    }
}

#[tokio::test]
async fn unavailable_authenticated_nats_keeps_status_reachable_and_logs_safely() {
    let grpc_listener = TcpListener::bind("127.0.0.1:0").expect("reserve gRPC port");
    let grpc_port = grpc_listener.local_addr().unwrap().port();
    drop(grpc_listener);

    // Keep this socket bound. TCP may connect, but no NATS INFO handshake can
    // complete, making the endpoint deterministically unavailable as NATS.
    let unavailable_nats = TcpListener::bind("127.0.0.1:0").expect("reserve NATS port");
    let nats_port = unavailable_nats.local_addr().unwrap().port();

    let data_dir = tempfile::tempdir().expect("temporary data directory");
    let mut child = Command::new(env!("CARGO_BIN_EXE_peat-node"))
        .arg("--listen")
        .arg(format!("tcp://127.0.0.1:{grpc_port}"))
        .arg("--data-dir")
        .arg(data_dir.path())
        .arg("--node-id")
        .arg("nats-outage-test")
        .arg("--nats-url")
        .arg(format!("nats://test-user:p%61ssword@127.0.0.1:{nats_port}"))
        .arg("--nats-mapping")
        .arg("vision.summary=frames")
        .env(
            "RUST_LOG",
            "peat_node=debug,peat_mesh=warn,peat_protocol=warn,iroh=warn",
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn peat-node");

    let stdout = child.stdout.take().expect("capture stdout");
    let stderr = child.stderr.take().expect("capture stderr");
    let output = Arc::new(Mutex::new(String::new()));
    let stdout_output = Arc::clone(&output);
    let stdout_reader = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut captured = stdout_output.lock().await;
            captured.push_str(&line);
            captured.push('\n');
        }
    });
    let reader_output = Arc::clone(&output);
    let stderr_reader = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut captured = reader_output.lock().await;
            captured.push_str(&line);
            captured.push('\n');
        }
    });
    let mut child = ChildGuard(child);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let status_url = format!("http://127.0.0.1:{grpc_port}/peat.sidecar.v1.PeatSidecar/GetStatus");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    let mut status = None;
    let mut saw_not_ready = false;
    let mut saw_unavailable = false;
    let mut saw_retry = false;

    while tokio::time::Instant::now() < deadline {
        if status.is_none() {
            if let Ok(response) = client
                .post(&status_url)
                .header("content-type", "application/json")
                .json(&serde_json::json!({}))
                .send()
                .await
            {
                if response.status().is_success() {
                    status = response.json::<serde_json::Value>().await.ok();
                }
            }
        }

        let captured = output.lock().await.clone();
        saw_not_ready |= captured.contains("bridge_ready=false");
        saw_unavailable |= captured.contains("broker_unavailable");
        saw_retry |= captured.contains("retry_scheduled");
        if status.is_some() && saw_not_ready && saw_unavailable && saw_retry {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    child.0.start_kill().ok();
    let _ = child.0.wait().await;
    let _ = stdout_reader.await;
    let _ = stderr_reader.await;
    drop(unavailable_nats);

    let status = status.expect("GetStatus should respond while NATS is unavailable");
    assert!(status.get("nodeId").is_some());
    assert!(status.get("syncActive").is_some());
    assert!(status.get("phase").is_some());
    assert!(status.get("bridgeReady").is_none());

    let captured = output.lock().await.clone();
    assert!(
        saw_not_ready,
        "missing bridge_ready=false event: {captured}"
    );
    assert!(
        saw_unavailable,
        "missing safe unavailable reason: {captured}"
    );
    assert!(saw_retry, "missing retry evidence: {captured}");
    for secret in ["test-user", "p%61ssword", "password"] {
        assert!(!captured.contains(secret));
    }
}
