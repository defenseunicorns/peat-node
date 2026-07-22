//! Regression coverage for bounded client-connection resource lifetimes.

use std::io::Read as _;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tokio::io::AsyncReadExt as _;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve TCP port")
        .local_addr()
        .expect("read reserved port")
        .port()
}

async fn wait_ready(client: &reqwest::Client, base: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let url = format!("{base}/peat.sidecar.v1.PeatSidecar/GetStatus");
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
            "peat-node did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn put(client: &reqwest::Client, base: &str, sequence: u64) {
    let response = client
        .post(format!("{base}/peat.sidecar.v1.PeatSidecar/PutDocument"))
        .header("content-type", "application/json")
        .json(&serde_json::json!({
            "collection": "telemetry",
            "docId": "stable",
            "jsonData": format!(r#"{{"sequence":{sequence}}}"#),
        }))
        .send()
        .await
        .expect("send PutDocument");
    assert!(
        response.status().is_success(),
        "PutDocument failed: {response:?}"
    );
}

#[tokio::test]
async fn stalled_plain_tcp_connections_are_reclaimed_while_healthy_traffic_continues() {
    let port = reserve_port();
    let data_dir = tempfile::tempdir().expect("temporary data directory");
    let base = format!("http://127.0.0.1:{port}");
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_peat-node"))
            .args([
                "--listen",
                &format!("tcp://127.0.0.1:{port}"),
                "--data-dir",
                data_dir.path().to_str().expect("UTF-8 data path"),
                "--node-id",
                "connection-limits-test",
                "--disable-mdns",
                "--http-header-read-timeout-secs",
                "1",
                "--http-max-connection-idle-secs",
                "1",
                "--rpc-default-timeout-secs",
                "2",
                "--rpc-max-timeout-secs",
                "3",
            ])
            .env("PEAT_NODE_AUTO_SYNC", "false")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start peat-node"),
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build HTTP client");
    wait_ready(&client, &base).await;

    let mut stalled = Vec::new();
    for _ in 0..32 {
        stalled.push(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .expect("open stalled connection"),
        );
    }

    for sequence in 0..40 {
        put(&client, &base, sequence).await;
    }

    for stream in &mut stalled {
        let mut byte = [0_u8; 1];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {}
            Ok(Ok(count)) => panic!("stalled connection returned {count} unexpected bytes"),
            Err(_) => panic!("stalled connection was not reclaimed by the header timeout"),
        }
    }

    put(&client, &base, 40).await;

    let status = child.0.try_wait().expect("check peat-node status");
    if let Some(status) = status {
        let mut output = String::new();
        if let Some(mut stderr) = child.0.stderr.take() {
            let _ = stderr.read_to_string(&mut output);
        }
        panic!("peat-node exited early with {status}: {output}");
    }
}
