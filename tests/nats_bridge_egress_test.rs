//! Real two-node evidence for remote-only Core NATS egress.
//!
//! The test deliberately uses two independent, bounded scripted NATS peers
//! and real Peat/Iroh synchronization. The peers expose raw HPUB frames so
//! payload and private-header identity are asserted without JSON rewriting.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use peat_node::nats_bridge::config::{BridgeConfig, EnabledBridgeConfig};
use peat_node::nats_bridge::envelope::{
    BridgeEnvelope, BRIDGE_ENVELOPE_KIND, BRIDGE_ENVELOPE_VERSION,
};
use peat_node::nats_bridge::runtime::{BridgeRuntime, BridgeRuntimeHandle};
use peat_node::node::{SidecarConfig, SidecarNode};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};

const STEP_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const NO_PUBLISH_WINDOW: Duration = Duration::from_millis(300);
const BARRIER_SUBJECT: &str = "_PEAT.NATS_BRIDGE.READINESS";
const ORIGIN_HEADER: &str = "Peat-Nats-Bridge-Origin";
const INFO: &[u8] = b"INFO {\"server_id\":\"PEATTEST0000000000000000000000000000000000000000000000000000\",\"server_name\":\"peat-egress-test\",\"version\":\"2.10.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\"max_payload\":2097152,\"headers\":true}\r\n";

#[derive(Debug)]
enum PeerCommand {
    Inject { subject: String, payload: Vec<u8> },
    Stop,
}

#[derive(Debug)]
enum PeerEvent {
    Ready,
    Publish {
        subject: String,
        headers: Vec<u8>,
        payload: Vec<u8>,
    },
}

struct ScriptedNats {
    url: String,
    commands: mpsc::Sender<PeerCommand>,
    events: mpsc::Receiver<PeerEvent>,
    task: JoinHandle<io::Result<()>>,
}

impl Drop for ScriptedNats {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ScriptedNats {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind scripted NATS peer");
        let address = listener.local_addr().expect("scripted NATS address");
        let (command_tx, command_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = mpsc::channel(16);
        let task = tokio::spawn(run_nats_peer(listener, command_rx, event_tx));
        Self {
            url: format!("nats://test-user:test-password@{address}"),
            commands: command_tx,
            events: event_rx,
            task,
        }
    }

    async fn wait_ready(&mut self) {
        assert!(matches!(
            timeout(STEP_TIMEOUT, self.events.recv())
                .await
                .expect("NATS readiness timeout"),
            Some(PeerEvent::Ready)
        ));
    }

    async fn inject(&self, subject: &str, payload: &[u8]) {
        timeout(
            STEP_TIMEOUT,
            self.commands.send(PeerCommand::Inject {
                subject: subject.to_owned(),
                payload: payload.to_vec(),
            }),
        )
        .await
        .expect("NATS inject timeout")
        .expect("scripted NATS peer active");
    }

    async fn next_publish(&mut self) -> (String, Vec<u8>, Vec<u8>) {
        match timeout(STEP_TIMEOUT, self.events.recv())
            .await
            .expect("HPUB timeout")
            .expect("scripted NATS peer active")
        {
            PeerEvent::Publish {
                subject,
                headers,
                payload,
            } => (subject, headers, payload),
            PeerEvent::Ready => panic!("duplicate readiness event"),
        }
    }

    async fn assert_no_publish(&mut self) {
        match timeout(NO_PUBLISH_WINDOW, self.events.recv()).await {
            Err(_) => {}
            Ok(Some(PeerEvent::Publish { .. })) => panic!("unexpected local NATS publication"),
            Ok(Some(PeerEvent::Ready)) => panic!("duplicate readiness event"),
            Ok(None) => panic!("scripted NATS peer stopped"),
        }
    }

    async fn finish(mut self) {
        self.commands
            .send(PeerCommand::Stop)
            .await
            .expect("scripted NATS peer active");
        timeout(STEP_TIMEOUT, &mut self.task)
            .await
            .expect("scripted NATS shutdown timeout")
            .expect("scripted NATS task panicked")
            .expect("scripted NATS protocol error");
    }
}

#[derive(Debug)]
enum ClientFrame {
    Connect,
    Sub {
        subject: String,
        sid: String,
    },
    Ping,
    Pong,
    Request {
        reply: String,
    },
    Publish {
        subject: String,
        headers: Vec<u8>,
        payload: Vec<u8>,
    },
}

async fn run_nats_peer(
    listener: TcpListener,
    mut commands: mpsc::Receiver<PeerCommand>,
    events: mpsc::Sender<PeerEvent>,
) -> io::Result<()> {
    let (stream, _) = timeout(STEP_TIMEOUT, listener.accept())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "accept timeout"))??;
    run_nats_connection(stream, &mut commands, &events).await
}

async fn run_nats_connection(
    stream: TcpStream,
    commands: &mut mpsc::Receiver<PeerCommand>,
    events: &mpsc::Sender<PeerEvent>,
) -> io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    writer.write_all(INFO).await?;
    writer.flush().await?;
    let mut subscriptions = BTreeMap::new();
    let mut ready = false;

    loop {
        tokio::select! {
            command = commands.recv(), if ready => match command {
                Some(PeerCommand::Inject { subject, payload }) => {
                    let sid = subscriptions.get(&subject).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "unsubscribed injection")
                    })?;
                    writer.write_all(format!("MSG {subject} {sid} {}\r\n", payload.len()).as_bytes()).await?;
                    writer.write_all(&payload).await?;
                    writer.write_all(b"\r\n").await?;
                    writer.flush().await?;
                }
                Some(PeerCommand::Stop) | None => return Ok(()),
            },
            frame = read_client_frame(&mut reader) => match frame? {
                ClientFrame::Connect | ClientFrame::Pong => {}
                ClientFrame::Sub { subject, sid } => {
                    subscriptions.insert(subject, sid);
                }
                ClientFrame::Ping => {
                    writer.write_all(b"PONG\r\n").await?;
                    writer.flush().await?;
                }
                ClientFrame::Request { reply } => {
                    let reply_sid = subscriptions
                        .iter()
                        .find_map(|(subject, sid)| subject.strip_suffix('*').is_some_and(|prefix| reply.starts_with(prefix)).then_some(sid))
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing reply subscription"))?;
                    const STATUS: &[u8] = b"NATS/1.0 503\r\n\r\n";
                    writer.write_all(format!("HMSG {reply} {reply_sid} {} {}\r\n", STATUS.len(), STATUS.len()).as_bytes()).await?;
                    writer.write_all(STATUS).await?;
                    writer.write_all(b"\r\n").await?;
                    writer.flush().await?;
                    if !ready {
                        ready = true;
                        events.send(PeerEvent::Ready).await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "event receiver dropped"))?;
                    }
                }
                ClientFrame::Publish { subject, headers, payload } => {
                    events.send(PeerEvent::Publish { subject, headers, payload }).await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "event receiver dropped"))?;
                }
            }
        }
    }
}

async fn read_client_frame<R>(reader: &mut R) -> io::Result<ClientFrame>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut line = Vec::new();
    let count = timeout(STEP_TIMEOUT, reader.read_until(b'\n', &mut line))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "client frame timeout"))??;
    if count == 0 || line.len() > 8192 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid client frame",
        ));
    }
    let text = std::str::from_utf8(&line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 client frame"))?
        .trim_end_matches(['\r', '\n']);
    if let Some(rest) = text.strip_prefix("CONNECT ") {
        let json: serde_json::Value = serde_json::from_str(rest)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid CONNECT JSON"))?;
        if json.get("echo").and_then(serde_json::Value::as_bool) != Some(false) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "CONNECT echo enabled",
            ));
        }
        return Ok(ClientFrame::Connect);
    }
    if text == "PING" {
        return Ok(ClientFrame::Ping);
    }
    if text == "PONG" {
        return Ok(ClientFrame::Pong);
    }
    let fields = text.split_ascii_whitespace().collect::<Vec<_>>();
    match fields.as_slice() {
        ["SUB", subject, sid] => Ok(ClientFrame::Sub {
            subject: (*subject).to_owned(),
            sid: (*sid).to_owned(),
        }),
        ["PUB", subject, reply, payload_len]
            if *subject == BARRIER_SUBJECT && *payload_len == "0" =>
        {
            let mut terminator = [0_u8; 2];
            reader.read_exact(&mut terminator).await?;
            if terminator != *b"\r\n" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid PUB terminator",
                ));
            }
            Ok(ClientFrame::Request {
                reply: (*reply).to_owned(),
            })
        }
        ["HPUB", subject, header_len, total_len] => {
            let header_len = header_len.parse::<usize>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid HPUB header length")
            })?;
            let total_len = total_len.parse::<usize>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid HPUB total length")
            })?;
            if header_len > total_len || total_len > 2 * 1024 * 1024 + 64 * 1024 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "oversized HPUB"));
            }
            let mut body = vec![0_u8; total_len + 2];
            reader.read_exact(&mut body).await?;
            if body[total_len..] != *b"\r\n" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid HPUB terminator",
                ));
            }
            Ok(ClientFrame::Publish {
                subject: (*subject).to_owned(),
                headers: body[..header_len].to_vec(),
                payload: body[header_len..total_len].to_vec(),
            })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected client frame",
        )),
    }
}

fn enabled_config(url: &str) -> EnabledBridgeConfig {
    match BridgeConfig::from_raw(Some(url), &["vision.summary=frames".to_owned()])
        .expect("valid bridge config")
    {
        BridgeConfig::Enabled(config) => config,
        BridgeConfig::Disabled => panic!("bridge must be enabled"),
    }
}

async fn mesh_node(node_id: &str, directory: &std::path::Path) -> Arc<SidecarNode> {
    Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: node_id.to_owned(),
            app_id: "nats-egress-test".to_owned(),
            data_dir: directory.to_path_buf(),
            iroh_udp_port: Some(0),
            disable_mdns: true,
            ..Default::default()
        })
        .await
        .expect("create mesh node"),
    )
}

async fn wait_runtime_ready(handle: &BridgeRuntimeHandle) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    while !handle.readiness().snapshot().is_ready() {
        assert!(
            Instant::now() < deadline,
            "bridge runtime did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_document(node: &SidecarNode, collection: &str, doc_id: &str, present: bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let found = node
            .get_document(collection, doc_id)
            .await
            .expect("read synchronized document")
            .is_some();
        if found == present {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "document synchronization timeout for {collection}/{doc_id} (present={present})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn envelope(subject: &str, source: &str, payload: &str) -> String {
    serde_json::to_string(&BridgeEnvelope {
        kind: BRIDGE_ENVELOPE_KIND.to_owned(),
        version: BRIDGE_ENVELOPE_VERSION,
        subject: subject.to_owned(),
        source_node_id: source.to_owned(),
        payload: payload.to_owned(),
    })
    .expect("encode bridge envelope")
}

#[tokio::test]
#[serial_test::serial(iroh_two_node)]
async fn real_sync_is_remote_only_byte_exact_and_fail_closed() {
    timeout(TEST_TIMEOUT, async {
        let mut nats_a = ScriptedNats::start().await;
        let mut nats_b = ScriptedNats::start().await;
        let dir_a = tempfile::tempdir().expect("node A directory");
        let dir_b = tempfile::tempdir().expect("node B directory");
        let node_a = mesh_node("node-a", dir_a.path()).await;
        let node_b = mesh_node("node-b", dir_b.path()).await;
        let runtime_a = BridgeRuntime::spawn(
            enabled_config(&nats_a.url),
            "node-a".to_owned(),
            Arc::clone(&node_a),
        );
        let runtime_b = BridgeRuntime::spawn(
            enabled_config(&nats_b.url),
            "node-b".to_owned(),
            Arc::clone(&node_b),
        );
        nats_a.wait_ready().await;
        nats_b.wait_ready().await;
        wait_runtime_ready(&runtime_a).await;
        wait_runtime_ready(&runtime_b).await;

        let a_port = node_a.bound_udp_port().expect("node A UDP port");
        node_b
            .connect_peer(
                &node_a.endpoint_addr(),
                &[format!("127.0.0.1:{a_port}")],
                "",
            )
            .await
            .expect("connect B to A");
        let peer_deadline = Instant::now() + STEP_TIMEOUT;
        while node_a.connected_peer_count() == 0 || node_b.connected_peer_count() == 0 {
            assert!(
                Instant::now() < peer_deadline,
                "Peat handshake did not settle"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Match the canonical sync integration fixture: transport attachment
        // can precede coordinator readiness by a short interval.
        tokio::time::sleep(Duration::from_secs(2)).await;
        node_a.start_sync().await.expect("start A sync");
        node_b.start_sync().await.expect("start B sync");

        // The methods below are the exact SidecarNode methods dispatched by
        // PutDocument/DeleteDocument; no test-only raw-store mutation is used.
        node_b
            .put_document("frames", "local-rpc", r#"{"local":true}"#)
            .await
            .expect("local RPC-equivalent write");
        node_b
            .delete_document("frames", "local-rpc")
            .await
            .expect("local RPC-equivalent delete");
        node_b
            .create_bridge_document(
                "frames",
                "local-bridge",
                &envelope("vision.summary", "node-b", r#"{"local_bridge":true}"#),
            )
            .await
            .expect("local bridge write");
        nats_b
            .inject("vision.summary", br#"{"local_ingress":true}"#)
            .await;
        let ingress_deadline = Instant::now() + STEP_TIMEOUT;
        while runtime_b.stats().snapshot().stored == 0 {
            assert!(
                Instant::now() < ingress_deadline,
                "local ingress was not stored"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        nats_b.assert_no_publish().await;

        for (doc_id, document) in [
            ("ordinary", r#"{"ordinary":true}"#.to_owned()),
            (
                "malformed-envelope",
                r#"{"kind":"peat.nats-bridge"}"#.to_owned(),
            ),
            (
                "unsupported",
                serde_json::to_string(&BridgeEnvelope {
                    kind: BRIDGE_ENVELOPE_KIND.to_owned(),
                    version: 2,
                    subject: "vision.summary".to_owned(),
                    source_node_id: "node-a".to_owned(),
                    payload: r#"{"unsupported":true}"#.to_owned(),
                })
                .expect("unsupported envelope"),
            ),
            (
                "route-mismatch",
                envelope("Vision.Summary", "node-a", r#"{"mismatch":true}"#),
            ),
        ] {
            node_a
                .put_document("frames", doc_id, &document)
                .await
                .expect("remote negative write");
            wait_document(&node_b, "frames", doc_id, true).await;
            nats_b.assert_no_publish().await;
        }

        // A real Remote(node-a) arrival is suppressed because its durable
        // source is node-b. This is not described as an ordinary fanout echo.
        node_a
            .create_bridge_document(
                "frames",
                "receiver-sourced",
                &envelope("vision.summary", "node-b", r#"{"returned":true}"#),
            )
            .await
            .expect("receiver-sourced envelope");
        wait_document(&node_b, "frames", "receiver-sourced", true).await;
        nats_b.assert_no_publish().await;
        node_a
            .delete_document("frames", "receiver-sourced")
            .await
            .expect("remote tombstone");
        node_a.start_sync().await.expect("flush remote tombstone");
        node_b.start_sync().await.expect("receive remote tombstone");
        // The node-level remote-origin test deterministically injects
        // delete_with_origin and proves the private stream excludes the
        // tombstone. Here the real transport assertion is intentionally only
        // that a delete never becomes HPUB: absence replication timing is an
        // upstream Peat concern and is not used as the egress oracle.
        nats_b.assert_no_publish().await;

        let exact = "  {\"frame\":1.0,\"label\":\"\\u03bb\"}\n\t ";
        let a_published_before = runtime_a.egress_snapshot().published;
        node_a
            .create_bridge_document(
                "frames",
                "remote-valid",
                &envelope("vision.summary", "node-a", exact),
            )
            .await
            .expect("remote bridge write");
        wait_document(&node_b, "frames", "remote-valid", true).await;
        let (subject, headers, payload) = nats_b.next_publish().await;
        assert_eq!(subject, "vision.summary");
        assert_eq!(payload, exact.as_bytes());
        assert_eq!(
            headers,
            b"NATS/1.0\r\nPeat-Nats-Bridge-Origin: node-b\r\n\r\n"
        );
        assert_eq!(
            runtime_a.egress_snapshot().published,
            a_published_before,
            "node A's local creation must not publish on node A"
        );
        assert_eq!(runtime_b.egress_snapshot().published, 1);

        // A second remote change notification for the same document key is
        // terminally suppressed by process-lifetime deduplication.
        node_a
            .put_document(
                "frames",
                "remote-valid",
                &envelope("vision.summary", "node-a", r#"{"duplicate":true}"#),
            )
            .await
            .expect("duplicate remote notification");
        let duplicate_deadline = Instant::now() + Duration::from_secs(20);
        while node_b
            .get_document("frames", "remote-valid")
            .await
            .expect("read duplicate")
            .as_deref()
            .is_none_or(|json| !json.contains("duplicate"))
        {
            assert!(
                Instant::now() < duplicate_deadline,
                "duplicate update did not synchronize"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        nats_b.assert_no_publish().await;

        let later = r#"{"later_valid":true}"#;
        node_a
            .create_bridge_document(
                "frames",
                "later-valid",
                &envelope("vision.summary", "node-a", later),
            )
            .await
            .expect("later valid write");
        wait_document(&node_b, "frames", "later-valid", true).await;
        let (subject, headers, payload) = nats_b.next_publish().await;
        assert_eq!(subject, "vision.summary");
        assert_eq!(payload, later.as_bytes());
        assert_eq!(
            headers,
            format!("NATS/1.0\r\n{ORIGIN_HEADER}: node-b\r\n\r\n").as_bytes()
        );
        assert_eq!(runtime_b.egress_snapshot().published, 2);
        assert_eq!(runtime_b.egress_snapshot().publish_failed, 0);
        assert!(!runtime_a.is_finished());
        assert!(!runtime_b.is_finished());

        nats_a.finish().await;
        nats_b.finish().await;
    })
    .await
    .expect("bounded real two-node egress test timed out");
}
