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
use tokio::time::timeout;

const TEST_TIMEOUT: Duration = Duration::from_secs(60);
const BARRIER_SUBJECT: &str = "_PEAT.NATS_BRIDGE.READINESS";
const ORIGIN_HEADER: &str = "Peat-Nats-Bridge-Origin";
const INFO: &[u8] = b"INFO {\"server_id\":\"PEATTEST0000000000000000000000000000000000000000000000000000\",\"server_name\":\"peat-egress-test\",\"version\":\"2.10.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\"max_payload\":2097152,\"headers\":true}\r\n";

#[derive(Debug)]
enum PeerCommand {
    ReleaseBarrier,
    Inject { subject: String, payload: Vec<u8> },
    Stop,
}

#[derive(Debug)]
enum PeerEvent {
    BarrierObserved,
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

    async fn next_event(&mut self) -> PeerEvent {
        match self.events.recv().await {
            Some(event) => event,
            None => {
                let result = (&mut self.task).await;
                panic!("scripted NATS peer stopped before the expected event: {result:?}");
            }
        }
    }

    async fn send_command(&mut self, command: PeerCommand) {
        if self.commands.send(command).await.is_err() {
            let result = (&mut self.task).await;
            panic!("scripted NATS peer stopped before the expected command: {result:?}");
        }
    }

    async fn wait_barrier(&mut self) {
        assert!(
            matches!(self.next_event().await, PeerEvent::BarrierObserved),
            "NATS publication preceded the readiness barrier"
        );
    }

    async fn release_barrier(&mut self) {
        self.send_command(PeerCommand::ReleaseBarrier).await;
    }

    async fn inject(&mut self, subject: &str, payload: &[u8]) {
        self.send_command(PeerCommand::Inject {
            subject: subject.to_owned(),
            payload: payload.to_vec(),
        })
        .await;
    }

    async fn next_publish(&mut self) -> (String, Vec<u8>, Vec<u8>) {
        match self.next_event().await {
            PeerEvent::Publish {
                subject,
                headers,
                payload,
            } => (subject, headers, payload),
            PeerEvent::BarrierObserved => panic!("duplicate readiness barrier"),
        }
    }

    fn assert_no_buffered_publish(&mut self) {
        match self.events.try_recv() {
            Err(mpsc::error::TryRecvError::Empty) => {}
            Err(mpsc::error::TryRecvError::Disconnected) => {
                panic!("scripted NATS peer stopped")
            }
            Ok(PeerEvent::Publish { .. }) => panic!("unexpected NATS publication"),
            Ok(PeerEvent::BarrierObserved) => panic!("duplicate readiness barrier"),
        }
    }

    async fn finish(mut self) {
        self.send_command(PeerCommand::Stop).await;
        (&mut self.task)
            .await
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
    let (stream, _) = listener.accept().await?;
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
                Some(PeerCommand::ReleaseBarrier) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "duplicate barrier release"));
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
                    if ready {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "duplicate readiness request",
                        ));
                    }
                    let reply_sid = subscriptions
                        .iter()
                        .find_map(|(subject, sid)| subject.strip_suffix('*').is_some_and(|prefix| reply.starts_with(prefix)).then_some(sid))
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing reply subscription"))?;
                    events
                        .send(PeerEvent::BarrierObserved)
                        .await
                        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "event receiver dropped"))?;
                    match commands.recv().await {
                        Some(PeerCommand::ReleaseBarrier) => {}
                        Some(PeerCommand::Stop) | None => return Ok(()),
                        Some(PeerCommand::Inject { .. }) => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "injection preceded barrier release",
                            ));
                        }
                    }
                    const STATUS: &[u8] = b"NATS/1.0 503\r\n\r\n";
                    writer.write_all(format!("HMSG {reply} {reply_sid} {} {}\r\n", STATUS.len(), STATUS.len()).as_bytes()).await?;
                    writer.write_all(STATUS).await?;
                    writer.write_all(b"\r\n").await?;
                    writer.flush().await?;
                    ready = true;
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
    let count = reader.read_until(b'\n', &mut line).await?;
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
    let mut status = handle.readiness().subscribe();
    loop {
        if status.borrow().is_ready() {
            return;
        }
        status
            .changed()
            .await
            .expect("bridge readiness publisher stopped");
    }
}

async fn wait_document(node: &SidecarNode, collection: &str, doc_id: &str, present: bool) {
    loop {
        let found = node
            .get_document(collection, doc_id)
            .await
            .expect("read synchronized document")
            .is_some();
        if found == present {
            return;
        }
        tokio::task::yield_now().await;
    }
}

async fn wait_connected(node_a: &SidecarNode, node_b: &SidecarNode) {
    while node_a.connected_peer_count() == 0 || node_b.connected_peer_count() == 0 {
        tokio::task::yield_now().await;
    }
}

async fn wait_ingress_stored(handle: &BridgeRuntimeHandle) {
    while handle.stats().snapshot().stored == 0 {
        tokio::task::yield_now().await;
    }
}

async fn wait_egress_published(handle: &BridgeRuntimeHandle, expected: u64) {
    while handle.egress_snapshot().published < expected {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        handle.egress_snapshot().published,
        expected,
        "unexpected number of terminal egress publications"
    );
}

async fn wait_document_contains(
    node: &SidecarNode,
    collection: &str,
    doc_id: &str,
    expected: &str,
) {
    loop {
        if node
            .get_document(collection, doc_id)
            .await
            .expect("read synchronized document")
            .as_deref()
            .is_some_and(|json| json.contains(expected))
        {
            return;
        }
        tokio::task::yield_now().await;
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
        let (runtime_a, runtime_b) = tokio::join!(
            BridgeRuntime::spawn(
                enabled_config(&nats_a.url),
                "node-a".to_owned(),
                Arc::clone(&node_a),
            ),
            BridgeRuntime::spawn(
                enabled_config(&nats_b.url),
                "node-b".to_owned(),
                Arc::clone(&node_b),
            ),
        );
        tokio::join!(nats_a.wait_barrier(), nats_b.wait_barrier());
        tokio::join!(nats_a.release_barrier(), nats_b.release_barrier());
        tokio::join!(
            wait_runtime_ready(&runtime_a),
            wait_runtime_ready(&runtime_b)
        );

        let a_port = node_a.bound_udp_port().expect("node A UDP port");
        node_b
            .connect_peer(
                &node_a.endpoint_addr(),
                &[format!("127.0.0.1:{a_port}")],
                "",
            )
            .await
            .expect("connect B to A");
        wait_connected(&node_a, &node_b).await;
        node_a.start_sync().await.expect("start A sync");
        node_b.start_sync().await.expect("start B sync");

        // The methods below are the exact SidecarNode methods dispatched by
        // PutDocument/DeleteDocument; no test-only raw-store mutation is used.
        // The first later remote publication is the causal fence proving these
        // earlier local operations emitted no NATS frames.
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
        wait_ingress_stored(&runtime_b).await;

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
        }

        // A real Remote(node-a) arrival is suppressed because its durable
        // source is node-b. The following valid publication is the causal
        // fence for this and every earlier negative event.
        node_a
            .create_bridge_document(
                "frames",
                "receiver-sourced",
                &envelope("vision.summary", "node-b", r#"{"returned":true}"#),
            )
            .await
            .expect("receiver-sourced envelope");
        wait_document(&node_b, "frames", "receiver-sourced", true).await;

        let exact = "  {\"frame\":1.0,\"label\":\"\\u03bb\"}\n\t ";
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
        wait_egress_published(&runtime_b, 1).await;

        // A second remote change notification for the same document key is
        // terminally suppressed by durable deduplication.
        node_a
            .put_document(
                "frames",
                "remote-valid",
                &envelope("vision.summary", "node-a", r#"{"duplicate":true}"#),
            )
            .await
            .expect("duplicate remote notification");
        wait_document_contains(&node_b, "frames", "remote-valid", "duplicate").await;

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
        wait_egress_published(&runtime_b, 2).await;
        nats_b.assert_no_buffered_publish();

        // Node B's local bridge creation and local NATS ingress are valid
        // remote deliveries on node A. The receiver-sourced envelope was
        // authored locally on A; bounded sync no longer echoes that unchanged
        // document back from B. Consume the exact remaining set without
        // assuming CRDT document ordering.
        wait_egress_published(&runtime_a, 2).await;
        let mut initial_a_payloads = Vec::with_capacity(2);
        for _ in 0..2 {
            let (subject, headers, payload) = nats_a.next_publish().await;
            assert_eq!(subject, "vision.summary");
            assert_eq!(
                headers,
                format!("NATS/1.0\r\n{ORIGIN_HEADER}: node-a\r\n\r\n").as_bytes()
            );
            initial_a_payloads.push(payload);
        }
        initial_a_payloads.sort();
        let mut expected_initial_a_payloads = vec![
            br#"{"local_bridge":true}"#.to_vec(),
            br#"{"local_ingress":true}"#.to_vec(),
        ];
        expected_initial_a_payloads.sort();
        assert_eq!(initial_a_payloads, expected_initial_a_payloads);
        nats_a.assert_no_buffered_publish();

        // A final reverse-direction publication proves the bridge continues
        // after the exact node-A publication set above.
        let reverse = r#"{"reverse_fence":true}"#;
        node_b
            .create_bridge_document(
                "frames",
                "reverse-fence",
                &envelope("vision.summary", "node-b", reverse),
            )
            .await
            .expect("reverse remote bridge write");
        wait_document(&node_a, "frames", "reverse-fence", true).await;
        let (subject, headers, payload) = nats_a.next_publish().await;
        assert_eq!(subject, "vision.summary");
        assert_eq!(payload, reverse.as_bytes());
        assert_eq!(
            headers,
            format!("NATS/1.0\r\n{ORIGIN_HEADER}: node-a\r\n\r\n").as_bytes()
        );
        wait_egress_published(&runtime_a, 3).await;
        nats_a.assert_no_buffered_publish();

        let operations_a = runtime_a.operations_snapshot();
        let operations_b = runtime_b.operations_snapshot();
        assert_eq!(operations_a.remote_published, 3);
        assert_eq!(operations_b.remote_published, 2);
        assert_eq!(operations_a.publish_failures, 0);
        assert_eq!(operations_b.publish_failures, 0);
        assert_eq!(operations_a.queue_loss, 0);
        assert_eq!(operations_b.queue_loss, 0);
        assert_eq!(operations_a.ledger_failures, 0);
        assert_eq!(operations_b.ledger_failures, 0);
        assert!(operations_a.ready);
        assert!(operations_b.ready);
        assert!(!runtime_a.is_finished());
        assert!(!runtime_b.is_finished());

        nats_a.finish().await;
        nats_b.finish().await;
    })
    .await
    .expect("bounded real two-node egress test timed out");
}
