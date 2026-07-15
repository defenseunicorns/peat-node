//! Bounded protocol-level evidence for the real Core NATS ingress path.
//!
//! The scripted peer implements only the server frames needed by async-nats:
//! `INFO`, `PONG`, the server-generated no-responder status, and injected
//! `MSG`. Client traffic is restricted to `CONNECT`, `SUB`, `PING`, and the
//! private readiness request; every read and coordination point is bounded so
//! a protocol regression fails instead of hanging the test suite.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use peat_node::nats_bridge::config::{BridgeConfig, EnabledBridgeConfig};
use peat_node::nats_bridge::envelope::{
    BridgeEnvelope, BRIDGE_ENVELOPE_KIND, BRIDGE_ENVELOPE_VERSION,
};
use peat_node::nats_bridge::ingress::MAX_INGRESS_PAYLOAD_BYTES;
use peat_node::nats_bridge::runtime::{
    BridgeRuntime, BridgeRuntimeHandle, DeliveredLifecycleEvent, LifecycleSnapshot,
};
use peat_node::node::{SidecarConfig, SidecarNode};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};
use uuid::Version;

const STEP_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_TIMEOUT: Duration = Duration::from_secs(20);
const CLIENT_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CLIENT_FRAME_BYTES: usize = 8 * 1024;
const INFO: &[u8] = b"INFO {\"server_id\":\"PEATTEST0000000000000000000000000000000000000000000000000000\",\"server_name\":\"peat-test\",\"version\":\"2.10.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\"max_payload\":1048576,\"headers\":true}\r\n";
const BARRIER_SUBJECT: &str = "_PEAT.NATS_BRIDGE.READINESS";
const BRIDGE_ORIGIN_HEADER: &str = "Peat-Nats-Bridge-Origin";

#[derive(Clone, Debug, Eq, PartialEq)]
enum ClientFrame {
    Connect {
        echo: Option<bool>,
    },
    Sub {
        subject: String,
        sid: String,
    },
    Ping,
    Pong,
    Request {
        subject: String,
        reply: String,
        payload_len: usize,
    },
    Publish {
        subject: String,
        headers: Vec<u8>,
        payload: Vec<u8>,
    },
}

#[derive(Debug)]
enum PeerEvent {
    Frame {
        connection: usize,
        frame: ClientFrame,
    },
    Barrier {
        connection: usize,
        subjects: BTreeSet<String>,
    },
}

enum PeerCommand {
    ConfirmNoResponders,
    DenySubscription,
    WithholdBarrier,
    Send {
        subject: String,
        payload: Vec<u8>,
    },
    SendHeaders {
        subject: String,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    },
    SendSized {
        subject: String,
        payload_len: usize,
    },
    Disconnect,
    Stop,
}

struct ScriptedPeer {
    url: String,
    commands: mpsc::Sender<PeerCommand>,
    events: mpsc::Receiver<PeerEvent>,
    task: JoinHandle<io::Result<()>>,
}

impl Drop for ScriptedPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ScriptedPeer {
    async fn start(
        expected_subjects: BTreeSet<String>,
        pre_barrier_message: Option<(String, Vec<u8>)>,
        connection_count: usize,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind scripted NATS peer");
        let addr = listener.local_addr().expect("scripted peer address");
        let (command_tx, command_rx) = mpsc::channel(16);
        let (event_tx, event_rx) = mpsc::channel(64);
        let task = tokio::spawn(run_peer(
            listener,
            expected_subjects,
            pre_barrier_message,
            connection_count,
            command_rx,
            event_tx,
        ));
        Self {
            url: format!("nats://test-user:test-password@{addr}"),
            commands: command_tx,
            events: event_rx,
            task,
        }
    }

    async fn command(&self, command: PeerCommand) {
        timeout(STEP_TIMEOUT, self.commands.send(command))
            .await
            .expect("peer command timeout")
            .expect("scripted peer remains active");
    }

    async fn event(&mut self) -> PeerEvent {
        timeout(STEP_TIMEOUT, self.events.recv())
            .await
            .expect("peer event timeout")
            .expect("scripted peer remains active")
    }

    async fn event_within(&mut self, duration: Duration) -> Option<PeerEvent> {
        match timeout(duration, self.events.recv()).await {
            Ok(Some(event)) => Some(event),
            Ok(None) => panic!("scripted peer ended before expected event"),
            Err(_) => None,
        }
    }

    async fn finish(mut self) {
        self.command(PeerCommand::Stop).await;
        let result = timeout(STEP_TIMEOUT, &mut self.task)
            .await
            .expect("scripted peer shutdown timeout")
            .expect("scripted peer task panicked");
        result.expect("scripted peer protocol error");
    }
}

async fn run_peer(
    listener: TcpListener,
    expected_subjects: BTreeSet<String>,
    pre_barrier_message: Option<(String, Vec<u8>)>,
    connection_count: usize,
    mut commands: mpsc::Receiver<PeerCommand>,
    events: mpsc::Sender<PeerEvent>,
) -> io::Result<()> {
    for connection in 0..connection_count {
        let (stream, _) = timeout(STEP_TIMEOUT, listener.accept())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "accept timeout"))??;
        let pre_barrier = (connection == 0)
            .then(|| pre_barrier_message.clone())
            .flatten();
        let continue_running = run_connection(
            stream,
            connection,
            &expected_subjects,
            pre_barrier,
            &mut commands,
            &events,
        )
        .await?;
        if !continue_running {
            return Ok(());
        }
    }
    match timeout(STEP_TIMEOUT, commands.recv()).await {
        Ok(Some(PeerCommand::Stop)) | Ok(None) => Ok(()),
        Ok(Some(_)) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected command after final connection",
        )),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "scripted peer stop timeout",
        )),
    }
}

async fn run_connection(
    stream: TcpStream,
    connection: usize,
    expected_subjects: &BTreeSet<String>,
    pre_barrier_message: Option<(String, Vec<u8>)>,
    commands: &mut mpsc::Receiver<PeerCommand>,
    events: &mpsc::Sender<PeerEvent>,
) -> io::Result<bool> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    writer.write_all(INFO).await?;
    writer.flush().await?;

    let mut subscriptions = BTreeMap::new();
    let mut reply_subscription = None;
    loop {
        let frame = read_client_frame(&mut reader).await?;
        events
            .send(PeerEvent::Frame {
                connection,
                frame: frame.clone(),
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "event receiver dropped"))?;
        match frame {
            ClientFrame::Connect { .. } | ClientFrame::Pong => {}
            ClientFrame::Sub { subject, sid } => {
                if expected_subjects.contains(&subject) {
                    subscriptions.insert(subject, sid);
                } else if subject.starts_with("_INBOX.") && subject.ends_with(".*") {
                    reply_subscription = Some((subject, sid));
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected subscription subject",
                    ));
                }
            }
            ClientFrame::Ping => {
                writer.write_all(b"PONG\r\n").await?;
                writer.flush().await?;
            }
            ClientFrame::Request {
                subject,
                reply,
                payload_len,
            } => {
                if subject != BARRIER_SUBJECT || payload_len != 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected request frame",
                    ));
                }
                if subscriptions.keys().collect::<BTreeSet<_>>()
                    != expected_subjects.iter().collect::<BTreeSet<_>>()
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "readiness request preceded complete SUB set",
                    ));
                }
                let (reply_pattern, reply_sid) = reply_subscription.as_ref().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing reply subscription")
                })?;
                if !reply_pattern
                    .strip_suffix('*')
                    .is_some_and(|prefix| reply.starts_with(prefix))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "request reply does not match reply subscription",
                    ));
                }
                if let Some((subject, payload)) = pre_barrier_message {
                    send_message(&mut writer, &subscriptions, &subject, &payload).await?;
                }
                events
                    .send(PeerEvent::Barrier {
                        connection,
                        subjects: subscriptions.keys().cloned().collect(),
                    })
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "event receiver dropped")
                    })?;
                match timeout(STEP_TIMEOUT, commands.recv()).await {
                    Ok(Some(PeerCommand::ConfirmNoResponders)) => {
                        send_no_responders(&mut writer, &reply, reply_sid).await?;
                        break;
                    }
                    Ok(Some(PeerCommand::WithholdBarrier)) => {
                        break;
                    }
                    Ok(Some(PeerCommand::DenySubscription)) => {
                        writer
                            .write_all(
                                b"-ERR 'Permissions Violation for Subscription to \"vision.summary\"'\r\n",
                            )
                            .await?;
                        writer.flush().await?;
                        match timeout(STEP_TIMEOUT, commands.recv()).await {
                            Ok(Some(PeerCommand::ConfirmNoResponders)) => {
                                send_no_responders(&mut writer, &reply, reply_sid).await?;
                                break;
                            }
                            _ => {
                                return Err(io::Error::new(
                                    io::ErrorKind::TimedOut,
                                    "post-denial barrier release timeout",
                                ));
                            }
                        }
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "barrier release timeout",
                        ));
                    }
                }
            }
            ClientFrame::Publish { .. } => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "publish preceded readiness barrier",
                ));
            }
        }
    }

    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(PeerCommand::Send { subject, payload }) => {
                    send_message(&mut writer, &subscriptions, &subject, &payload).await?;
                }
                Some(PeerCommand::SendHeaders { subject, headers, payload }) => {
                    send_header_message(&mut writer, &subscriptions, &subject, &headers, &payload).await?;
                }
                Some(PeerCommand::SendSized { subject, payload_len }) => {
                    send_sized_message(&mut writer, &subscriptions, &subject, payload_len).await?;
                }
                Some(PeerCommand::Disconnect) => return Ok(true),
                Some(PeerCommand::Stop) | None => return Ok(false),
                Some(PeerCommand::ConfirmNoResponders | PeerCommand::DenySubscription | PeerCommand::WithholdBarrier) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "duplicate barrier release"));
                }
            },
            frame = read_client_frame(&mut reader) => match frame? {
                ClientFrame::Ping => {
                    writer.write_all(b"PONG\r\n").await?;
                    writer.flush().await?;
                }
                ClientFrame::Pong => {}
                ClientFrame::Publish { .. } => {}
                ClientFrame::Connect { .. } | ClientFrame::Sub { .. } | ClientFrame::Request { .. } => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected active client frame"));
                }
            }
        }
    }
}

async fn read_client_frame<R>(reader: &mut R) -> io::Result<ClientFrame>
where
    R: AsyncBufReadExt + Unpin,
{
    let mut bytes = Vec::new();
    let count = timeout(CLIENT_FRAME_TIMEOUT, reader.read_until(b'\n', &mut bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "client frame timeout"))??;
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "client stream ended",
        ));
    }
    if bytes.len() > MAX_CLIENT_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "oversized client frame",
        ));
    }
    let line = std::str::from_utf8(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 client frame"))?
        .trim_end_matches(['\r', '\n']);
    if line.starts_with("HPUB ") {
        let mut fields = line.split_ascii_whitespace();
        let _hpub = fields.next();
        let subject = fields.next();
        let header_len = fields.next().and_then(|value| value.parse::<usize>().ok());
        let total_len = fields.next().and_then(|value| value.parse::<usize>().ok());
        let (Some(subject), Some(header_len), Some(total_len), None) =
            (subject, header_len, total_len, fields.next())
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid HPUB frame",
            ));
        };
        if header_len > total_len || total_len > MAX_INGRESS_PAYLOAD_BYTES + 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized HPUB body",
            ));
        }
        let mut body = vec![0_u8; total_len + 2];
        timeout(STEP_TIMEOUT, reader.read_exact(&mut body))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "HPUB body timeout"))??;
        if body[total_len..] != *b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid HPUB terminator",
            ));
        }
        return Ok(ClientFrame::Publish {
            subject: subject.to_owned(),
            headers: body[..header_len].to_vec(),
            payload: body[header_len..total_len].to_vec(),
        });
    }
    let frame = parse_client_frame(line)?;
    if let ClientFrame::Request { payload_len, .. } = frame {
        if payload_len > MAX_CLIENT_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized request payload",
            ));
        }
        let mut payload_and_crlf = vec![0_u8; payload_len + 2];
        timeout(STEP_TIMEOUT, reader.read_exact(&mut payload_and_crlf))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "request payload timeout"))??;
        if payload_and_crlf[payload_len..] != *b"\r\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid request terminator",
            ));
        }
        return Ok(frame);
    }
    Ok(frame)
}

fn parse_client_frame(line: &str) -> io::Result<ClientFrame> {
    if line.starts_with("CONNECT ") {
        let connect = line.strip_prefix("CONNECT ").expect("prefix checked");
        let json: serde_json::Value = serde_json::from_str(connect)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid CONNECT JSON"))?;
        return Ok(ClientFrame::Connect {
            echo: json.get("echo").and_then(serde_json::Value::as_bool),
        });
    }
    if line == "PING" {
        return Ok(ClientFrame::Ping);
    }
    if line == "PONG" {
        return Ok(ClientFrame::Pong);
    }
    let mut fields = line.split_ascii_whitespace();
    if fields.next() == Some("PUB") {
        let subject = fields.next();
        let reply = fields.next();
        let payload_len = fields.next().and_then(|value| value.parse().ok());
        if let (Some(subject), Some(reply), Some(payload_len), None) =
            (subject, reply, payload_len, fields.next())
        {
            return Ok(ClientFrame::Request {
                subject: subject.to_owned(),
                reply: reply.to_owned(),
                payload_len,
            });
        }
    }
    let mut fields = line.split_ascii_whitespace();
    if fields.next() == Some("SUB") {
        let subject = fields.next();
        let sid = fields.next();
        if let (Some(subject), Some(sid), None) = (subject, sid, fields.next()) {
            return Ok(ClientFrame::Sub {
                subject: subject.to_owned(),
                sid: sid.to_owned(),
            });
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "unexpected client frame",
    ))
}

async fn send_no_responders<W>(writer: &mut W, reply: &str, sid: &str) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    const HEADERS: &[u8] = b"NATS/1.0 503\r\n\r\n";
    writer
        .write_all(format!("HMSG {reply} {sid} {} {}\r\n", HEADERS.len(), HEADERS.len()).as_bytes())
        .await?;
    writer.write_all(HEADERS).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await
}

async fn send_message<W>(
    writer: &mut W,
    subscriptions: &BTreeMap<String, String>,
    subject: &str,
    payload: &[u8],
) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let sid = subscriptions
        .get(subject)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unsubscribed subject"))?;
    writer
        .write_all(format!("MSG {subject} {sid} {}\r\n", payload.len()).as_bytes())
        .await?;
    writer.write_all(payload).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await
}

async fn send_header_message<W>(
    writer: &mut W,
    subscriptions: &BTreeMap<String, String>,
    subject: &str,
    headers: &[(String, String)],
    payload: &[u8],
) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    let sid = subscriptions
        .get(subject)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unsubscribed subject"))?;
    let mut header_block = b"NATS/1.0\r\n".to_vec();
    for (name, value) in headers {
        header_block.extend_from_slice(name.as_bytes());
        header_block.extend_from_slice(b": ");
        header_block.extend_from_slice(value.as_bytes());
        header_block.extend_from_slice(b"\r\n");
    }
    header_block.extend_from_slice(b"\r\n");
    let total_len = header_block.len() + payload.len();
    writer
        .write_all(
            format!(
                "HMSG {subject} {sid} {} {total_len}\r\n",
                header_block.len()
            )
            .as_bytes(),
        )
        .await?;
    writer.write_all(&header_block).await?;
    writer.write_all(payload).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await
}

async fn send_sized_message<W>(
    writer: &mut W,
    subscriptions: &BTreeMap<String, String>,
    subject: &str,
    payload_len: usize,
) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    const CHUNK: &[u8; 4096] = &[b'x'; 4096];
    let sid = subscriptions
        .get(subject)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "unsubscribed subject"))?;
    writer
        .write_all(format!("MSG {subject} {sid} {payload_len}\r\n").as_bytes())
        .await?;
    let mut remaining = payload_len;
    while remaining > 0 {
        let count = remaining.min(CHUNK.len());
        writer.write_all(&CHUNK[..count]).await?;
        remaining -= count;
    }
    writer.write_all(b"\r\n").await?;
    writer.flush().await
}

fn subjects() -> BTreeSet<String> {
    ["vision.summary", "node.health"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn enabled_config(url: &str) -> EnabledBridgeConfig {
    let mappings = vec![
        "vision.summary=frames".to_owned(),
        "node.health=health".to_owned(),
    ];
    match BridgeConfig::from_raw(Some(url), &mappings).expect("valid test bridge config") {
        BridgeConfig::Enabled(config) => config,
        BridgeConfig::Disabled => panic!("test bridge must be enabled"),
    }
}

async fn test_node(dir: &std::path::Path) -> Arc<SidecarNode> {
    Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: "effective-test-node".to_owned(),
            app_id: "nats-ingress-test".to_owned(),
            data_dir: dir.to_path_buf(),
            disable_mdns: true,
            ..Default::default()
        })
        .await
        .expect("create test node"),
    )
}

async fn mesh_node(node_id: &str, dir: &std::path::Path) -> Arc<SidecarNode> {
    Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: node_id.to_owned(),
            app_id: "nats-egress-test".to_owned(),
            data_dir: dir.to_path_buf(),
            iroh_udp_port: Some(0),
            disable_mdns: true,
            ..Default::default()
        })
        .await
        .expect("create mesh test node"),
    )
}

async fn wait_ready(handle: &BridgeRuntimeHandle, ready: bool) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        if handle.readiness().snapshot().is_ready() == ready {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "readiness did not become {ready}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_stats(handle: &BridgeRuntimeHandle, received: u64, stored: u64) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let snapshot = handle.stats().snapshot();
        if snapshot.received == received && snapshot.stored == stored {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "unexpected final stats: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_received(handle: &BridgeRuntimeHandle, received: u64) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let snapshot = handle.stats().snapshot();
        if snapshot.received == received {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "unexpected received count: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_oversized(handle: &BridgeRuntimeHandle, oversized_payloads: u64) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let snapshot = handle.stats().snapshot();
        if snapshot.oversized_payloads == oversized_payloads {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "unexpected oversized count: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_self_suppressed(handle: &BridgeRuntimeHandle, self_suppressed: u64) {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let snapshot = handle.stats().snapshot();
        if snapshot.self_suppressed == self_suppressed {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "unexpected self-suppressed count: {snapshot:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_lifecycle(
    handle: &BridgeRuntimeHandle,
    predicate: impl Fn(LifecycleSnapshot) -> bool,
) -> LifecycleSnapshot {
    let deadline = Instant::now() + STEP_TIMEOUT;
    loop {
        let snapshot = handle.lifecycle_snapshot();
        if predicate(snapshot) {
            return snapshot;
        }
        assert!(
            Instant::now() < deadline,
            "lifecycle diagnostic did not advance"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn connect_uses_no_echo_on_the_shared_bridge_connection() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ScriptedPeer::start(subjects(), None, 1).await;
        let dir = tempfile::tempdir().expect("temporary node directory");
        let node = test_node(dir.path()).await;
        let _handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "divergent-caller-id".to_owned(),
            node,
        );

        loop {
            match peer.event().await {
                PeerEvent::Frame {
                    frame: ClientFrame::Connect { echo },
                    ..
                } => {
                    assert_eq!(echo, Some(false));
                }
                PeerEvent::Barrier { .. } => break,
                PeerEvent::Frame { .. } => {}
            }
        }
        peer.command(PeerCommand::ConfirmNoResponders).await;
        peer.finish().await;
    })
    .await
    .expect("bounded no-echo integration test timed out");
}

#[tokio::test]
async fn origin_marker_only_exact_single_own_value_is_suppressed() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ScriptedPeer::start(subjects(), None, 1).await;
        let dir = tempfile::tempdir().expect("temporary node directory");
        let node = test_node(dir.path()).await;
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "effective-test-node".to_owned(),
            Arc::clone(&node),
        );
        loop {
            if matches!(peer.event().await, PeerEvent::Barrier { .. }) {
                break;
            }
        }
        peer.command(PeerCommand::ConfirmNoResponders).await;
        wait_ready(&handle, true).await;

        peer.command(PeerCommand::SendHeaders {
            subject: "vision.summary".to_owned(),
            headers: vec![(
                BRIDGE_ORIGIN_HEADER.to_owned(),
                "effective-test-node".to_owned(),
            )],
            payload: br#"{"suppressed":true}"#.to_vec(),
        })
        .await;
        wait_self_suppressed(&handle, 1).await;
        assert_eq!(handle.stats().snapshot().received, 0);
        assert!(node
            .list_documents("frames")
            .await
            .expect("list frames")
            .is_empty());

        peer.command(PeerCommand::Send {
            subject: "vision.summary".to_owned(),
            payload: br#"{"case":"absent"}"#.to_vec(),
        })
        .await;
        wait_stats(&handle, 1, 1).await;
        let cases = [
            vec![(BRIDGE_ORIGIN_HEADER.to_owned(), "foreign-node".to_owned())],
            vec![(BRIDGE_ORIGIN_HEADER.to_owned(), String::new())],
            vec![(BRIDGE_ORIGIN_HEADER.to_owned(), "%malformed%".to_owned())],
            vec![(
                BRIDGE_ORIGIN_HEADER.to_owned(),
                "EFFECTIVE-TEST-NODE".to_owned(),
            )],
            vec![
                (
                    BRIDGE_ORIGIN_HEADER.to_owned(),
                    "effective-test-node".to_owned(),
                ),
                (
                    BRIDGE_ORIGIN_HEADER.to_owned(),
                    "effective-test-node".to_owned(),
                ),
            ],
            vec![
                (
                    BRIDGE_ORIGIN_HEADER.to_owned(),
                    "effective-test-node".to_owned(),
                ),
                (BRIDGE_ORIGIN_HEADER.to_owned(), "foreign-node".to_owned()),
            ],
        ];
        for (index, headers) in cases.into_iter().enumerate() {
            peer.command(PeerCommand::SendHeaders {
                subject: "vision.summary".to_owned(),
                headers,
                payload: format!(r#"{{"case":{index}}}"#).into_bytes(),
            })
            .await;
            let expected = u64::try_from(index + 2).expect("small case count");
            wait_stats(&handle, expected, expected).await;
        }
        let documents = node.list_documents("frames").await.expect("list frames");
        assert_eq!(documents.len(), 7);
        let ids = documents.iter().collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 7);
        let stats = handle.stats().snapshot();
        assert_eq!(stats.self_suppressed, 1);
        assert_eq!(stats.oversized_payloads, 0);
        peer.finish().await;
    })
    .await
    .expect("bounded origin-marker integration test timed out");
}

#[tokio::test]
#[serial_test::serial(iroh_two_node)]
async fn egress_remote_acceptance_continues_after_header_aware_max_payload() {
    timeout(Duration::from_secs(60), async {
        let mut peer = ScriptedPeer::start(subjects(), None, 1).await;
        let dir_a = tempfile::tempdir().expect("node-a directory");
        let dir_b = tempfile::tempdir().expect("node-b directory");
        let node_a = mesh_node("egress-node-a", dir_a.path()).await;
        let node_b = mesh_node("egress-node-b", dir_b.path()).await;
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "egress-node-b".to_owned(),
            Arc::clone(&node_b),
        );

        loop {
            if matches!(peer.event().await, PeerEvent::Barrier { .. }) {
                break;
            }
        }
        peer.command(PeerCommand::ConfirmNoResponders).await;
        wait_ready(&handle, true).await;

        let a_port = node_a.bound_udp_port().expect("node-a UDP port");
        node_b
            .connect_peer(
                &node_a.endpoint_addr(),
                &[format!("127.0.0.1:{a_port}")],
                "",
            )
            .await
            .expect("connect mesh peers");
        node_a.start_sync().await.expect("start node-a sync");
        node_b.start_sync().await.expect("start node-b sync");

        let exact_limit_payload = format!("0{}", " ".repeat(MAX_INGRESS_PAYLOAD_BYTES - 1));
        let oversize_envelope = BridgeEnvelope {
            kind: BRIDGE_ENVELOPE_KIND.to_owned(),
            version: BRIDGE_ENVELOPE_VERSION,
            subject: "vision.summary".to_owned(),
            source_node_id: "egress-node-a".to_owned(),
            payload: exact_limit_payload,
        };
        node_a
            .create_bridge_document(
                "frames",
                "max-payload-document",
                &serde_json::to_string(&oversize_envelope).expect("encode max envelope"),
            )
            .await
            .expect("create max bridge document");

        let sync_deadline = Instant::now() + Duration::from_secs(30);
        while node_b
            .get_document("frames", "max-payload-document")
            .await
            .expect("read max document")
            .is_none()
        {
            assert!(
                Instant::now() < sync_deadline,
                "max document did not synchronize"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let no_publish_deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < no_publish_deadline {
            if let Some(PeerEvent::Frame {
                frame: ClientFrame::Publish { .. },
                ..
            }) = peer.event_within(Duration::from_millis(50)).await
            {
                panic!("header-inclusive max payload unexpectedly emitted HPUB");
            }
        }

        let exact_payload = b" {\"frame\":1.0,\"label\":\"\\u03bb\"} \n".to_vec();
        let fitting_envelope = BridgeEnvelope {
            kind: BRIDGE_ENVELOPE_KIND.to_owned(),
            version: BRIDGE_ENVELOPE_VERSION,
            subject: "vision.summary".to_owned(),
            source_node_id: "egress-node-a".to_owned(),
            payload: String::from_utf8(exact_payload.clone()).expect("UTF-8 payload"),
        };
        node_a
            .create_bridge_document(
                "frames",
                "fitting-document",
                &serde_json::to_string(&fitting_envelope).expect("encode fitting envelope"),
            )
            .await
            .expect("create fitting bridge document");

        let fitting_sync_deadline = Instant::now() + Duration::from_secs(30);
        while node_b
            .get_document("frames", "fitting-document")
            .await
            .expect("read fitting document")
            .is_none()
        {
            assert!(
                Instant::now() < fitting_sync_deadline,
                "fitting document did not synchronize"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(handle.readiness().snapshot().is_ready());
        assert!(!handle.is_finished());
        assert_eq!(handle.egress_snapshot().max_payload_exceeded, 1);
        let publish_deadline = Instant::now() + STEP_TIMEOUT;
        while handle.egress_snapshot().published == 0 {
            assert!(
                Instant::now() < publish_deadline,
                "fitting event was not accepted by publisher: {:?}",
                handle.egress_snapshot()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(handle.egress_snapshot().published, 1);
        assert_eq!(handle.egress_snapshot().unavailable, 0);
        assert_eq!(handle.egress_snapshot().publish_failed, 0);
        peer.finish().await;
    })
    .await
    .expect("bounded remote egress integration test timed out");
}

#[tokio::test]
async fn real_core_nats_ingress_preserves_payload_identity_and_atomic_readiness() {
    timeout(TEST_TIMEOUT, async {
        let pre_barrier = b" {\"before\":true} \n".to_vec();
        let mut peer = ScriptedPeer::start(
            subjects(),
            Some(("node.health".to_owned(), pre_barrier.clone())),
            1,
        )
        .await;
        let dir = tempfile::tempdir().expect("temporary node directory");
        let node = test_node(dir.path()).await;
        let mut changes = node.subscribe();
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "effective-test-node".to_owned(),
            Arc::clone(&node),
        );

        let mut traffic = Vec::new();
        loop {
            let event = peer.event().await;
            match event {
                PeerEvent::Frame { connection, frame } => {
                    assert_eq!(connection, 0);
                    traffic.push(frame);
                }
                PeerEvent::Barrier {
                    connection,
                    subjects: seen,
                } => {
                    assert_eq!(connection, 0);
                    assert_eq!(seen, subjects());
                    break;
                }
            }
        }
        let barrier_request = traffic
            .iter()
            .rposition(|frame| matches!(frame, ClientFrame::Request { subject, .. } if subject == BARRIER_SUBJECT))
            .expect("readiness barrier request recorded");
        for subject in subjects() {
            let sub = traffic.iter().position(|frame| {
                matches!(frame, ClientFrame::Sub { subject: actual, .. } if actual == &subject)
            });
            assert!(sub.is_some_and(|position| position < barrier_request));
        }

        let pending = handle.readiness().snapshot();
        assert!(pending.connected);
        assert!(!pending.is_ready());
        assert!(pending.established_subjects.is_empty());
        let pre_event = timeout(STEP_TIMEOUT, changes.recv())
            .await
            .expect("pre-barrier ingest timeout")
            .expect("pre-barrier observer event");
        assert_eq!(pre_event.collection, "health");
        let pre_document = node
            .get_document("health", &pre_event.doc_id)
            .await
            .expect("read pre-barrier document")
            .expect("pre-barrier document exists");
        let pre_envelope: BridgeEnvelope =
            serde_json::from_str(&pre_document).expect("decode pre-barrier envelope");
        assert_eq!(pre_envelope.payload.as_bytes(), pre_barrier);
        assert!(!handle.readiness().snapshot().is_ready());

        peer.command(PeerCommand::ConfirmNoResponders).await;
        wait_ready(&handle, true).await;
        let ready = handle.readiness().snapshot();
        assert_eq!(ready.established_subjects.len(), 2);

        let exact_payloads = [
            b"  { \"alpha\": 1, \"beta\": 2 }  ".as_slice(),
            b"{\"beta\":2,\"alpha\":1}",
            b"{\"value\":1.0}",
            b"{\"label\":\"\\u03bb\"}",
            "{\"label\":\"λ\"}".as_bytes(),
            b"{\"ok\":true}\n\t ",
        ];
        let mut expected_received = 1;
        let mut expected_stored = 1;
        for payload in exact_payloads {
            peer.command(PeerCommand::Send {
                subject: "vision.summary".to_owned(),
                payload: payload.to_vec(),
            })
            .await;
            expected_received += 1;
            expected_stored += 1;
            wait_stats(&handle, expected_received, expected_stored).await;
        }
        let duplicate = b"{\"same\":true}".to_vec();
        for _ in 0..2 {
            peer.command(PeerCommand::Send {
                subject: "vision.summary".to_owned(),
                payload: duplicate.clone(),
            })
            .await;
            expected_received += 1;
            expected_stored += 1;
            wait_stats(&handle, expected_received, expected_stored).await;
        }
        peer.command(PeerCommand::Send {
            subject: "vision.summary".to_owned(),
            payload: vec![0xff, 0xfe],
        })
        .await;
        expected_received += 1;
        wait_received(&handle, expected_received).await;
        peer.command(PeerCommand::Send {
            subject: "vision.summary".to_owned(),
            payload: b"{\"broken\":".to_vec(),
        })
        .await;
        expected_received += 1;
        wait_received(&handle, expected_received).await;

        wait_stats(&handle, expected_received, expected_stored).await;
        let stats = handle.stats().snapshot();
        assert_eq!(stats.invalid_utf8, 1);
        assert_eq!(stats.invalid_json, 1);
        assert_eq!(stats.final_store_failures, 0);

        let mut ids = Vec::new();
        let mut observed_payloads = Vec::new();
        for _ in 0..8 {
            let event = timeout(STEP_TIMEOUT, changes.recv())
                .await
                .expect("observer event timeout")
                .expect("observer event");
            assert_eq!(event.collection, "frames");
            ids.push(event.doc_id.clone());
            let document = node
                .get_document("frames", &event.doc_id)
                .await
                .expect("read bridge document")
                .expect("bridge document exists");
            let value: serde_json::Value =
                serde_json::from_str(&document).expect("bridge document JSON");
            let object = value.as_object().expect("bridge envelope object");
            assert_eq!(object.len(), 5);
            assert_eq!(object["kind"], BRIDGE_ENVELOPE_KIND);
            assert_eq!(object["version"], BRIDGE_ENVELOPE_VERSION);
            assert_eq!(object["subject"], "vision.summary");
            assert_eq!(object["source_node_id"], "effective-test-node");
            let envelope: BridgeEnvelope =
                serde_json::from_value(value).expect("decode bridge envelope");
            observed_payloads.push(envelope.payload.into_bytes());
        }
        assert!(
            changes.try_recv().is_err(),
            "invalid input emitted an observer event"
        );

        let unique_ids = ids.iter().collect::<BTreeSet<_>>();
        assert_eq!(unique_ids.len(), ids.len());
        for id in &ids {
            let parsed = uuid::Uuid::parse_str(id).expect("document ID is a UUID");
            assert_eq!(parsed.get_version(), Some(Version::Random));
        }
        let duplicate_ids = ids
            .iter()
            .zip(&observed_payloads)
            .filter_map(|(id, payload)| (payload == &duplicate).then_some(id))
            .collect::<Vec<_>>();
        assert_eq!(duplicate_ids.len(), 2);
        assert_ne!(duplicate_ids[0], duplicate_ids[1]);
        for payload in exact_payloads {
            assert!(observed_payloads.iter().any(|actual| actual == payload));
        }

        let rendered = format!("{stats:?} {ready:?}");
        for secret in [
            "test-user",
            "test-password",
            "same",
            "broken",
            "parser excerpt",
            "store detail",
        ] {
            assert!(!rendered.contains(secret));
        }
        peer.finish().await;
    })
    .await
    .expect("bounded ingress integration test timed out");
}

#[tokio::test]
async fn reconnect_replays_the_complete_literal_subscription_set_before_readiness() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ScriptedPeer::start(subjects(), None, 2).await;
        let dir = tempfile::tempdir().expect("temporary node directory");
        let node = test_node(dir.path()).await;
        let mut changes = node.subscribe();
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "effective-test-node".to_owned(),
            Arc::clone(&node),
        );

        for connection in 0..2 {
            loop {
                match peer.event().await {
                    PeerEvent::Frame { .. } => {}
                    PeerEvent::Barrier {
                        connection: actual,
                        subjects: seen,
                    } => {
                        assert_eq!(actual, connection);
                        assert_eq!(seen, subjects());
                        break;
                    }
                }
            }
            assert!(!handle.readiness().snapshot().is_ready());
            peer.command(PeerCommand::ConfirmNoResponders).await;
            wait_ready(&handle, true).await;
            if connection == 0 {
                peer.command(PeerCommand::Disconnect).await;
                wait_ready(&handle, false).await;
            }
        }

        let payload = b" {\"after_reconnect\":true} \n".to_vec();
        peer.command(PeerCommand::Send {
            subject: "vision.summary".to_owned(),
            payload: payload.clone(),
        })
        .await;
        wait_stats(&handle, 1, 1).await;
        let event = timeout(STEP_TIMEOUT, changes.recv())
            .await
            .expect("post-reconnect observer timeout")
            .expect("post-reconnect observer event");
        assert_eq!(event.collection, "frames");
        let document = node
            .get_document("frames", &event.doc_id)
            .await
            .expect("read post-reconnect document")
            .expect("post-reconnect document exists");
        let envelope: BridgeEnvelope =
            serde_json::from_str(&document).expect("decode post-reconnect envelope");
        assert_eq!(envelope.payload.as_bytes(), payload);
        peer.finish().await;
    })
    .await
    .expect("bounded reconnect integration test timed out");
}

#[tokio::test]
async fn readiness_barrier_timeout_leaves_the_complete_generation_not_ready() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ScriptedPeer::start(subjects(), None, 1).await;
        let dir = tempfile::tempdir().expect("temporary node directory");
        let node = test_node(dir.path()).await;
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "effective-test-node".to_owned(),
            node,
        );

        loop {
            if let PeerEvent::Barrier { subjects: seen, .. } = peer.event().await {
                assert_eq!(seen, subjects());
                break;
            }
        }
        assert!(!handle.readiness().snapshot().is_ready());
        peer.command(PeerCommand::WithholdBarrier).await;
        tokio::time::sleep(Duration::from_millis(2_250)).await;
        let status = handle.readiness().snapshot();
        assert!(status.connected);
        assert!(status.established_subjects.is_empty());
        assert!(!status.is_ready());
        peer.finish().await;
    })
    .await
    .expect("bounded barrier timeout test timed out");
}

#[tokio::test]
async fn readiness_permission_error_delivered_before_503_invalidates_the_barrier() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ScriptedPeer::start(subjects(), None, 1).await;
        let dir = tempfile::tempdir().expect("temporary node directory");
        let node = test_node(dir.path()).await;
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "effective-test-node".to_owned(),
            node,
        );

        loop {
            if let PeerEvent::Barrier { subjects: seen, .. } = peer.event().await {
                assert_eq!(seen, subjects());
                break;
            }
        }
        let before = handle.lifecycle_snapshot();
        peer.command(PeerCommand::DenySubscription).await;
        let delivered = wait_lifecycle(&handle, |snapshot| {
            snapshot.invalidation_epoch > before.invalidation_epoch
                && snapshot.last_event == DeliveredLifecycleEvent::Error
        })
        .await;
        assert!(delivered.connected);
        assert!(handle
            .readiness()
            .snapshot()
            .established_subjects
            .is_empty());

        peer.command(PeerCommand::ConfirmNoResponders).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let status = handle.readiness().snapshot();
        assert!(!status.is_ready());
        assert!(status.established_subjects.is_empty());
        peer.finish().await;
    })
    .await
    .expect("bounded delivered permission error test timed out");
}

#[tokio::test]
async fn readiness_disconnect_cancels_withheld_barrier_before_timeout() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ScriptedPeer::start(subjects(), None, 2).await;
        let dir = tempfile::tempdir().expect("temporary node directory");
        let node = test_node(dir.path()).await;
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "effective-test-node".to_owned(),
            node,
        );

        loop {
            if let PeerEvent::Barrier { connection: 0, .. } = peer.event().await {
                break;
            }
        }
        peer.command(PeerCommand::WithholdBarrier).await;
        let before = handle.lifecycle_snapshot();
        let started = Instant::now();
        peer.command(PeerCommand::Disconnect).await;
        let disconnected = wait_lifecycle(&handle, |snapshot| {
            snapshot.invalidation_epoch > before.invalidation_epoch && !snapshot.connected
        })
        .await;
        assert_eq!(
            disconnected.last_event,
            DeliveredLifecycleEvent::Disconnected
        );
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(!handle.readiness().snapshot().is_ready());

        loop {
            if let PeerEvent::Barrier { connection: 1, .. } = peer.event().await {
                break;
            }
        }
        assert!(!handle.readiness().snapshot().is_ready());
        peer.command(PeerCommand::ConfirmNoResponders).await;
        wait_ready(&handle, true).await;
        peer.finish().await;
    })
    .await
    .expect("bounded disconnect cancellation test timed out");
}

#[tokio::test]
async fn oversized_message_is_rejected_before_ingress_and_later_json_is_stored() {
    timeout(TEST_TIMEOUT, async {
        let mut peer = ScriptedPeer::start(subjects(), None, 1).await;
        let dir = tempfile::tempdir().expect("temporary node directory");
        let node = test_node(dir.path()).await;
        let mut changes = node.subscribe();
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "effective-test-node".to_owned(),
            Arc::clone(&node),
        );

        loop {
            if let PeerEvent::Barrier { subjects: seen, .. } = peer.event().await {
                assert_eq!(seen, subjects());
                break;
            }
        }
        peer.command(PeerCommand::ConfirmNoResponders).await;
        wait_ready(&handle, true).await;

        peer.command(PeerCommand::SendSized {
            subject: "vision.summary".to_owned(),
            payload_len: MAX_INGRESS_PAYLOAD_BYTES + 1,
        })
        .await;
        wait_oversized(&handle, 1).await;
        let rejected = handle.stats().snapshot();
        assert_eq!(rejected.received, 0);
        assert_eq!(rejected.stored, 0);
        assert_eq!(rejected.invalid_utf8, 0);
        assert_eq!(rejected.invalid_json, 0);
        assert_eq!(rejected.final_store_failures, 0);
        assert!(
            changes.try_recv().is_err(),
            "oversize emitted an observer event"
        );
        assert!(node
            .list_documents("frames")
            .await
            .expect("list frame documents")
            .is_empty());

        let valid = b" {\"after_oversize\":1.0} \n".to_vec();
        peer.command(PeerCommand::Send {
            subject: "vision.summary".to_owned(),
            payload: valid.clone(),
        })
        .await;
        wait_stats(&handle, 1, 1).await;
        let event = timeout(STEP_TIMEOUT, changes.recv())
            .await
            .expect("post-oversize observer timeout")
            .expect("post-oversize observer event");
        let document = node
            .get_document("frames", &event.doc_id)
            .await
            .expect("read post-oversize document")
            .expect("post-oversize document exists");
        let envelope: BridgeEnvelope =
            serde_json::from_str(&document).expect("decode post-oversize envelope");
        assert_eq!(envelope.payload.as_bytes(), valid);
        assert_eq!(handle.stats().snapshot().oversized_payloads, 1);
        peer.finish().await;
    })
    .await
    .expect("bounded oversized ingress test timed out");
}

#[tokio::test]
async fn scripted_peer_rejects_unexpected_and_oversized_client_frames() {
    let error = parse_client_frame("PUB vision.summary 4").expect_err("PUB is out of scope");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);

    let oversized = format!("{}\r\n", "X".repeat(MAX_CLIENT_FRAME_BYTES + 1));
    let mut reader = BufReader::new(oversized.as_bytes());
    let error = read_client_frame(&mut reader)
        .await
        .expect_err("oversized client frame must be rejected");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}
