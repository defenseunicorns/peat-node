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
const MAX_CLIENT_FRAME_BYTES: usize = 8 * 1024;
const INFO: &[u8] = b"INFO {\"server_id\":\"PEATTEST0000000000000000000000000000000000000000000000000000\",\"server_name\":\"peat-test\",\"version\":\"2.10.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\"max_payload\":1048576,\"headers\":true}\r\n";
const BARRIER_SUBJECT: &str = "_PEAT.NATS_BRIDGE.READINESS";

#[derive(Clone, Debug, Eq, PartialEq)]
enum ClientFrame {
    Connect,
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
    Send { subject: String, payload: Vec<u8> },
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
            ClientFrame::Connect | ClientFrame::Pong => {}
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
        }
    }

    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(PeerCommand::Send { subject, payload }) => {
                    send_message(&mut writer, &subscriptions, &subject, &payload).await?;
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
                ClientFrame::Connect | ClientFrame::Sub { .. } | ClientFrame::Request { .. } => {
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
    let count = timeout(STEP_TIMEOUT, reader.read_until(b'\n', &mut bytes))
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
        return Ok(ClientFrame::Connect);
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
        let handle = BridgeRuntime::spawn(
            enabled_config(&peer.url),
            "effective-test-node".to_owned(),
            node,
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
