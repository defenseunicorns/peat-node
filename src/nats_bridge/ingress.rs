//! Bounded, serial persistence pipeline for messages received from Core NATS.
//!
//! All configured subjects share one FIFO. Senders await capacity rather than
//! deliberately dropping at this boundary, and exactly one receiver validates
//! messages and performs create-only Peat writes. A message's UUID and encoded
//! envelope are created once before bounded retries so one accepted message
//! cannot become multiple documents.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::Subject;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::nats_bridge::envelope::{BridgeEnvelope, IngressValidationError};
use crate::node::{CreateBridgeDocumentError, SidecarNode};

/// Process-wide number of raw NATS messages allowed to await persistence.
pub const INGRESS_QUEUE_CAPACITY: usize = 256;

/// Largest Core NATS payload the bridge will clone into its ingress queue.
pub const MAX_INGRESS_PAYLOAD_BYTES: usize = 1_048_576;

/// Maximum create-only storage calls made for one accepted NATS message.
pub const STORE_MAX_ATTEMPTS: usize = 3;

/// Delay after the first transient storage failure.
pub const STORE_RETRY_DELAY_FIRST: Duration = Duration::from_millis(50);

/// Delay after the second transient storage failure.
pub const STORE_RETRY_DELAY_SECOND: Duration = Duration::from_millis(200);

const STORE_ATTEMPT_DELAYS: [Option<Duration>; STORE_MAX_ATTEMPTS] = [
    Some(STORE_RETRY_DELAY_FIRST),
    Some(STORE_RETRY_DELAY_SECOND),
    None,
];

/// Cadence for per-subject summaries of suppressed invalid input warnings.
pub const INVALID_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// Minimum interval between oversized-payload warnings for one configured subject.
pub const OVERSIZED_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// Cloneable, label-free ingress counters shared with runtime diagnostics.
#[derive(Clone, Default)]
pub struct IngressStats {
    inner: Arc<IngressStatsInner>,
}

#[derive(Default)]
struct IngressStatsInner {
    received: AtomicU64,
    stored: AtomicU64,
    invalid_utf8: AtomicU64,
    invalid_json: AtomicU64,
    oversized_payloads: AtomicU64,
    final_store_failures: AtomicU64,
    slow_consumer_events: AtomicU64,
}

/// Point-in-time values for the bounded ingress counter set.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IngressStatsSnapshot {
    pub received: u64,
    pub stored: u64,
    pub invalid_utf8: u64,
    pub invalid_json: u64,
    pub oversized_payloads: u64,
    pub final_store_failures: u64,
    pub slow_consumer_events: u64,
}

impl IngressStats {
    /// Read every counter without introducing payload- or subject-derived labels.
    pub fn snapshot(&self) -> IngressStatsSnapshot {
        IngressStatsSnapshot {
            received: self.inner.received.load(Ordering::Relaxed),
            stored: self.inner.stored.load(Ordering::Relaxed),
            invalid_utf8: self.inner.invalid_utf8.load(Ordering::Relaxed),
            invalid_json: self.inner.invalid_json.load(Ordering::Relaxed),
            oversized_payloads: self.inner.oversized_payloads.load(Ordering::Relaxed),
            final_store_failures: self.inner.final_store_failures.load(Ordering::Relaxed),
            slow_consumer_events: self.inner.slow_consumer_events.load(Ordering::Relaxed),
        }
    }

    /// Record one Core NATS slow-consumer event without adding a dynamic label.
    pub fn record_slow_consumer(&self) {
        self.inner
            .slow_consumer_events
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one payload rejected before ingress allocation or queueing.
    pub fn record_oversized_payload(&self) {
        self.inner
            .oversized_payloads
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Fixed reason carried by a payload-safe ingress action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressActionKind {
    OversizedPayload,
    InvalidOccurrence,
    InvalidWarning,
    InvalidSummary,
    StoreRetry,
    StoreFailure,
}

/// Fixed error classifications safe to expose in bridge diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressErrorKind {
    OversizedPayload,
    InvalidUtf8,
    InvalidJson,
    InvalidInputSummary,
    EnvelopeEncoding,
    AlreadyExists,
    InvalidInput,
    Encryption,
    Conversion,
    StoreRead,
    StoreWrite,
}

impl IngressErrorKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::OversizedPayload => "oversized_payload",
            Self::InvalidUtf8 => "invalid_utf8",
            Self::InvalidJson => "invalid_json",
            Self::InvalidInputSummary => "invalid_input_summary",
            Self::EnvelopeEncoding => "envelope_encoding",
            Self::AlreadyExists => "already_exists",
            Self::InvalidInput => "invalid_input",
            Self::Encryption => "encryption",
            Self::Conversion => "conversion",
            Self::StoreRead => "store_read",
            Self::StoreWrite => "store_write",
        }
    }
}

/// Complete typed data behind one ingress log event.
///
/// The shape intentionally has no payload, URL, parser error, source error, or
/// error-chain field. All strings are validated route metadata or generated IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IngressAction {
    pub kind: IngressActionKind,
    pub subject: Option<String>,
    pub collection: Option<String>,
    pub payload_bytes: Option<usize>,
    pub document_id: Option<String>,
    pub attempt: Option<usize>,
    pub delay_ms: Option<u64>,
    pub suppressed_count: Option<u64>,
    pub error_kind: IngressErrorKind,
}

impl IngressAction {
    fn emit(&self) {
        let error_kind = self.error_kind.as_str();
        let subject = self.subject.as_deref().unwrap_or("");
        let collection = self.collection.as_deref().unwrap_or("");
        let payload_bytes = self.payload_bytes.unwrap_or_default();
        let document_id = self.document_id.as_deref().unwrap_or("");
        let attempt = self.attempt.unwrap_or_default();
        let delay_ms = self.delay_ms.unwrap_or_default();
        let suppressed_count = self.suppressed_count.unwrap_or_default();
        match self.kind {
            IngressActionKind::InvalidOccurrence | IngressActionKind::StoreRetry => debug!(
                subject,
                collection,
                payload_bytes,
                document_id,
                attempt,
                delay_ms,
                error_kind,
                "NATS bridge ingress action"
            ),
            IngressActionKind::OversizedPayload
            | IngressActionKind::InvalidWarning
            | IngressActionKind::InvalidSummary
            | IngressActionKind::StoreFailure => warn!(
                subject,
                collection,
                payload_bytes,
                document_id,
                attempt,
                suppressed_count,
                error_kind,
                "NATS bridge ingress failure"
            ),
        }
    }
}

trait IngressActionEmitter: Send + Sync + 'static {
    fn emit(&self, action: IngressAction);
}

impl<T> IngressActionEmitter for Arc<T>
where
    T: IngressActionEmitter + ?Sized,
{
    fn emit(&self, action: IngressAction) {
        (**self).emit(action);
    }
}

#[derive(Clone, Copy)]
struct TracingIngressEmitter;

impl IngressActionEmitter for TracingIngressEmitter {
    fn emit(&self, action: IngressAction) {
        action.emit();
    }
}

#[derive(Default)]
struct InvalidWarningState {
    warning_emitted: bool,
    suppressed: u64,
}

#[derive(Default)]
struct OversizedWarningState {
    next_warning: Option<Instant>,
    suppressed: u64,
}

/// Cloneable, bounded diagnostics state for pre-queue oversized rejections.
#[derive(Clone)]
pub struct IngressDiagnostics {
    warnings: Arc<Mutex<HashMap<String, OversizedWarningState>>>,
    emitter: Arc<dyn IngressActionEmitter>,
}

impl IngressDiagnostics {
    /// Pre-seed diagnostics exclusively from validated configured routes.
    pub fn new(routes: impl IntoIterator<Item = (Subject, String)>) -> Self {
        Self::with_emitter(routes, TracingIngressEmitter)
    }

    fn with_emitter<E>(routes: impl IntoIterator<Item = (Subject, String)>, emitter: E) -> Self
    where
        E: IngressActionEmitter,
    {
        let warnings = routes
            .into_iter()
            .map(|(subject, _collection)| (subject.to_string(), OversizedWarningState::default()))
            .collect();
        Self {
            warnings: Arc::new(Mutex::new(warnings)),
            emitter: Arc::new(emitter),
        }
    }

    /// Emit at most one safe warning per configured subject per interval.
    pub fn record_oversized(&self, subject: &Subject, collection: &str, payload_bytes: usize) {
        self.record_oversized_at(subject, collection, payload_bytes, Instant::now());
    }

    fn record_oversized_at(
        &self,
        subject: &Subject,
        collection: &str,
        payload_bytes: usize,
        now: Instant,
    ) {
        let mut warnings = self
            .warnings
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // The map is seeded only from configured subjects and never grows from input.
        let Some(state) = warnings.get_mut(subject.as_str()) else {
            return;
        };
        if state.next_warning.is_some_and(|deadline| now < deadline) {
            state.suppressed = state.suppressed.saturating_add(1);
            return;
        }
        let suppressed_count = state.suppressed;
        state.suppressed = 0;
        state.next_warning = Some(now + OVERSIZED_WARNING_INTERVAL);
        drop(warnings);

        self.emitter.emit(IngressAction {
            kind: IngressActionKind::OversizedPayload,
            subject: Some(subject.to_string()),
            collection: Some(collection.to_owned()),
            payload_bytes: Some(payload_bytes),
            document_id: None,
            attempt: None,
            delay_ms: None,
            suppressed_count: Some(suppressed_count),
            error_kind: IngressErrorKind::OversizedPayload,
        });
    }
}

/// Decide admission without allocating or touching the shared ingress queue.
pub(crate) fn is_payload_oversized(payload_bytes: usize) -> bool {
    payload_bytes > MAX_INGRESS_PAYLOAD_BYTES
}

/// One routed message awaiting validation and persistence.
#[derive(Debug)]
pub struct IngressItem {
    subject: Subject,
    collection: String,
    payload: Vec<u8>,
}

impl IngressItem {
    /// Construct an item from validated literal route metadata and raw bytes.
    pub fn new(subject: Subject, collection: String, payload: Vec<u8>) -> Self {
        Self {
            subject,
            collection,
            payload,
        }
    }
}

/// Cloneable producer for the one process-wide ingress FIFO.
#[derive(Clone)]
pub struct IngressSender {
    tx: mpsc::Sender<IngressItem>,
}

impl IngressSender {
    /// Await shared queue capacity and enqueue one raw message in FIFO order.
    pub async fn send(&self, item: IngressItem) -> Result<(), mpsc::error::SendError<IngressItem>> {
        self.tx.send(item).await
    }
}

/// Create the sole bounded ingress channel used by all subject readers.
pub fn ingress_channel() -> (IngressSender, mpsc::Receiver<IngressItem>) {
    let (tx, rx) = mpsc::channel(INGRESS_QUEUE_CAPACITY);
    (IngressSender { tx }, rx)
}

/// Narrow create-only persistence seam used by the serial processor.
pub trait BridgeDocumentWriter: Send + Sync + 'static {
    fn create_bridge_document<'a>(
        &'a self,
        collection: &'a str,
        doc_id: &'a str,
        envelope_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), CreateBridgeDocumentError>> + Send + 'a>>;
}

impl BridgeDocumentWriter for Arc<SidecarNode> {
    fn create_bridge_document<'a>(
        &'a self,
        collection: &'a str,
        doc_id: &'a str,
        envelope_json: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), CreateBridgeDocumentError>> + Send + 'a>> {
        Box::pin(SidecarNode::create_bridge_document(
            self,
            collection,
            doc_id,
            envelope_json,
        ))
    }
}

/// Drain one ingress receiver serially until every sender has been dropped.
pub async fn run_ingress_processor<W>(
    rx: mpsc::Receiver<IngressItem>,
    source_node_id: String,
    writer: W,
    stats: IngressStats,
    configured_subjects: impl IntoIterator<Item = Subject>,
) where
    W: BridgeDocumentWriter,
{
    run_ingress_processor_with_emitter(
        rx,
        source_node_id,
        writer,
        stats,
        configured_subjects,
        TracingIngressEmitter,
    )
    .await;
}

async fn run_ingress_processor_with_emitter<W, E>(
    mut rx: mpsc::Receiver<IngressItem>,
    source_node_id: String,
    writer: W,
    stats: IngressStats,
    configured_subjects: impl IntoIterator<Item = Subject>,
    emitter: E,
) where
    W: BridgeDocumentWriter,
    E: IngressActionEmitter,
{
    let mut invalid_warnings = configured_subjects
        .into_iter()
        .map(|subject| (subject.to_string(), InvalidWarningState::default()))
        .collect::<HashMap<_, _>>();
    let mut summary_interval = tokio::time::interval_at(
        Instant::now() + INVALID_WARNING_INTERVAL,
        INVALID_WARNING_INTERVAL,
    );
    summary_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            item = rx.recv() => {
                let Some(item) = item else { return; };
                process_item(
                    &source_node_id,
                    &writer,
                    &stats,
                    &emitter,
                    &mut invalid_warnings,
                    item,
                ).await;
            }
            _ = summary_interval.tick() => {
                emit_invalid_summaries(&emitter, &mut invalid_warnings);
            }
        }
    }
}

async fn process_item<W, E>(
    source_node_id: &str,
    writer: &W,
    stats: &IngressStats,
    emitter: &E,
    invalid_warnings: &mut HashMap<String, InvalidWarningState>,
    item: IngressItem,
) where
    W: BridgeDocumentWriter,
    E: IngressActionEmitter,
{
    stats.inner.received.fetch_add(1, Ordering::Relaxed);
    let envelope =
        match BridgeEnvelope::from_payload(item.subject.as_str(), source_node_id, &item.payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                record_invalid(stats, emitter, invalid_warnings, &item, error);
                return;
            }
        };

    let doc_id = Uuid::new_v4().to_string();
    let Ok(envelope_json) = serde_json::to_string(&envelope) else {
        emitter.emit(store_action(
            IngressActionKind::StoreFailure,
            &item,
            &doc_id,
            0,
            None,
            IngressErrorKind::EnvelopeEncoding,
        ));
        return;
    };

    for (attempt, retry_delay) in STORE_ATTEMPT_DELAYS.into_iter().enumerate() {
        match writer
            .create_bridge_document(&item.collection, &doc_id, &envelope_json)
            .await
        {
            Ok(()) => {
                stats.inner.stored.fetch_add(1, Ordering::Relaxed);
                return;
            }
            Err(error) if is_transient(error) && retry_delay.is_some() => {
                let delay = retry_delay.expect("guard proves retry delay exists");
                emitter.emit(store_action(
                    IngressActionKind::StoreRetry,
                    &item,
                    &doc_id,
                    attempt + 1,
                    Some(delay),
                    error.into(),
                ));
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                stats
                    .inner
                    .final_store_failures
                    .fetch_add(1, Ordering::Relaxed);
                emitter.emit(store_action(
                    IngressActionKind::StoreFailure,
                    &item,
                    &doc_id,
                    attempt + 1,
                    None,
                    error.into(),
                ));
                return;
            }
        }
    }
}

fn record_invalid<E: IngressActionEmitter>(
    stats: &IngressStats,
    emitter: &E,
    invalid_warnings: &mut HashMap<String, InvalidWarningState>,
    item: &IngressItem,
    error: IngressValidationError,
) {
    let error_kind = match error {
        IngressValidationError::InvalidUtf8 => {
            stats.inner.invalid_utf8.fetch_add(1, Ordering::Relaxed);
            IngressErrorKind::InvalidUtf8
        }
        IngressValidationError::InvalidJson => {
            stats.inner.invalid_json.fetch_add(1, Ordering::Relaxed);
            IngressErrorKind::InvalidJson
        }
    };
    emitter.emit(invalid_action(
        IngressActionKind::InvalidOccurrence,
        item,
        error_kind,
        None,
    ));

    // The map is seeded exclusively from validated configured subjects. An
    // unexpected subject is still counted/debugged but never creates a key.
    let Some(state) = invalid_warnings.get_mut(item.subject.as_str()) else {
        return;
    };
    if state.warning_emitted {
        state.suppressed = state.suppressed.saturating_add(1);
    } else {
        state.warning_emitted = true;
        emitter.emit(invalid_action(
            IngressActionKind::InvalidWarning,
            item,
            error_kind,
            None,
        ));
    }
}

fn emit_invalid_summaries<E: IngressActionEmitter>(
    emitter: &E,
    invalid_warnings: &mut HashMap<String, InvalidWarningState>,
) {
    for (subject, state) in invalid_warnings {
        if state.suppressed > 0 {
            emitter.emit(IngressAction {
                kind: IngressActionKind::InvalidSummary,
                subject: Some(subject.to_string()),
                collection: None,
                payload_bytes: None,
                document_id: None,
                attempt: None,
                delay_ms: None,
                suppressed_count: Some(state.suppressed),
                error_kind: IngressErrorKind::InvalidInputSummary,
            });
        }
        state.warning_emitted = false;
        state.suppressed = 0;
    }
}

fn invalid_action(
    kind: IngressActionKind,
    item: &IngressItem,
    error_kind: IngressErrorKind,
    suppressed_count: Option<u64>,
) -> IngressAction {
    IngressAction {
        kind,
        subject: Some(item.subject.to_string()),
        collection: Some(item.collection.clone()),
        payload_bytes: Some(item.payload.len()),
        document_id: None,
        attempt: None,
        delay_ms: None,
        suppressed_count,
        error_kind,
    }
}

fn store_action(
    kind: IngressActionKind,
    item: &IngressItem,
    doc_id: &str,
    attempt: usize,
    delay: Option<Duration>,
    error_kind: IngressErrorKind,
) -> IngressAction {
    IngressAction {
        kind,
        subject: Some(item.subject.to_string()),
        collection: Some(item.collection.clone()),
        payload_bytes: Some(item.payload.len()),
        document_id: Some(doc_id.to_owned()),
        attempt: Some(attempt),
        delay_ms: delay.map(|value| value.as_millis().min(u128::from(u64::MAX)) as u64),
        suppressed_count: None,
        error_kind,
    }
}

impl From<CreateBridgeDocumentError> for IngressErrorKind {
    fn from(value: CreateBridgeDocumentError) -> Self {
        match value {
            CreateBridgeDocumentError::AlreadyExists => Self::AlreadyExists,
            CreateBridgeDocumentError::InvalidInput => Self::InvalidInput,
            CreateBridgeDocumentError::Encryption => Self::Encryption,
            CreateBridgeDocumentError::Conversion => Self::Conversion,
            CreateBridgeDocumentError::StoreRead => Self::StoreRead,
            CreateBridgeDocumentError::StoreWrite => Self::StoreWrite,
        }
    }
}

fn is_transient(error: CreateBridgeDocumentError) -> bool {
    matches!(
        error,
        CreateBridgeDocumentError::StoreRead | CreateBridgeDocumentError::StoreWrite
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::{self, Write};
    use std::sync::Mutex;

    use tokio::sync::{mpsc, Notify};
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[derive(Clone, Debug)]
    struct WriteCall {
        collection: String,
        doc_id: String,
        envelope_json: String,
        at: tokio::time::Instant,
    }

    #[derive(Clone, Default)]
    struct FakeWriter {
        calls: Arc<Mutex<Vec<WriteCall>>>,
        results: Arc<Mutex<VecDeque<Result<(), CreateBridgeDocumentError>>>>,
        call_tx: Arc<Mutex<Option<mpsc::Sender<WriteCall>>>>,
        blocked: Arc<Mutex<bool>>,
        release: Arc<Notify>,
    }

    #[derive(Clone, Default)]
    struct RecordingEmitter {
        actions: Arc<Mutex<Vec<IngressAction>>>,
    }

    impl RecordingEmitter {
        fn actions(&self) -> Vec<IngressAction> {
            self.actions.lock().expect("actions lock").clone()
        }
    }

    impl IngressActionEmitter for RecordingEmitter {
        fn emit(&self, action: IngressAction) {
            self.actions.lock().expect("actions lock").push(action);
        }
    }

    #[derive(Clone, Default)]
    struct CaptureMakeWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    struct CaptureWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes
                .lock()
                .expect("capture lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CaptureMakeWriter {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CaptureWriter {
                bytes: Arc::clone(&self.bytes),
            }
        }
    }

    impl FakeWriter {
        fn with_results(
            results: impl IntoIterator<Item = Result<(), CreateBridgeDocumentError>>,
        ) -> Self {
            Self {
                results: Arc::new(Mutex::new(results.into_iter().collect())),
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<WriteCall> {
            self.calls.lock().expect("calls lock").clone()
        }

        fn observe_calls(&self) -> mpsc::Receiver<WriteCall> {
            let (tx, rx) = mpsc::channel(16);
            *self.call_tx.lock().expect("call sender lock") = Some(tx);
            rx
        }

        fn block(&self) {
            *self.blocked.lock().expect("blocked lock") = true;
        }

        fn release(&self) {
            *self.blocked.lock().expect("blocked lock") = false;
            self.release.notify_waiters();
        }
    }

    impl BridgeDocumentWriter for FakeWriter {
        fn create_bridge_document<'a>(
            &'a self,
            collection: &'a str,
            doc_id: &'a str,
            envelope_json: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), CreateBridgeDocumentError>> + Send + 'a>>
        {
            Box::pin(async move {
                let call = WriteCall {
                    collection: collection.to_owned(),
                    doc_id: doc_id.to_owned(),
                    envelope_json: envelope_json.to_owned(),
                    at: tokio::time::Instant::now(),
                };
                self.calls.lock().expect("calls lock").push(call.clone());
                let call_tx = self.call_tx.lock().expect("call sender lock").clone();
                if let Some(tx) = call_tx {
                    let _ = tx.send(call).await;
                }
                while *self.blocked.lock().expect("blocked lock") {
                    self.release.notified().await;
                }
                self.results
                    .lock()
                    .expect("results lock")
                    .pop_front()
                    .unwrap_or(Ok(()))
            })
        }
    }

    fn item(subject: &str, collection: &str, payload: &[u8]) -> IngressItem {
        IngressItem::new(subject.into(), collection.to_owned(), payload.to_vec())
    }

    fn configured_subjects() -> [Subject; 2] {
        ["alpha".into(), "beta".into()]
    }

    async fn wait_for_received(stats: &IngressStats, expected: u64) {
        for _ in 0..100 {
            if stats.snapshot().received == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("processor did not receive {expected} items");
    }

    #[tokio::test]
    async fn queue_capacity_blocks_the_257th_send() {
        let (sender, _rx) = ingress_channel();
        for sequence in 0..INGRESS_QUEUE_CAPACITY {
            sender
                .send(item(
                    "vision.summary",
                    "frames",
                    sequence.to_string().as_bytes(),
                ))
                .await
                .expect("queue should have capacity");
        }

        let blocked = tokio::spawn({
            let sender = sender.clone();
            async move { sender.send(item("vision.summary", "frames", b"257")).await }
        });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished(), "the 257th send must await capacity");
        blocked.abort();
    }

    #[tokio::test]
    async fn blocked_writer_preserves_global_fifo_across_subjects() {
        let writer = FakeWriter::default();
        writer.block();
        let mut observed = writer.observe_calls();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
            IngressStats::default(),
            configured_subjects(),
        ));

        sender
            .send(item("alpha", "collection-a", br#"{"sequence":1}"#))
            .await
            .expect("first send");
        let first = observed.recv().await.expect("first write call");
        sender
            .send(item("beta", "collection-b", br#"{"sequence":2}"#))
            .await
            .expect("second send");
        sender
            .send(item("alpha", "collection-a", br#"{"sequence":3}"#))
            .await
            .expect("third send");
        tokio::task::yield_now().await;
        assert!(
            observed.try_recv().is_err(),
            "one blocked writer must serialize work"
        );

        writer.release();
        let second = observed.recv().await.expect("second write call");
        let third = observed.recv().await.expect("third write call");
        assert_eq!(
            [first.collection, second.collection, third.collection],
            ["collection-a", "collection-b", "collection-a"]
        );

        drop(sender);
        worker.await.expect("worker should finish");
    }

    #[tokio::test]
    async fn invalid_utf8_and_json_never_reach_writer_and_processing_continues() {
        let writer = FakeWriter::default();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
            IngressStats::default(),
            configured_subjects(),
        ));

        sender
            .send(item("alpha", "frames", &[0xff, 0xfe]))
            .await
            .expect("invalid UTF-8 send");
        sender
            .send(item("alpha", "frames", br#"{"broken":"#))
            .await
            .expect("invalid JSON send");
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("valid send");
        drop(sender);
        worker.await.expect("worker should finish");

        assert_eq!(writer.calls().len(), 1);
    }

    #[tokio::test]
    async fn identical_messages_receive_distinct_uuid_v4_ids() {
        let writer = FakeWriter::default();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
            IngressStats::default(),
            configured_subjects(),
        ));
        let payload = br#"{"same":true}"#;

        sender
            .send(item("alpha", "frames", payload))
            .await
            .expect("first send");
        sender
            .send(item("alpha", "frames", payload))
            .await
            .expect("second send");
        drop(sender);
        worker.await.expect("worker should finish");

        let calls = writer.calls();
        assert_eq!(calls.len(), 2);
        assert_ne!(calls[0].doc_id, calls[1].doc_id);
        for call in calls {
            let parsed = Uuid::parse_str(&call.doc_id).expect("document ID should be UUID");
            assert_eq!(parsed.get_version_num(), 4);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn transient_retries_reuse_id_and_envelope_with_exact_bounded_schedule() {
        let writer = FakeWriter::with_results([
            Err(CreateBridgeDocumentError::StoreRead),
            Err(CreateBridgeDocumentError::StoreWrite),
            Ok(()),
        ]);
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
            IngressStats::default(),
            configured_subjects(),
        ));
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("send");
        drop(sender);

        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 1);
        tokio::time::advance(STORE_RETRY_DELAY_FIRST - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(writer.calls().len(), 2);
        tokio::time::advance(STORE_RETRY_DELAY_SECOND).await;
        tokio::task::yield_now().await;
        worker.await.expect("worker should finish");

        let calls = writer.calls();
        assert_eq!(calls.len(), STORE_MAX_ATTEMPTS);
        assert!(calls.iter().all(|call| call.doc_id == calls[0].doc_id));
        assert!(calls
            .iter()
            .all(|call| call.envelope_json == calls[0].envelope_json));
        assert_eq!(calls[1].at - calls[0].at, STORE_RETRY_DELAY_FIRST);
        assert_eq!(calls[2].at - calls[1].at, STORE_RETRY_DELAY_SECOND);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_stop_after_three_attempts() {
        let writer = FakeWriter::with_results(std::iter::repeat_n(
            Err(CreateBridgeDocumentError::StoreWrite),
            STORE_MAX_ATTEMPTS,
        ));
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
            IngressStats::default(),
            configured_subjects(),
        ));
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("send");
        drop(sender);
        tokio::time::advance(STORE_RETRY_DELAY_FIRST + STORE_RETRY_DELAY_SECOND).await;
        tokio::task::yield_now().await;
        worker.await.expect("worker should finish");
        assert_eq!(writer.calls().len(), STORE_MAX_ATTEMPTS);
    }

    #[tokio::test(start_paused = true)]
    async fn permanent_failure_does_not_retry_or_sleep() {
        let writer = FakeWriter::with_results([Err(CreateBridgeDocumentError::AlreadyExists)]);
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer.clone(),
            IngressStats::default(),
            configured_subjects(),
        ));
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("send");
        drop(sender);
        worker.await.expect("worker should finish");
        assert_eq!(writer.calls().len(), 1);
        assert_eq!(tokio::time::Instant::now(), writer.calls()[0].at);
    }

    #[tokio::test]
    async fn stats_distinguish_validation_storage_and_slow_consumer_outcomes() {
        let writer =
            FakeWriter::with_results([Ok(()), Err(CreateBridgeDocumentError::AlreadyExists)]);
        let stats = IngressStats::default();
        stats.record_slow_consumer();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor(
            rx,
            "source-a".to_owned(),
            writer,
            stats.clone(),
            configured_subjects(),
        ));
        for message in [
            item("alpha", "frames", &[0xff]),
            item("alpha", "frames", br#"{"broken":"#),
            item("alpha", "frames", br#"{"stored":true}"#),
            item("alpha", "frames", br#"{"collision":true}"#),
        ] {
            sender.send(message).await.expect("send");
        }
        drop(sender);
        worker.await.expect("worker should finish");

        assert_eq!(
            stats.snapshot(),
            IngressStatsSnapshot {
                received: 4,
                stored: 1,
                invalid_utf8: 1,
                invalid_json: 1,
                oversized_payloads: 0,
                final_store_failures: 1,
                slow_consumer_events: 1,
            }
        );
    }

    #[test]
    fn payload_ceiling_accepts_exactly_one_mebibyte_and_rejects_the_next_byte() {
        assert_eq!(MAX_INGRESS_PAYLOAD_BYTES, 1_048_576);
        assert!(!is_payload_oversized(MAX_INGRESS_PAYLOAD_BYTES));
        assert!(is_payload_oversized(MAX_INGRESS_PAYLOAD_BYTES + 1));
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_diagnostics_are_bounded_rate_limited_and_payload_safe() {
        let emitter = RecordingEmitter::default();
        let diagnostics = IngressDiagnostics::with_emitter(
            [(Subject::from("vision.summary"), "frames".to_owned())],
            emitter.clone(),
        );
        let stats = IngressStats::default();
        let subject = Subject::from("vision.summary");
        let now = Instant::now();

        for offset in 0..3 {
            stats.record_oversized_payload();
            diagnostics.record_oversized_at(
                &subject,
                "frames",
                MAX_INGRESS_PAYLOAD_BYTES + 1 + offset,
                now + Duration::from_secs(offset as u64),
            );
        }
        assert_eq!(stats.snapshot().oversized_payloads, 3);
        assert_eq!(emitter.actions().len(), 1);
        assert_eq!(emitter.actions()[0].suppressed_count, Some(0));

        diagnostics.record_oversized_at(
            &subject,
            "frames",
            MAX_INGRESS_PAYLOAD_BYTES + 4,
            now + OVERSIZED_WARNING_INTERVAL,
        );
        let actions = emitter.actions();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[1].suppressed_count, Some(2));
        assert_eq!(actions[1].error_kind, IngressErrorKind::OversizedPayload);
        let rendered = format!("{actions:?}");
        for forbidden in [
            r#"{"secret_payload":"do-not-log"}"#,
            "raw-user:raw-password@broker.internal",
            "expected value at line 1 column 19",
        ] {
            assert!(!rendered.contains(forbidden));
        }

        diagnostics.record_oversized_at(
            &Subject::from("payload-derived-subject"),
            "untrusted-collection",
            usize::MAX,
            now + Duration::from_secs(120),
        );
        assert_eq!(
            emitter.actions().len(),
            2,
            "unconfigured input cannot grow state"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_warnings_are_immediate_summarized_at_sixty_seconds_and_reset() {
        let writer = FakeWriter::default();
        let stats = IngressStats::default();
        let emitter = RecordingEmitter::default();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor_with_emitter(
            rx,
            "source-a".to_owned(),
            writer,
            stats.clone(),
            [Subject::from("alpha")],
            emitter.clone(),
        ));

        sender
            .send(item("alpha", "frames", br#"{"broken":"#))
            .await
            .expect("first invalid send");
        sender
            .send(item("alpha", "frames", br#"{"broken-again":"#))
            .await
            .expect("second invalid send");
        wait_for_received(&stats, 2).await;
        let before_summary = emitter.actions();
        assert_eq!(
            before_summary
                .iter()
                .filter(|action| action.kind == IngressActionKind::InvalidOccurrence)
                .count(),
            2
        );
        assert_eq!(
            before_summary
                .iter()
                .filter(|action| action.kind == IngressActionKind::InvalidWarning)
                .count(),
            1
        );

        tokio::time::advance(INVALID_WARNING_INTERVAL).await;
        tokio::task::yield_now().await;
        let summaries = emitter
            .actions()
            .into_iter()
            .filter(|action| action.kind == IngressActionKind::InvalidSummary)
            .collect::<Vec<_>>();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].suppressed_count, Some(1));

        sender
            .send(item("alpha", "frames", &[0xff]))
            .await
            .expect("post-reset invalid send");
        wait_for_received(&stats, 3).await;
        assert_eq!(
            emitter
                .actions()
                .iter()
                .filter(|action| action.kind == IngressActionKind::InvalidWarning)
                .count(),
            2,
            "the next interval must permit a new immediate warning"
        );
        drop(sender);
        worker.await.expect("worker should finish");
    }

    #[tokio::test(start_paused = true)]
    async fn three_transient_failures_emit_two_debug_retries_and_one_final_warning() {
        let writer = FakeWriter::with_results(std::iter::repeat_n(
            Err(CreateBridgeDocumentError::StoreWrite),
            STORE_MAX_ATTEMPTS,
        ));
        let stats = IngressStats::default();
        let emitter = RecordingEmitter::default();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor_with_emitter(
            rx,
            "source-a".to_owned(),
            writer,
            stats.clone(),
            [Subject::from("alpha")],
            emitter.clone(),
        ));
        sender
            .send(item("alpha", "frames", br#"{"valid":true}"#))
            .await
            .expect("send");
        drop(sender);
        tokio::time::advance(STORE_RETRY_DELAY_FIRST + STORE_RETRY_DELAY_SECOND).await;
        tokio::task::yield_now().await;
        worker.await.expect("worker should finish");

        let actions = emitter.actions();
        assert_eq!(
            actions
                .iter()
                .filter(|action| action.kind == IngressActionKind::StoreRetry)
                .count(),
            2
        );
        assert_eq!(
            actions
                .iter()
                .filter(|action| action.kind == IngressActionKind::StoreFailure)
                .count(),
            1
        );
        assert_eq!(stats.snapshot().final_store_failures, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn warning_state_never_grows_from_unconfigured_subjects() {
        let stats = IngressStats::default();
        let emitter = RecordingEmitter::default();
        let (sender, rx) = ingress_channel();
        let worker = tokio::spawn(run_ingress_processor_with_emitter(
            rx,
            "source-a".to_owned(),
            FakeWriter::default(),
            stats.clone(),
            [Subject::from("configured")],
            emitter.clone(),
        ));
        sender
            .send(item("payload-derived-key", "frames", &[0xff]))
            .await
            .expect("send");
        wait_for_received(&stats, 1).await;
        tokio::time::advance(INVALID_WARNING_INTERVAL).await;
        tokio::task::yield_now().await;
        assert!(emitter.actions().iter().all(|action| {
            !matches!(
                action.kind,
                IngressActionKind::InvalidWarning | IngressActionKind::InvalidSummary
            )
        }));
        drop(sender);
        worker.await.expect("worker should finish");
    }

    #[test]
    fn formatted_actions_and_captured_logs_exclude_payload_urls_and_source_details() {
        let action = IngressAction {
            kind: IngressActionKind::StoreFailure,
            subject: Some("vision.summary".to_owned()),
            collection: Some("frames".to_owned()),
            payload_bytes: Some(87),
            document_id: Some("550e8400-e29b-41d4-a716-446655440000".to_owned()),
            attempt: Some(3),
            delay_ms: None,
            suppressed_count: None,
            error_kind: IngressErrorKind::StoreWrite,
        };
        let capture = CaptureMakeWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_writer(capture.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || action.emit());

        let captured = String::from_utf8(capture.bytes.lock().expect("capture lock").clone())
            .expect("captured logs should be UTF-8");
        let rendered = format!("{action:?} {captured}");
        for forbidden in [
            r#"{"secret_payload":"do-not-log"}"#,
            "raw-user:raw-password@broker.internal",
            "expected value at line 1 column 19",
            "/private/data/peat/store.db: permission denied",
            "caused by: database source chain",
        ] {
            assert!(!rendered.contains(forbidden));
        }
        assert!(captured.contains("store_write"));
        assert!(captured.contains("payload_bytes=87"));
    }
}
