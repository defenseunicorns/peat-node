//! Bounded, byte-exact Core NATS egress for remote Peat bridge documents.
//!
//! Eligibility is deliberately independent of connection state: a remote
//! store event must first prove the durable envelope kind/version, its exact
//! configured route, and non-local provenance. The payload string is then
//! moved directly into [`Bytes`] without parsing or serialization.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_nats::connection::State;
use async_nats::{Client, HeaderMap, HeaderValue, PublishErrorKind, Subject};
use buffa::bytes::Bytes;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::nats_bridge::config::SubjectMapping;
use crate::nats_bridge::envelope::{BridgeEnvelope, BRIDGE_ENVELOPE_KIND, BRIDGE_ENVELOPE_VERSION};
use crate::nats_bridge::ingress::MAX_INGRESS_PAYLOAD_BYTES;
use crate::nats_bridge::ledger::{document_digest, DeliveryLedger, LedgerDigest, ReserveResult};
use crate::nats_bridge::readiness::BridgeReadiness;
use crate::node::BridgeChangeEvent;

/// Stable private marker added to bridge-owned Core NATS publications.
pub(crate) const BRIDGE_ORIGIN_HEADER: &str = "Peat-Nats-Bridge-Origin";

/// Maximum eligible egress items retained by the bridge-owned FIFO.
pub(crate) const EGRESS_QUEUE_CAPACITY: usize = 256;

/// Minimum interval between diagnostics in one fixed egress classification.
pub(crate) const EGRESS_DIAGNOSTIC_INTERVAL: Duration = Duration::from_secs(60);

const EGRESS_DIAGNOSTIC_CLASSIFICATIONS: usize = 17;
const EGRESS_FLUSH_TIMEOUT: Duration = Duration::from_secs(2);

/// Fixed, payload-safe reason that a remote Peat upsert is ineligible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EgressSkipKind {
    MalformedEnvelope,
    UnsupportedKind,
    UnsupportedVersion,
    UnmappedCollection,
    RouteMismatch,
    ReturnedLocal,
    OversizedPayload,
    Duplicate,
}

/// One byte-exact publish request after all envelope and route gates pass.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct EgressItem {
    pub digest: LedgerDigest,
    pub subject: Subject,
    pub payload: Bytes,
    /// Index in the validated finite startup mapping list.
    pub route_index: usize,
}

/// Fixed terminal delivery failure; source error text is deliberately discarded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EgressFailureKind {
    EventLagged,
    QueueFull,
    QueueClosed,
    Unavailable,
    PublishFailed,
    MaxPayloadExceeded,
    FlushFailed,
    LedgerUnavailable,
}

/// Fixed action shape safe for logs and bounded diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EgressActionKind {
    Skipped(EgressSkipKind),
    Lost(EgressFailureKind),
    Published,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EgressAction {
    pub kind: EgressActionKind,
    /// Index in the finite startup mapping list, never event-derived text.
    pub route_index: Option<usize>,
    pub payload_bytes: Option<usize>,
    /// Events suppressed in this fixed classification since its prior emit.
    pub suppressed_count: u64,
}

impl EgressAction {
    fn emit(&self) {
        let route_index = self.route_index;
        let payload_bytes = self.payload_bytes.unwrap_or_default();
        let suppressed_count = self.suppressed_count;
        match self.kind {
            EgressActionKind::Published => debug!(
                ?route_index,
                payload_bytes, suppressed_count, "NATS bridge egress publish enqueued"
            ),
            EgressActionKind::Skipped(kind) => debug!(
                ?route_index,
                payload_bytes,
                suppressed_count,
                reason = ?kind,
                "NATS bridge egress skipped"
            ),
            EgressActionKind::Lost(kind) => warn!(
                ?route_index,
                payload_bytes,
                suppressed_count,
                reason = ?kind,
                "NATS bridge egress terminal loss"
            ),
        }
    }
}

trait EgressActionEmitter: Send + Sync + 'static {
    fn emit(&self, action: EgressAction);
}

#[derive(Clone, Copy)]
struct TracingEgressEmitter;

impl EgressActionEmitter for TracingEgressEmitter {
    fn emit(&self, action: EgressAction) {
        action.emit();
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct DiagnosticBucket {
    next_emit: Option<Instant>,
    suppressed: u64,
}

/// Fixed-size, label-free rate limiter shared by every egress producer.
#[derive(Clone)]
pub(crate) struct EgressDiagnostics {
    buckets: Arc<Mutex<Box<[DiagnosticBucket]>>>,
    route_count: usize,
    emitter: Arc<dyn EgressActionEmitter>,
}

impl Default for EgressDiagnostics {
    fn default() -> Self {
        Self::new(0)
    }
}

impl EgressDiagnostics {
    fn new(route_count: usize) -> Self {
        Self::with_emitter(route_count, TracingEgressEmitter)
    }

    fn with_emitter<E: EgressActionEmitter>(route_count: usize, emitter: E) -> Self {
        // The first classification bank is route-less. Each validated startup
        // route receives its own fixed bank so suppression can never attribute
        // one route's flood to another route's later diagnostic.
        let bucket_count = route_count
            .saturating_add(1)
            .saturating_mul(EGRESS_DIAGNOSTIC_CLASSIFICATIONS);
        Self {
            buckets: Arc::new(Mutex::new(
                vec![DiagnosticBucket::default(); bucket_count].into_boxed_slice(),
            )),
            route_count,
            emitter: Arc::new(emitter),
        }
    }

    fn record(&self, action: EgressAction) {
        self.record_at(action, Instant::now());
    }

    fn record_at(&self, mut action: EgressAction, now: Instant) {
        let classification = diagnostic_classification(action.kind);
        let route_bank = action
            .route_index
            .filter(|index| *index < self.route_count)
            .map_or(0, |index| index + 1);
        let index = route_bank * EGRESS_DIAGNOSTIC_CLASSIFICATIONS + classification;
        let suppressed = {
            let mut buckets = self
                .buckets
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let bucket = &mut buckets[index];
            if bucket.next_emit.is_some_and(|deadline| now < deadline) {
                bucket.suppressed = bucket.suppressed.saturating_add(1);
                return;
            }
            let suppressed = bucket.suppressed;
            bucket.suppressed = 0;
            bucket.next_emit = Some(now + EGRESS_DIAGNOSTIC_INTERVAL);
            suppressed
        };
        action.suppressed_count = suppressed;
        self.emitter.emit(action);
    }
}

fn diagnostic_classification(kind: EgressActionKind) -> usize {
    match kind {
        EgressActionKind::Skipped(EgressSkipKind::MalformedEnvelope) => 0,
        EgressActionKind::Skipped(EgressSkipKind::UnsupportedKind) => 1,
        EgressActionKind::Skipped(EgressSkipKind::UnsupportedVersion) => 2,
        EgressActionKind::Skipped(EgressSkipKind::UnmappedCollection) => 3,
        EgressActionKind::Skipped(EgressSkipKind::RouteMismatch) => 4,
        EgressActionKind::Skipped(EgressSkipKind::ReturnedLocal) => 5,
        EgressActionKind::Skipped(EgressSkipKind::OversizedPayload) => 6,
        EgressActionKind::Skipped(EgressSkipKind::Duplicate) => 7,
        EgressActionKind::Lost(EgressFailureKind::EventLagged) => 8,
        EgressActionKind::Lost(EgressFailureKind::QueueFull) => 9,
        EgressActionKind::Lost(EgressFailureKind::QueueClosed) => 10,
        EgressActionKind::Lost(EgressFailureKind::Unavailable) => 11,
        EgressActionKind::Lost(EgressFailureKind::PublishFailed) => 12,
        EgressActionKind::Lost(EgressFailureKind::MaxPayloadExceeded) => 13,
        EgressActionKind::Lost(EgressFailureKind::FlushFailed) => 14,
        EgressActionKind::Lost(EgressFailureKind::LedgerUnavailable) => 15,
        EgressActionKind::Published => 16,
    }
}

/// Label-free monotonic egress counters.
#[derive(Clone, Default)]
pub(crate) struct EgressStats {
    inner: Arc<EgressStatsInner>,
}

#[derive(Default)]
struct EgressStatsInner {
    malformed: AtomicU64,
    unsupported: AtomicU64,
    unmapped: AtomicU64,
    route_mismatch: AtomicU64,
    returned_local: AtomicU64,
    oversized_payload: AtomicU64,
    duplicate: AtomicU64,
    queue_full: AtomicU64,
    queue_closed: AtomicU64,
    unavailable: AtomicU64,
    publish_failed: AtomicU64,
    max_payload_exceeded: AtomicU64,
    flush_failed: AtomicU64,
    ledger_unavailable: AtomicU64,
    event_lagged: AtomicU64,
    reserved: AtomicU64,
    completed: AtomicU64,
    published: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct EgressStatsSnapshot {
    pub malformed: u64,
    pub unsupported: u64,
    pub unmapped: u64,
    pub route_mismatch: u64,
    pub returned_local: u64,
    pub oversized_payload: u64,
    pub duplicate: u64,
    pub queue_full: u64,
    pub queue_closed: u64,
    pub unavailable: u64,
    pub publish_failed: u64,
    pub max_payload_exceeded: u64,
    pub flush_failed: u64,
    pub ledger_unavailable: u64,
    pub event_lagged: u64,
    pub reserved: u64,
    pub completed: u64,
    pub published: u64,
}

impl EgressStats {
    pub fn snapshot(&self) -> EgressStatsSnapshot {
        EgressStatsSnapshot {
            malformed: self.inner.malformed.load(Ordering::Relaxed),
            unsupported: self.inner.unsupported.load(Ordering::Relaxed),
            unmapped: self.inner.unmapped.load(Ordering::Relaxed),
            route_mismatch: self.inner.route_mismatch.load(Ordering::Relaxed),
            returned_local: self.inner.returned_local.load(Ordering::Relaxed),
            oversized_payload: self.inner.oversized_payload.load(Ordering::Relaxed),
            duplicate: self.inner.duplicate.load(Ordering::Relaxed),
            queue_full: self.inner.queue_full.load(Ordering::Relaxed),
            queue_closed: self.inner.queue_closed.load(Ordering::Relaxed),
            unavailable: self.inner.unavailable.load(Ordering::Relaxed),
            publish_failed: self.inner.publish_failed.load(Ordering::Relaxed),
            max_payload_exceeded: self.inner.max_payload_exceeded.load(Ordering::Relaxed),
            flush_failed: self.inner.flush_failed.load(Ordering::Relaxed),
            ledger_unavailable: self.inner.ledger_unavailable.load(Ordering::Relaxed),
            event_lagged: self.inner.event_lagged.load(Ordering::Relaxed),
            reserved: self.inner.reserved.load(Ordering::Relaxed),
            completed: self.inner.completed.load(Ordering::Relaxed),
            published: self.inner.published.load(Ordering::Relaxed),
        }
    }

    fn record_skip(&self, kind: EgressSkipKind) {
        let counter = match kind {
            EgressSkipKind::MalformedEnvelope => &self.inner.malformed,
            EgressSkipKind::UnsupportedKind | EgressSkipKind::UnsupportedVersion => {
                &self.inner.unsupported
            }
            EgressSkipKind::UnmappedCollection => &self.inner.unmapped,
            EgressSkipKind::RouteMismatch => &self.inner.route_mismatch,
            EgressSkipKind::ReturnedLocal => &self.inner.returned_local,
            EgressSkipKind::OversizedPayload => &self.inner.oversized_payload,
            EgressSkipKind::Duplicate => &self.inner.duplicate,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self, kind: EgressFailureKind) {
        let counter = match kind {
            EgressFailureKind::EventLagged => &self.inner.event_lagged,
            EgressFailureKind::QueueFull => &self.inner.queue_full,
            EgressFailureKind::QueueClosed => &self.inner.queue_closed,
            EgressFailureKind::Unavailable => &self.inner.unavailable,
            EgressFailureKind::PublishFailed => &self.inner.publish_failed,
            EgressFailureKind::MaxPayloadExceeded => &self.inner.max_payload_exceeded,
            EgressFailureKind::FlushFailed => &self.inner.flush_failed,
            EgressFailureKind::LedgerUnavailable => &self.inner.ledger_unavailable,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn record_event_lagged(&self, dropped: u64) {
        self.inner
            .event_lagged
            .fetch_add(dropped, Ordering::Relaxed);
    }
}

/// Finite collection-to-subject table derived only from validated startup config.
#[derive(Clone)]
pub(crate) struct EgressClassifier {
    routes: HashMap<String, Subject>,
    route_indexes: HashMap<String, usize>,
    local_node_id: String,
}

impl EgressClassifier {
    pub fn new(mappings: &[SubjectMapping], local_node_id: &str) -> Self {
        let routes = mappings
            .iter()
            .map(|mapping| (mapping.collection().to_owned(), mapping.subject().clone()))
            .collect();
        let route_indexes = mappings
            .iter()
            .enumerate()
            .map(|(index, mapping)| (mapping.collection().to_owned(), index))
            .collect();
        Self {
            routes,
            route_indexes,
            local_node_id: local_node_id.to_owned(),
        }
    }

    fn route_index(&self, collection: &str) -> Option<usize> {
        self.route_indexes.get(collection).copied()
    }

    /// Classify one private remote-only node event without retaining event data.
    pub fn classify(&self, event: BridgeChangeEvent) -> Result<EgressItem, EgressSkipKind> {
        let digest = document_digest(&event.collection, &event.doc_id);
        let envelope: BridgeEnvelope = serde_json::from_str(&event.json_data)
            .map_err(|_| EgressSkipKind::MalformedEnvelope)?;
        if envelope.kind != BRIDGE_ENVELOPE_KIND {
            return Err(EgressSkipKind::UnsupportedKind);
        }
        if envelope.version != BRIDGE_ENVELOPE_VERSION {
            return Err(EgressSkipKind::UnsupportedVersion);
        }
        let subject = self
            .routes
            .get(&event.collection)
            .ok_or(EgressSkipKind::UnmappedCollection)?;
        let route_index = self
            .route_index(&event.collection)
            .ok_or(EgressSkipKind::UnmappedCollection)?;
        if envelope.subject != subject.as_str() {
            return Err(EgressSkipKind::RouteMismatch);
        }
        if envelope.source_node_id == self.local_node_id {
            return Err(EgressSkipKind::ReturnedLocal);
        }
        if envelope.payload.len() > MAX_INGRESS_PAYLOAD_BYTES {
            return Err(EgressSkipKind::OversizedPayload);
        }

        Ok(EgressItem {
            digest,
            subject: subject.clone(),
            payload: Bytes::from(envelope.payload),
            route_index,
        })
    }

    #[cfg(test)]
    fn route_count(&self) -> usize {
        self.routes.len()
    }
}

/// Sole durable admission path shared by live events and reconciliation.
#[derive(Clone)]
pub(crate) struct DeliveryCoordinator {
    classifier: EgressClassifier,
    tx: mpsc::Sender<EgressItem>,
    delivery: DeliveryLedger,
    readiness: BridgeReadiness,
    stats: EgressStats,
    diagnostics: EgressDiagnostics,
}

impl DeliveryCoordinator {
    pub fn new(
        mappings: &[SubjectMapping],
        local_node_id: &str,
        stats: EgressStats,
        delivery: DeliveryLedger,
        readiness: BridgeReadiness,
    ) -> (Self, mpsc::Receiver<EgressItem>) {
        Self::with_capacity(
            mappings,
            local_node_id,
            stats,
            delivery,
            readiness,
            EGRESS_QUEUE_CAPACITY,
            TracingEgressEmitter,
        )
    }

    fn with_capacity<E>(
        mappings: &[SubjectMapping],
        local_node_id: &str,
        stats: EgressStats,
        delivery: DeliveryLedger,
        readiness: BridgeReadiness,
        queue_capacity: usize,
        emitter: E,
    ) -> (Self, mpsc::Receiver<EgressItem>)
    where
        E: EgressActionEmitter,
    {
        let (tx, rx) = mpsc::channel(queue_capacity);
        (
            Self {
                classifier: EgressClassifier::new(mappings, local_node_id),
                tx,
                delivery,
                readiness,
                stats,
                diagnostics: EgressDiagnostics::with_emitter(mappings.len(), emitter),
            },
            rx,
        )
    }

    pub async fn deliver(&self, event: BridgeChangeEvent) -> Result<(), EgressActionKind> {
        let route_index = self.classifier.route_index(&event.collection);
        let item = match self.classifier.classify(event) {
            Ok(item) => item,
            Err(kind) => {
                self.record_action(EgressActionKind::Skipped(kind), route_index, None);
                return Err(EgressActionKind::Skipped(kind));
            }
        };
        let status = self.readiness.snapshot();
        if !status.accepting || !status.delivery_healthy || !status.is_ready() {
            let kind = EgressFailureKind::Unavailable;
            self.record_action(
                EgressActionKind::Lost(kind),
                route_index,
                Some(item.payload.len()),
            );
            return Err(EgressActionKind::Lost(kind));
        }
        match self.delivery.check_and_reserve(item.digest).await {
            Ok(ReserveResult::Suppressed) => {
                let kind = EgressSkipKind::Duplicate;
                self.record_action(EgressActionKind::Skipped(kind), route_index, None);
                return Err(EgressActionKind::Skipped(kind));
            }
            Ok(ReserveResult::Reserved) => {
                self.stats.inner.reserved.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.readiness.set_delivery_healthy(false);
                let kind = EgressFailureKind::LedgerUnavailable;
                self.record_action(
                    EgressActionKind::Lost(kind),
                    route_index,
                    Some(item.payload.len()),
                );
                return Err(EgressActionKind::Lost(kind));
            }
        }

        let payload_bytes = item.payload.len();
        match self.tx.try_send(item) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                let kind = EgressFailureKind::QueueFull;
                self.record_action(
                    EgressActionKind::Lost(kind),
                    route_index,
                    Some(payload_bytes),
                );
                Err(EgressActionKind::Lost(kind))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                let kind = EgressFailureKind::QueueClosed;
                self.record_action(
                    EgressActionKind::Lost(kind),
                    route_index,
                    Some(payload_bytes),
                );
                Err(EgressActionKind::Lost(kind))
            }
        }
    }

    fn record_action(
        &self,
        kind: EgressActionKind,
        route_index: Option<usize>,
        payload_bytes: Option<usize>,
    ) {
        match kind {
            EgressActionKind::Skipped(kind) => self.stats.record_skip(kind),
            EgressActionKind::Lost(kind) => self.stats.record_failure(kind),
            EgressActionKind::Published => {
                self.stats.inner.published.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.diagnostics.record(EgressAction {
            kind,
            route_index,
            payload_bytes,
            suppressed_count: 0,
        });
    }

    pub(crate) fn diagnostics(&self) -> EgressDiagnostics {
        self.diagnostics.clone()
    }
}

/// Fallible serial publish seam. Success means client enqueue, not broker acknowledgement.
pub(crate) trait BridgePublisher: Send + Sync + 'static {
    fn publish<'a>(
        &'a self,
        subject: Subject,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), EgressFailureKind>> + Send + 'a>>;

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), EgressFailureKind>> + Send + '_>>;
}

impl BridgePublisher for async_nats::Client {
    fn publish<'a>(
        &'a self,
        subject: Subject,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), EgressFailureKind>> + Send + 'a>> {
        Box::pin(async move {
            self.publish_with_headers(subject, headers, payload)
                .await
                .map_err(|error| match error.kind() {
                    PublishErrorKind::MaxPayloadExceeded => EgressFailureKind::MaxPayloadExceeded,
                    PublishErrorKind::InvalidSubject | PublishErrorKind::Send => {
                        EgressFailureKind::PublishFailed
                    }
                })
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), EgressFailureKind>> + Send + '_>> {
        Box::pin(async move {
            self.flush()
                .await
                .map_err(|_| EgressFailureKind::FlushFailed)
        })
    }
}

/// Production publisher over the sole shared async-nats connection.
pub(crate) struct NatsBridgePublisher {
    client: Client,
    readiness: BridgeReadiness,
}

impl NatsBridgePublisher {
    pub fn new(client: Client, readiness: BridgeReadiness) -> Self {
        Self { client, readiness }
    }
}

impl BridgePublisher for NatsBridgePublisher {
    fn publish<'a>(
        &'a self,
        subject: Subject,
        headers: HeaderMap,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), EgressFailureKind>> + Send + 'a>> {
        Box::pin(async move {
            if !self.readiness.snapshot().is_ready()
                || self.client.connection_state() != State::Connected
            {
                return Err(EgressFailureKind::Unavailable);
            }
            BridgePublisher::publish(&self.client, subject, headers, payload).await
        })
    }

    fn flush(&self) -> Pin<Box<dyn Future<Output = Result<(), EgressFailureKind>> + Send + '_>> {
        Box::pin(async move {
            if !self.readiness.snapshot().is_ready() {
                return Err(EgressFailureKind::Unavailable);
            }
            BridgePublisher::flush(&self.client).await
        })
    }
}

/// Forward private node events without ever awaiting bridge-owned FIFO space.
pub(crate) async fn run_bridge_event_router(
    mut events: tokio::sync::broadcast::Receiver<BridgeChangeEvent>,
    coordinator: DeliveryCoordinator,
    stats: EgressStats,
    diagnostics: EgressDiagnostics,
    reconcile: Option<super::reconcile::ReconcileTrigger>,
) {
    loop {
        match events.recv().await {
            Ok(event) => {
                let _ = coordinator.deliver(event).await;
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                stats.record_event_lagged(dropped);
                if let Some(trigger) = &reconcile {
                    trigger.trigger(super::reconcile::ReconcileReason::EventLagged);
                }
                diagnostics.record(EgressAction {
                    kind: EgressActionKind::Lost(EgressFailureKind::EventLagged),
                    route_index: None,
                    payload_bytes: None,
                    suppressed_count: 0,
                });
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// Drain the sole FIFO serially; failures are terminal and later items continue.
pub(crate) async fn run_egress_worker<P: BridgePublisher>(
    mut rx: mpsc::Receiver<EgressItem>,
    origin_header_value: HeaderValue,
    publisher: P,
    delivery: DeliveryLedger,
    readiness: BridgeReadiness,
    stats: EgressStats,
    diagnostics: EgressDiagnostics,
) {
    while let Some(item) = rx.recv().await {
        let payload_bytes = item.payload.len();
        let route_index = Some(item.route_index);
        let mut headers = HeaderMap::new();
        headers.insert(BRIDGE_ORIGIN_HEADER, origin_header_value.clone());
        let outcome = match publisher.publish(item.subject, headers, item.payload).await {
            Ok(()) => match tokio::time::timeout(EGRESS_FLUSH_TIMEOUT, publisher.flush()).await {
                Ok(result) => result,
                Err(_) => Err(EgressFailureKind::FlushFailed),
            },
            Err(kind) => Err(kind),
        };
        match outcome {
            Ok(()) => {
                if delivery.mark_completed(item.digest).await.is_err() {
                    readiness.set_delivery_healthy(false);
                    let kind = EgressFailureKind::LedgerUnavailable;
                    stats.record_failure(kind);
                    diagnostics.record(EgressAction {
                        kind: EgressActionKind::Lost(kind),
                        route_index,
                        payload_bytes: Some(payload_bytes),
                        suppressed_count: 0,
                    });
                    continue;
                }
                stats.inner.completed.fetch_add(1, Ordering::Relaxed);
                stats.inner.published.fetch_add(1, Ordering::Relaxed);
                diagnostics.record(EgressAction {
                    kind: EgressActionKind::Published,
                    route_index,
                    payload_bytes: Some(payload_bytes),
                    suppressed_count: 0,
                });
            }
            Err(kind @ EgressFailureKind::Unavailable)
            | Err(kind @ EgressFailureKind::PublishFailed)
            | Err(kind @ EgressFailureKind::MaxPayloadExceeded)
            | Err(kind @ EgressFailureKind::FlushFailed)
            | Err(kind @ EgressFailureKind::LedgerUnavailable) => {
                stats.record_failure(kind);
                diagnostics.record(EgressAction {
                    kind: EgressActionKind::Lost(kind),
                    route_index,
                    payload_bytes: Some(payload_bytes),
                    suppressed_count: 0,
                });
            }
            Err(
                EgressFailureKind::EventLagged
                | EgressFailureKind::QueueFull
                | EgressFailureKind::QueueClosed,
            ) => {
                unreachable!("publisher cannot report FIFO admission failures")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_bridge::config::BridgeConfig;

    fn mappings() -> Vec<SubjectMapping> {
        let raw = vec![
            "Vision.Summary=Frame_Store-1".to_owned(),
            "telemetry.reading=telemetry".to_owned(),
        ];
        let BridgeConfig::Enabled(config) =
            BridgeConfig::from_raw(Some("nats://127.0.0.1:4222"), &raw).expect("valid mappings")
        else {
            panic!("mappings must enable bridge");
        };
        config.mappings().to_vec()
    }

    fn classifier() -> EgressClassifier {
        EgressClassifier::new(&mappings(), "local-node")
    }

    fn envelope(subject: &str, source_node_id: &str, payload: &str) -> BridgeEnvelope {
        BridgeEnvelope {
            kind: BRIDGE_ENVELOPE_KIND.to_owned(),
            version: BRIDGE_ENVELOPE_VERSION,
            subject: subject.to_owned(),
            source_node_id: source_node_id.to_owned(),
            payload: payload.to_owned(),
        }
    }

    fn event(collection: &str, envelope: &BridgeEnvelope) -> BridgeChangeEvent {
        event_with_id(collection, "untrusted-document-id", envelope)
    }

    fn event_with_id(
        collection: &str,
        doc_id: &str,
        envelope: &BridgeEnvelope,
    ) -> BridgeChangeEvent {
        BridgeChangeEvent {
            collection: collection.to_owned(),
            doc_id: doc_id.to_owned(),
            remote_peer_id: "untrusted-immediate-peer".to_owned(),
            json_data: serde_json::to_string(envelope).expect("serialize envelope"),
        }
    }

    #[test]
    fn classifier_accepts_only_exact_supported_envelope_and_route() {
        let classifier = classifier();
        let valid = envelope("Vision.Summary", "remote-node", r#"{"ok":true}"#);
        assert_eq!(
            classifier.classify(event("Frame_Store-1", &valid)),
            Ok(EgressItem {
                digest: document_digest("Frame_Store-1", "untrusted-document-id"),
                subject: Subject::from("Vision.Summary"),
                payload: Bytes::from_static(br#"{"ok":true}"#),
                route_index: 0,
            })
        );

        let mut unsupported_kind = valid.clone();
        unsupported_kind.kind = "peat.nats-bridge.other".to_owned();
        assert_eq!(
            classifier.classify(event("Frame_Store-1", &unsupported_kind)),
            Err(EgressSkipKind::UnsupportedKind)
        );
        let mut unsupported_version = valid.clone();
        unsupported_version.version += 1;
        assert_eq!(
            classifier.classify(event("Frame_Store-1", &unsupported_version)),
            Err(EgressSkipKind::UnsupportedVersion)
        );
    }

    #[test]
    fn classifier_has_fixed_outcomes_for_malformed_ordinary_unmapped_and_mismatch() {
        let classifier = classifier();
        for json_data in ["not-json", r#"{"ordinary":true}"#] {
            let malformed = BridgeChangeEvent {
                collection: "Frame_Store-1".to_owned(),
                doc_id: "id".to_owned(),
                remote_peer_id: "peer".to_owned(),
                json_data: json_data.to_owned(),
            };
            assert_eq!(
                classifier.classify(malformed),
                Err(EgressSkipKind::MalformedEnvelope)
            );
        }

        let valid = envelope("Vision.Summary", "remote-node", "1");
        assert_eq!(
            classifier.classify(event("unmapped", &valid)),
            Err(EgressSkipKind::UnmappedCollection)
        );
        for subject in ["vision.summary", "Vision.Summary ", "telemetry.reading"] {
            let mismatch = envelope(subject, "remote-node", "1");
            assert_eq!(
                classifier.classify(event("Frame_Store-1", &mismatch)),
                Err(EgressSkipKind::RouteMismatch)
            );
        }
    }

    #[test]
    fn classifier_suppresses_returned_local_using_durable_provenance_only() {
        let classifier = classifier();
        let returned = envelope("Vision.Summary", "local-node", "true");
        let mut remote_event = event("Frame_Store-1", &returned);
        remote_event.remote_peer_id = "definitely-not-local-node".to_owned();
        assert_eq!(
            classifier.classify(remote_event),
            Err(EgressSkipKind::ReturnedLocal)
        );
    }

    #[test]
    fn classifier_preserves_every_payload_byte_and_leaks_no_envelope_metadata() {
        let classifier = classifier();
        for payload in [
            r#"  { "alpha": 1, "beta": 2 }  "#,
            r#"{"beta":2,"alpha":1}"#,
            r#"{"value":1.0}"#,
            r#"{"label":"\u03bb"}"#,
            r#"{"label":"λ"}"#,
            "{\"ok\":true}\n\t ",
        ] {
            let expected = payload.as_bytes().to_vec();
            let valid = envelope("Vision.Summary", "remote-node", payload);
            let item = classifier
                .classify(event("Frame_Store-1", &valid))
                .expect("eligible envelope");
            assert_eq!(item.payload.as_ref(), expected);
            assert!(!item
                .payload
                .windows(BRIDGE_ENVELOPE_KIND.len())
                .any(|part| { part == BRIDGE_ENVELOPE_KIND.as_bytes() }));
            assert!(!item
                .payload
                .windows("remote-node".len())
                .any(|part| { part == b"remote-node" }));
        }
    }

    #[test]
    fn classifier_route_table_is_fixed_by_validated_startup_mappings() {
        let classifier = classifier();
        assert_eq!(classifier.route_count(), 2);
        for sequence in 0..100 {
            let valid = envelope("dynamic", "remote-node", "null");
            assert_eq!(
                classifier.classify(event(&format!("attacker-{sequence}"), &valid)),
                Err(EgressSkipKind::UnmappedCollection)
            );
        }
        assert_eq!(classifier.route_count(), 2);
    }

    #[test]
    fn classifier_bounds_fifo_payload_bytes_without_truncation() {
        let classifier = classifier();
        let exact = format!("0{}", " ".repeat(MAX_INGRESS_PAYLOAD_BYTES - 1));
        let accepted = envelope("Vision.Summary", "remote-node", &exact);
        let item = classifier
            .classify(event("Frame_Store-1", &accepted))
            .expect("exact ingress ceiling remains eligible");
        assert_eq!(item.payload.len(), MAX_INGRESS_PAYLOAD_BYTES);
        assert_eq!(item.payload.as_ref(), exact.as_bytes());

        let over = format!("{exact} ");
        let rejected = envelope("Vision.Summary", "remote-node", &over);
        assert_eq!(
            classifier.classify(event("Frame_Store-1", &rejected)),
            Err(EgressSkipKind::OversizedPayload)
        );
    }

    #[test]
    fn classifier_header_name_is_stable_and_valid() {
        assert_eq!(BRIDGE_ORIGIN_HEADER, "Peat-Nats-Bridge-Origin");
        let _: async_nats::HeaderName = BRIDGE_ORIGIN_HEADER.parse().expect("valid header name");
    }

    #[derive(Clone, Debug)]
    struct PublishCall {
        subject: Subject,
        headers: HeaderMap,
        payload: Bytes,
    }

    #[derive(Clone, Default)]
    struct FakePublisher {
        calls: Arc<Mutex<Vec<PublishCall>>>,
        publish_results: Arc<Mutex<Vec<Result<(), EgressFailureKind>>>>,
        flush_results: Arc<Mutex<Vec<Result<(), EgressFailureKind>>>>,
    }

    impl FakePublisher {
        fn with_results(
            publish_results: impl IntoIterator<Item = Result<(), EgressFailureKind>>,
            flush_results: impl IntoIterator<Item = Result<(), EgressFailureKind>>,
        ) -> Self {
            let mut publish_results = publish_results.into_iter().collect::<Vec<_>>();
            publish_results.reverse();
            let mut flush_results = flush_results.into_iter().collect::<Vec<_>>();
            flush_results.reverse();
            Self {
                publish_results: Arc::new(Mutex::new(publish_results)),
                flush_results: Arc::new(Mutex::new(flush_results)),
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<PublishCall> {
            self.calls.lock().expect("calls lock").clone()
        }
    }

    impl BridgePublisher for FakePublisher {
        fn publish<'a>(
            &'a self,
            subject: Subject,
            headers: HeaderMap,
            payload: Bytes,
        ) -> Pin<Box<dyn Future<Output = Result<(), EgressFailureKind>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.lock().expect("calls lock").push(PublishCall {
                    subject,
                    headers,
                    payload,
                });
                self.publish_results
                    .lock()
                    .expect("results lock")
                    .pop()
                    .unwrap_or(Ok(()))
            })
        }

        fn flush(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<(), EgressFailureKind>> + Send + '_>> {
            Box::pin(async {
                self.flush_results
                    .lock()
                    .expect("flush results lock")
                    .pop()
                    .unwrap_or(Ok(()))
            })
        }
    }

    fn ready() -> BridgeReadiness {
        let readiness = BridgeReadiness::new([
            Subject::from("Vision.Summary"),
            Subject::from("telemetry.reading"),
        ]);
        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        readiness
    }

    fn journals() -> (tempfile::TempDir, DeliveryLedger) {
        let dir = tempfile::tempdir().unwrap();
        let ledger = crate::nats_bridge::ledger::BridgeLedger::open(dir.path()).unwrap();
        (dir, ledger.delivery())
    }

    #[tokio::test]
    async fn durable_reservation_precedes_fifo_and_racing_sources_publish_once() {
        let (_dir, delivery) = journals();
        let readiness = ready();
        let stats = EgressStats::default();
        let (coordinator, mut rx) = DeliveryCoordinator::new(
            &mappings(),
            "local-node",
            stats.clone(),
            delivery.clone(),
            readiness,
        );
        let valid = envelope("Vision.Summary", "remote-node", r#"{"frame":1}"#);
        let first = coordinator.clone();
        let second = coordinator.clone();
        let a = tokio::spawn(async move {
            first
                .deliver(event_with_id("Frame_Store-1", "same", &valid))
                .await
        });
        let valid = envelope("Vision.Summary", "remote-node", r#"{"frame":1}"#);
        let b = tokio::spawn(async move {
            second
                .deliver(event_with_id("Frame_Store-1", "same", &valid))
                .await
        });
        let results = [a.await.unwrap(), b.await.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            rx.recv().await.unwrap().digest,
            document_digest("Frame_Store-1", "same")
        );
        assert!(rx.try_recv().is_err());
        assert!(delivery
            .is_suppressed(document_digest("Frame_Store-1", "same"))
            .await
            .unwrap());
        assert_eq!(stats.snapshot().duplicate, 1);
    }

    #[tokio::test]
    async fn unready_and_unhealthy_paths_never_reserve_or_enqueue() {
        let (_dir, delivery) = journals();
        let readiness = BridgeReadiness::new([
            Subject::from("Vision.Summary"),
            Subject::from("telemetry.reading"),
        ]);
        let (coordinator, mut rx) = DeliveryCoordinator::new(
            &mappings(),
            "local-node",
            EgressStats::default(),
            delivery.clone(),
            readiness.clone(),
        );
        let valid = envelope("Vision.Summary", "remote-node", "true");
        assert_eq!(
            coordinator
                .deliver(event_with_id("Frame_Store-1", "unready", &valid))
                .await,
            Err(EgressActionKind::Lost(EgressFailureKind::Unavailable))
        );
        assert!(!delivery
            .is_suppressed(document_digest("Frame_Store-1", "unready"))
            .await
            .unwrap());
        assert!(rx.try_recv().is_err());

        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        delivery.stop();
        let valid = envelope("Vision.Summary", "remote-node", "true");
        assert_eq!(
            coordinator
                .deliver(event_with_id("Frame_Store-1", "bad-ledger", &valid))
                .await,
            Err(EgressActionKind::Lost(EgressFailureKind::LedgerUnavailable))
        );
        assert!(!readiness.snapshot().delivery_healthy);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn publish_flush_completion_is_durable_and_exact() {
        let (dir, delivery) = journals();
        let readiness = ready();
        let stats = EgressStats::default();
        let (coordinator, rx) = DeliveryCoordinator::new(
            &mappings(),
            "local-node",
            stats.clone(),
            delivery.clone(),
            readiness.clone(),
        );
        let publisher = FakePublisher::default();
        let calls = publisher.clone();
        let worker = tokio::spawn(run_egress_worker(
            rx,
            "local-node".parse().unwrap(),
            publisher,
            delivery.clone(),
            readiness,
            stats.clone(),
            coordinator.diagnostics(),
        ));
        let payload = r#" {"frame":1.0,"label":"\u03bb"} "#;
        let valid = envelope("Vision.Summary", "remote-node", payload);
        coordinator
            .deliver(event_with_id("Frame_Store-1", "complete", &valid))
            .await
            .unwrap();
        drop(coordinator);
        worker.await.unwrap();
        assert_eq!(calls.calls()[0].payload.as_ref(), payload.as_bytes());
        assert_eq!(calls.calls()[0].subject, Subject::from("Vision.Summary"));
        assert_eq!(
            calls.calls()[0]
                .headers
                .get_all(BRIDGE_ORIGIN_HEADER)
                .map(|value| value.as_str())
                .collect::<Vec<_>>(),
            ["local-node"]
        );
        assert!(delivery
            .is_suppressed(document_digest("Frame_Store-1", "complete"))
            .await
            .unwrap());
        assert_eq!(stats.snapshot().published, 1);

        delivery.request_stop();
        delivery.join().unwrap();
        drop(delivery);
        let reopened = crate::nats_bridge::ledger::BridgeLedger::open(dir.path()).unwrap();
        let restarted_stats = EgressStats::default();
        let (restarted, mut restarted_rx) = DeliveryCoordinator::new(
            &mappings(),
            "local-node",
            restarted_stats.clone(),
            reopened.delivery(),
            ready(),
        );
        for _ in 0..3 {
            assert_eq!(
                restarted
                    .deliver(event_with_id("Frame_Store-1", "complete", &valid))
                    .await,
                Err(EgressActionKind::Skipped(EgressSkipKind::Duplicate))
            );
        }
        assert!(restarted_rx.try_recv().is_err());
        assert_eq!(restarted_stats.snapshot().published, 0);
        assert_eq!(restarted_stats.snapshot().duplicate, 3);
        reopened.join().unwrap();
    }

    #[tokio::test]
    async fn queue_publish_and_flush_failures_remain_terminal_reserved() {
        let (_dir, delivery) = journals();
        let readiness = ready();
        let stats = EgressStats::default();
        let (coordinator, rx) = DeliveryCoordinator::with_capacity(
            &mappings(),
            "local-node",
            stats.clone(),
            delivery.clone(),
            readiness.clone(),
            2,
            TracingEgressEmitter,
        );
        for id in ["publish-fail", "flush-fail", "queue-full"] {
            let valid = envelope("Vision.Summary", "remote-node", "true");
            let result = coordinator
                .deliver(event_with_id("Frame_Store-1", id, &valid))
                .await;
            if id == "queue-full" {
                assert_eq!(
                    result,
                    Err(EgressActionKind::Lost(EgressFailureKind::QueueFull))
                );
            } else {
                result.unwrap();
            }
        }
        let publisher = FakePublisher::with_results(
            [Err(EgressFailureKind::PublishFailed), Ok(())],
            [Err(EgressFailureKind::FlushFailed)],
        );
        drop(coordinator);
        run_egress_worker(
            rx,
            "local-node".parse().unwrap(),
            publisher,
            delivery.clone(),
            readiness,
            stats.clone(),
            EgressDiagnostics::new(2),
        )
        .await;
        for id in ["publish-fail", "flush-fail", "queue-full"] {
            let digest = document_digest("Frame_Store-1", id);
            assert!(delivery.is_suppressed(digest).await.unwrap());
        }
        assert_eq!(stats.snapshot().publish_failed, 1);
        assert_eq!(stats.snapshot().flush_failed, 1);
        assert_eq!(stats.snapshot().queue_full, 1);
    }
}
