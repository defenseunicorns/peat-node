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

use async_nats::connection::State;
use async_nats::{Client, HeaderMap, HeaderValue, PublishErrorKind, Subject};
use buffa::bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::nats_bridge::config::SubjectMapping;
use crate::nats_bridge::envelope::{BridgeEnvelope, BRIDGE_ENVELOPE_KIND, BRIDGE_ENVELOPE_VERSION};
use crate::nats_bridge::ingress::MAX_INGRESS_PAYLOAD_BYTES;
use crate::nats_bridge::readiness::BridgeReadiness;
use crate::node::BridgeChangeEvent;

/// Stable private marker added to bridge-owned Core NATS publications.
pub(crate) const BRIDGE_ORIGIN_HEADER: &str = "Peat-Nats-Bridge-Origin";

/// Maximum eligible egress items retained by the bridge-owned FIFO.
pub(crate) const EGRESS_QUEUE_CAPACITY: usize = 256;

/// Exact count of process-lifetime document digests retained for deduplication.
pub(crate) const EGRESS_DEDUP_CAPACITY: usize = 4096;

const EGRESS_DOCUMENT_DIGEST_DOMAIN: &[u8] = b"peat-node/egress-document/v1\0";

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
    DedupExhausted,
}

/// One byte-exact publish request after all envelope and route gates pass.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct EgressItem {
    pub subject: Subject,
    pub payload: Bytes,
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
}

impl EgressAction {
    fn emit(&self) {
        let route_index = self.route_index.unwrap_or_default();
        let payload_bytes = self.payload_bytes.unwrap_or_default();
        match self.kind {
            EgressActionKind::Published => debug!(
                route_index,
                payload_bytes, "NATS bridge egress publish enqueued"
            ),
            EgressActionKind::Skipped(kind) => debug!(
                route_index,
                payload_bytes,
                reason = ?kind,
                "NATS bridge egress skipped"
            ),
            EgressActionKind::Lost(kind) => warn!(
                route_index,
                payload_bytes,
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
    dedup_exhausted: AtomicU64,
    queue_full: AtomicU64,
    queue_closed: AtomicU64,
    unavailable: AtomicU64,
    publish_failed: AtomicU64,
    max_payload_exceeded: AtomicU64,
    event_lagged: AtomicU64,
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
    pub dedup_exhausted: u64,
    pub queue_full: u64,
    pub queue_closed: u64,
    pub unavailable: u64,
    pub publish_failed: u64,
    pub max_payload_exceeded: u64,
    pub event_lagged: u64,
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
            dedup_exhausted: self.inner.dedup_exhausted.load(Ordering::Relaxed),
            queue_full: self.inner.queue_full.load(Ordering::Relaxed),
            queue_closed: self.inner.queue_closed.load(Ordering::Relaxed),
            unavailable: self.inner.unavailable.load(Ordering::Relaxed),
            publish_failed: self.inner.publish_failed.load(Ordering::Relaxed),
            max_payload_exceeded: self.inner.max_payload_exceeded.load(Ordering::Relaxed),
            event_lagged: self.inner.event_lagged.load(Ordering::Relaxed),
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
            EgressSkipKind::DedupExhausted => &self.inner.dedup_exhausted,
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
            subject: subject.clone(),
            payload: Bytes::from(envelope.payload),
        })
    }

    #[cfg(test)]
    fn route_count(&self) -> usize {
        self.routes.len()
    }
}

type DocumentDigestFn = fn(&str, &str) -> Option<[u8; 32]>;

fn document_digest(collection: &str, doc_id: &str) -> Option<[u8; 32]> {
    let collection_len = u64::try_from(collection.len()).ok()?;
    let doc_id_len = u64::try_from(doc_id.len()).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(EGRESS_DOCUMENT_DIGEST_DOMAIN);
    hasher.update(collection_len.to_be_bytes());
    hasher.update(collection.as_bytes());
    hasher.update(doc_id_len.to_be_bytes());
    hasher.update(doc_id.as_bytes());
    Some(hasher.finalize().into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DedupDisposition {
    Inserted,
    Duplicate,
    Exhausted,
}

/// Exact-size, non-evicting process-lifetime digest table.
struct EgressDedup {
    slots: Box<[[u8; 32]; EGRESS_DEDUP_CAPACITY]>,
    len: usize,
    exhausted: bool,
    digest_fn: DocumentDigestFn,
}

impl EgressDedup {
    #[cfg(test)]
    fn new() -> Self {
        Self::with_digest_fn(document_digest)
    }

    fn with_digest_fn(digest_fn: DocumentDigestFn) -> Self {
        Self {
            slots: Box::new([[0; 32]; EGRESS_DEDUP_CAPACITY]),
            len: 0,
            exhausted: false,
            digest_fn,
        }
    }

    fn check_and_insert_digest(&mut self, digest: Option<[u8; 32]>) -> DedupDisposition {
        let Some(digest) = digest else {
            self.exhausted = true;
            return DedupDisposition::Exhausted;
        };
        if self.slots[..self.len].contains(&digest) {
            return DedupDisposition::Duplicate;
        }
        if self.exhausted || self.len == EGRESS_DEDUP_CAPACITY {
            self.exhausted = true;
            return DedupDisposition::Exhausted;
        }
        self.slots[self.len] = digest;
        self.len += 1;
        if self.len == EGRESS_DEDUP_CAPACITY {
            self.exhausted = true;
        }
        DedupDisposition::Inserted
    }
}

/// Non-blocking producer that classifies and deduplicates before FIFO admission.
pub(crate) struct EgressRouter {
    classifier: EgressClassifier,
    dedup: Mutex<EgressDedup>,
    tx: mpsc::Sender<EgressItem>,
    stats: EgressStats,
    emitter: Arc<dyn EgressActionEmitter>,
}

impl EgressRouter {
    pub fn new(
        mappings: &[SubjectMapping],
        local_node_id: &str,
        stats: EgressStats,
    ) -> (Self, mpsc::Receiver<EgressItem>) {
        Self::with_capacity_and_digest(
            mappings,
            local_node_id,
            stats,
            EGRESS_QUEUE_CAPACITY,
            document_digest,
            TracingEgressEmitter,
        )
    }

    fn with_capacity_and_digest<E>(
        mappings: &[SubjectMapping],
        local_node_id: &str,
        stats: EgressStats,
        queue_capacity: usize,
        digest_fn: DocumentDigestFn,
        emitter: E,
    ) -> (Self, mpsc::Receiver<EgressItem>)
    where
        E: EgressActionEmitter,
    {
        let (tx, rx) = mpsc::channel(queue_capacity);
        (
            Self {
                classifier: EgressClassifier::new(mappings, local_node_id),
                dedup: Mutex::new(EgressDedup::with_digest_fn(digest_fn)),
                tx,
                stats,
                emitter: Arc::new(emitter),
            },
            rx,
        )
    }

    pub fn admit(&self, event: BridgeChangeEvent) -> Result<(), EgressActionKind> {
        let route_index = self.classifier.route_index(&event.collection);
        let digest = (self
            .dedup
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .digest_fn)(&event.collection, &event.doc_id);
        let item = match self.classifier.classify(event) {
            Ok(item) => item,
            Err(kind) => {
                self.record_action(EgressActionKind::Skipped(kind), route_index, None);
                return Err(EgressActionKind::Skipped(kind));
            }
        };
        let disposition = self
            .dedup
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .check_and_insert_digest(digest);
        match disposition {
            DedupDisposition::Duplicate => {
                let kind = EgressSkipKind::Duplicate;
                self.record_action(EgressActionKind::Skipped(kind), route_index, None);
                return Err(EgressActionKind::Skipped(kind));
            }
            DedupDisposition::Exhausted => {
                let kind = EgressSkipKind::DedupExhausted;
                self.record_action(EgressActionKind::Skipped(kind), route_index, None);
                return Err(EgressActionKind::Skipped(kind));
            }
            DedupDisposition::Inserted => {}
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
        self.emitter.emit(EgressAction {
            kind,
            route_index,
            payload_bytes,
        });
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
}

/// Forward private node events without ever awaiting bridge-owned FIFO space.
pub(crate) async fn run_bridge_event_router(
    mut events: tokio::sync::broadcast::Receiver<BridgeChangeEvent>,
    router: EgressRouter,
    stats: EgressStats,
) {
    loop {
        match events.recv().await {
            Ok(event) => {
                let _ = router.admit(event);
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                stats.record_event_lagged(dropped);
                EgressAction {
                    kind: EgressActionKind::Lost(EgressFailureKind::EventLagged),
                    route_index: None,
                    payload_bytes: None,
                }
                .emit();
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// Drain the sole FIFO serially; failures are terminal and later items continue.
pub(crate) async fn run_egress_worker<P: BridgePublisher>(
    mut rx: mpsc::Receiver<EgressItem>,
    local_node_id: String,
    publisher: P,
    stats: EgressStats,
) {
    let origin_header_value = local_node_id.parse::<HeaderValue>().ok();
    while let Some(item) = rx.recv().await {
        let payload_bytes = item.payload.len();
        let Some(origin_header_value) = origin_header_value.clone() else {
            stats.record_failure(EgressFailureKind::PublishFailed);
            EgressAction {
                kind: EgressActionKind::Lost(EgressFailureKind::PublishFailed),
                route_index: None,
                payload_bytes: Some(payload_bytes),
            }
            .emit();
            continue;
        };
        let mut headers = HeaderMap::new();
        headers.insert(BRIDGE_ORIGIN_HEADER, origin_header_value);
        match publisher.publish(item.subject, headers, item.payload).await {
            Ok(()) => {
                stats.inner.published.fetch_add(1, Ordering::Relaxed);
                EgressAction {
                    kind: EgressActionKind::Published,
                    route_index: None,
                    payload_bytes: Some(payload_bytes),
                }
                .emit();
            }
            Err(kind @ EgressFailureKind::Unavailable)
            | Err(kind @ EgressFailureKind::PublishFailed)
            | Err(kind @ EgressFailureKind::MaxPayloadExceeded) => {
                stats.record_failure(kind);
                EgressAction {
                    kind: EgressActionKind::Lost(kind),
                    route_index: None,
                    payload_bytes: Some(payload_bytes),
                }
                .emit();
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
                subject: Subject::from("Vision.Summary"),
                payload: Bytes::from_static(br#"{"ok":true}"#),
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

    #[derive(Clone, Default)]
    struct RecordingEmitter {
        actions: Arc<Mutex<Vec<EgressAction>>>,
    }

    impl EgressActionEmitter for RecordingEmitter {
        fn emit(&self, action: EgressAction) {
            self.actions.lock().expect("actions lock").push(action);
        }
    }

    impl RecordingEmitter {
        fn actions(&self) -> Vec<EgressAction> {
            self.actions.lock().expect("actions lock").clone()
        }
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
        results: Arc<Mutex<Vec<Result<(), EgressFailureKind>>>>,
    }

    impl FakePublisher {
        fn with_results(results: impl IntoIterator<Item = Result<(), EgressFailureKind>>) -> Self {
            let mut results = results.into_iter().collect::<Vec<_>>();
            results.reverse();
            Self {
                results: Arc::new(Mutex::new(results)),
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
                self.results
                    .lock()
                    .expect("results lock")
                    .pop()
                    .unwrap_or(Ok(()))
            })
        }
    }

    fn constant_digest(_collection: &str, _doc_id: &str) -> Option<[u8; 32]> {
        Some([0x5a; 32])
    }

    #[test]
    fn dedup_digest_is_domain_separated_and_length_framed() {
        let actual = document_digest("ab", "c").expect("digest");
        let mut expected = Sha256::new();
        expected.update(b"peat-node/egress-document/v1\0");
        expected.update(2_u64.to_be_bytes());
        expected.update(b"ab");
        expected.update(1_u64.to_be_bytes());
        expected.update(b"c");
        assert_eq!(actual, <[u8; 32]>::from(expected.finalize()));
        assert_ne!(actual, document_digest("a", "bc").expect("digest"));
        assert_ne!(
            document_digest("same", "id").expect("digest"),
            document_digest("same-id", "").expect("digest")
        );
    }

    #[test]
    fn dedup_table_has_exact_fixed_width_storage_and_no_dynamic_entries() {
        let table = EgressDedup::new();
        assert_eq!(table.slots.len(), EGRESS_DEDUP_CAPACITY);
        assert_eq!(
            std::mem::size_of_val(table.slots.as_ref()),
            EGRESS_DEDUP_CAPACITY * 32
        );
        assert_eq!(EGRESS_DEDUP_CAPACITY * 32, 131_072);
        assert_eq!(table.len, 0);
    }

    #[test]
    fn dedup_distinguishes_collections_and_collision_only_suppresses() {
        let mut table = EgressDedup::new();
        let a = document_digest("collection-a", "same-id");
        let b = document_digest("collection-b", "same-id");
        assert_eq!(table.check_and_insert_digest(a), DedupDisposition::Inserted);
        assert_eq!(table.check_and_insert_digest(b), DedupDisposition::Inserted);

        let mut collision_table = EgressDedup::with_digest_fn(constant_digest);
        let first = (collision_table.digest_fn)("a", "first");
        let collision = (collision_table.digest_fn)("b", "second");
        assert_eq!(
            collision_table.check_and_insert_digest(first),
            DedupDisposition::Inserted
        );
        assert_eq!(
            collision_table.check_and_insert_digest(collision),
            DedupDisposition::Duplicate
        );
        assert_eq!(collision_table.len, 1);
    }

    #[test]
    fn dedup_exhaustion_is_non_evicting_and_sticky_for_unseen_documents() {
        let mut table = EgressDedup::new();
        let first = document_digest("frames", "0");
        for sequence in 0..EGRESS_DEDUP_CAPACITY {
            assert_eq!(
                table.check_and_insert_digest(document_digest(
                    "frames",
                    sequence.to_string().as_str()
                )),
                DedupDisposition::Inserted
            );
        }
        assert_eq!(table.len, EGRESS_DEDUP_CAPACITY);
        assert!(table.exhausted);
        assert_eq!(
            table.check_and_insert_digest(document_digest("frames", "unseen")),
            DedupDisposition::Exhausted
        );
        assert_eq!(
            table.check_and_insert_digest(first),
            DedupDisposition::Duplicate
        );
        assert_eq!(table.len, EGRESS_DEDUP_CAPACITY);
    }

    #[test]
    fn digest_conversion_failure_is_sticky_fail_closed() {
        let mut table = EgressDedup::new();
        assert_eq!(
            table.check_and_insert_digest(None),
            DedupDisposition::Exhausted
        );
        assert!(table.exhausted);
        assert_eq!(
            table.check_and_insert_digest(document_digest("frames", "later")),
            DedupDisposition::Exhausted
        );
    }

    #[test]
    fn queue_failure_is_terminal_and_duplicate_cannot_retry() {
        let stats = EgressStats::default();
        let emitter = RecordingEmitter::default();
        let (router, _rx) = EgressRouter::with_capacity_and_digest(
            &mappings(),
            "local-node",
            stats.clone(),
            1,
            document_digest,
            emitter.clone(),
        );
        let valid = envelope("Vision.Summary", "remote-node", "1");
        router
            .admit(event_with_id("Frame_Store-1", "first", &valid))
            .expect("first item fits");
        assert_eq!(
            router.admit(event_with_id("Frame_Store-1", "lost", &valid)),
            Err(EgressActionKind::Lost(EgressFailureKind::QueueFull))
        );
        assert_eq!(
            router.admit(event_with_id("Frame_Store-1", "lost", &valid)),
            Err(EgressActionKind::Skipped(EgressSkipKind::Duplicate))
        );
        assert_eq!(stats.snapshot().queue_full, 1);
        assert_eq!(stats.snapshot().duplicate, 1);
        assert_eq!(emitter.actions().len(), 2);
    }

    #[test]
    fn closed_queue_is_terminal_and_does_not_enable_retry() {
        let stats = EgressStats::default();
        let (router, rx) = EgressRouter::new(&mappings(), "local-node", stats.clone());
        drop(rx);
        let valid = envelope("Vision.Summary", "remote-node", "true");
        assert_eq!(
            router.admit(event_with_id("Frame_Store-1", "closed", &valid)),
            Err(EgressActionKind::Lost(EgressFailureKind::QueueClosed))
        );
        assert_eq!(
            router.admit(event_with_id("Frame_Store-1", "closed", &valid)),
            Err(EgressActionKind::Skipped(EgressSkipKind::Duplicate))
        );
        assert_eq!(stats.snapshot().queue_closed, 1);
    }

    #[tokio::test]
    async fn serial_worker_preserves_fifo_and_continues_after_terminal_failure() {
        let stats = EgressStats::default();
        let (router, rx) = EgressRouter::new(&mappings(), "local-node", stats.clone());
        let publisher = FakePublisher::with_results([
            Err(EgressFailureKind::Unavailable),
            Ok(()),
            Err(EgressFailureKind::PublishFailed),
            Err(EgressFailureKind::MaxPayloadExceeded),
            Ok(()),
        ]);
        let worker = tokio::spawn(run_egress_worker(
            rx,
            "local-node".to_owned(),
            publisher.clone(),
            stats.clone(),
        ));
        let payloads = ["1", "2", "3", "4", "5"];
        for (sequence, payload) in payloads.into_iter().enumerate() {
            let valid = envelope("Vision.Summary", "remote-node", payload);
            router
                .admit(event_with_id(
                    "Frame_Store-1",
                    &format!("doc-{sequence}"),
                    &valid,
                ))
                .expect("queue admission");
        }
        drop(router);
        worker.await.expect("worker completes");

        let calls = publisher.calls();
        assert_eq!(
            calls
                .iter()
                .map(|call| call.payload.as_ref())
                .collect::<Vec<_>>(),
            payloads.map(str::as_bytes)
        );
        assert!(calls
            .iter()
            .all(|call| call.subject == Subject::from("Vision.Summary")));
        assert!(calls.iter().all(|call| {
            call.headers
                .get_all(BRIDGE_ORIGIN_HEADER)
                .map(|value| value.as_str())
                .collect::<Vec<_>>()
                == ["local-node"]
        }));
        assert_eq!(stats.snapshot().unavailable, 1);
        assert_eq!(stats.snapshot().publish_failed, 1);
        assert_eq!(stats.snapshot().max_payload_exceeded, 1);
        assert_eq!(stats.snapshot().published, 2);
    }

    #[tokio::test]
    async fn invalid_origin_header_value_fails_safely_without_panicking_or_publishing() {
        let stats = EgressStats::default();
        let (router, rx) = EgressRouter::new(&mappings(), "local\nnode", stats.clone());
        let publisher = FakePublisher::default();
        let worker = tokio::spawn(run_egress_worker(
            rx,
            "local\nnode".to_owned(),
            publisher.clone(),
            stats.clone(),
        ));
        for sequence in 0..2 {
            let valid = envelope("Vision.Summary", "remote-node", "true");
            router
                .admit(event_with_id(
                    "Frame_Store-1",
                    &format!("invalid-header-{sequence}"),
                    &valid,
                ))
                .expect("queue admission");
        }
        drop(router);
        worker.await.expect("worker continues and closes");
        assert!(publisher.calls().is_empty());
        assert_eq!(stats.snapshot().publish_failed, 2);
    }

    #[test]
    fn diagnostic_actions_are_fixed_width_and_exclude_all_untrusted_text() {
        let stats = EgressStats::default();
        let emitter = RecordingEmitter::default();
        let (router, rx) = EgressRouter::with_capacity_and_digest(
            &mappings(),
            "local-node",
            stats,
            1,
            document_digest,
            emitter.clone(),
        );
        drop(rx);
        let secret_payload = r#"{"secret":"do-not-log"}"#;
        let secret_source = "source-secret";
        let valid = envelope("Vision.Summary", secret_source, secret_payload);
        let mut untrusted = event_with_id("Frame_Store-1", "document-secret", &valid);
        untrusted.remote_peer_id = "peer-secret".to_owned();
        let _ = router.admit(untrusted);

        let rendered = format!("{:?}", emitter.actions());
        for forbidden in [
            secret_payload,
            secret_source,
            "document-secret",
            "peer-secret",
            BRIDGE_ORIGIN_HEADER,
            "raw-user:raw-password@broker",
            "source error chain",
        ] {
            assert!(!rendered.contains(forbidden));
        }
        assert_eq!(
            emitter.actions(),
            [EgressAction {
                kind: EgressActionKind::Lost(EgressFailureKind::QueueClosed),
                route_index: Some(0),
                payload_bytes: Some(secret_payload.len()),
            }]
        );
    }

    #[test]
    fn router_counts_every_fixed_classification_without_dynamic_labels() {
        let stats = EgressStats::default();
        let (router, _rx) = EgressRouter::new(&mappings(), "local-node", stats.clone());
        let malformed = BridgeChangeEvent {
            collection: "Frame_Store-1".to_owned(),
            doc_id: "a".to_owned(),
            remote_peer_id: "peer".to_owned(),
            json_data: "ordinary".to_owned(),
        };
        assert_eq!(
            router.admit(malformed),
            Err(EgressActionKind::Skipped(EgressSkipKind::MalformedEnvelope))
        );
        let returned = envelope("Vision.Summary", "local-node", "null");
        assert_eq!(
            router.admit(event_with_id("Frame_Store-1", "b", &returned)),
            Err(EgressActionKind::Skipped(EgressSkipKind::ReturnedLocal))
        );
        assert_eq!(stats.snapshot().malformed, 1);
        assert_eq!(stats.snapshot().returned_local, 1);
    }
}
