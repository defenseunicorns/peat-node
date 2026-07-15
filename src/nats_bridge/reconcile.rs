//! Bounded, single-flight recovery of missed remote bridge notifications.
//!
//! Pinned peat-mesh returns one complete key vector per collection. That
//! inherited transient is explicit; bridge-owned retained work is limited to
//! 64-key processing batches, one hydrated body, and 16 hydrations per second.
//! A missing exclusion entry only makes a snapshot eligible for the exact
//! envelope classifier; it is not historical-origin proof. Envelope-shaped
//! documents created before the exclusion journal existed remain explicitly
//! ambiguous, and `source_node_id` is never promoted into provenance.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use super::config::SubjectMapping;
use super::egress::DeliveryCoordinator;
use super::ledger::{document_digest, DeliveryLedger, LocalExclusionLedger};
use super::readiness::BridgeReadiness;
use crate::node::{BridgeChangeEvent, SidecarNode};

pub(crate) const RECONCILE_BATCH_KEYS: usize = 64;
pub(crate) const RECONCILE_HYDRATIONS_PER_SECOND: u64 = 16;
const HYDRATION_INTERVAL: Duration = Duration::from_millis(1_000 / RECONCILE_HYDRATIONS_PER_SECOND);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReconcileReason {
    StartupReady,
    ReconnectReady,
    EventLagged,
}

#[derive(Clone, Default)]
pub(crate) struct ReconcileStats(Arc<ReconcileStatsInner>);

#[derive(Default)]
struct ReconcileStatsInner {
    triggers: AtomicU64,
    coalesced: AtomicU64,
    scans: AtomicU64,
    hydrated: AtomicU64,
    suppressed: AtomicU64,
    failures: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReconcileSnapshot {
    pub triggers: u64,
    pub coalesced: u64,
    pub scans: u64,
    pub hydrated: u64,
    pub suppressed: u64,
    pub failures: u64,
}

impl ReconcileStats {
    pub(crate) fn snapshot(&self) -> ReconcileSnapshot {
        ReconcileSnapshot {
            triggers: self.0.triggers.load(Ordering::Relaxed),
            coalesced: self.0.coalesced.load(Ordering::Relaxed),
            scans: self.0.scans.load(Ordering::Relaxed),
            hydrated: self.0.hydrated.load(Ordering::Relaxed),
            suppressed: self.0.suppressed.load(Ordering::Relaxed),
            failures: self.0.failures.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ReconcileTrigger {
    tx: mpsc::Sender<ReconcileReason>,
    pending: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    stats: ReconcileStats,
}

impl ReconcileTrigger {
    pub(crate) fn trigger(&self, reason: ReconcileReason) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        self.stats.0.triggers.fetch_add(1, Ordering::Relaxed);
        if self.tx.try_send(reason).is_err() && !self.pending.swap(true, Ordering::AcqRel) {
            self.stats.0.coalesced.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.pending.store(false, Ordering::Release);
    }
}

trait ReconcileReader: Send + Sync + 'static {
    fn keys_with_prefix(&self, prefix: &str) -> anyhow::Result<Vec<String>>;
    fn get_document<'a>(
        &'a self,
        collection: &'a str,
        doc_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<String>>> + Send + 'a>>;
}

impl ReconcileReader for SidecarNode {
    fn keys_with_prefix(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        self.document_store().keys_with_prefix(prefix)
    }

    fn get_document<'a>(
        &'a self,
        collection: &'a str,
        doc_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<String>>> + Send + 'a>> {
        Box::pin(async move { self.get_bridge_document(collection, doc_id) })
    }
}

trait ReconcileClock: Clone + Send + Sync + 'static {
    fn delay(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
struct TokioClock;

impl ReconcileClock for TokioClock {
    fn delay(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[allow(dead_code)]
pub(crate) fn spawn_reconciler(
    node: Arc<SidecarNode>,
    mappings: Vec<SubjectMapping>,
    exclusion: LocalExclusionLedger,
    delivery: DeliveryLedger,
    coordinator: DeliveryCoordinator,
    readiness: BridgeReadiness,
) -> (
    ReconcileTrigger,
    ReconcileStats,
    tokio::task::JoinHandle<()>,
) {
    spawn_reconciler_with(
        node,
        mappings,
        exclusion,
        delivery,
        coordinator,
        readiness,
        TokioClock,
    )
}

fn spawn_reconciler_with<R, C>(
    reader: Arc<R>,
    mappings: Vec<SubjectMapping>,
    exclusion: LocalExclusionLedger,
    delivery: DeliveryLedger,
    coordinator: DeliveryCoordinator,
    readiness: BridgeReadiness,
    clock: C,
) -> (
    ReconcileTrigger,
    ReconcileStats,
    tokio::task::JoinHandle<()>,
)
where
    R: ReconcileReader,
    C: ReconcileClock,
{
    let (tx, mut rx) = mpsc::channel(1);
    let pending = Arc::new(AtomicBool::new(false));
    let closed = Arc::new(AtomicBool::new(false));
    let stats = ReconcileStats::default();
    let trigger = ReconcileTrigger {
        tx,
        pending: Arc::clone(&pending),
        closed: Arc::clone(&closed),
        stats: stats.clone(),
    };
    let task_stats = stats.clone();
    let task = tokio::spawn(async move {
        while rx.recv().await.is_some() {
            if closed.load(Ordering::Acquire) {
                break;
            }
            loop {
                task_stats.0.scans.fetch_add(1, Ordering::Relaxed);
                scan_once(
                    reader.as_ref(),
                    &mappings,
                    &exclusion,
                    &delivery,
                    &coordinator,
                    &readiness,
                    &clock,
                    &closed,
                    &task_stats,
                )
                .await;
                if closed.load(Ordering::Acquire) || !pending.swap(false, Ordering::AcqRel) {
                    break;
                }
            }
        }
    });
    (trigger, stats, task)
}

#[allow(clippy::too_many_arguments)]
async fn scan_once<R, C>(
    reader: &R,
    mappings: &[SubjectMapping],
    exclusion: &LocalExclusionLedger,
    delivery: &DeliveryLedger,
    coordinator: &DeliveryCoordinator,
    readiness: &BridgeReadiness,
    clock: &C,
    closed: &AtomicBool,
    stats: &ReconcileStats,
) where
    R: ReconcileReader,
    C: ReconcileClock,
{
    for mapping in mappings {
        if !can_scan(readiness, exclusion, delivery, closed) {
            return;
        }
        let prefix = format!("{}:", mapping.collection());
        // This complete Vec is inherited from pinned peat-mesh. Batching
        // below bounds retained bridge work but deliberately makes no RSS claim.
        let keys = match reader.keys_with_prefix(&prefix) {
            Ok(keys) => keys,
            Err(_) => {
                stats.0.failures.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        for batch in keys.chunks(RECONCILE_BATCH_KEYS) {
            for key in batch {
                if !can_scan(readiness, exclusion, delivery, closed) {
                    return;
                }
                let Some(doc_id) = key.strip_prefix(&prefix) else {
                    continue;
                };
                let digest = document_digest(mapping.collection(), doc_id);
                match exclusion.contains(digest).await {
                    Ok(true) => {
                        stats.0.suppressed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Ok(false) => {}
                    Err(_) => {
                        readiness.set_exclusion_healthy(false);
                        stats.0.failures.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
                match delivery.is_suppressed(digest).await {
                    Ok(true) => {
                        stats.0.suppressed.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Ok(false) => {}
                    Err(_) => {
                        readiness.set_delivery_healthy(false);
                        stats.0.failures.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
                clock.delay(HYDRATION_INTERVAL).await;
                if !can_scan(readiness, exclusion, delivery, closed) {
                    return;
                }
                let json_data = match reader.get_document(mapping.collection(), doc_id).await {
                    Ok(Some(json)) => json,
                    Ok(None) => continue,
                    Err(_) => {
                        stats.0.failures.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                stats.0.hydrated.fetch_add(1, Ordering::Relaxed);
                let _ = coordinator
                    .deliver(BridgeChangeEvent {
                        collection: mapping.collection().to_owned(),
                        doc_id: doc_id.to_owned(),
                        remote_peer_id: String::new(),
                        json_data,
                    })
                    .await;
            }
            tokio::task::yield_now().await;
        }
    }
}

fn can_scan(
    readiness: &BridgeReadiness,
    exclusion: &LocalExclusionLedger,
    delivery: &DeliveryLedger,
    closed: &AtomicBool,
) -> bool {
    !closed.load(Ordering::Acquire)
        && exclusion.is_healthy()
        && delivery.is_healthy()
        && readiness.snapshot().is_ready()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_bridge::config::BridgeConfig;
    use crate::nats_bridge::egress::EgressStats;
    use crate::nats_bridge::envelope::{
        BridgeEnvelope, BRIDGE_ENVELOPE_KIND, BRIDGE_ENVELOPE_VERSION,
    };
    use crate::nats_bridge::ledger::BridgeLedger;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeReader {
        keys: Vec<String>,
        docs: Mutex<HashMap<String, String>>,
        hydrated: AtomicU64,
    }

    impl ReconcileReader for FakeReader {
        fn keys_with_prefix(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            Ok(self
                .keys
                .iter()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }

        fn get_document<'a>(
            &'a self,
            collection: &'a str,
            doc_id: &'a str,
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<Option<String>>> + Send + 'a>> {
            Box::pin(async move {
                self.hydrated.fetch_add(1, Ordering::Relaxed);
                Ok(self
                    .docs
                    .lock()
                    .unwrap()
                    .get(&format!("{collection}:{doc_id}"))
                    .cloned())
            })
        }
    }

    #[derive(Clone, Default)]
    struct FakeClock(Arc<AtomicU64>);

    impl ReconcileClock for FakeClock {
        fn delay(&self, _duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {})
        }
    }

    fn mappings() -> Vec<SubjectMapping> {
        let BridgeConfig::Enabled(config) = BridgeConfig::from_raw(
            Some("nats://127.0.0.1:9"),
            &["vision.summary=frames".to_owned()],
        )
        .unwrap() else {
            panic!("enabled mapping")
        };
        config.mappings().to_vec()
    }

    fn envelope() -> String {
        serde_json::to_string(&BridgeEnvelope {
            kind: BRIDGE_ENVELOPE_KIND.to_owned(),
            version: BRIDGE_ENVELOPE_VERSION,
            subject: "vision.summary".to_owned(),
            source_node_id: "remote".to_owned(),
            payload: r#"{"frame":1}"#.to_owned(),
        })
        .unwrap()
    }

    fn ready() -> BridgeReadiness {
        let readiness = BridgeReadiness::new([async_nats::Subject::from("vision.summary")]);
        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        readiness
    }

    #[tokio::test]
    async fn indexes_skip_before_hydration_and_unknown_uses_one_body() {
        let dir = tempfile::tempdir().unwrap();
        let journals = BridgeLedger::open(dir.path()).unwrap();
        let exclusion = journals.exclusion();
        let delivery = journals.delivery();
        exclusion
            .record_local_excluded(document_digest("frames", "local"))
            .await
            .unwrap();
        delivery
            .check_and_reserve(document_digest("frames", "delivered"))
            .await
            .unwrap();
        let reader = Arc::new(FakeReader {
            keys: vec![
                "frames:local".into(),
                "frames:delivered".into(),
                "frames:remote".into(),
            ],
            docs: Mutex::new([("frames:remote".into(), envelope())].into_iter().collect()),
            ..Default::default()
        });
        let readiness = ready();
        let (coordinator, mut rx) = DeliveryCoordinator::new(
            &mappings(),
            "local",
            EgressStats::default(),
            delivery.clone(),
            readiness.clone(),
        );
        let clock = FakeClock::default();
        let (trigger, stats, task) = spawn_reconciler_with(
            Arc::clone(&reader),
            mappings(),
            exclusion,
            delivery,
            coordinator,
            readiness,
            clock.clone(),
        );
        trigger.trigger(ReconcileReason::StartupReady);
        let item = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.digest, document_digest("frames", "remote"));
        assert_eq!(reader.hydrated.load(Ordering::Relaxed), 1);
        assert_eq!(clock.0.load(Ordering::Relaxed), 1);
        assert_eq!(stats.snapshot().suppressed, 2);
        trigger.close();
        task.abort();
    }

    #[tokio::test]
    async fn repeated_triggers_bound_follow_up_and_close_discards_work() {
        let dir = tempfile::tempdir().unwrap();
        let journals = BridgeLedger::open(dir.path()).unwrap();
        let exclusion = journals.exclusion();
        let delivery = journals.delivery();
        let readiness = ready();
        let (coordinator, _rx) = DeliveryCoordinator::new(
            &mappings(),
            "local",
            EgressStats::default(),
            delivery.clone(),
            readiness.clone(),
        );
        let (trigger, stats, task) = spawn_reconciler_with(
            Arc::new(FakeReader::default()),
            mappings(),
            exclusion,
            delivery,
            coordinator,
            readiness,
            FakeClock::default(),
        );
        trigger.trigger(ReconcileReason::StartupReady);
        for _ in 0..100 {
            trigger.trigger(ReconcileReason::EventLagged);
        }
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(stats.snapshot().scans <= 2);
        assert!(stats.snapshot().coalesced <= 1);
        trigger.close();
        let before = stats.snapshot().scans;
        trigger.trigger(ReconcileReason::ReconnectReady);
        tokio::task::yield_now().await;
        assert_eq!(stats.snapshot().scans, before);
        task.abort();
    }
}
