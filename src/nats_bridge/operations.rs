//! Fixed-cardinality operational observations for the native NATS bridge.
//!
//! This module deliberately exposes only typed state, booleans, and cumulative
//! integers. Subjects, document identities, peers, payloads, broker URLs, and
//! source error strings never cross this boundary.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{info, warn};

use super::egress::{EgressStats, EgressStatsSnapshot};
use super::ingress::{IngressStats, IngressStatsSnapshot};
use super::readiness::{BridgeReadiness, BridgeStatus};
use super::reconcile::{ReconcileSnapshot, ReconcileStats};
use super::runtime::{BridgeShutdownFailure, BridgeShutdownReport, LifecycleSnapshot};

/// Cadence for enabled-bridge cumulative operational summaries.
pub const OPERATIONS_SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct Outcomes {
    shutdown_clean: AtomicU64,
    shutdown_failure: AtomicU64,
    shutdown_timeout: AtomicU64,
    shutdown_aborted_tasks: AtomicU64,
    ledger_io_unjoined: AtomicU64,
}

/// Cloneable owner of every fixed-cardinality process-lifetime observation.
#[derive(Clone)]
pub struct BridgeOperations {
    ingress: IngressStats,
    egress: EgressStats,
    reconcile: ReconcileStats,
    readiness: BridgeReadiness,
    outcomes: Arc<Outcomes>,
}

/// Complete label-free cumulative bridge snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BridgeOperationsSnapshot {
    pub enabled: bool,
    pub accepting: bool,
    pub ready: bool,
    pub connected: bool,
    pub delivery_healthy: bool,
    pub exclusion_healthy: bool,
    pub received: u64,
    pub stored: u64,
    pub invalid_input: u64,
    pub self_suppressed: u64,
    pub store_failures: u64,
    pub slow_consumer: u64,
    pub remote_published: u64,
    pub publish_failures: u64,
    pub reconnects: u64,
    pub event_lagged: u64,
    pub queue_loss: u64,
    pub ledger_failures: u64,
    pub delivery_reservations: u64,
    pub delivery_completions: u64,
    pub delivery_reservations_uncertain: u64,
    pub reconciliation_triggers: u64,
    pub reconciliation_coalesced: u64,
    pub reconciliation_scans: u64,
    pub reconciliation_hydrated: u64,
    pub reconciliation_suppressed: u64,
    pub reconciliation_failures: u64,
    pub shutdown_clean: u64,
    pub shutdown_failure: u64,
    pub shutdown_timeout: u64,
    pub shutdown_aborted_tasks: u64,
    pub ledger_io_unjoined: u64,
}

impl BridgeOperations {
    pub(crate) fn new(
        ingress: IngressStats,
        egress: EgressStats,
        reconcile: ReconcileStats,
        readiness: BridgeReadiness,
    ) -> Self {
        Self {
            ingress,
            egress,
            reconcile,
            readiness,
            outcomes: Arc::new(Outcomes::default()),
        }
    }

    /// Capture one immutable view. Every counter is monotonic for this process.
    pub fn snapshot(&self, lifecycle: LifecycleSnapshot) -> BridgeOperationsSnapshot {
        let ingress = self.ingress.snapshot();
        let egress = self.egress.snapshot();
        let reconcile = self.reconcile.snapshot();
        let readiness = self.readiness.snapshot();
        aggregate(
            ingress,
            egress,
            reconcile,
            readiness,
            lifecycle,
            &self.outcomes,
        )
    }

    pub(crate) fn record_shutdown(&self, report: &BridgeShutdownReport) {
        if report.is_clean() {
            self.outcomes.shutdown_clean.fetch_add(1, Ordering::Relaxed);
        } else {
            self.outcomes
                .shutdown_failure
                .fetch_add(1, Ordering::Relaxed);
        }
        self.outcomes
            .shutdown_aborted_tasks
            .fetch_add(report.aborted_tasks as u64, Ordering::Relaxed);
        match report.failure {
            Some(BridgeShutdownFailure::DeadlineExceeded) => {
                self.outcomes
                    .shutdown_timeout
                    .fetch_add(1, Ordering::Relaxed);
            }
            Some(BridgeShutdownFailure::LedgerIoUnjoined(_)) => {
                self.outcomes
                    .ledger_io_unjoined
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub(crate) fn emit_transition(&self, lifecycle: LifecycleSnapshot, reason: &'static str) {
        emit(self.snapshot(lifecycle), reason, false);
    }

    pub(crate) fn emit_final(&self, lifecycle: LifecycleSnapshot) {
        emit(self.snapshot(lifecycle), "final", true);
    }
}

fn aggregate(
    ingress: IngressStatsSnapshot,
    egress: EgressStatsSnapshot,
    reconcile: ReconcileSnapshot,
    readiness: BridgeStatus,
    lifecycle: LifecycleSnapshot,
    outcomes: &Outcomes,
) -> BridgeOperationsSnapshot {
    BridgeOperationsSnapshot {
        enabled: readiness.enabled,
        accepting: readiness.accepting,
        ready: readiness.is_ready(),
        connected: readiness.connected,
        delivery_healthy: readiness.delivery_healthy,
        exclusion_healthy: readiness.exclusion_healthy,
        received: ingress.received,
        stored: ingress.stored,
        invalid_input: ingress.invalid_utf8.saturating_add(ingress.invalid_json),
        self_suppressed: ingress.self_suppressed,
        store_failures: ingress.final_store_failures,
        slow_consumer: ingress.slow_consumer_events,
        remote_published: egress.published,
        publish_failures: egress
            .publish_failed
            .saturating_add(egress.max_payload_exceeded)
            .saturating_add(egress.flush_failed),
        reconnects: lifecycle.connection_epoch.saturating_sub(1),
        event_lagged: egress.event_lagged,
        queue_loss: egress.queue_full.saturating_add(egress.queue_closed),
        ledger_failures: egress.ledger_unavailable,
        delivery_reservations: egress.reserved,
        delivery_completions: egress.completed,
        // Reserve-first at-most-once semantics make every non-completed
        // reservation uncertain, including queue and publication failures.
        delivery_reservations_uncertain: egress.reserved.saturating_sub(egress.completed),
        reconciliation_triggers: reconcile.triggers,
        reconciliation_coalesced: reconcile.coalesced,
        reconciliation_scans: reconcile.scans,
        reconciliation_hydrated: reconcile.hydrated,
        reconciliation_suppressed: reconcile.suppressed,
        reconciliation_failures: reconcile.failures,
        shutdown_clean: outcomes.shutdown_clean.load(Ordering::Relaxed),
        shutdown_failure: outcomes.shutdown_failure.load(Ordering::Relaxed),
        shutdown_timeout: outcomes.shutdown_timeout.load(Ordering::Relaxed),
        shutdown_aborted_tasks: outcomes.shutdown_aborted_tasks.load(Ordering::Relaxed),
        ledger_io_unjoined: outcomes.ledger_io_unjoined.load(Ordering::Relaxed),
    }
}

fn emit(snapshot: BridgeOperationsSnapshot, reason: &'static str, final_snapshot: bool) {
    if snapshot.ready && snapshot.delivery_healthy && snapshot.exclusion_healthy {
        info!(
            reason,
            final_snapshot,
            bridge_ready = snapshot.ready,
            accepting = snapshot.accepting,
            connected = snapshot.connected,
            delivery_healthy = snapshot.delivery_healthy,
            exclusion_healthy = snapshot.exclusion_healthy,
            received = snapshot.received,
            stored = snapshot.stored,
            invalid_input = snapshot.invalid_input,
            self_suppressed = snapshot.self_suppressed,
            store_failures = snapshot.store_failures,
            slow_consumer = snapshot.slow_consumer,
            remote_published = snapshot.remote_published,
            publish_failures = snapshot.publish_failures,
            reconnects = snapshot.reconnects,
            event_lagged = snapshot.event_lagged,
            queue_loss = snapshot.queue_loss,
            ledger_failures = snapshot.ledger_failures,
            delivery_reservations = snapshot.delivery_reservations,
            delivery_completions = snapshot.delivery_completions,
            delivery_reservations_uncertain = snapshot.delivery_reservations_uncertain,
            reconciliation_scans = snapshot.reconciliation_scans,
            shutdown_clean = snapshot.shutdown_clean,
            shutdown_failure = snapshot.shutdown_failure,
            ledger_io_unjoined = snapshot.ledger_io_unjoined,
            "NATS bridge operations"
        );
    } else {
        warn!(
            reason,
            final_snapshot,
            bridge_ready = snapshot.ready,
            accepting = snapshot.accepting,
            connected = snapshot.connected,
            delivery_healthy = snapshot.delivery_healthy,
            exclusion_healthy = snapshot.exclusion_healthy,
            received = snapshot.received,
            stored = snapshot.stored,
            invalid_input = snapshot.invalid_input,
            self_suppressed = snapshot.self_suppressed,
            store_failures = snapshot.store_failures,
            slow_consumer = snapshot.slow_consumer,
            remote_published = snapshot.remote_published,
            publish_failures = snapshot.publish_failures,
            reconnects = snapshot.reconnects,
            event_lagged = snapshot.event_lagged,
            queue_loss = snapshot.queue_loss,
            ledger_failures = snapshot.ledger_failures,
            delivery_reservations = snapshot.delivery_reservations,
            delivery_completions = snapshot.delivery_completions,
            delivery_reservations_uncertain = snapshot.delivery_reservations_uncertain,
            reconciliation_scans = snapshot.reconciliation_scans,
            shutdown_clean = snapshot.shutdown_clean,
            shutdown_failure = snapshot.shutdown_failure,
            ledger_io_unjoined = snapshot.ledger_io_unjoined,
            "NATS bridge operations"
        );
    }
}

pub(crate) fn spawn_periodic_operations(
    operations: BridgeOperations,
    mut lifecycle: watch::Receiver<LifecycleSnapshot>,
    mut shutdown: watch::Receiver<Option<Instant>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval_at(
            Instant::now() + OPERATIONS_SUMMARY_INTERVAL,
            OPERATIONS_SUMMARY_INTERVAL,
        );
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => operations.emit_transition(*lifecycle.borrow(), "periodic"),
                changed = lifecycle.changed() => {
                    if changed.is_err() { return; }
                    operations.emit_transition(*lifecycle.borrow_and_update(), "lifecycle");
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || shutdown.borrow_and_update().is_some() { return; }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::Subject;

    #[tokio::test(start_paused = true)]
    async fn operations_are_fixed_monotonic_and_periodic_skips_missed_ticks() {
        let readiness = BridgeReadiness::new([Subject::from("fixed.subject")]);
        let operations = BridgeOperations::new(
            IngressStats::default(),
            EgressStats::default(),
            ReconcileStats::default(),
            readiness,
        );
        let (lifecycle_tx, lifecycle_rx) = watch::channel(LifecycleSnapshot::default());
        let (shutdown_tx, shutdown_rx) = watch::channel(None);
        let task = spawn_periodic_operations(operations.clone(), lifecycle_rx, shutdown_rx);
        tokio::time::advance(Duration::from_secs(180)).await;
        tokio::task::yield_now().await;
        let before = operations.snapshot(*lifecycle_tx.borrow());
        assert_eq!(before, operations.snapshot(*lifecycle_tx.borrow()));
        shutdown_tx.send(Some(Instant::now())).unwrap();
        task.await.unwrap();
    }

    #[test]
    fn shutdown_residual_can_never_report_clean() {
        let readiness = BridgeReadiness::new([Subject::from("fixed.subject")]);
        let operations = BridgeOperations::new(
            IngressStats::default(),
            EgressStats::default(),
            ReconcileStats::default(),
            readiness,
        );
        operations.record_shutdown(&BridgeShutdownReport {
            phase: super::super::runtime::BridgeShutdownPhase::Aborting,
            stage: super::super::runtime::BridgeShutdownStage::LedgerDelivery,
            joined_tasks: 2,
            aborted_tasks: 1,
            failure: Some(BridgeShutdownFailure::LedgerIoUnjoined(
                super::super::runtime::LedgerWorkerKind::Delivery,
            )),
        });
        let snapshot = operations.snapshot(LifecycleSnapshot::default());
        assert_eq!(snapshot.shutdown_clean, 0);
        assert_eq!(snapshot.shutdown_failure, 1);
        assert_eq!(snapshot.ledger_io_unjoined, 1);
    }
}
