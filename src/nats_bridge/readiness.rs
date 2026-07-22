//! Internal, subscription-aware readiness state for the Core NATS bridge.
//!
//! This state intentionally does not participate in the public sidecar status
//! RPC. Phase 2 drives the establishment seam after each real subscribe and
//! flush; Phase 1 can therefore connect without falsely reporting ready.

use std::collections::BTreeSet;

use async_nats::Subject;
use tokio::sync::watch;

/// A watch-observable snapshot of bridge readiness inputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BridgeStatus {
    /// Whether this status belongs to a configured bridge runtime.
    pub enabled: bool,
    /// Whether the runtime still accepts new bridge work.
    pub accepting: bool,
    /// Whether remote-delivery suppression state is usable.
    pub delivery_healthy: bool,
    /// Whether local-authorship exclusion state is usable.
    pub exclusion_healthy: bool,
    /// Whether a Core NATS connection is currently active.
    pub connected: bool,
    /// Number of configured literal subjects that must be established.
    pub expected_subscriptions: usize,
    /// Distinct configured subjects established for this connection generation.
    pub established_subjects: BTreeSet<Subject>,
    configured_subjects: BTreeSet<Subject>,
}

impl BridgeStatus {
    /// True only when connected and every configured literal subject is established.
    pub fn is_ready(&self) -> bool {
        self.enabled
            && self.accepting
            && self.delivery_healthy
            && self.exclusion_healthy
            && self.connected
            && self.established_subjects == self.configured_subjects
    }
}

/// Readiness information returned from every state transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadinessTransition {
    pub was_ready: bool,
    pub is_ready: bool,
    pub changed: bool,
    status: BridgeStatus,
}

impl ReadinessTransition {
    /// True exactly once at each not-ready to fully-ready boundary.
    pub fn became_ready(&self) -> bool {
        !self.was_ready && self.is_ready
    }

    /// Snapshot captured atomically with this transition.
    pub fn status(&self) -> &BridgeStatus {
        &self.status
    }
}

/// Shared readiness state and watch publisher for one enabled bridge runtime.
#[derive(Clone)]
pub struct BridgeReadiness {
    tx: watch::Sender<BridgeStatus>,
}

impl BridgeReadiness {
    /// Create disconnected readiness state for the configured literal subjects.
    pub fn new(subjects: impl IntoIterator<Item = Subject>) -> Self {
        let configured_subjects: BTreeSet<_> = subjects.into_iter().collect();
        let status = BridgeStatus {
            enabled: true,
            accepting: true,
            delivery_healthy: true,
            exclusion_healthy: true,
            connected: false,
            expected_subscriptions: configured_subjects.len(),
            established_subjects: BTreeSet::new(),
            configured_subjects,
        };
        Self {
            tx: watch::channel(status).0,
        }
    }

    /// Current immutable snapshot.
    pub fn snapshot(&self) -> BridgeStatus {
        self.tx.borrow().clone()
    }

    /// Subscribe to future snapshots, beginning with the current value.
    pub fn subscribe(&self) -> watch::Receiver<BridgeStatus> {
        self.tx.subscribe()
    }

    /// Mark the broker connection active without implying subscriptions exist.
    pub fn set_connected(&self) -> ReadinessTransition {
        self.update(|status| status.connected = true)
    }

    /// Atomically mark the complete configured subscription batch established.
    pub fn mark_all_subscriptions_established(&self) -> ReadinessTransition {
        self.update(|status| {
            status.established_subjects = status.configured_subjects.clone();
        })
    }

    /// Invalidate the complete subscription generation without claiming disconnect.
    pub fn invalidate_subscription_generation(&self) -> ReadinessTransition {
        self.update(|status| status.established_subjects.clear())
    }

    /// Atomically clear connection and subscription-generation state.
    pub fn mark_disconnected(&self) -> ReadinessTransition {
        self.update(|status| {
            status.connected = false;
            status.established_subjects.clear();
        })
    }

    /// Change only delivery-journal health, preserving connection,
    /// subscriptions, and the independently owned exclusion writer.
    pub fn set_delivery_healthy(&self, healthy: bool) -> ReadinessTransition {
        self.update(|status| status.delivery_healthy = healthy)
    }

    /// Change only local-exclusion health.
    pub fn set_exclusion_healthy(&self, healthy: bool) -> ReadinessTransition {
        self.update(|status| status.exclusion_healthy = healthy)
    }

    /// Stop new admission immediately without falsifying any other fact.
    pub fn begin_shutdown(&self) -> ReadinessTransition {
        self.update(|status| status.accepting = false)
    }

    fn update(&self, mutate: impl FnOnce(&mut BridgeStatus)) -> ReadinessTransition {
        let mut transition = None;
        self.tx.send_modify(|status| {
            let before = status.clone();
            mutate(status);
            transition = Some(ReadinessTransition {
                was_ready: before.is_ready(),
                is_ready: status.is_ready(),
                changed: before != *status,
                status: status.clone(),
            });
        });
        transition.expect("watch mutation always captures a readiness transition")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject(value: &str) -> Subject {
        Subject::validated(value).expect("test subject should be valid")
    }

    fn readiness() -> BridgeReadiness {
        BridgeReadiness::new([subject("vision.summary"), subject("node.health")])
    }

    #[test]
    fn enabled_starts_disconnected_and_not_ready() {
        let status = readiness().snapshot();
        assert!(status.enabled);
        assert!(status.accepting);
        assert!(status.delivery_healthy);
        assert!(status.exclusion_healthy);
        assert!(!status.connected);
        assert_eq!(status.expected_subscriptions, 2);
        assert!(status.established_subjects.is_empty());
        assert!(!status.is_ready());
    }

    #[test]
    fn connection_with_missing_subjects_is_not_ready() {
        let readiness = readiness();
        let transition = readiness.set_connected();
        assert!(!transition.became_ready());
        assert!(!readiness.snapshot().is_ready());
    }

    #[test]
    fn establish_all_publishes_one_complete_ready_snapshot() {
        let readiness = readiness();
        let mut snapshots = readiness.subscribe();
        readiness.set_connected();
        let connected = snapshots.borrow_and_update().clone();
        assert!(connected.connected);
        assert_eq!(connected.established_subjects.len(), 0);
        assert!(!connected.is_ready());

        let transition = readiness.mark_all_subscriptions_established();
        assert!(transition.became_ready());
        assert!(snapshots.has_changed().expect("sender remains alive"));
        let established = snapshots.borrow_and_update().clone();
        assert_eq!(established.established_subjects.len(), 2);
        assert!(established.is_ready());
        assert!(!snapshots.has_changed().expect("sender remains alive"));

        let duplicate = readiness.mark_all_subscriptions_established();
        assert!(!duplicate.changed);
        assert!(!duplicate.became_ready());
    }

    #[test]
    fn disconnect_clears_connection_and_established_subjects_in_one_snapshot() {
        let readiness = readiness();
        let mut snapshots = readiness.subscribe();
        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        let transition = readiness.mark_disconnected();
        assert!(transition.changed);

        let status = snapshots.borrow_and_update().clone();
        assert!(!status.connected);
        assert!(status.established_subjects.is_empty());
        assert!(!status.is_ready());
    }

    #[test]
    fn generation_invalidation_preserves_connection_but_clears_readiness() {
        let readiness = readiness();
        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        assert!(readiness.snapshot().is_ready());

        let transition = readiness.invalidate_subscription_generation();
        assert!(transition.changed);
        assert!(transition.status().connected);
        assert!(transition.status().established_subjects.is_empty());
        assert!(!transition.status().is_ready());
    }

    #[test]
    fn reconnect_and_generation_restore_require_establish_all() {
        let readiness = readiness();
        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        assert!(readiness.snapshot().is_ready());

        readiness.mark_disconnected();
        readiness.set_connected();
        assert!(!readiness.snapshot().is_ready());
        assert!(readiness
            .mark_all_subscriptions_established()
            .became_ready());

        readiness.invalidate_subscription_generation();
        assert!(readiness.snapshot().connected);
        assert!(!readiness.snapshot().is_ready());
        assert!(readiness
            .mark_all_subscriptions_established()
            .became_ready());
    }

    #[test]
    fn independent_health_and_admission_inputs_clear_only_readiness() {
        let readiness = readiness();
        readiness.set_connected();
        readiness.mark_all_subscriptions_established();
        assert!(readiness.snapshot().is_ready());

        readiness.set_delivery_healthy(false);
        let delivery_bad = readiness.snapshot();
        assert!(!delivery_bad.is_ready());
        assert!(delivery_bad.connected);
        assert_eq!(delivery_bad.established_subjects.len(), 2);
        assert!(delivery_bad.exclusion_healthy);

        readiness.set_delivery_healthy(true);
        assert!(readiness.snapshot().is_ready());
        readiness.set_exclusion_healthy(false);
        assert!(!readiness.snapshot().is_ready());
        readiness.set_exclusion_healthy(true);
        assert!(readiness.snapshot().is_ready());

        readiness.begin_shutdown();
        let shutdown = readiness.snapshot();
        assert!(!shutdown.is_ready());
        assert!(!shutdown.accepting);
        assert!(shutdown.connected);
        assert!(shutdown.delivery_healthy);
        assert!(shutdown.exclusion_healthy);
    }
}
