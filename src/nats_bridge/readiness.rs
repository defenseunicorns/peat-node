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
        self.enabled && self.connected && self.established_subjects == self.configured_subjects
    }
}

/// Readiness information returned from every state transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadinessTransition {
    pub was_ready: bool,
    pub is_ready: bool,
    pub changed: bool,
}

impl ReadinessTransition {
    /// True exactly once at each not-ready to fully-ready boundary.
    pub fn became_ready(self) -> bool {
        !self.was_ready && self.is_ready
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

    /// Mark one configured subject established. Duplicate and unknown subjects are inert.
    pub fn mark_subscription_established(&self, subject: &Subject) -> ReadinessTransition {
        self.update(|status| {
            if status.configured_subjects.contains(subject) {
                status.established_subjects.insert(subject.clone());
            }
        })
    }

    /// Atomically clear connection and subscription-generation state.
    pub fn mark_disconnected(&self) -> ReadinessTransition {
        self.update(|status| {
            status.connected = false;
            status.established_subjects.clear();
        })
    }

    fn update(&self, mutate: impl FnOnce(&mut BridgeStatus)) -> ReadinessTransition {
        let before = self.snapshot();
        self.tx.send_modify(mutate);
        let after = self.snapshot();
        ReadinessTransition {
            was_ready: before.is_ready(),
            is_ready: after.is_ready(),
            changed: before != after,
        }
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
    fn final_distinct_configured_subject_becomes_ready_once() {
        let readiness = readiness();
        readiness.set_connected();
        let first = readiness.mark_subscription_established(&subject("vision.summary"));
        let final_subject = readiness.mark_subscription_established(&subject("node.health"));
        assert!(!first.became_ready());
        assert!(final_subject.became_ready());
        assert!(readiness.snapshot().is_ready());
        assert!(!readiness
            .mark_subscription_established(&subject("node.health"))
            .became_ready());
    }

    #[test]
    fn duplicate_establishment_is_idempotent() {
        let readiness = readiness();
        readiness.set_connected();
        let first = readiness.mark_subscription_established(&subject("vision.summary"));
        let duplicate = readiness.mark_subscription_established(&subject("vision.summary"));
        assert!(first.changed);
        assert!(!duplicate.changed);
        assert_eq!(readiness.snapshot().established_subjects.len(), 1);
        assert!(!duplicate.became_ready());
    }

    #[test]
    fn unknown_subject_cannot_satisfy_readiness() {
        let readiness = readiness();
        readiness.set_connected();
        let transition = readiness.mark_subscription_established(&subject("other.subject"));
        assert!(!transition.changed);
        assert!(!readiness.snapshot().is_ready());
    }

    #[test]
    fn disconnect_clears_connection_and_established_subjects_in_one_snapshot() {
        let readiness = readiness();
        let mut snapshots = readiness.subscribe();
        readiness.set_connected();
        readiness.mark_subscription_established(&subject("vision.summary"));
        let transition = readiness.mark_disconnected();
        assert!(transition.changed);

        let status = snapshots.borrow_and_update().clone();
        assert!(!status.connected);
        assert!(status.established_subjects.is_empty());
        assert!(!status.is_ready());
    }

    #[test]
    fn reconnect_requires_every_subject_to_be_reestablished() {
        let readiness = readiness();
        readiness.set_connected();
        readiness.mark_subscription_established(&subject("vision.summary"));
        readiness.mark_subscription_established(&subject("node.health"));
        assert!(readiness.snapshot().is_ready());

        readiness.mark_disconnected();
        readiness.set_connected();
        assert!(!readiness.snapshot().is_ready());
        readiness.mark_subscription_established(&subject("vision.summary"));
        assert!(!readiness.snapshot().is_ready());
        assert!(readiness
            .mark_subscription_established(&subject("node.health"))
            .became_ready());
    }
}
