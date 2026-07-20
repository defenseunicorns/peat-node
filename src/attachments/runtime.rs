//! Per-bundle runtime state for the subscribe-progress fan-out (PRD-006
//! Step 7b). Kept separate from [`BundleRegistry`] because it carries
//! proto-typed broadcast channels and mutable per-distribution state that
//! the cloned-on-read registry isn't shaped for.
//!
//! Lifecycle:
//!
//! - `register` is called by `handlers::send_attachments` right after
//!   `registry.insert`. It allocates the [`tokio::sync::broadcast`]
//!   channel and the per-distribution slot vector.
//! - A watcher task per distribution updates the runtime slot and broadcasts
//!   an `AttachmentProgress` frame on every change. Terminal transitions
//!   bump a counter so the subscribe handler knows when to close the stream.
//! - `unregister` drops the runtime entry (called from registry-eviction
//!   integration in a follow-up; for now bundles linger for the process
//!   lifetime, which matches the in-memory handle-table semantics PRD
//!   §Configuration documents).
//!
//! Subscribers attach via [`BundleRuntime::progress_tx`]; the broadcast
//! channel buffers a small backlog (`PROGRESS_BACKLOG`) — lagging
//! subscribers drop intermediate frames but never miss the final terminal
//! frame because the subscribe handler also emits a snapshot for every
//! distribution already terminal at attach time (the late-subscribe
//! contract from the proto doc-comments).
//!
//! [`BundleRegistry`]: crate::attachments::registry::BundleRegistry

#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::broadcast;

use crate::pb;

/// Broadcast backlog per bundle. Small — progress updates are bursty during
/// transfer but a slow subscriber catching up out-of-band is a degraded
/// path. The handler tolerates lag by dropping intermediate frames; the
/// final terminal snapshot in the handler covers the missed completion.
pub const PROGRESS_BACKLOG: usize = 64;

/// Per-distribution state tracked by the runtime. Parallels
/// `BundleRecord.handles` 1:1 by index.
#[derive(Clone, Debug)]
pub struct PerDistributionProgress {
    pub state: DistributionState,
    pub bytes_transferred: u64,
    pub bytes_total: u64,
    pub error: Option<String>,
    /// The last `AttachmentProgress` emitted for this distribution.
    /// Late-attaching subscribers see this verbatim as the snapshot frame
    /// (only when `state.is_terminal()`).
    pub last_progress: pb::AttachmentProgress,
}

/// State machine for a single distribution. Maps onto the proto's
/// `DistributionStatus` enum but kept local so the runtime layer doesn't
/// have to thread proto identifiers through every comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistributionState {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl DistributionState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Runtime state for one bundle. Lives behind an `Arc` so the watcher
/// tasks and the subscribe handler share it without cloning the inner
/// channel.
pub struct BundleRuntime {
    /// Live progress fan-out. Subscribers attach via `subscribe()` on the
    /// underlying `Sender`. Closed when the last `Sender` clone (held by
    /// watchers + this runtime entry) is dropped.
    progress_tx: broadcast::Sender<pb::AttachmentProgress>,
    /// Per-distribution state, parallel to `BundleRecord.handles`. Behind
    /// a `Mutex` so watcher tasks can update without taking the parent
    /// registry's lock.
    per_distribution: Mutex<Vec<PerDistributionProgress>>,
    /// Count of distributions that have reached a terminal state. The
    /// subscribe handler closes the stream when this equals
    /// `total_distributions`.
    terminal_count: AtomicUsize,
    total_distributions: usize,
}

impl BundleRuntime {
    /// Number of distributions tracked by this bundle.
    pub fn total(&self) -> usize {
        self.total_distributions
    }

    /// How many distributions have already reached a terminal state.
    pub fn terminal_count(&self) -> usize {
        self.terminal_count.load(Ordering::Acquire)
    }

    /// True once every distribution has terminated. The subscribe handler
    /// uses this to decide when to close the gRPC stream.
    pub fn all_terminal(&self) -> bool {
        self.terminal_count() >= self.total_distributions
    }

    /// Subscribe to the live progress fan-out. Returns a fresh receiver
    /// that will deliver every `AttachmentProgress` event broadcast after
    /// this call. Events broadcast *before* this call are not replayed —
    /// the subscribe handler covers them via a snapshot from
    /// `per_distribution_snapshot`.
    pub fn subscribe(&self) -> broadcast::Receiver<pb::AttachmentProgress> {
        self.progress_tx.subscribe()
    }

    /// Snapshot the current per-distribution state. Used by the subscribe
    /// handler to build the late-subscribe terminal-snapshot phase.
    pub fn per_distribution_snapshot(&self) -> Vec<PerDistributionProgress> {
        self.per_distribution
            .lock()
            .expect("per_distribution Mutex poisoned (no panic holding it)")
            .clone()
    }

    /// Apply a progress update from a watcher task. Updates the
    /// per-distribution slot, bumps the terminal counter on a fresh
    /// terminal transition, and broadcasts the frame to live subscribers.
    /// Returns `true` if this update represented a fresh terminal
    /// transition (caller may want to update the registry's
    /// `BundleStatus`).
    pub fn apply_progress(
        &self,
        file_index: usize,
        new_state: DistributionState,
        progress: pb::AttachmentProgress,
    ) -> bool {
        let became_terminal = {
            let mut slots = self
                .per_distribution
                .lock()
                .expect("per_distribution Mutex poisoned");
            let slot = match slots.get_mut(file_index) {
                Some(s) => s,
                None => return false,
            };
            // Terminal is final. Once a distribution reaches
            // Completed / Failed / Cancelled it is not transitioned
            // again — a later observation never overwrites the first
            // terminal, and no second frame is broadcast. This keeps
            // an explicit Cancel observable as CANCELLED even though
            // peat-protocol's `cancel()` represents the per-node
            // transfers as `Failed("Distribution cancelled")`: the
            // per-distribution watcher observes that broadcast and
            // would otherwise `apply_progress(Failed)` over the
            // cancel handler's `apply_progress(Cancelled)`, relabeling
            // the bundle FAILED (surfaced by PRD §Testing Plan
            // test 24). Also prevents a late COMPLETED from
            // un-FAILing a distribution, etc.
            if slot.state.is_terminal() {
                return false;
            }
            // Reaching here, the slot was non-terminal, so this is a
            // fresh terminal transition iff the new state is terminal.
            slot.state = new_state;
            slot.bytes_transferred = progress.bytes_transferred;
            slot.bytes_total = progress.bytes_total;
            slot.error = progress.error.clone();
            slot.last_progress = progress.clone();
            new_state.is_terminal()
        };
        if became_terminal {
            self.terminal_count.fetch_add(1, Ordering::AcqRel);
        }
        // Broadcast is best-effort — if there are no subscribers,
        // `send` returns Err but the runtime state is already updated.
        let _ = self.progress_tx.send(progress);
        became_terminal
    }

    /// Record an accepted API cancellation.
    ///
    /// The protocol substrate represents cancellation as a failed per-node
    /// transfer and may publish that frame before `cancel()` returns.  Unlike
    /// ordinary progress updates, an accepted cancellation therefore owns a
    /// concurrent `Failed` terminal state and relabels it as `Cancelled`.
    /// The terminal counter is only incremented when the prior state was
    /// non-terminal.
    pub fn apply_cancellation(&self, file_index: usize, progress: pb::AttachmentProgress) -> bool {
        let became_terminal = {
            let mut slots = self
                .per_distribution
                .lock()
                .expect("per_distribution Mutex poisoned");
            let slot = match slots.get_mut(file_index) {
                Some(s) => s,
                None => return false,
            };
            if matches!(
                slot.state,
                DistributionState::Completed | DistributionState::Cancelled
            ) {
                return false;
            }
            let became_terminal = !slot.state.is_terminal();
            slot.state = DistributionState::Cancelled;
            slot.bytes_transferred = progress.bytes_transferred;
            slot.bytes_total = progress.bytes_total;
            slot.error = progress.error.clone();
            slot.last_progress = progress.clone();
            became_terminal
        };
        if became_terminal {
            self.terminal_count.fetch_add(1, Ordering::AcqRel);
        }
        let _ = self.progress_tx.send(progress);
        became_terminal
    }
}

/// Process-wide store of per-bundle runtime state. Keyed by `bundle_id`.
pub struct BundleRuntimeStore {
    inner: RwLock<HashMap<String, Arc<BundleRuntime>>>,
}

impl BundleRuntimeStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Allocate a fresh runtime entry for a new bundle. Each slot starts
    /// in `Pending` with a default `AttachmentProgress` carrying the
    /// distribution_id and blob_token so an immediate snapshot before
    /// any update fires still has correlated identifiers.
    pub fn register(
        &self,
        bundle_id: &str,
        slots: Vec<PerDistributionProgress>,
    ) -> Arc<BundleRuntime> {
        let total = slots.len();
        let runtime = Arc::new(BundleRuntime {
            progress_tx: broadcast::channel(PROGRESS_BACKLOG).0,
            per_distribution: Mutex::new(slots),
            terminal_count: AtomicUsize::new(0),
            total_distributions: total,
        });
        let mut inner = self
            .inner
            .write()
            .expect("BundleRuntimeStore RwLock poisoned");
        // Replace any prior runtime — caller has already cleared the
        // registry's bundle record (terminal-reuse or fresh ingest).
        inner.insert(bundle_id.to_string(), runtime.clone());
        runtime
    }

    pub fn get(&self, bundle_id: &str) -> Option<Arc<BundleRuntime>> {
        let inner = self
            .inner
            .read()
            .expect("BundleRuntimeStore RwLock poisoned");
        inner.get(bundle_id).cloned()
    }

    /// Drop a bundle's runtime entry. The `Arc` may still be live in
    /// watcher tasks and subscribers — `apply_progress` and the broadcast
    /// channel continue to function. Eventual cleanup happens when those
    /// references drop.
    pub fn unregister(&self, bundle_id: &str) {
        let mut inner = self
            .inner
            .write()
            .expect("BundleRuntimeStore RwLock poisoned");
        inner.remove(bundle_id);
    }
}

impl Default for BundleRuntimeStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(distribution_id: &str, bytes_transferred: u64) -> pb::AttachmentProgress {
        pb::AttachmentProgress {
            distribution_id: distribution_id.to_string(),
            blob_token: format!("tok-{distribution_id}"),
            bytes_transferred,
            bytes_total: 100,
            ..Default::default()
        }
    }

    fn slot(distribution_id: &str) -> PerDistributionProgress {
        PerDistributionProgress {
            state: DistributionState::Pending,
            bytes_transferred: 0,
            bytes_total: 100,
            error: None,
            last_progress: frame(distribution_id, 0),
        }
    }

    #[test]
    fn register_creates_runtime_with_pending_slots() {
        let store = BundleRuntimeStore::new();
        let rt = store.register("B", vec![slot("d-a"), slot("d-b")]);
        assert_eq!(rt.total(), 2);
        assert_eq!(rt.terminal_count(), 0);
        assert!(!rt.all_terminal());
        let snap = rt.per_distribution_snapshot();
        assert_eq!(snap.len(), 2);
        assert!(matches!(snap[0].state, DistributionState::Pending));
    }

    #[tokio::test]
    async fn apply_progress_bumps_terminal_count_and_broadcasts() {
        let store = BundleRuntimeStore::new();
        let rt = store.register("B", vec![slot("d-a")]);
        let mut rx = rt.subscribe();

        let became_terminal = rt.apply_progress(0, DistributionState::Completed, frame("d-a", 100));
        assert!(became_terminal);
        assert_eq!(rt.terminal_count(), 1);
        assert!(rt.all_terminal());

        let event = rx
            .recv()
            .await
            .expect("broadcast must deliver to live subscriber");
        assert_eq!(event.distribution_id, "d-a");
        assert_eq!(event.bytes_transferred, 100);
    }

    #[tokio::test]
    async fn second_terminal_transition_does_not_double_count() {
        let store = BundleRuntimeStore::new();
        let rt = store.register("B", vec![slot("d-a")]);
        rt.apply_progress(0, DistributionState::Completed, frame("d-a", 100));
        // A spurious second Completed (e.g., from a late progress event)
        // must not bump the terminal counter — that would falsely signal
        // "all terminal" when there are still other distributions in flight.
        let again = rt.apply_progress(0, DistributionState::Completed, frame("d-a", 100));
        assert!(!again);
        assert_eq!(rt.terminal_count(), 1);
    }

    #[tokio::test]
    async fn accepted_cancellation_relabels_substrate_failure_without_double_counting() {
        let store = BundleRuntimeStore::new();
        let rt = store.register("B", vec![slot("d-a")]);
        rt.apply_progress(0, DistributionState::Failed, frame("d-a", 20));

        let mut cancelled = frame("d-a", 20);
        cancelled.status =
            buffa::EnumValue::from(pb::DistributionStatus::DISTRIBUTION_STATUS_CANCELLED as i32);
        let became_terminal = rt.apply_cancellation(0, cancelled);

        assert!(!became_terminal);
        assert_eq!(rt.terminal_count(), 1);
        let snapshot = rt.per_distribution_snapshot();
        assert!(matches!(snapshot[0].state, DistributionState::Cancelled));
        assert_eq!(
            snapshot[0].last_progress.status.as_known(),
            Some(pb::DistributionStatus::DISTRIBUTION_STATUS_CANCELLED)
        );
    }

    #[tokio::test]
    async fn snapshot_reflects_terminal_state_for_late_subscribers() {
        let store = BundleRuntimeStore::new();
        let rt = store.register("B", vec![slot("d-a"), slot("d-b")]);
        rt.apply_progress(0, DistributionState::Completed, frame("d-a", 100));
        // d-b still pending.
        let snap = rt.per_distribution_snapshot();
        assert!(snap[0].state.is_terminal());
        assert!(!snap[1].state.is_terminal());
    }

    #[test]
    fn unregister_removes_entry() {
        let store = BundleRuntimeStore::new();
        store.register("B", vec![slot("d-a")]);
        assert!(store.get("B").is_some());
        store.unregister("B");
        assert!(store.get("B").is_none());
    }
}
