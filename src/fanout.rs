//! QoS-priority relay fanout queue (peat-node#138; mirrors peat-mesh#247 /
//! ADR-0013 on the relay side).
//!
//! `SidecarNode::sync_on_change` is a relay: it reacts to every document
//! change — local writes *and* sync-received writes (transitive gossip via
//! the origin-tagged channel) — by fanning the document out to peers. Driven
//! inline, that loop awaits each document's full fanout before pulling the
//! next change, so a latency-sensitive document queued behind a backlog of
//! lower-priority ones is head-of-line-blocked (the peat-mesh#247 symptom, on
//! the relay).
//!
//! This queue is the relay-side analog of peat-mesh's coordinator fanout
//! queue. The listener enqueues `(doc_key, FanoutKind)` non-blockingly; a
//! single worker drains it **highest-QoS-first** and performs the actual
//! fanout via the released peat-mesh coordinator API. Single worker preserves
//! the per-(peer, channel) ordering + backpressure the inline loop gave.
//!
//! Why a peat-node-local queue rather than peat-mesh's `enqueue_fanout`:
//! peat-mesh's queue fans every document to *all* peers, but the relay must
//! exclude the source peer of a remote-origin change (echo suppression — the
//! peat-mesh#239 gossip-amplification guard). [`FanoutKind`] carries that
//! per-change distinction, which a doc-key-only queue cannot.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use peat_mesh::qos::QoSClass;
use peat_mesh::storage::{AutomergeSyncCoordinator, MeshSyncTransport, SyncTransport};
use tokio::sync::Notify;
use tracing::warn;

/// Which peers a queued change fans out to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutKind {
    /// Local write → fan to every connected peer.
    AllPeers,
    /// Remote write whose source peer is `EndpointId::to_string()` == this
    /// string → fan to every connected peer **except** the source, so we
    /// don't echo the change straight back to whoever sent it.
    ExcludeSource(String),
}

impl FanoutKind {
    /// Merge the fanout intent when a document is re-enqueued before it has
    /// drained (coalescing). `AllPeers` is the superset and always wins — a
    /// local write must reach everyone, including a peer that earlier sent us
    /// a remote-origin change. Two remote changes from *different* sources
    /// also widen to `AllPeers` (sending to either source is a no-op via
    /// per-peer sync state, so it's safe and simpler than tracking a set).
    fn merge(self, other: FanoutKind) -> FanoutKind {
        match (self, other) {
            (FanoutKind::AllPeers, _) | (_, FanoutKind::AllPeers) => FanoutKind::AllPeers,
            (FanoutKind::ExcludeSource(a), FanoutKind::ExcludeSource(b)) => {
                if a == b {
                    FanoutKind::ExcludeSource(a)
                } else {
                    FanoutKind::AllPeers
                }
            }
        }
    }
}

/// Inner state guarded by [`PriorityFanout::inner`].
#[derive(Default)]
struct Inner {
    /// Pending keys ordered `(qos_rank ASC, seq ASC)`: `first()` is the
    /// highest-priority, earliest-enqueued entry (what the worker pops);
    /// `last()` is the lowest-priority, latest (the eviction victim).
    /// `qos_rank = QoSClass as u8` — `Critical = 1` (highest) … `Bulk = 5`.
    pending: BTreeSet<(u8, u64, String)>,
    /// `doc_key -> (qos_rank, seq, kind)` for O(log n) coalescing/removal and
    /// to recover the fanout kind when the worker pops a key.
    entries: HashMap<String, (u8, u64, FanoutKind)>,
    /// Monotonic enqueue sequence — FIFO tiebreak within a QoS class.
    next_seq: u64,
    /// Per-QoS-class shed counters (index = `QoSClass as u8`; slot 0 unused).
    dropped_by_class: [u64; 6],
}

/// QoS-priority relay fanout queue + its drain-worker wakeup.
pub struct PriorityFanout {
    inner: Mutex<Inner>,
    notify: Notify,
    /// Set by [`Self::close`] so the worker drains what remains and exits
    /// (ties the worker's lifetime to the listener / change broadcast).
    closed: AtomicBool,
}

impl PriorityFanout {
    /// Absolute bound on pending entries. Coalescing collapses same-document
    /// churn, so this tracks distinct in-flight documents, not change rate.
    /// Matches peat-mesh's `MAX_PENDING_FANOUT`.
    const MAX_PENDING: usize = 4096;

    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    /// Signal the worker to drain any remaining entries and exit. Called by
    /// the listener when the change broadcast closes (node shutdown).
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.notify.notify_one();
    }

    fn qos_rank(doc_key: &str) -> u8 {
        let collection = doc_key.split(':').next().unwrap_or("");
        QoSClass::for_collection(collection) as u8
    }

    /// Enqueue a document for prioritized fanout. Non-blocking and infallible:
    /// it never blocks or errors the listener.
    ///
    /// - **Coalescing:** a `doc_key` already pending merges its [`FanoutKind`]
    ///   ([`FanoutKind::merge`]) and keeps its queue position — one entry
    ///   covers any number of intervening changes (the worker fans the current
    ///   store state when it reaches it).
    /// - **Bounded:** at [`Self::MAX_PENDING`] the lowest-priority pending
    ///   entry is evicted (possibly this one), counted per class. An evicted
    ///   fanout is recovered by the next change / periodic re-sync.
    pub fn enqueue(&self, doc_key: &str, kind: FanoutKind) {
        let rank = Self::qos_rank(doc_key);
        {
            let mut q = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = q.entries.get_mut(doc_key) {
                // Coalesce: keep position (rank, seq), widen the kind.
                let merged = entry.2.clone().merge(kind);
                entry.2 = merged;
                return;
            }
            let seq = q.next_seq;
            q.next_seq += 1;
            q.pending.insert((rank, seq, doc_key.to_string()));
            q.entries.insert(doc_key.to_string(), (rank, seq, kind));

            while q.pending.len() > Self::MAX_PENDING {
                let Some(victim) = q.pending.iter().next_back().cloned() else {
                    break;
                };
                q.pending.remove(&victim);
                q.entries.remove(&victim.2);
                if let Some(slot) = q.dropped_by_class.get_mut(victim.0 as usize) {
                    *slot += 1;
                }
                warn!(
                    doc_key = victim.2,
                    qos_rank = victim.0,
                    max = Self::MAX_PENDING,
                    "relay fanout queue full; shed lowest-priority entry (peat-node#138)"
                );
            }
        }
        self.notify.notify_one();
    }

    /// Pop the highest-priority pending `(doc_key, kind)`, or `None` if empty.
    fn pop(&self) -> Option<(String, FanoutKind)> {
        let mut q = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let key = q.pending.iter().next().cloned()?;
        q.pending.remove(&key);
        let (_, _, kind) = q.entries.remove(&key.2)?;
        Some((key.2, kind))
    }

    /// Per-QoS-class counts of entries shed under the bound (index =
    /// `QoSClass as u8`; slot 0 unused). Observability for the drop policy.
    pub fn dropped_by_class(&self) -> [u64; 6] {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .dropped_by_class
    }

    /// Drain loop: pop the highest-priority document and fan it out, parking on
    /// the notify when empty. Single worker — preserves per-(peer, channel)
    /// ordering + backpressure. Runs until the task is aborted. The queue lock
    /// is never held across an `.await`.
    pub async fn run(
        self: Arc<Self>,
        coordinator: Arc<AutomergeSyncCoordinator>,
        transport: Arc<MeshSyncTransport>,
    ) {
        loop {
            match self.pop() {
                Some((key, FanoutKind::AllPeers)) => {
                    if let Err(e) = coordinator.sync_document_with_all_peers(&key).await {
                        warn!(doc_key = %key, "sync to peers failed: {e}");
                    }
                }
                Some((key, FanoutKind::ExcludeSource(source))) => {
                    for peer in transport.connected_peers() {
                        if peer.to_string() == source {
                            continue;
                        }
                        if let Err(e) = coordinator.sync_document_with_peer(&key, peer).await {
                            warn!(doc_key = %key, %peer, "fanout sync failed: {e}");
                        }
                    }
                }
                None => {
                    // Drained: exit if closed, otherwise park for the next
                    // enqueue. `notify_one`'s stored permit covers a close/
                    // enqueue that races this empty check.
                    if self.closed.load(Ordering::Relaxed) {
                        break;
                    }
                    self.notify.notified().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `sitreps`→Bulk (default), `alerts`→Critical, `nodes`→High,
    // `beacons`→Normal (peat_mesh::qos::QoSClass::for_collection).

    #[test]
    fn coalesces_duplicate_doc_keys() {
        let q = PriorityFanout::new();
        q.enqueue("alerts:a", FanoutKind::AllPeers);
        q.enqueue("alerts:a", FanoutKind::AllPeers);
        q.enqueue("alerts:a", FanoutKind::AllPeers);
        let inner = q.inner.lock().unwrap();
        assert_eq!(inner.pending.len(), 1, "duplicate doc_keys must coalesce");
        assert_eq!(
            inner.entries.len(),
            1,
            "entries stays consistent with pending"
        );
    }

    #[test]
    fn coalesce_widens_kind_to_all_peers() {
        let q = PriorityFanout::new();
        // Remote-from-X then a local write to the same doc → must widen to
        // AllPeers so the local write also reaches X.
        q.enqueue("alerts:a", FanoutKind::ExcludeSource("X".into()));
        q.enqueue("alerts:a", FanoutKind::AllPeers);
        assert_eq!(q.pop(), Some(("alerts:a".into(), FanoutKind::AllPeers)));

        // Two different remote sources also widen to AllPeers.
        let q = PriorityFanout::new();
        q.enqueue("alerts:b", FanoutKind::ExcludeSource("X".into()));
        q.enqueue("alerts:b", FanoutKind::ExcludeSource("Y".into()));
        assert_eq!(q.pop(), Some(("alerts:b".into(), FanoutKind::AllPeers)));

        // Same source stays excluded.
        let q = PriorityFanout::new();
        q.enqueue("alerts:c", FanoutKind::ExcludeSource("X".into()));
        q.enqueue("alerts:c", FanoutKind::ExcludeSource("X".into()));
        assert_eq!(
            q.pop(),
            Some(("alerts:c".into(), FanoutKind::ExcludeSource("X".into())))
        );
    }

    #[test]
    fn pops_highest_qos_first() {
        let q = PriorityFanout::new();
        q.enqueue("sitreps:b", FanoutKind::AllPeers); // Bulk
        q.enqueue("alerts:a", FanoutKind::AllPeers); // Critical
        q.enqueue("beacons:n", FanoutKind::AllPeers); // Normal
                                                      // Critical first, then Normal, then Bulk — regardless of enqueue order.
        assert_eq!(q.pop().unwrap().0, "alerts:a");
        assert_eq!(q.pop().unwrap().0, "beacons:n");
        assert_eq!(q.pop().unwrap().0, "sitreps:b");
        assert_eq!(q.pop(), None);
    }

    #[test]
    fn fifo_within_a_qos_class() {
        let q = PriorityFanout::new();
        q.enqueue("sitreps:b0", FanoutKind::AllPeers);
        q.enqueue("sitreps:b1", FanoutKind::AllPeers);
        q.enqueue("sitreps:b2", FanoutKind::AllPeers);
        assert_eq!(q.pop().unwrap().0, "sitreps:b0");
        assert_eq!(q.pop().unwrap().0, "sitreps:b1");
        assert_eq!(q.pop().unwrap().0, "sitreps:b2");
    }

    #[test]
    fn sheds_lowest_priority_when_full_and_counts_it() {
        let q = PriorityFanout::new();
        for i in 0..PriorityFanout::MAX_PENDING {
            q.enqueue(&format!("sitreps:b{i}"), FanoutKind::AllPeers); // Bulk
        }
        assert_eq!(
            q.inner.lock().unwrap().pending.len(),
            PriorityFanout::MAX_PENDING
        );
        assert_eq!(q.dropped_by_class(), [0; 6]);

        // A Critical arrival over the bound is admitted, evicting a Bulk entry.
        q.enqueue("alerts:urgent", FanoutKind::AllPeers);
        {
            let inner = q.inner.lock().unwrap();
            assert_eq!(inner.pending.len(), PriorityFanout::MAX_PENDING);
            assert!(
                inner.entries.contains_key("alerts:urgent"),
                "Critical arrival must be admitted, not dropped"
            );
        }
        let dropped = q.dropped_by_class();
        assert_eq!(
            dropped[QoSClass::Bulk as usize],
            1,
            "one Bulk entry shed + counted"
        );
        assert_eq!(
            dropped[QoSClass::Critical as usize],
            0,
            "Critical never shed here"
        );
    }
}
