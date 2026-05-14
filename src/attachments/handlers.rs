//! PRD-006 RPC handler bodies. The service trait impl in `crate::service`
//! is a thin dispatcher onto these functions so the wire surface and the
//! attachment-domain logic stay separable.
//!
//! Step 7a covers the non-streaming RPCs:
//!
//! - [`send_attachments`] — validate → registry idempotency check →
//!   ingest → registry insert.
//! - [`get_attachment_distribution`] — registry lookup by distribution_id
//!   → `FileDistribution::status` → mapped to the proto response. Falls
//!   back to a registry-side terminal status (`Cancelled`) when the
//!   bundle's status overrides the per-node aggregate.
//! - [`cancel_attachment_distribution`] — registry lookup → bundle status
//!   transition to `Cancelled` → `FileDistribution::cancel`.
//!
//! `subscribe_attachment_bundle` is implemented in Step 7b.

#![allow(clippy::result_large_err)]

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;

use connectrpc::ConnectError;
use futures::stream::{Stream, StreamExt};
use peat_protocol::storage::file_distribution::{
    DistributionHandle, FileDistribution, IrohFileDistribution, NodeTransferStatus,
    TransferPriority, TransferState,
};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tracing::warn;
use uuid::Uuid;

use crate::attachments::config::AttachmentPriorityCli;
use crate::attachments::ingest::{self, IngestedBlob};
use crate::attachments::registry::{
    AttachmentHandleRecord, BundleIdentity, BundleLookup, BundleRecord, BundleRegistry,
    BundleStatus,
};
use crate::attachments::runtime::{BundleRuntime, DistributionState, PerDistributionProgress};
use crate::attachments::validate;
use crate::node::SidecarNode;
use crate::pb;

/// PRD §Validation Rule 11 concurrency cap. Counted as the number of
/// resident bundles whose status is non-terminal — terminal bundles
/// within the retention window do not count against the cap. Uses
/// `BundleRegistry::non_terminal_count` so the meaning of "in flight"
/// matches the docs (an earlier draft used `registry.len()` which
/// over-counted at the boundary; the PRD-006 QA review flagged the drift).
fn in_flight_count(registry: &BundleRegistry) -> usize {
    registry.non_terminal_count()
}

pub async fn send_attachments(
    node: &Arc<SidecarNode>,
    request: pb::SendAttachmentsRequest,
) -> Result<pb::SendAttachmentsResponse, ConnectError> {
    let cfg = node.attachment_config();
    if !cfg.has_roots() {
        return Err(unimplemented_no_roots("SendAttachments"));
    }

    // 1. Validate (PRD rules 1-10 minus the bits owned by ingest / registry).
    let validated = validate::validate_request(&request, cfg)?;

    // 2. Concurrency cap (PRD rule 11). v1 reject-only; queue_when_full
    //    queue path is deferred.
    if cfg.queue_when_full {
        // v1: queue-when-full is honored as a config knob but the queueing
        // behavior is deferred — for now accept without enforcing the cap
        // so the operator-facing semantic ("don't reject") is preserved.
        // A v2 queue implementation drops the bypass and waits for an
        // in-flight slot.
    } else if in_flight_count(node.bundle_registry()) >= cfg.max_concurrent_distributions as usize {
        return Err(ConnectError::resource_exhausted(format!(
            "max_concurrent_distributions={} reached; \
             try again later or set --attachment-queue-when-full",
            cfg.max_concurrent_distributions
        )));
    }

    // 3. PRD Rule 12 idempotency. Caller-supplied bundle_id wins;
    //    otherwise mint a UUIDv4.
    let bundle_id = request
        .bundle_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let identity = BundleIdentity::from_validated(&validated.files);

    match node.bundle_registry().check_resubmit(&bundle_id, &identity) {
        BundleLookup::Idempotent(existing) => {
            // PRD Rule 12 idempotent branch: return the existing handles
            // without re-reading any file or creating any blob. Optional
            // metadata on the resubmit is ignored — the original record's
            // content_type / display_name is preserved.
            return Ok(build_response(&bundle_id, &existing.handles));
        }
        BundleLookup::Conflict { created_at } => {
            return Err(ConnectError::already_exists(format!(
                "bundle_id `{bundle_id}` already exists (created_at={:?}) \
                 with a different FileSpec set",
                created_at
            )));
        }
        BundleLookup::NotFound => {
            // Either never seen, evicted, or terminal-reusable
            // (Failed / Cancelled). Proceed with fresh ingest.
        }
    }

    // 4. Map proto priority → peat-protocol TransferPriority. UNSPECIFIED
    //    falls back to the configured default_priority.
    let priority = proto_priority_to_transfer(request.priority, cfg.default_priority);

    // 5. Ingest + distribute. Atomic on failure — see ingest module.
    let file_distribution = node
        .file_distribution()
        .expect("file_distribution must be present when has_roots() is true");
    let ingested = ingest::ingest_bundle(
        validated,
        node.blob_store(),
        file_distribution.as_ref(),
        priority,
    )
    .await?;

    // 6. Insert the record. check_resubmit cleared any prior FAILED /
    //    CANCELLED entry — insert overwrites cleanly.
    let handles = build_handle_records(&request, &ingested);
    let record = BundleRecord::new(bundle_id.clone(), identity, handles.clone());
    node.bundle_registry().insert(record);

    // 7. Register runtime state for the subscribe fan-out (Step 7b) and
    //    spawn per-distribution watcher tasks. The runtime is created
    //    *before* watchers start so a subscriber attaching between
    //    SendAttachments returning and the watcher's first event still
    //    sees the empty slot (Pending, last_progress with distribution_id).
    let runtime_slots = handles
        .iter()
        .map(|h| {
            let bytes_total = ingested
                .iter()
                .find(|ib| ib.file_index == h.file_index)
                .map(|ib| ib.blob_token.size_bytes)
                .unwrap_or(0);
            PerDistributionProgress {
                state: DistributionState::Pending,
                bytes_transferred: 0,
                bytes_total,
                error: None,
                last_progress: pb::AttachmentProgress {
                    distribution_id: h.distribution_id().to_string(),
                    blob_token: h.blob_token_hash.clone(),
                    status: buffa::EnumValue::from(
                        pb::DistributionStatus::DISTRIBUTION_STATUS_PENDING as i32,
                    ),
                    bytes_transferred: 0,
                    bytes_total,
                    ..Default::default()
                },
            }
        })
        .collect::<Vec<_>>();
    let runtime = node.bundle_runtime().register(&bundle_id, runtime_slots);

    for h in &handles {
        spawn_distribution_watcher(
            Arc::clone(file_distribution),
            Arc::clone(node.bundle_registry()),
            Arc::clone(&runtime),
            bundle_id.clone(),
            h.file_index,
            h.distribution_handle.clone(),
            h.blob_token_hash.clone(),
        );
    }

    Ok(build_response(&bundle_id, &handles))
}

pub async fn get_attachment_distribution(
    node: &Arc<SidecarNode>,
    request: pb::GetAttachmentDistributionRequest,
) -> Result<pb::GetAttachmentDistributionResponse, ConnectError> {
    let cfg = node.attachment_config();
    if !cfg.has_roots() {
        return Err(unimplemented_no_roots("GetAttachmentDistribution"));
    }

    let (_bundle_id, bundle) = node
        .bundle_registry()
        .lookup_distribution(&request.distribution_id)
        .ok_or_else(|| {
            ConnectError::not_found(format!(
                "distribution_id `{}` not found",
                request.distribution_id
            ))
        })?;

    let handle_rec = bundle
        .handles
        .iter()
        .find(|h| h.distribution_id() == request.distribution_id)
        .expect("lookup_distribution returned bundle but handle index missing");

    let file_distribution = node
        .file_distribution()
        .expect("file_distribution must be present when has_roots() is true");
    let status = file_distribution
        .status(&handle_rec.distribution_handle)
        .await
        .map_err(|e| ConnectError::internal(format!("status query failed: {e}")))?;

    // peat-protocol's DistributionStatus indexes per-peer state by
    // node_id in a HashMap; collect a snapshot for both aggregate and
    // per-node response fields.
    let per_node: Vec<&NodeTransferStatus> = status.node_statuses.values().collect();

    let proto_status = if bundle.status.is_terminal() {
        // Registry-side terminal status (e.g., Cancelled set by the
        // cancel handler) takes precedence over per-node aggregation.
        bundle_status_to_proto(bundle.status)
    } else {
        per_node_aggregate(&per_node)
    };

    let bytes_transferred: u64 = per_node.iter().map(|p| p.progress_bytes).sum();
    // Fall back to the ingested file's size_bytes from the bundle
    // identity — peat-protocol's per_node entries don't appear until a
    // peer starts fetching, and a callerasking right after SendAttachments
    // would otherwise see bytes_total=0 (or worse, the hex hash length —
    // an earlier draft used that fallback and reported ~64 for every
    // pre-fetch query).
    let fallback_total = bundle
        .identity
        .files
        .get(handle_rec.file_index)
        .map(|f| f.size_bytes)
        .unwrap_or(0);
    let bytes_total: u64 = per_node
        .first()
        .map(|p| p.total_bytes)
        .unwrap_or(fallback_total);

    Ok(pb::GetAttachmentDistributionResponse {
        status: buffa::EnumValue::from(proto_status as i32),
        bytes_transferred,
        bytes_total,
        per_node: per_node.iter().map(|p| node_state_to_proto(p)).collect(),
        error: per_node.iter().find_map(|p| p.error.clone()),
        ..Default::default()
    })
}

pub async fn cancel_attachment_distribution(
    node: &Arc<SidecarNode>,
    request: pb::CancelAttachmentDistributionRequest,
) -> Result<pb::CancelAttachmentDistributionResponse, ConnectError> {
    let cfg = node.attachment_config();
    if !cfg.has_roots() {
        return Err(unimplemented_no_roots("CancelAttachmentDistribution"));
    }

    let (bundle_id, bundle) = node
        .bundle_registry()
        .lookup_distribution(&request.distribution_id)
        .ok_or_else(|| {
            ConnectError::not_found(format!(
                "distribution_id `{}` not found",
                request.distribution_id
            ))
        })?;

    // If the bundle has already reached a terminal state, report
    // was_cancelled=false rather than fabricating a cancel that didn't
    // happen.
    if bundle.status.is_terminal() {
        return Ok(pb::CancelAttachmentDistributionResponse {
            was_cancelled: false,
            ..Default::default()
        });
    }

    let handle_rec = bundle
        .handles
        .iter()
        .find(|h| h.distribution_id() == request.distribution_id)
        .expect("lookup_distribution returned bundle but handle index missing");

    let file_distribution = node
        .file_distribution()
        .expect("file_distribution must be present when has_roots() is true");
    file_distribution
        .cancel(&handle_rec.distribution_handle)
        .await
        .map_err(|e| ConnectError::internal(format!("cancel failed: {e}")))?;

    // Mirror the cancel into the runtime so subscribers see a Cancelled
    // terminal frame for this specific distribution.
    if let Some(runtime) = node.bundle_runtime().get(&bundle_id) {
        let progress = pb::AttachmentProgress {
            distribution_id: request.distribution_id.clone(),
            blob_token: handle_rec.blob_token_hash.clone(),
            status: buffa::EnumValue::from(
                pb::DistributionStatus::DISTRIBUTION_STATUS_CANCELLED as i32,
            ),
            ..Default::default()
        };
        runtime.apply_progress(
            handle_rec.file_index,
            DistributionState::Cancelled,
            progress,
        );
        maybe_finalize_bundle(node.bundle_registry(), &runtime, &bundle_id);
    } else {
        // No runtime entry (e.g., 7a-era bundle inserted before the
        // runtime store existed). Fall back to the bundle-level status.
        node.bundle_registry()
            .update_status(&bundle_id, BundleStatus::Cancelled);
    }

    Ok(pb::CancelAttachmentDistributionResponse {
        was_cancelled: true,
        ..Default::default()
    })
}

pub async fn subscribe_attachment_bundle(
    node: &Arc<SidecarNode>,
    request: pb::SubscribeAttachmentBundleRequest,
) -> Result<
    Pin<Box<dyn Stream<Item = Result<pb::AttachmentProgress, ConnectError>> + Send>>,
    ConnectError,
> {
    let cfg = node.attachment_config();
    if !cfg.has_roots() {
        return Err(unimplemented_no_roots("SubscribeAttachmentBundle"));
    }

    let runtime = node
        .bundle_runtime()
        .get(&request.bundle_id)
        .ok_or_else(|| {
            ConnectError::not_found(format!("bundle_id `{}` not found", request.bundle_id))
        })?;

    // Build the late-subscribe stream per PRD doc-comments on
    // SubscribeAttachmentBundle:
    //
    // 1. Subscribe FIRST (before snapshotting) so events between snapshot
    //    and subscribe aren't lost.
    // 2. Snapshot the per-distribution state. Emit one synthetic frame
    //    per *already-terminal* distribution carrying the terminal
    //    status. Pending / InProgress distributions are NOT snapshotted —
    //    the live stream will deliver their updates.
    // 3. Forward the live broadcast, filtering out any frame whose
    //    distribution_id we already emitted via snapshot (those
    //    distributions' terminal frames are stale on the broadcast at
    //    this point).
    // 4. Close the stream when the total number of terminal events
    //    delivered (snapshot + live) equals total_distributions, OR when
    //    the broadcast closes.
    let live_rx = runtime.subscribe();
    let snapshot = runtime.per_distribution_snapshot();
    let total = runtime.total();

    let (snapshot_frames, snapshot_terminal_ids): (Vec<_>, HashSet<_>) = {
        let mut frames = Vec::new();
        let mut ids = HashSet::new();
        for slot in snapshot {
            if slot.state.is_terminal() {
                ids.insert(slot.last_progress.distribution_id.clone());
                frames.push(slot.last_progress);
            }
        }
        (frames, ids)
    };

    let stream = build_subscribe_stream(snapshot_frames, snapshot_terminal_ids, live_rx, total);
    Ok(Box::pin(stream))
}

/// Build the multiplexed stream: snapshot frames first, then a filtered
/// view of the live broadcast that closes once `total` terminal frames
/// have been delivered overall.
fn build_subscribe_stream(
    snapshot_frames: Vec<pb::AttachmentProgress>,
    snapshot_terminal_ids: HashSet<String>,
    live_rx: broadcast::Receiver<pb::AttachmentProgress>,
    total: usize,
) -> impl Stream<Item = Result<pb::AttachmentProgress, ConnectError>> + Send {
    use futures::stream;

    let snapshot_count = snapshot_frames.len();
    let snapshot_stream = stream::iter(snapshot_frames.into_iter().map(Ok));

    let live_stream = BroadcastStream::new(live_rx).filter_map(move |r| {
        let ids = snapshot_terminal_ids.clone();
        async move {
            match r {
                Ok(progress) => {
                    if ids.contains(&progress.distribution_id) {
                        // Already emitted this distribution's terminal
                        // frame via snapshot. Suppress the live duplicate.
                        None
                    } else {
                        Some(Ok::<_, ConnectError>(progress))
                    }
                }
                Err(_) => None, // skip lag / closed errors
            }
        }
    });

    // Snapshot frames count toward the terminal total *as they are
    // delivered*, not at construction — pre-closing the stream based on
    // the snapshot count would consume the closure budget before the
    // snapshot frames flow through, leaving the subscriber with an
    // immediately-closed stream and zero frames.
    let _ = snapshot_count;
    let combined = snapshot_stream.chain(live_stream);
    StreamCloser::new(combined, total)
}

/// Wraps a stream and closes it after `total` terminal-status frames
/// have been observed. Used to enforce the SubscribeAttachmentBundle
/// PRD contract that the stream closes when every distribution has
/// reached a terminal state.
struct StreamCloser<S> {
    inner: Pin<Box<S>>,
    terminal_count: usize,
    total: usize,
    closed: bool,
}

impl<S> StreamCloser<S>
where
    S: Stream<Item = Result<pb::AttachmentProgress, ConnectError>> + Send + 'static,
{
    fn new(inner: S, total: usize) -> Self {
        Self {
            inner: Box::pin(inner),
            terminal_count: 0,
            total,
            // Bundles with zero distributions are vacuously complete —
            // close immediately to avoid hanging on an empty stream.
            closed: total == 0,
        }
    }
}

impl<S> Stream for StreamCloser<S>
where
    S: Stream<Item = Result<pb::AttachmentProgress, ConnectError>> + Send + 'static,
{
    type Item = Result<pb::AttachmentProgress, ConnectError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.closed {
            return std::task::Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(progress))) => {
                if is_terminal_status(progress.status) {
                    self.terminal_count += 1;
                    if self.terminal_count >= self.total {
                        self.closed = true;
                    }
                }
                std::task::Poll::Ready(Some(Ok(progress)))
            }
            other => other,
        }
    }
}

fn is_terminal_status(s: buffa::EnumValue<pb::DistributionStatus>) -> bool {
    use pb::DistributionStatus as D;
    matches!(
        s.as_known(),
        Some(D::DISTRIBUTION_STATUS_COMPLETED)
            | Some(D::DISTRIBUTION_STATUS_PARTIAL)
            | Some(D::DISTRIBUTION_STATUS_FAILED)
            | Some(D::DISTRIBUTION_STATUS_CANCELLED)
    )
}

// ----- watcher task ---------------------------------------------------------

/// Spawn a watcher task for one distribution. Translates peat-protocol
/// progress updates into `AttachmentProgress` frames, updates the runtime
/// state, and bumps the registry's BundleStatus on terminal transitions.
fn spawn_distribution_watcher(
    file_distribution: Arc<IrohFileDistribution>,
    registry: Arc<BundleRegistry>,
    runtime: Arc<BundleRuntime>,
    bundle_id: String,
    file_index: usize,
    distribution_handle: DistributionHandle,
    blob_token_hash: String,
) {
    tokio::spawn(async move {
        // Initial status check. peat-protocol considers a distribution
        // "complete" when `completed + failed >= total_targets`, which
        // is *immediately* true for zero-peer scopes (total_targets=0).
        // We need to fold that into a terminal frame at watcher start
        // so subscribers don't wait forever for an event the substrate
        // never emits.
        match file_distribution.status(&distribution_handle).await {
            Ok(s) if s.total_targets == 0 => {
                // Zero-peer distribution. peat-protocol's `is_complete`
                // returns true here; map to COMPLETED (no peers to
                // succeed or fail). The PRD §v1-honesty rule says
                // sender-side COMPLETED means "every targeted peer
                // connected and pulled all bytes from this sender" —
                // zero peers vacuously satisfies that.
                let progress = pb::AttachmentProgress {
                    distribution_id: distribution_handle.distribution_id.clone(),
                    blob_token: blob_token_hash.clone(),
                    status: buffa::EnumValue::from(
                        pb::DistributionStatus::DISTRIBUTION_STATUS_COMPLETED as i32,
                    ),
                    bytes_transferred: 0,
                    bytes_total: 0,
                    ..Default::default()
                };
                let _ = runtime.apply_progress(file_index, DistributionState::Completed, progress);
                maybe_finalize_bundle(&registry, &runtime, &bundle_id);
                return;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    distribution_id = %distribution_handle.distribution_id,
                    error = %e,
                    "watcher: initial status check failed; marking distribution failed"
                );
                let progress = pb::AttachmentProgress {
                    distribution_id: distribution_handle.distribution_id.clone(),
                    blob_token: blob_token_hash.clone(),
                    status: buffa::EnumValue::from(
                        pb::DistributionStatus::DISTRIBUTION_STATUS_FAILED as i32,
                    ),
                    error: Some(format!("initial status check failed: {e}")),
                    ..Default::default()
                };
                let _ = runtime.apply_progress(file_index, DistributionState::Failed, progress);
                maybe_finalize_bundle(&registry, &runtime, &bundle_id);
                return;
            }
        };

        // Subscribe to live progress.
        let mut rx = match file_distribution
            .subscribe_progress(&distribution_handle)
            .await
        {
            Ok(rx) => rx,
            Err(e) => {
                warn!(
                    distribution_id = %distribution_handle.distribution_id,
                    error = %e,
                    "watcher: subscribe_progress failed; marking distribution failed"
                );
                let progress = pb::AttachmentProgress {
                    distribution_id: distribution_handle.distribution_id.clone(),
                    blob_token: blob_token_hash.clone(),
                    status: buffa::EnumValue::from(
                        pb::DistributionStatus::DISTRIBUTION_STATUS_FAILED as i32,
                    ),
                    error: Some(format!("subscribe_progress failed: {e}")),
                    ..Default::default()
                };
                let _ = runtime.apply_progress(file_index, DistributionState::Failed, progress);
                maybe_finalize_bundle(&registry, &runtime, &bundle_id);
                return;
            }
        };

        // Forward events until the distribution terminates.
        loop {
            match rx.recv().await {
                Ok(status) => {
                    let per_node: Vec<&NodeTransferStatus> =
                        status.node_statuses.values().collect();
                    let bytes_transferred: u64 = per_node.iter().map(|p| p.progress_bytes).sum();
                    let bytes_total: u64 = per_node.first().map(|p| p.total_bytes).unwrap_or(0);
                    let aggregated = per_node_aggregate(&per_node);
                    let dist_state = aggregated_to_distribution_state(aggregated);
                    let progress = pb::AttachmentProgress {
                        distribution_id: distribution_handle.distribution_id.clone(),
                        blob_token: blob_token_hash.clone(),
                        status: buffa::EnumValue::from(aggregated as i32),
                        bytes_transferred,
                        bytes_total,
                        error: per_node.iter().find_map(|p| p.error.clone()),
                        ..Default::default()
                    };
                    runtime.apply_progress(file_index, dist_state, progress);
                    if status.is_complete() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }

        maybe_finalize_bundle(&registry, &runtime, &bundle_id);
    });
}

/// Map a per-node aggregate proto status to the runtime's
/// `DistributionState` enum.
fn aggregated_to_distribution_state(s: pb::DistributionStatus) -> DistributionState {
    use pb::DistributionStatus as D;
    match s {
        D::DISTRIBUTION_STATUS_PENDING | D::DISTRIBUTION_STATUS_UNSPECIFIED => {
            DistributionState::Pending
        }
        D::DISTRIBUTION_STATUS_IN_PROGRESS => DistributionState::InProgress,
        D::DISTRIBUTION_STATUS_COMPLETED | D::DISTRIBUTION_STATUS_PARTIAL => {
            DistributionState::Completed
        }
        D::DISTRIBUTION_STATUS_FAILED => DistributionState::Failed,
        D::DISTRIBUTION_STATUS_CANCELLED => DistributionState::Cancelled,
    }
}

/// Once a watcher hits a terminal state, check if every distribution in
/// the bundle has terminated. If so, set the registry's BundleStatus to
/// the aggregated terminal value so GetAttachmentDistribution's
/// "terminal-precedence" branch picks the right enum.
fn maybe_finalize_bundle(registry: &BundleRegistry, runtime: &BundleRuntime, bundle_id: &str) {
    if !runtime.all_terminal() {
        return;
    }
    let snap = runtime.per_distribution_snapshot();
    let mut any_failed = false;
    let mut any_cancelled = false;
    for slot in &snap {
        match slot.state {
            DistributionState::Failed => any_failed = true,
            DistributionState::Cancelled => any_cancelled = true,
            _ => {}
        }
    }
    let final_status = if any_failed {
        BundleStatus::Failed
    } else if any_cancelled {
        BundleStatus::Cancelled
    } else {
        BundleStatus::Completed
    };
    registry.update_status(bundle_id, final_status);
}

// ----- helpers ---------------------------------------------------------------

fn unimplemented_no_roots(rpc: &str) -> ConnectError {
    ConnectError::unimplemented(format!("{rpc} requires --attachment-root to be configured"))
}

fn build_handle_records(
    request: &pb::SendAttachmentsRequest,
    ingested: &[IngestedBlob],
) -> Vec<AttachmentHandleRecord> {
    ingested
        .iter()
        .map(|ib| {
            let original = request
                .files
                .get(ib.file_index)
                .expect("ingest preserves file_index ordering from the original request");
            AttachmentHandleRecord {
                file_index: ib.file_index,
                blob_token_hash: ib.blob_token.hash.0.clone(),
                distribution_handle: ib.distribution_handle.clone(),
                content_type: original.content_type.clone(),
                display_name: original.display_name.clone(),
            }
        })
        .collect()
}

fn build_response(
    bundle_id: &str,
    handles: &[AttachmentHandleRecord],
) -> pb::SendAttachmentsResponse {
    pb::SendAttachmentsResponse {
        bundle_id: bundle_id.to_string(),
        handles: handles
            .iter()
            .map(|h| pb::AttachmentHandle {
                file_index: h.file_index as u32,
                blob_token: h.blob_token_hash.clone(),
                distribution_id: h.distribution_id().to_string(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

fn proto_priority_to_transfer(
    priority: buffa::EnumValue<pb::AttachmentPriority>,
    fallback: AttachmentPriorityCli,
) -> TransferPriority {
    // peat-protocol's TransferPriority has 4 tiers (Critical / High /
    // Normal / Low); the wire surface exposes 5 (BULK is below LOW per
    // QoSClass). Until peat-protocol grows a Bulk variant on
    // TransferPriority (or migrates to QoSClass directly), BULK collapses
    // onto Low here — peat-node records the classification on the
    // wire / distribution doc, but the priority handed to the substrate
    // is the closest 4-tier equivalent. PRD §AttachmentPriority calls
    // this out as v1-honesty (no wire-level preemption either way).
    use pb::AttachmentPriority as P;
    let resolved: P = match priority.as_known() {
        Some(v) if v != P::ATTACHMENT_PRIORITY_UNSPECIFIED => v,
        _ => cli_priority_to_proto(fallback),
    };
    match resolved {
        P::ATTACHMENT_PRIORITY_BULK | P::ATTACHMENT_PRIORITY_LOW => TransferPriority::Low,
        P::ATTACHMENT_PRIORITY_ROUTINE => TransferPriority::Normal,
        P::ATTACHMENT_PRIORITY_PRIORITY => TransferPriority::High,
        P::ATTACHMENT_PRIORITY_CRITICAL => TransferPriority::Critical,
        P::ATTACHMENT_PRIORITY_UNSPECIFIED => TransferPriority::Normal,
    }
}

fn cli_priority_to_proto(cli: AttachmentPriorityCli) -> pb::AttachmentPriority {
    use pb::AttachmentPriority as P;
    match cli {
        AttachmentPriorityCli::Bulk => P::ATTACHMENT_PRIORITY_BULK,
        AttachmentPriorityCli::Low => P::ATTACHMENT_PRIORITY_LOW,
        AttachmentPriorityCli::Routine => P::ATTACHMENT_PRIORITY_ROUTINE,
        AttachmentPriorityCli::Priority => P::ATTACHMENT_PRIORITY_PRIORITY,
        AttachmentPriorityCli::Critical => P::ATTACHMENT_PRIORITY_CRITICAL,
    }
}

fn bundle_status_to_proto(s: BundleStatus) -> pb::DistributionStatus {
    use pb::DistributionStatus as D;
    match s {
        BundleStatus::Pending => D::DISTRIBUTION_STATUS_PENDING,
        BundleStatus::InProgress => D::DISTRIBUTION_STATUS_IN_PROGRESS,
        BundleStatus::Completed => D::DISTRIBUTION_STATUS_COMPLETED,
        BundleStatus::Partial => D::DISTRIBUTION_STATUS_PARTIAL,
        BundleStatus::Failed => D::DISTRIBUTION_STATUS_FAILED,
        BundleStatus::Cancelled => D::DISTRIBUTION_STATUS_CANCELLED,
    }
}

fn transfer_state_to_proto(s: TransferState) -> pb::DistributionStatus {
    use pb::DistributionStatus as D;
    match s {
        TransferState::Pending => D::DISTRIBUTION_STATUS_PENDING,
        TransferState::Connecting | TransferState::Transferring => {
            D::DISTRIBUTION_STATUS_IN_PROGRESS
        }
        TransferState::Completed => D::DISTRIBUTION_STATUS_COMPLETED,
        TransferState::Failed => D::DISTRIBUTION_STATUS_FAILED,
    }
}

/// Aggregate per-peer state into a single bundle-level proto status per the
/// PRD §v1-honesty contract: COMPLETED iff every peer completed, FAILED on
/// any explicit failure, IN_PROGRESS while at least one peer is still
/// running, PENDING with no observed peers. PARTIAL is reserved for v2.
fn per_node_aggregate(per_node: &[&NodeTransferStatus]) -> pb::DistributionStatus {
    use pb::DistributionStatus as D;
    if per_node.is_empty() {
        return D::DISTRIBUTION_STATUS_PENDING;
    }
    let mut any_failed = false;
    let mut any_running = false;
    let mut all_completed = true;
    for p in per_node {
        match p.status {
            TransferState::Failed => any_failed = true,
            TransferState::Pending | TransferState::Connecting | TransferState::Transferring => {
                any_running = true;
                all_completed = false;
            }
            TransferState::Completed => {}
        }
    }
    if any_failed {
        D::DISTRIBUTION_STATUS_FAILED
    } else if any_running {
        D::DISTRIBUTION_STATUS_IN_PROGRESS
    } else if all_completed {
        D::DISTRIBUTION_STATUS_COMPLETED
    } else {
        D::DISTRIBUTION_STATUS_PENDING
    }
}

fn node_state_to_proto(p: &NodeTransferStatus) -> pb::NodeTransferState {
    pb::NodeTransferState {
        node_id: p.node_id.clone(),
        status: buffa::EnumValue::from(transfer_state_to_proto(p.status.clone()) as i32),
        bytes_transferred: p.progress_bytes,
        ..Default::default()
    }
}
