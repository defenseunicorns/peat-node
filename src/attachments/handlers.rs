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

use std::sync::Arc;

use connectrpc::ConnectError;
use peat_protocol::storage::file_distribution::{
    FileDistribution, NodeTransferStatus, TransferPriority, TransferState,
};
use uuid::Uuid;

use crate::attachments::config::AttachmentPriorityCli;
use crate::attachments::ingest::{self, IngestedBlob};
use crate::attachments::registry::{
    AttachmentHandleRecord, BundleIdentity, BundleLookup, BundleRecord, BundleRegistry,
    BundleStatus,
};
use crate::attachments::validate;
use crate::node::SidecarNode;
use crate::pb;

/// PRD §Validation Rule 11 concurrency cap. Counted as the number of
/// resident bundles whose status is non-terminal — a partial proxy for
/// "in flight" that's correct under the v1 sender-side-only status model
/// (no v2 observer hooks fold receiver state in).
fn in_flight_count(registry: &BundleRegistry) -> usize {
    // Iterating the registry would require additional API surface. For
    // v1 we don't have a direct count helper, so we approximate via
    // registry.len() — which counts all resident bundles, terminal or
    // not. That over-counts in_flight at the boundary (terminal bundles
    // still in the retention window are charged), making the limit
    // *stricter* than spec. v2 should add a `non_terminal_count()`
    // helper to the registry once the status broadcast (Step 7b) wires
    // up terminal transitions reliably.
    registry.len()
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
    let bytes_total: u64 = per_node
        .first()
        .map(|p| p.total_bytes)
        .unwrap_or(handle_rec.distribution_handle.blob_hash.0.len() as u64);

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

    node.bundle_registry()
        .update_status(&bundle_id, BundleStatus::Cancelled);

    Ok(pb::CancelAttachmentDistributionResponse {
        was_cancelled: true,
        ..Default::default()
    })
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
