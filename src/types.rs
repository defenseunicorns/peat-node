// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! CRDT document schemas for P2P package distribution.
//!
//! These types enforce the locked schemas for `deployment_requests` and
//! `available_packages` CRDT collections at compile time. All fields are
//! non-optional to prevent partial writes.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Status for one side of a deployment request.
///
/// Using separate sender/receiver statuses prevents Automerge LWW race conditions
/// that would occur if both sides wrote to a shared `status` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentStatus {
    Pending,
    Fetching,
    Deploying,
    Deployed,
    Failed,
}

impl Default for DeploymentStatus {
    fn default() -> Self {
        Self::Pending
    }
}

/// Locked schema for `deployment_requests/{uuid}` CRDT documents.
///
/// Schema is enforced at compile time — all fields required, no shared status
/// field (see D-02: prevents Automerge LWW race between sender and receiver).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRequest {
    /// UUID identifying this deployment request.
    pub id: String,
    /// Node ID of the intended receiver.
    pub target_agent_id: String,
    pub package_name: String,
    pub package_version: String,
    /// Target architecture (e.g. "arm64", "amd64").
    pub architecture: String,
    /// BLAKE3 content hash of the Zarf package blob.
    pub iroh_blob_hash: String,
    /// Iroh endpoint ID of the sender node (for blob peer wiring in Phase 3).
    pub sender_endpoint_id: String,
    /// Zarf variable overrides for deployment.
    pub zarf_vars: HashMap<String, String>,
    /// Status as seen and updated by the sender node.
    pub sender_status: DeploymentStatus,
    /// Status as seen and updated by the receiver node.
    pub receiver_status: DeploymentStatus,
    /// Unix timestamp (seconds) when this request was created.
    pub created_at: i64,
    /// JSON-encoded blob ticket: {"hash":"<hex>","size_bytes":<u64>,"sender_endpoint_id":"<hex>"}.
    /// Phase 3 parses this to wire the blob peer index before calling fetch_blob.
    pub blob_ticket: String,
}

/// Locked schema for `available_packages/{pkg_ref}` CRDT documents.
///
/// Broadcast (no target_agent_id). Immutable after first write — the write path
/// must check `get_document` before calling `put_document` (Phase 2 concern).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailablePackage {
    pub name: String,
    pub version: String,
    /// Package architecture (e.g. "arm64", "amd64").
    pub architecture: String,
    /// BLAKE3 content hash of the Zarf package blob.
    pub iroh_blob_hash: String,
    /// Iroh endpoint ID of the node that published this blob.
    pub sender_endpoint_id: String,
    /// Unix timestamp (seconds) when this package was first published.
    pub published_at: i64,
}
