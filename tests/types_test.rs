// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! Unit tests for CRDT schema types — DeploymentRequest, AvailablePackage.
//!
//! Locks the schema shape at runtime (CRDT-01, SYNC-02) to complement the
//! compile-time enforcement provided by the Rust type system.

use std::collections::HashMap;

use peat_node::types::{AvailablePackage, DeploymentRequest, DeploymentStatus};

fn sample_deployment_request() -> DeploymentRequest {
    let mut vars = HashMap::new();
    vars.insert("K".to_string(), "V".to_string());
    DeploymentRequest {
        id: "req-1".to_string(),
        target_agent_id: "node-b".to_string(),
        package_name: "demo".to_string(),
        package_version: "1.0.0".to_string(),
        architecture: "arm64".to_string(),
        iroh_blob_hash: "abc".to_string(),
        sender_endpoint_id: "ep-a".to_string(),
        zarf_vars: vars,
        sender_status: DeploymentStatus::Pending,
        receiver_status: DeploymentStatus::Pending,
        created_at: 1_700_000_000,
        blob_ticket: "ticket-a".to_string(),
    }
}

fn sample_available_package() -> AvailablePackage {
    AvailablePackage {
        name: "demo".to_string(),
        version: "1.0.0".to_string(),
        architecture: "arm64".to_string(),
        iroh_blob_hash: "abc".to_string(),
        sender_endpoint_id: "ep-a".to_string(),
        published_at: 1_700_000_000,
    }
}

#[test]
fn test_deployment_request_serde_round_trip() {
    let original = sample_deployment_request();
    let json = serde_json::to_string(&original).expect("serialize");
    let parsed: DeploymentRequest = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(parsed.id, original.id);
    assert_eq!(parsed.target_agent_id, original.target_agent_id);
    assert_eq!(parsed.package_name, original.package_name);
    assert_eq!(parsed.package_version, original.package_version);
    assert_eq!(parsed.architecture, original.architecture);
    assert_eq!(parsed.iroh_blob_hash, original.iroh_blob_hash);
    assert_eq!(parsed.sender_endpoint_id, original.sender_endpoint_id);
    assert_eq!(parsed.zarf_vars, original.zarf_vars);
    assert_eq!(parsed.sender_status, original.sender_status);
    assert_eq!(parsed.receiver_status, original.receiver_status);
    assert_eq!(parsed.created_at, original.created_at);
    assert_eq!(parsed.blob_ticket, original.blob_ticket);
}

#[test]
fn test_deployment_request_has_no_shared_status_field() {
    let req = sample_deployment_request();
    let json = serde_json::to_value(&req).expect("serialize");
    let obj = json.as_object().expect("top-level is object");

    assert!(obj.contains_key("sender_status"), "must carry sender_status");
    assert!(obj.contains_key("receiver_status"), "must carry receiver_status");
    assert!(
        !obj.contains_key("status"),
        "must NOT carry a shared `status` field (Automerge LWW race guard per D-02)"
    );
    assert!(
        obj.contains_key("blob_ticket"),
        "DeploymentRequest must carry blob_ticket field (Phase 2)"
    );
}

#[test]
fn test_available_package_has_no_target_agent_id() {
    let pkg = sample_available_package();
    let json = serde_json::to_value(&pkg).expect("serialize");
    let obj = json.as_object().expect("top-level is object");

    assert!(
        !obj.contains_key("target_agent_id"),
        "AvailablePackage is broadcast; must NOT carry target_agent_id (D-03)"
    );

    for required in ["name", "version", "architecture", "iroh_blob_hash", "sender_endpoint_id", "published_at"] {
        assert!(obj.contains_key(required), "missing required field: {required}");
    }
}

#[test]
fn test_deployment_status_snake_case_serialization() {
    assert_eq!(serde_json::to_string(&DeploymentStatus::Pending).unwrap(), "\"pending\"");
    assert_eq!(serde_json::to_string(&DeploymentStatus::Fetching).unwrap(), "\"fetching\"");
    assert_eq!(serde_json::to_string(&DeploymentStatus::Deploying).unwrap(), "\"deploying\"");
    assert_eq!(serde_json::to_string(&DeploymentStatus::Deployed).unwrap(), "\"deployed\"");
    assert_eq!(serde_json::to_string(&DeploymentStatus::Failed).unwrap(), "\"failed\"");
}

#[test]
fn test_deployment_status_default_is_pending() {
    assert_eq!(DeploymentStatus::default(), DeploymentStatus::Pending);
}
