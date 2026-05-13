// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! SYNC-01 integration tests — watcher auto-publishes deployed packages to available_packages.
//!
//! Tests bypass the RA /status and ListPackages HTTP calls by invoking the
//! public helper sync_available_packages directly with a DeployedPackageRef list.
//! This follows Recommendation A from 02-RESEARCH.md: blobs must already be
//! published locally (via publish_blob) to be advertised.

use std::sync::Arc;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::types::AvailablePackage;
use peat_node::watcher::{sync_available_packages, DeployedPackageRef};

async fn make_node() -> (Arc<SidecarNode>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let blob_work_dir = dir.path().join("blobs");
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: "test-sync-01".to_string(),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir,
            peers: vec![],
            encryption_key: None,
            enable_deployer: false,
            blob_work_dir,
            download_timeout_secs: 30,
        })
        .await
        .unwrap(),
    );
    (node, dir)
}

async fn publish_fake(node: &SidecarNode, name: &str, content: &[u8]) -> String {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), content).unwrap();
    let token = node.publish_blob(tmp.path(), name).await.unwrap();
    token.hash.as_hex().to_string()
}

#[tokio::test]
async fn test_sync_01_auto_publishes_deployed_package() {
    let (node, _dir) = make_node().await;
    let hash = publish_fake(&node, "demo-0.1.0-arm64.zarf.tar.zst", b"fake zarf bytes").await;

    let pkgs = vec![DeployedPackageRef {
        name: "demo".to_string(),
        version: "0.1.0".to_string(),
        status_is_deployed: true,
    }];

    sync_available_packages(&node, &pkgs, "arm64", 1_700_000_000).await;

    let doc = node
        .get_document("available_packages", "demo-0.1.0-arm64")
        .await
        .unwrap()
        .expect("available_packages doc must exist");
    let avail: AvailablePackage = serde_json::from_str(&doc).unwrap();

    assert_eq!(avail.name, "demo");
    assert_eq!(avail.version, "0.1.0");
    assert_eq!(avail.architecture, "arm64");
    assert_eq!(avail.iroh_blob_hash, hash);
    assert_eq!(avail.sender_endpoint_id, node.endpoint_addr());
    assert_eq!(avail.published_at, 1_700_000_000);
}

#[tokio::test]
async fn test_sync_01_skips_packages_without_matching_blob() {
    let (node, _dir) = make_node().await;

    let pkgs = vec![DeployedPackageRef {
        name: "no-blob".to_string(),
        version: "9.9.9".to_string(),
        status_is_deployed: true,
    }];

    sync_available_packages(&node, &pkgs, "arm64", 1_700_000_000).await;

    let doc = node
        .get_document("available_packages", "no-blob-9.9.9-arm64")
        .await
        .unwrap();
    assert!(doc.is_none(), "package with no matching blob must not be advertised");
}

#[tokio::test]
async fn test_sync_01_is_idempotent_after_first_publish() {
    let (node, _dir) = make_node().await;

    publish_fake(&node, "demo-0.1.0-arm64.zarf.tar.zst", b"fake").await;
    let pkgs = vec![DeployedPackageRef {
        name: "demo".to_string(),
        version: "0.1.0".to_string(),
        status_is_deployed: true,
    }];

    sync_available_packages(&node, &pkgs, "arm64", 1_700_000_000).await;
    sync_available_packages(&node, &pkgs, "arm64", 1_700_000_010).await;

    let doc = node
        .get_document("available_packages", "demo-0.1.0-arm64")
        .await
        .unwrap()
        .expect("doc present after idempotent re-run");
    let avail: AvailablePackage = serde_json::from_str(&doc).unwrap();
    assert_eq!(
        avail.published_at, 1_700_000_000,
        "SYNC-02 immutability: second poll must not overwrite the first doc"
    );
}

#[tokio::test]
async fn test_sync_01_skips_non_deployed_packages() {
    let (node, _dir) = make_node().await;

    publish_fake(&node, "demo-0.1.0-arm64.zarf.tar.zst", b"fake").await;
    let pkgs = vec![DeployedPackageRef {
        name: "demo".to_string(),
        version: "0.1.0".to_string(),
        status_is_deployed: false,
    }];

    sync_available_packages(&node, &pkgs, "arm64", 1_700_000_000).await;

    let doc = node
        .get_document("available_packages", "demo-0.1.0-arm64")
        .await
        .unwrap();
    assert!(doc.is_none(), "non-deployed packages must not be advertised");
}
