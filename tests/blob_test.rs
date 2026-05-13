// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: LicenseRef-Defense-Unicorns-Commercial
//! Integration tests for peat-node blob store wiring.
//!
//! Covers BLOB-01 (publish returns hash + size) and BLOB-02 (startup re-import
//! survives SidecarNode drop/recreate against the same blob_work_dir).

use peat_node::node::{SidecarConfig, SidecarNode};

async fn test_node_at(dir: &std::path::Path) -> SidecarNode {
    SidecarNode::new(SidecarConfig {
        node_id: "test-node".to_string(),
        app_id: "test".to_string(),
        shared_key: String::new(),
        data_dir: dir.to_path_buf(),
        peers: vec![],
        encryption_key: None,
        enable_deployer: false,
        blob_work_dir: dir.join("blobs"),
        download_timeout_secs: 30,
    })
    .await
    .expect("failed to create node")
}

#[tokio::test]
async fn test_publish_blob_returns_hash_and_size() {
    let tmp = tempfile::tempdir().unwrap();
    let node = test_node_at(tmp.path()).await;

    let file_path = tmp.path().join("input.bin");
    std::fs::write(&file_path, b"File content for testing").unwrap();

    let token = node
        .publish_blob(&file_path, "input.bin")
        .await
        .expect("publish_blob failed");

    assert_eq!(token.size_bytes, 24, "file is 24 bytes");
    assert!(!token.hash.as_hex().is_empty(), "hash must be populated");
}

#[tokio::test]
async fn test_publish_blob_creates_sidecar_on_disk() {
    let tmp = tempfile::tempdir().unwrap();
    let blob_work_dir = tmp.path().join("blobs");
    let node = test_node_at(tmp.path()).await;

    let file_path = tmp.path().join("input.bin");
    std::fs::write(&file_path, b"File content for testing").unwrap();
    let token = node.publish_blob(&file_path, "input.bin").await.unwrap();

    let sidecar = blob_work_dir.join(format!("{}.meta.json", token.hash.as_hex()));
    assert!(
        sidecar.exists(),
        "sidecar {} must exist after publish_blob (required for BLOB-02 startup re-import)",
        sidecar.display()
    );
}

#[tokio::test]
async fn test_blob_reimport_across_restart() {
    // Use a persistent root dir for blob_work_dir (the part that must survive restart),
    // but separate data_dirs for each SidecarNode instance to avoid redb lock contention
    // (AutomergeStore opens an exclusive redb lock on data_dir).
    let root = tempfile::tempdir().unwrap();
    let blob_work_dir = root.path().join("blobs");

    // --- Session 1: publish a blob ---
    let published_hash_hex = {
        let data_dir1 = root.path().join("node1");
        std::fs::create_dir_all(&data_dir1).unwrap();
        let node = SidecarNode::new(SidecarConfig {
            node_id: "test-node-1".to_string(),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: data_dir1,
            peers: vec![],
            encryption_key: None,
            enable_deployer: false,
            blob_work_dir: blob_work_dir.clone(),
            download_timeout_secs: 30,
        })
        .await
        .expect("failed to create node1");

        let file_path = root.path().join("input.bin");
        std::fs::write(&file_path, b"File content for testing").unwrap();
        let token = node.publish_blob(&file_path, "input.bin").await.unwrap();
        token.hash.as_hex().to_string()
    }; // node1 dropped here — redb lock released

    // Sanity: sidecar survived the drop
    let sidecar = blob_work_dir.join(format!("{}.meta.json", &published_hash_hex));
    assert!(sidecar.exists(), "sidecar must persist on disk after node drop");

    // --- Session 2: new SidecarNode at the SAME blob_work_dir should re-import ---
    // Use a fresh data_dir (simulates a process restart with persistent blob storage)
    let data_dir2 = root.path().join("node2");
    std::fs::create_dir_all(&data_dir2).unwrap();
    let node2 = SidecarNode::new(SidecarConfig {
        node_id: "test-node-2".to_string(),
        app_id: "test".to_string(),
        shared_key: String::new(),
        data_dir: data_dir2,
        peers: vec![],
        encryption_key: None,
        enable_deployer: false,
        blob_work_dir: blob_work_dir.clone(),
        download_timeout_secs: 30,
    })
    .await
    .expect("failed to create node2");

    let tokens = node2.list_local_blobs();
    let found = tokens.iter().any(|t| t.hash.as_hex() == published_hash_hex);
    assert!(
        found,
        "BLOB-02: after restart, list_local_blobs must include the previously published blob {}",
        published_hash_hex
    );
}
