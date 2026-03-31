//! Integration tests for SidecarNode — CRDT document operations.

use peat_sidecar::node::{SidecarConfig, SidecarNode};
use std::path::PathBuf;

async fn test_node(dir: &std::path::Path) -> SidecarNode {
    SidecarNode::new(SidecarConfig {
        node_id: "test-node".to_string(),
        app_id: "test".to_string(),
        shared_key: String::new(),
        data_dir: dir.to_path_buf(),
        peers: vec![],
    })
    .await
    .expect("failed to create node")
}

#[tokio::test]
async fn put_get_document() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    node.put_document("test", "doc-1", r#"{"hello":"world"}"#)
        .await
        .unwrap();

    let result = node.get_document("test", "doc-1").await.unwrap();
    assert_eq!(result, Some(r#"{"hello":"world"}"#.to_string()));
}

#[tokio::test]
async fn get_missing_document_returns_none() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    let result = node.get_document("test", "nonexistent").await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn delete_document() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    node.put_document("test", "doc-1", r#"{"a":1}"#)
        .await
        .unwrap();
    node.delete_document("test", "doc-1").await.unwrap();

    let result = node.get_document("test", "doc-1").await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn list_documents() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    node.put_document("col", "a", r#"{"x":1}"#).await.unwrap();
    node.put_document("col", "b", r#"{"x":2}"#).await.unwrap();
    node.put_document("other", "c", r#"{"x":3}"#)
        .await
        .unwrap();

    let mut ids = node.list_documents("col").await.unwrap();
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);

    let other_ids = node.list_documents("other").await.unwrap();
    assert_eq!(other_ids, vec!["c"]);
}

#[tokio::test]
async fn overwrite_document() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    node.put_document("test", "doc-1", r#"{"v":1}"#)
        .await
        .unwrap();
    node.put_document("test", "doc-1", r#"{"v":2}"#)
        .await
        .unwrap();

    let result = node.get_document("test", "doc-1").await.unwrap();
    assert_eq!(result, Some(r#"{"v":2}"#.to_string()));
}

#[tokio::test]
async fn invalid_json_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    let result = node
        .put_document("test", "bad", "not valid json")
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn node_status() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    assert_eq!(node.node_id(), "test-node");
    assert!(!node.endpoint_addr().is_empty());
    assert_eq!(node.connected_peer_count(), 0);
}

#[tokio::test]
async fn subscribe_receives_changes() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    let mut rx = node.subscribe();

    node.put_document("test", "doc-1", r#"{"a":1}"#)
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("recv error");

    assert_eq!(event.collection, "test");
    assert_eq!(event.doc_id, "doc-1");
}
