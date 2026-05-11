//! Integration tests for SidecarNode — CRDT document operations.

use peat_node::node::{SidecarConfig, SidecarNode};

async fn test_node(dir: &std::path::Path) -> SidecarNode {
    test_node_with_encryption(dir, None).await
}

async fn test_node_with_encryption(
    dir: &std::path::Path,
    encryption_key: Option<String>,
) -> SidecarNode {
    SidecarNode::new(SidecarConfig {
        node_id: "test-node".to_string(),
        app_id: "test".to_string(),
        shared_key: String::new(),
        data_dir: dir.to_path_buf(),
        peers: vec![],
        encryption_key,
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
    node.put_document("other", "c", r#"{"x":3}"#).await.unwrap();

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

    let result = node.put_document("test", "bad", "not valid json").await;
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
async fn sync_stats_default_zero_on_fresh_node() {
    // Fresh single-node, no peers, no sync activity: counters must read
    // exactly zero. Guards the default behavior against accidental
    // pre-population, and pins the contract that bytes_sent / bytes_received
    // come from the sync coordinator (not from synthetic / hardcoded values).
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    let stats = node.sync_stats();
    assert!(!stats.sync_active);
    assert_eq!(stats.connected_peers, 0);
    assert_eq!(stats.bytes_sent, 0);
    assert_eq!(stats.bytes_received, 0);
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

// --- Encryption at rest tests ---

fn test_encryption_key() -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode([0x42u8; 32])
}

#[tokio::test]
async fn encrypted_put_get_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node_with_encryption(dir.path(), Some(test_encryption_key())).await;

    node.put_document("secure", "doc-1", r#"{"secret":"data"}"#)
        .await
        .unwrap();

    let result = node.get_document("secure", "doc-1").await.unwrap();
    assert_eq!(result, Some(r#"{"secret":"data"}"#.to_string()));
}

#[tokio::test]
async fn encrypted_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node_with_encryption(dir.path(), Some(test_encryption_key())).await;

    node.put_document("secure", "doc-1", r#"{"v":1}"#)
        .await
        .unwrap();
    node.put_document("secure", "doc-1", r#"{"v":2}"#)
        .await
        .unwrap();

    let result = node.get_document("secure", "doc-1").await.unwrap();
    assert_eq!(result, Some(r#"{"v":2}"#.to_string()));
}

#[tokio::test]
async fn encrypted_delete() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node_with_encryption(dir.path(), Some(test_encryption_key())).await;

    node.put_document("secure", "doc-1", r#"{"a":1}"#)
        .await
        .unwrap();
    node.delete_document("secure", "doc-1").await.unwrap();

    let result = node.get_document("secure", "doc-1").await.unwrap();
    assert_eq!(result, None);
}

#[tokio::test]
async fn encrypted_list_documents() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node_with_encryption(dir.path(), Some(test_encryption_key())).await;

    node.put_document("enc", "a", r#"{"x":1}"#).await.unwrap();
    node.put_document("enc", "b", r#"{"x":2}"#).await.unwrap();

    let mut ids = node.list_documents("enc").await.unwrap();
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);
}

#[tokio::test]
async fn encrypted_subscribe_decrypts_events() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node_with_encryption(dir.path(), Some(test_encryption_key())).await;

    let mut rx = node.subscribe();

    node.put_document("secure", "doc-1", r#"{"secret":"value"}"#)
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("recv error");

    assert_eq!(event.collection, "secure");
    assert_eq!(event.doc_id, "doc-1");
    // The event's json_data from the local write path contains the plaintext
    // (it's emitted before going through the store observer)
    assert!(event.json_data.is_some());
    let data = event.json_data.unwrap();
    assert!(
        data.contains("secret"),
        "expected decrypted JSON, got: {data}"
    );
}

#[tokio::test]
async fn encrypted_data_is_opaque_in_store() {
    use peat_node::crypto::StoreCipher;

    let cipher = StoreCipher::from_base64_key(&test_encryption_key()).unwrap();

    // Encrypt a payload the same way put_document does
    let original = r#"{"secret":"classified"}"#;
    let encrypted = cipher.encrypt(original).unwrap();

    // The stored value is opaque — no plaintext visible
    assert!(
        encrypted.starts_with("ENC:v1:"),
        "expected ENC:v1: prefix, got: {encrypted}"
    );
    assert!(
        !encrypted.contains("classified"),
        "plaintext should not appear in encrypted data"
    );

    // Decrypt recovers the original
    let decrypted = cipher.decrypt(&encrypted).unwrap();
    assert_eq!(decrypted, original);
}

#[tokio::test]
async fn wrong_encryption_key_fails_to_decrypt() {
    use base64::Engine;
    use peat_node::crypto::StoreCipher;

    let cipher1 = StoreCipher::from_base64_key(&test_encryption_key()).unwrap();
    let encrypted = cipher1.encrypt(r#"{"secret":"data"}"#).unwrap();

    // A different key cannot decrypt
    let wrong_key = base64::engine::general_purpose::STANDARD.encode([0xFFu8; 32]);
    let cipher2 = StoreCipher::from_base64_key(&wrong_key).unwrap();
    assert!(
        cipher2.decrypt(&encrypted).is_err(),
        "should fail to decrypt with wrong key"
    );
}
