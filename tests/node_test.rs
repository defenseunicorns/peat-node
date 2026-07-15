//! Integration tests for SidecarNode — CRDT document operations.

use peat_node::nats_bridge::envelope::BridgeEnvelope;
use peat_node::node::{CreateBridgeDocumentError, SidecarConfig, SidecarNode};

async fn test_node(dir: &std::path::Path) -> SidecarNode {
    test_node_with_encryption(dir, None).await
}

async fn test_node_with_encryption(
    dir: &std::path::Path,
    encryption_key: Option<String>,
) -> SidecarNode {
    SidecarNode::new(SidecarConfig {
        blob_stall_timeout: None,
        node_id: "test-node".to_string(),
        app_id: "test".to_string(),
        shared_key: String::new(),
        data_dir: dir.to_path_buf(),
        peers: vec![],
        encryption_key,
        iroh_udp_port: None,
        iroh_secret_key: None,
        attachment_config: Default::default(),
        disable_mdns: true,
        tombstone_ttl_hours: None,
        gc_interval_secs: None,
        gc_batch_size: None,
        ..Default::default()
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
    // exactly zero. Pins the default behavior; the live "counters
    // increment under real sync" guard is in `tests/sync_test.rs`
    // (in-process two-node) and `tests/sync_subprocess_test.rs`
    // (two real binaries).
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

#[tokio::test]
async fn subscribe_change_event_includes_json_data() {
    // Regression for peat-node#7: after switching put_document to structured
    // Automerge storage (no {"value":"<json>"} wrapper), forward_store_changes
    // must use the same two-format fallback as get_document — otherwise
    // json_data is None for all gRPC-written docs.
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;
    let mut rx = node.subscribe();

    node.put_document("test", "doc-1", r#"{"name":"alice"}"#)
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("recv error");

    let json_data = event
        .json_data
        .expect("json_data must be present for gRPC writes");
    let v: serde_json::Value = serde_json::from_str(&json_data).unwrap();
    assert_eq!(v["name"], "alice");
}

#[tokio::test]
async fn structured_doc_with_value_field_not_corrupted() {
    // Regression for peat-node#7 (blocker): a user document that happens to
    // have a top-level "value":"<string>" field must survive a put/get round-
    // trip intact. Before the is_encrypted() gate, get_document extracted only
    // the inner string and dropped all other fields.
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;

    node.put_document("test", "d", r#"{"value":"hello","name":"alice"}"#)
        .await
        .unwrap();

    let result = node.get_document("test", "d").await.unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(v["value"], "hello", "value field must be preserved");
    assert_eq!(v["name"], "alice", "name field must not be dropped");
}

#[tokio::test]
async fn subscribe_change_event_value_field_not_corrupted() {
    // Regression for peat-node#7 (blocker): the same is_encrypted() gate must
    // hold in forward_store_changes — a doc with "value":"..." must arrive with
    // all fields intact in the change event's json_data.
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;
    let mut rx = node.subscribe();

    node.put_document("test", "d", r#"{"value":"hello","name":"alice"}"#)
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("recv error");

    let json_data = event.json_data.expect("json_data must be present");
    let v: serde_json::Value = serde_json::from_str(&json_data).unwrap();
    assert_eq!(v["value"], "hello", "value field must be preserved");
    assert_eq!(v["name"], "alice", "name field must not be dropped");
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

fn bridge_envelope(payload: &str) -> String {
    serde_json::to_string(
        &BridgeEnvelope::from_payload("vision.summary", "test-node", payload.as_bytes())
            .expect("test payload should be valid"),
    )
    .expect("envelope should serialize")
}

fn assert_bridge_payload(stored: &str, expected_payload: &str) {
    let envelope: BridgeEnvelope = serde_json::from_str(stored).expect("stored bridge envelope");
    assert_eq!(envelope.payload.as_bytes(), expected_payload.as_bytes());
}

#[tokio::test]
async fn bridge_create_plain_round_trip_preserves_payload_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;
    let payload = "  {\"value\":1.0,\"label\":\"λ\"}\n ";

    node.create_bridge_document("frames", "bridge-plain", &bridge_envelope(payload))
        .await
        .unwrap();

    let stored = node
        .get_document("frames", "bridge-plain")
        .await
        .unwrap()
        .expect("bridge document should exist");
    assert_bridge_payload(&stored, payload);
}

#[tokio::test]
async fn bridge_create_encrypted_round_trip_preserves_payload_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node_with_encryption(dir.path(), Some(test_encryption_key())).await;
    let payload = r#"{"label":"\u03bb","value":1.0} "#;

    node.create_bridge_document(
        "secure-frames",
        "bridge-encrypted",
        &bridge_envelope(payload),
    )
    .await
    .unwrap();

    let stored = node
        .get_document("secure-frames", "bridge-encrypted")
        .await
        .unwrap()
        .expect("encrypted bridge document should exist");
    assert_bridge_payload(&stored, payload);
}

#[tokio::test]
async fn bridge_create_collision_preserves_first_document() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;
    let first = bridge_envelope(r#"{"frame":1}"#);
    let replacement = bridge_envelope(r#"{"frame":2}"#);

    node.create_bridge_document("frames", "same-id", &first)
        .await
        .unwrap();
    let error = node
        .create_bridge_document("frames", "same-id", &replacement)
        .await
        .expect_err("create-only write must reject collisions");

    assert_eq!(error, CreateBridgeDocumentError::AlreadyExists);
    let stored = node
        .get_document("frames", "same-id")
        .await
        .unwrap()
        .expect("first document should remain");
    assert_bridge_payload(&stored, r#"{"frame":1}"#);
}

#[tokio::test]
async fn bridge_create_emits_exactly_one_observer_event() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;
    let mut rx = node.subscribe();
    let envelope = bridge_envelope(r#"{"frame":1}"#);

    node.create_bridge_document("frames", "one-event", &envelope)
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("observer event timeout")
        .expect("observer event receive error");
    assert_eq!(event.collection, "frames");
    assert_eq!(event.doc_id, "one-event");
    assert!(matches!(
        event.change_type,
        peat_node::node::ChangeType::Upsert
    ));
    assert_bridge_payload(
        event
            .json_data
            .as_deref()
            .expect("event should contain JSON"),
        r#"{"frame":1}"#,
    );

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .is_err(),
        "one create must not emit a duplicate observer event"
    );
}

#[tokio::test]
async fn bridge_create_errors_are_fixed_and_payload_safe() {
    let dir = tempfile::tempdir().unwrap();
    let node = test_node(dir.path()).await;
    let secret_payload = "secret-parser-excerpt";

    let invalid = node
        .create_bridge_document("frames", "bad", secret_payload)
        .await
        .expect_err("malformed envelope should fail");
    assert_eq!(invalid, CreateBridgeDocumentError::InvalidInput);

    let envelope = bridge_envelope(r#"{"frame":1}"#);
    node.create_bridge_document("frames", "collision", &envelope)
        .await
        .unwrap();
    let collision = node
        .create_bridge_document("frames", "collision", &envelope)
        .await
        .expect_err("collision should fail");
    assert_eq!(collision, CreateBridgeDocumentError::AlreadyExists);

    let rendered = format!("{invalid:?} {invalid} {collision:?} {collision}");
    for forbidden in [
        secret_payload,
        "{\"frame\":1}",
        "ENC:v1:",
        "/tmp/",
        "caused by",
    ] {
        assert!(
            !rendered.contains(forbidden),
            "unsafe error text: {rendered}"
        );
    }
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

/// The whole point of deterministic identity (peat-node#63 gap-4d): the
/// `EndpointId` a node actually presents on the wire must equal what any holder
/// of the shared key computes offline from `(shared_key, node_id)` via
/// `derive-id`. If these ever diverge, a peer that pre-fills `PEAT_NODE_PEERS`
/// with the derived id would fail to authenticate the QUIC handshake.
#[tokio::test]
async fn deterministic_identity_matches_offline_derivation() {
    // Base64 of 32 bytes of 0x2a — same shape as the compose examples' key.
    const KEY: &str = "KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio=";

    let seed = peat_node::identity::derive_iroh_secret_seed(KEY, "node-a")
        .expect("valid base64 key")
        .expect("non-empty key yields a seed");

    let dir = tempfile::tempdir().unwrap();
    let node = SidecarNode::new(SidecarConfig {
        node_id: "node-a".to_string(),
        app_id: "test".to_string(),
        shared_key: KEY.to_string(),
        data_dir: dir.path().to_path_buf(),
        iroh_secret_key: Some(seed),
        disable_mdns: true,
        ..Default::default()
    })
    .await
    .expect("node boot");

    let expected = peat_node::identity::derive_endpoint_id(KEY, "node-a").unwrap();
    assert_eq!(
        node.endpoint_addr(),
        expected,
        "the booted node's wire identity must match the offline-derived id"
    );
}
