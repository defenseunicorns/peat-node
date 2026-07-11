//! Subscribe coverage — multi-subscriber fanout and DELETE event
//! delivery. Filter-by-collection is exercised via the in-process
//! service surface so we don't have to build a Connect streaming
//! consumer just for this.
//!
//! Mirrors the deleted Go `functest` Phase 1 subscribe tests.

use std::time::Duration;

use peat_node::node::{ChangeType, SidecarConfig, SidecarNode};

async fn fresh_node() -> SidecarNode {
    let dir = tempfile::tempdir().unwrap();
    SidecarNode::new(SidecarConfig {
        blob_stall_timeout: None,
        node_id: "test-sub".to_string(),
        app_id: "test".to_string(),
        shared_key: String::new(),
        data_dir: dir.keep(),
        peers: vec![],
        encryption_key: None,
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
    .expect("boot node")
}

#[tokio::test]
async fn delete_event_is_delivered_to_subscribers() {
    let node = fresh_node().await;
    let mut rx = node.subscribe();

    node.put_document("col", "doc-1", r#"{"v":1}"#)
        .await
        .unwrap();
    node.delete_document("col", "doc-1").await.unwrap();

    // Collect events until we observe the DELETE for doc-1.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut saw_delete = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(event)) => {
                if event.collection == "col"
                    && event.doc_id == "doc-1"
                    && matches!(event.change_type, ChangeType::Delete)
                {
                    saw_delete = true;
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => break,
        }
    }
    assert!(saw_delete, "expected a DELETE event for col/doc-1");
}

#[tokio::test]
async fn multiple_subscribers_receive_same_events() {
    let node = fresh_node().await;
    let mut rx1 = node.subscribe();
    let mut rx2 = node.subscribe();

    node.put_document("multi", "doc-1", r#"{"a":1}"#)
        .await
        .unwrap();

    let event1 = tokio::time::timeout(Duration::from_secs(1), rx1.recv())
        .await
        .expect("rx1 timeout")
        .expect("rx1 recv");
    let event2 = tokio::time::timeout(Duration::from_secs(1), rx2.recv())
        .await
        .expect("rx2 timeout")
        .expect("rx2 recv");

    assert_eq!(event1.collection, "multi");
    assert_eq!(event1.doc_id, "doc-1");
    assert_eq!(event2.collection, "multi");
    assert_eq!(event2.doc_id, "doc-1");
    assert!(matches!(event1.change_type, ChangeType::Upsert));
    assert!(matches!(event2.change_type, ChangeType::Upsert));
}

#[tokio::test]
async fn subscriber_receives_events_for_multiple_collections() {
    // Documents writes nudge the broadcast channel regardless of
    // collection; service-layer filtering is what narrows the stream
    // for a particular subscriber. Here we verify the channel itself
    // is collection-agnostic — subscribers see events for *every*
    // collection they don't filter out.
    let node = fresh_node().await;
    let mut rx = node.subscribe();

    node.put_document("alpha", "a1", r#"{"x":1}"#)
        .await
        .unwrap();
    node.put_document("bravo", "b1", r#"{"x":2}"#)
        .await
        .unwrap();

    let mut collections = std::collections::BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while collections.len() < 2 && tokio::time::Instant::now() < deadline {
        if let Ok(Ok(event)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            collections.insert(event.collection);
        } else {
            break;
        }
    }
    assert!(
        collections.contains("alpha") && collections.contains("bravo"),
        "subscriber missed events: {collections:?}"
    );
}

#[tokio::test]
async fn rapid_writes_no_dropped_events() {
    let node = fresh_node().await;
    let mut rx = node.subscribe();

    let n = 50;
    for i in 0..n {
        node.put_document("rapid", &format!("doc-{i}"), &format!(r#"{{"i":{i}}}"#))
            .await
            .unwrap();
    }

    let mut received = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while received < n && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(event)) if event.collection == "rapid" => received += 1,
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert_eq!(received, n, "expected {n} events, got {received}");
}

#[tokio::test]
async fn overwrites_produce_events() {
    let node = fresh_node().await;
    let mut rx = node.subscribe();

    for v in 0..5 {
        node.put_document("ow", "same-doc", &format!(r#"{{"v":{v}}}"#))
            .await
            .unwrap();
    }

    let mut count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while count < 5 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(event)) if event.doc_id == "same-doc" => count += 1,
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert_eq!(count, 5, "each overwrite must produce an event");
}

#[tokio::test]
async fn subscriber_sees_json_data_in_events() {
    let node = fresh_node().await;
    let mut rx = node.subscribe();

    node.put_document("json", "d1", r#"{"key":"value"}"#)
        .await
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("timeout")
        .expect("recv");

    assert_eq!(event.collection, "json");
    assert_eq!(event.doc_id, "d1");
    let json = event.json_data.expect("event must carry json_data");
    assert!(
        json.contains(r#""key":"value""#) || json.contains(r#""key": "value""#),
        "json_data must contain the written payload, got: {json}"
    );
}

#[tokio::test]
async fn subscriber_receives_writes_after_subscribe() {
    let node = fresh_node().await;

    node.put_document("pre", "existing", r#"{"v":1}"#)
        .await
        .unwrap();

    // Let forward_store_changes drain pre-existing events.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut rx = node.subscribe();

    node.put_document("live", "new-doc", r#"{"v":2}"#)
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut saw_live = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(event)) if event.doc_id == "new-doc" => {
                saw_live = true;
                assert_eq!(event.collection, "live");
                assert!(matches!(event.change_type, ChangeType::Upsert));
                break;
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(saw_live, "subscriber must see writes after subscribe");
}

#[tokio::test]
async fn many_collections_all_events_delivered() {
    let node = fresh_node().await;
    let mut rx = node.subscribe();

    let n_collections = 30;
    for c in 0..n_collections {
        node.put_document(
            &format!("fleet/drone-{c}/telemetry"),
            "sensor-0",
            &format!(r#"{{"c":{c}}}"#),
        )
        .await
        .unwrap();
    }

    let mut seen_collections = std::collections::BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while seen_collections.len() < n_collections && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Ok(event)) if event.collection.starts_with("fleet/") => {
                seen_collections.insert(event.collection);
            }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert_eq!(
        seen_collections.len(),
        n_collections,
        "expected events from {n_collections} collections, got {}",
        seen_collections.len()
    );
}
