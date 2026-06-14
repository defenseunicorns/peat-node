//! End-to-end subscribe-with-query tests.
//!
//! Drives the `Subscribe` RPC through the in-process service implementation
//! (the same code path Connect/gRPC clients hit) so the wire-level filter
//! semantics are exercised — not just the matcher unit tests in
//! `src/query.rs`.
//!
//! Why the in-process service surface instead of an HTTP client:
//! Connect server-streaming with raw `reqwest` requires hand-decoding the
//! Connect envelope frames. The service-trait surface is what
//! `connectrpc::Server` dispatches to, so testing through it covers the
//! same filtering logic without the framing overhead.

use std::sync::Arc;
use std::time::Duration;

use buffa::OwnedView;
use connectrpc::{Context, ErrorCode};
use futures::StreamExt;
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::{
    self, subscription_query::Q, PeatSidecar, QueryAll, QueryAnd, QueryCmp, QueryEq, QueryOr,
    SubscribeRequest, SubscriptionQuery,
};
use peat_node::service::PeatSidecarService;

// --- Helpers ---

async fn fresh_service() -> (Arc<SidecarNode>, PeatSidecarService) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: "test-sub-query".to_string(),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: None,
            attachment_config: Default::default(),
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
            ..Default::default()
        })
        .await
        .expect("boot node"),
    );
    let service = PeatSidecarService::new(Arc::clone(&node));
    (node, service)
}

fn pq(q: Q) -> SubscriptionQuery {
    SubscriptionQuery {
        q: Some(q),
        ..Default::default()
    }
}

fn pq_eq(field: &str, value: &str) -> SubscriptionQuery {
    pq(Q::Eq(Box::new(QueryEq {
        field: field.into(),
        value: value.into(),
        ..Default::default()
    })))
}

fn pq_gt(field: &str, value: &str) -> SubscriptionQuery {
    pq(Q::Gt(Box::new(QueryCmp {
        field: field.into(),
        value: value.into(),
        ..Default::default()
    })))
}

fn pq_and(clauses: Vec<SubscriptionQuery>) -> SubscriptionQuery {
    pq(Q::And(Box::new(QueryAnd {
        clauses,
        ..Default::default()
    })))
}

fn pq_or(clauses: Vec<SubscriptionQuery>) -> SubscriptionQuery {
    pq(Q::Or(Box::new(QueryOr {
        clauses,
        ..Default::default()
    })))
}

fn pq_all() -> SubscriptionQuery {
    pq(Q::All(Box::<QueryAll>::default()))
}

fn subscribe_view(req: SubscribeRequest) -> OwnedView<pb::SubscribeRequestView<'static>> {
    OwnedView::from_owned(&req).expect("encode/decode subscribe request")
}

/// Collect change events for up to `total_budget`. Bails as soon as
/// `expected` events arrive; otherwise returns whatever it has when
/// the budget expires. Per-recv timeout is short so a quiet stream
/// doesn't waste the full budget.
///
/// Counting raw events (not unique triples) is correct because
/// `SidecarNode` emits exactly one `ChangeEvent` per local write —
/// see the comment in `node.rs::put_document` for why the direct
/// notification was removed in favor of the `forward_store_changes`
/// path. If that invariant ever breaks, these tests will start
/// counting duplicates and fail loudly, which is the desired
/// behavior.
async fn collect(
    stream: &mut (impl futures::Stream<Item = Result<pb::DocumentChange, connectrpc::ConnectError>>
              + Unpin),
    expected: usize,
    total_budget: Duration,
) -> Vec<pb::DocumentChange> {
    let deadline = tokio::time::Instant::now() + total_budget;
    let mut events = Vec::new();
    while events.len() < expected && tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let per_recv = remaining.min(Duration::from_millis(200));
        match tokio::time::timeout(per_recv, stream.next()).await {
            Ok(Some(Ok(change))) => events.push(change),
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {} // timeout — try again until total_budget expires
        }
    }
    events
}

async fn put(node: &SidecarNode, collection: &str, doc_id: &str, json: &str) {
    node.put_document(collection, doc_id, json)
        .await
        .expect("put_document");
}

// --- Tests ---

#[tokio::test]
async fn no_query_streams_every_collection_event() {
    // Backward compatibility: a SubscribeRequest with no query and no
    // collection filter must keep delivering every change exactly as
    // before this feature landed.
    let (node, service) = fresh_service().await;

    let view = subscribe_view(SubscribeRequest::default());
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), view)
        .await
        .expect("subscribe");

    put(&node, "alpha", "a1", r#"{"x":1}"#).await;
    put(&node, "bravo", "b1", r#"{"x":2}"#).await;

    let events = collect(&mut stream, 2, Duration::from_secs(2)).await;
    let collections: std::collections::BTreeSet<_> =
        events.iter().map(|e| e.collection.as_str()).collect();
    assert!(
        collections.contains("alpha") && collections.contains("bravo"),
        "expected events from both collections, got {collections:?}"
    );
}

#[tokio::test]
async fn eq_query_filters_to_matching_documents() {
    // The core customer scenario: Eq filter over a field should drop
    // non-matching upserts at the service layer.
    let (node, service) = fresh_service().await;

    let req = SubscribeRequest {
        collections: vec!["nodes".to_string()],
        query: buffa::MessageField::some(pq_eq("node_type", "\"vehicle\"")),
        ..Default::default()
    };
    let view = subscribe_view(req);
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), view)
        .await
        .expect("subscribe");

    // Three docs: 2 match, 1 doesn't.
    put(
        &node,
        "nodes",
        "p1",
        r#"{"node_type":"vehicle","name":"v1"}"#,
    )
    .await;
    put(
        &node,
        "nodes",
        "p2",
        r#"{"node_type":"aircraft","name":"a1"}"#,
    )
    .await;
    put(
        &node,
        "nodes",
        "p3",
        r#"{"node_type":"vehicle","name":"v2"}"#,
    )
    .await;

    let events = collect(&mut stream, 2, Duration::from_secs(2)).await;
    let doc_ids: std::collections::BTreeSet<_> = events.iter().map(|e| e.doc_id.as_str()).collect();
    assert_eq!(
        doc_ids,
        ["p1", "p3"].into_iter().collect(),
        "expected only the two vehicle documents; got {doc_ids:?}"
    );
    // And the aircraft document must not appear in any window we accept.
    assert!(
        !events.iter().any(|e| e.doc_id == "p2"),
        "aircraft document leaked past the filter"
    );
}

#[tokio::test]
async fn collection_filter_and_query_compose() {
    // The collection allowlist and the query predicate AND together.
    // A document matching the query but in the wrong collection must
    // not be delivered.
    let (node, service) = fresh_service().await;

    let req = SubscribeRequest {
        collections: vec!["nodes".to_string()],
        query: buffa::MessageField::some(pq_eq("status", "\"ready\"")),
        ..Default::default()
    };
    let view = subscribe_view(req);
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), view)
        .await
        .expect("subscribe");

    // Matches collection + predicate.
    put(&node, "nodes", "p1", r#"{"status":"ready"}"#).await;
    // Matches predicate but wrong collection.
    put(&node, "tracks", "t1", r#"{"status":"ready"}"#).await;
    // Right collection but doesn't match predicate.
    put(&node, "nodes", "p2", r#"{"status":"busy"}"#).await;

    let events = collect(&mut stream, 1, Duration::from_secs(2)).await;
    assert_eq!(
        events.len(),
        1,
        "expected exactly one match, got {events:?}"
    );
    assert_eq!(events[0].collection, "nodes");
    assert_eq!(events[0].doc_id, "p1");
}

#[tokio::test]
async fn delete_events_pass_through_query_filter() {
    // A DELETE has no payload to evaluate. The matcher passes it through
    // so subscribers learn the document is gone instead of holding a
    // stale entry. The collection filter still applies.
    let (node, service) = fresh_service().await;

    let req = SubscribeRequest {
        collections: vec!["nodes".to_string()],
        query: buffa::MessageField::some(pq_eq("node_type", "\"vehicle\"")),
        ..Default::default()
    };
    let view = subscribe_view(req);
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), view)
        .await
        .expect("subscribe");

    put(&node, "nodes", "p1", r#"{"node_type":"vehicle"}"#).await;
    node.delete_document("nodes", "p1")
        .await
        .expect("delete_document");

    let events = collect(&mut stream, 2, Duration::from_secs(2)).await;
    let saw_upsert = events
        .iter()
        .any(|e| e.doc_id == "p1" && e.change_type == pb::ChangeType::CHANGE_TYPE_UPSERT);
    let saw_delete = events
        .iter()
        .any(|e| e.doc_id == "p1" && e.change_type == pb::ChangeType::CHANGE_TYPE_DELETE);
    assert!(saw_upsert, "missed the upsert: {events:?}");
    assert!(
        saw_delete,
        "missed the delete despite query filter: {events:?}"
    );
}

#[tokio::test]
async fn and_combinator_filters_correctly() {
    // (node_type == "vehicle") AND (readiness > 0.5)
    let (node, service) = fresh_service().await;

    let req = SubscribeRequest {
        collections: vec!["nodes".to_string()],
        query: buffa::MessageField::some(pq_and(vec![
            pq_eq("node_type", "\"vehicle\""),
            pq_gt("readiness", "0.5"),
        ])),
        ..Default::default()
    };
    let view = subscribe_view(req);
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), view)
        .await
        .expect("subscribe");

    // Matches both clauses.
    put(
        &node,
        "nodes",
        "p1",
        r#"{"node_type":"vehicle","readiness":0.9}"#,
    )
    .await;
    // Matches type, not readiness.
    put(
        &node,
        "nodes",
        "p2",
        r#"{"node_type":"vehicle","readiness":0.2}"#,
    )
    .await;
    // Matches readiness, not type.
    put(
        &node,
        "nodes",
        "p3",
        r#"{"node_type":"aircraft","readiness":0.9}"#,
    )
    .await;

    let events = collect(&mut stream, 1, Duration::from_secs(2)).await;
    let doc_ids: std::collections::BTreeSet<_> = events.iter().map(|e| e.doc_id.as_str()).collect();
    assert_eq!(
        doc_ids,
        ["p1"].into_iter().collect(),
        "expected only p1; got {doc_ids:?}"
    );
}

#[tokio::test]
async fn or_combinator_filters_correctly() {
    // category == "a" OR category == "b"
    let (node, service) = fresh_service().await;

    let req = SubscribeRequest {
        collections: vec!["tags".to_string()],
        query: buffa::MessageField::some(pq_or(vec![
            pq_eq("category", "\"a\""),
            pq_eq("category", "\"b\""),
        ])),
        ..Default::default()
    };
    let view = subscribe_view(req);
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), view)
        .await
        .expect("subscribe");

    put(&node, "tags", "t1", r#"{"category":"a"}"#).await;
    put(&node, "tags", "t2", r#"{"category":"b"}"#).await;
    put(&node, "tags", "t3", r#"{"category":"c"}"#).await;

    let events = collect(&mut stream, 2, Duration::from_secs(2)).await;
    let doc_ids: std::collections::BTreeSet<_> = events.iter().map(|e| e.doc_id.as_str()).collect();
    assert_eq!(
        doc_ids,
        ["t1", "t2"].into_iter().collect(),
        "expected t1 and t2; got {doc_ids:?}"
    );
}

#[tokio::test]
async fn query_all_passes_every_upsert() {
    // SubscriptionQuery::All is the explicit "no filter" variant — the
    // wire encoding for callers that want to be explicit instead of
    // omitting the field. Should behave identically to omitting it.
    let (node, service) = fresh_service().await;

    let req = SubscribeRequest {
        query: buffa::MessageField::some(pq_all()),
        ..Default::default()
    };
    let view = subscribe_view(req);
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), view)
        .await
        .expect("subscribe");

    put(&node, "alpha", "a1", r#"{"x":1}"#).await;
    put(&node, "bravo", "b1", r#"{"x":2}"#).await;

    let events = collect(&mut stream, 2, Duration::from_secs(2)).await;
    let collections: std::collections::BTreeSet<_> =
        events.iter().map(|e| e.collection.as_str()).collect();
    assert!(
        collections.contains("alpha") && collections.contains("bravo"),
        "QueryAll dropped events: {collections:?}"
    );
}

#[tokio::test]
async fn empty_query_oneof_is_invalid_argument() {
    // A SubscriptionQuery with no variant set is malformed — surface it
    // as InvalidArgument so callers see a clear error rather than a
    // silently empty stream.
    let (_node, service) = fresh_service().await;

    let req = SubscribeRequest {
        query: buffa::MessageField::some(SubscriptionQuery {
            q: None,
            ..Default::default()
        }),
        ..Default::default()
    };
    let view = subscribe_view(req);
    match service.subscribe(Context::default(), view).await {
        Ok(_) => panic!("expected InvalidArgument; got Ok"),
        Err(err) => assert_eq!(
            err.code,
            ErrorCode::InvalidArgument,
            "expected InvalidArgument, got {err:?}"
        ),
    }
}

// --- Initial snapshot tests (peat-node#55) ---

#[tokio::test]
async fn subscribe_initial_snapshot_includes_existing_docs() {
    // Docs written BEFORE subscribe must arrive as the initial snapshot
    // before any live updates (peat-node#55 acceptance criteria).
    let (node, service) = fresh_service().await;

    // Write 3 docs to a collection before subscribing.
    put(&node, "nodes", "n1", r#"{"node_type":"vehicle"}"#).await;
    put(&node, "nodes", "n2", r#"{"node_type":"aircraft"}"#).await;
    put(&node, "nodes", "n3", r#"{"node_type":"vehicle"}"#).await;

    let req = SubscribeRequest {
        collections: vec!["nodes".to_string()],
        ..Default::default()
    };
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), subscribe_view(req))
        .await
        .expect("subscribe");

    // Expect the 3 snapshot events; they should arrive without any writes.
    let events = collect(&mut stream, 3, Duration::from_secs(2)).await;
    assert_eq!(
        events.len(),
        3,
        "expected 3 snapshot events; got {events:?}"
    );

    let mut doc_ids: Vec<_> = events.iter().map(|e| e.doc_id.clone()).collect();
    doc_ids.sort();
    assert_eq!(doc_ids, ["n1", "n2", "n3"]);
}

#[tokio::test]
async fn subscribe_initial_snapshot_filtered_by_query() {
    // The initial snapshot must respect the Eq query — only matching
    // docs arrive, not the whole collection.
    let (node, service) = fresh_service().await;

    put(&node, "nodes", "v1", r#"{"node_type":"vehicle"}"#).await;
    put(&node, "nodes", "a1", r#"{"node_type":"aircraft"}"#).await;
    put(&node, "nodes", "v2", r#"{"node_type":"vehicle"}"#).await;

    let req = SubscribeRequest {
        collections: vec!["nodes".to_string()],
        query: buffa::MessageField::some(pq_eq("node_type", "\"vehicle\"")),
        ..Default::default()
    };
    let (mut stream, _ctx) = service
        .subscribe(Context::default(), subscribe_view(req))
        .await
        .expect("subscribe");

    let events = collect(&mut stream, 2, Duration::from_secs(2)).await;
    assert_eq!(
        events.len(),
        2,
        "expected 2 snapshot events (vehicles only); got {events:?}"
    );

    let mut doc_ids: Vec<_> = events.iter().map(|e| e.doc_id.clone()).collect();
    doc_ids.sort();
    assert_eq!(doc_ids, ["v1", "v2"]);
    assert!(
        !events.iter().any(|e| e.doc_id == "a1"),
        "aircraft leaked past filter"
    );
}
