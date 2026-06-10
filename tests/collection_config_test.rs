//! Integration tests for SetCollectionConfig / GetCollectionConfig / ListCollectionConfigs.
//!
//! Exercises the service-layer RPCs introduced in peat-node#55 via the same
//! in-process trait surface as subscribe_query_test.rs.

use std::sync::Arc;

use buffa::OwnedView;
use connectrpc::{Context, ErrorCode};
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::{
    CollectionConfig, DeletionPolicy, GetCollectionConfigRequest, ListCollectionConfigsRequest,
    PeatSidecar, SetCollectionConfigRequest,
};
use peat_node::service::PeatSidecarService;

async fn fresh_service() -> (Arc<SidecarNode>, PeatSidecarService) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: "test-cc".to_string(),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: None,
            attachment_config: Default::default(),
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
        })
        .await
        .expect("boot node"),
    );
    let service = PeatSidecarService::new(Arc::clone(&node));
    (node, service)
}

fn set_request(cfg: CollectionConfig) -> OwnedView<peat_node::pb::SetCollectionConfigRequestView<'static>> {
    let req = SetCollectionConfigRequest {
        config: buffa::MessageField::some(cfg),
        ..Default::default()
    };
    OwnedView::from_owned(&req).expect("encode set_collection_config request")
}

fn get_request(collection: &str) -> OwnedView<peat_node::pb::GetCollectionConfigRequestView<'static>> {
    let req = GetCollectionConfigRequest {
        collection: collection.to_string(),
        ..Default::default()
    };
    OwnedView::from_owned(&req).expect("encode get_collection_config request")
}

fn list_request() -> OwnedView<peat_node::pb::ListCollectionConfigsRequestView<'static>> {
    let req = ListCollectionConfigsRequest::default();
    OwnedView::from_owned(&req).expect("encode list_collection_configs request")
}

#[tokio::test]
async fn set_and_get_collection_config_round_trip() {
    let (_node, service) = fresh_service().await;

    let cfg = CollectionConfig {
        collection: "tracks".to_string(),
        deletion_policy: buffa::EnumValue::from(DeletionPolicy::DELETION_POLICY_TOMBSTONE as i32),
        tombstone_ttl_secs: Some(3600),
        ..Default::default()
    };

    service
        .set_collection_config(Context::default(), set_request(cfg))
        .await
        .expect("set_collection_config");

    let (resp, _) = service
        .get_collection_config(Context::default(), get_request("tracks"))
        .await
        .expect("get_collection_config");

    let returned = resp.config.into_option().expect("config field present");
    assert_eq!(returned.collection, "tracks");
    assert_eq!(
        returned.deletion_policy.to_i32(),
        DeletionPolicy::DELETION_POLICY_TOMBSTONE as i32
    );
    assert_eq!(returned.tombstone_ttl_secs, Some(3600));
}

#[tokio::test]
async fn get_unconfigured_collection_returns_not_found() {
    let (_node, service) = fresh_service().await;

    match service
        .get_collection_config(Context::default(), get_request("nonexistent"))
        .await
    {
        Err(e) => assert_eq!(e.code, ErrorCode::NotFound),
        Ok(_) => panic!("expected NOT_FOUND"),
    }
}

#[tokio::test]
async fn list_collection_configs_returns_all_configured() {
    let (_node, service) = fresh_service().await;

    let cfg_a = CollectionConfig {
        collection: "nodes".to_string(),
        deletion_policy: buffa::EnumValue::from(DeletionPolicy::DELETION_POLICY_SOFT_DELETE as i32),
        ..Default::default()
    };
    let cfg_b = CollectionConfig {
        collection: "tracks".to_string(),
        deletion_policy: buffa::EnumValue::from(DeletionPolicy::DELETION_POLICY_TOMBSTONE as i32),
        ..Default::default()
    };

    service
        .set_collection_config(Context::default(), set_request(cfg_a))
        .await
        .expect("set nodes");
    service
        .set_collection_config(Context::default(), set_request(cfg_b))
        .await
        .expect("set tracks");

    let (resp, _) = service
        .list_collection_configs(Context::default(), list_request())
        .await
        .expect("list_collection_configs");

    let mut names: Vec<_> = resp.configs.iter().map(|c| c.collection.as_str()).collect();
    names.sort();
    assert_eq!(names, ["nodes", "tracks"]);
}

#[tokio::test]
async fn set_collection_config_requires_collection_name() {
    let (_node, service) = fresh_service().await;

    let cfg = CollectionConfig {
        collection: String::new(), // empty
        deletion_policy: buffa::EnumValue::from(DeletionPolicy::DELETION_POLICY_SOFT_DELETE as i32),
        ..Default::default()
    };

    match service
        .set_collection_config(Context::default(), set_request(cfg))
        .await
    {
        Err(e) => assert_eq!(e.code, ErrorCode::InvalidArgument),
        Ok(_) => panic!("expected InvalidArgument"),
    }
}

#[tokio::test]
async fn set_collection_config_persists_to_disk() {
    // Verify that SetCollectionConfig writes to collection_configs.json in the
    // data_dir so the config survives a node restart (peat-node#55 acceptance
    // criteria). We read the file directly rather than spinning up a second
    // SidecarNode against the same redb database (which would hit the file lock).
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: "persist-test".to_string(),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: data_dir.clone(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: None,
            attachment_config: Default::default(),
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
        })
        .await
        .expect("boot node"),
    );
    let service = PeatSidecarService::new(Arc::clone(&node));

    let cfg = CollectionConfig {
        collection: "commands".to_string(),
        deletion_policy: buffa::EnumValue::from(DeletionPolicy::DELETION_POLICY_IMMUTABLE as i32),
        ..Default::default()
    };
    service
        .set_collection_config(Context::default(), set_request(cfg))
        .await
        .expect("set");

    // Verify the JSON file was written with the correct content.
    let config_path = data_dir.join("collection_configs.json");
    let json_str = std::fs::read_to_string(&config_path)
        .expect("collection_configs.json must exist after set");
    let parsed: serde_json::Value =
        serde_json::from_str(&json_str).expect("valid JSON");
    let commands = &parsed["commands"];
    assert_eq!(commands["collection"], "commands");
    assert_eq!(commands["deletion_policy"], "Immutable");
}
