//! Integration tests for SetCollectionConfig / GetCollectionConfig / ListCollectionConfigs.
//!
//! Exercises the service-layer RPCs introduced in peat-node#55 via the same
//! in-process trait surface as subscribe_query_test.rs.

use std::sync::Arc;

use buffa::OwnedView;
use connectrpc::{ErrorCode, RequestContext, ServiceRequest, ServiceResult};
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::{
    CollectionConfig, DeletionPolicy, GetCollectionConfigRequest, GetCollectionConfigResponse,
    ListCollectionConfigsRequest, ListCollectionConfigsResponse, PeatSidecar,
    SetCollectionConfigRequest, SetCollectionConfigResponse,
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
            iroh_secret_key: None,
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

async fn set_config(
    service: &PeatSidecarService,
    cfg: CollectionConfig,
) -> ServiceResult<SetCollectionConfigResponse> {
    let message = SetCollectionConfigRequest {
        config: buffa::MessageField::some(cfg),
        ..Default::default()
    };
    let request: OwnedView<peat_node::pb::SetCollectionConfigRequestView<'static>> =
        OwnedView::from_owned(&message).expect("encode set_collection_config request");
    service
        .set_collection_config(
            RequestContext::default(),
            ServiceRequest::from_parts(request.reborrow(), request.bytes()),
        )
        .await
}

async fn get_config(
    service: &PeatSidecarService,
    collection: &str,
) -> ServiceResult<GetCollectionConfigResponse> {
    let message = GetCollectionConfigRequest {
        collection: collection.to_string(),
        ..Default::default()
    };
    let request: OwnedView<peat_node::pb::GetCollectionConfigRequestView<'static>> =
        OwnedView::from_owned(&message).expect("encode get_collection_config request");
    service
        .get_collection_config(
            RequestContext::default(),
            ServiceRequest::from_parts(request.reborrow(), request.bytes()),
        )
        .await
}

async fn list_configs(
    service: &PeatSidecarService,
) -> ServiceResult<ListCollectionConfigsResponse> {
    let message = ListCollectionConfigsRequest::default();
    let request: OwnedView<peat_node::pb::ListCollectionConfigsRequestView<'static>> =
        OwnedView::from_owned(&message).expect("encode list_collection_configs request");
    service
        .list_collection_configs(
            RequestContext::default(),
            ServiceRequest::from_parts(request.reborrow(), request.bytes()),
        )
        .await
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

    set_config(&service, cfg)
        .await
        .expect("set_collection_config");

    let resp = get_config(&service, "tracks")
        .await
        .expect("get_collection_config")
        .body;

    let returned = resp.config.into_option().expect("config field present");
    assert_eq!(returned.collection, "tracks");
    assert_eq!(
        returned.deletion_policy.to_i32(),
        DeletionPolicy::DELETION_POLICY_TOMBSTONE as i32
    );
    assert_eq!(returned.tombstone_ttl_secs, Some(3600));
}

#[tokio::test]
async fn get_unconfigured_collection_returns_empty() {
    let (_node, service) = fresh_service().await;

    // An unconfigured collection returns a 200 with no config field (not a NOT_FOUND
    // error). This lets callers check for the presence of a config without treating
    // absence as an error condition.
    let resp = get_config(&service, "nonexistent")
        .await
        .expect("get_collection_config must succeed for unknown collection")
        .body;
    assert!(
        resp.config.into_option().is_none(),
        "expected no config for unconfigured collection"
    );
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

    set_config(&service, cfg_a).await.expect("set nodes");
    set_config(&service, cfg_b).await.expect("set tracks");

    let resp = list_configs(&service)
        .await
        .expect("list_collection_configs")
        .body;

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

    match set_config(&service, cfg).await {
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
            iroh_secret_key: None,
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

    let cfg = CollectionConfig {
        collection: "commands".to_string(),
        deletion_policy: buffa::EnumValue::from(DeletionPolicy::DELETION_POLICY_IMMUTABLE as i32),
        ..Default::default()
    };
    set_config(&service, cfg).await.expect("set");

    // Verify the JSON file was written with the correct content.
    let config_path = data_dir.join("collection_configs.json");
    let json_str = std::fs::read_to_string(&config_path)
        .expect("collection_configs.json must exist after set");
    let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("valid JSON");
    let commands = &parsed["commands"];
    assert_eq!(commands["collection"], "commands");
    assert_eq!(commands["deletion_policy"], "Immutable");
}
