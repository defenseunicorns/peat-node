//! Connect RPC service implementation for PeatSidecar.
//!
//! Implements the generated `PeatSidecar` trait from connectrpc-build.
//! Supports Connect, gRPC, and gRPC-Web protocols on a single port.

use std::pin::Pin;
use std::sync::Arc;

use buffa::{MessageView, OwnedView};
use connectrpc::{ConnectError, Context};
use futures::stream::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::error;

use crate::node::{
    ChangeType as NodeChangeType, CollectionConfigEntry, SidecarNode, StoredDeletionPolicy,
};
use crate::pb;
use crate::query::{event_passes, Matcher};

/// Connect RPC service wrapping a SidecarNode.
pub struct PeatSidecarService {
    node: Arc<SidecarNode>,
}

impl PeatSidecarService {
    pub fn new(node: Arc<SidecarNode>) -> Self {
        Self { node }
    }
}

fn internal(e: anyhow::Error) -> ConnectError {
    error!("{e:#}");
    ConnectError::internal(e.to_string())
}

impl pb::PeatSidecar for PeatSidecarService {
    // --- Lifecycle ---

    async fn get_status(
        &self,
        ctx: Context,
        _request: OwnedView<pb::GetStatusRequestView<'static>>,
    ) -> Result<(pb::GetStatusResponse, Context), ConnectError> {
        let phase = if self.node.is_sync_active() {
            pb::NodePhase::NODE_PHASE_SYNCING
        } else {
            pb::NodePhase::NODE_PHASE_READY
        };

        Ok((
            pb::GetStatusResponse {
                node_id: self.node.node_id().to_string(),
                endpoint_addr: self.node.endpoint_addr(),
                sync_active: self.node.is_sync_active(),
                connected_peers: self.node.connected_peer_count(),
                phase: phase.into(),
                ..Default::default()
            },
            ctx,
        ))
    }

    // --- Peer Management ---

    async fn connect_peer(
        &self,
        ctx: Context,
        request: OwnedView<pb::ConnectPeerRequestView<'static>>,
    ) -> Result<(pb::ConnectPeerResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        self.node
            .connect_peer(&req.endpoint_id, &req.addresses, &req.relay_url)
            .await
            .map_err(internal)?;
        Ok((pb::ConnectPeerResponse::default(), ctx))
    }

    async fn disconnect_peer(
        &self,
        ctx: Context,
        request: OwnedView<pb::DisconnectPeerRequestView<'static>>,
    ) -> Result<(pb::DisconnectPeerResponse, Context), ConnectError> {
        self.node
            .disconnect_peer(request.endpoint_id)
            .await
            .map_err(internal)?;
        Ok((pb::DisconnectPeerResponse::default(), ctx))
    }

    async fn list_peers(
        &self,
        ctx: Context,
        _request: OwnedView<pb::ListPeersRequestView<'static>>,
    ) -> Result<(pb::ListPeersResponse, Context), ConnectError> {
        let peers = self
            .node
            .list_peers()
            .into_iter()
            .map(|p| pb::PeerInfo {
                endpoint_id: p.endpoint_id,
                addresses: p.addresses,
                connected: p.connected,
                ..Default::default()
            })
            .collect();
        Ok((
            pb::ListPeersResponse {
                peers,
                ..Default::default()
            },
            ctx,
        ))
    }

    // --- Generic Document CRUD ---

    async fn put_document(
        &self,
        ctx: Context,
        request: OwnedView<pb::PutDocumentRequestView<'static>>,
    ) -> Result<(pb::PutDocumentResponse, Context), ConnectError> {
        self.node
            .put_document(request.collection, request.doc_id, request.json_data)
            .await
            .map_err(internal)?;
        Ok((pb::PutDocumentResponse::default(), ctx))
    }

    async fn get_document(
        &self,
        ctx: Context,
        request: OwnedView<pb::GetDocumentRequestView<'static>>,
    ) -> Result<(pb::GetDocumentResponse, Context), ConnectError> {
        let json_data = self
            .node
            .get_document(request.collection, request.doc_id)
            .await
            .map_err(internal)?;
        Ok((
            pb::GetDocumentResponse {
                json_data,
                ..Default::default()
            },
            ctx,
        ))
    }

    async fn delete_document(
        &self,
        ctx: Context,
        request: OwnedView<pb::DeleteDocumentRequestView<'static>>,
    ) -> Result<(pb::DeleteDocumentResponse, Context), ConnectError> {
        self.node
            .delete_document(request.collection, request.doc_id)
            .await
            .map_err(internal)?;
        Ok((pb::DeleteDocumentResponse::default(), ctx))
    }

    async fn list_documents(
        &self,
        ctx: Context,
        request: OwnedView<pb::ListDocumentsRequestView<'static>>,
    ) -> Result<(pb::ListDocumentsResponse, Context), ConnectError> {
        let doc_ids = self
            .node
            .list_documents(request.collection)
            .await
            .map_err(internal)?;
        Ok((
            pb::ListDocumentsResponse {
                doc_ids,
                ..Default::default()
            },
            ctx,
        ))
    }

    // --- Typed Collections ---

    async fn put_node(
        &self,
        ctx: Context,
        request: OwnedView<pb::PutNodeRequestView<'static>>,
    ) -> Result<(pb::PutNodeResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let node = req
            .node
            .ok_or_else(|| ConnectError::invalid_argument("node is required"))?;
        let json = serde_json::to_string(&node_to_map(&node))
            .map_err(|e| ConnectError::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("nodes", &node.id, &json)
            .await
            .map_err(internal)?;
        Ok((pb::PutNodeResponse::default(), ctx))
    }

    async fn get_nodes(
        &self,
        ctx: Context,
        _request: OwnedView<pb::GetNodesRequestView<'static>>,
    ) -> Result<(pb::GetNodesResponse, Context), ConnectError> {
        let doc_ids = self.node.list_documents("nodes").await.map_err(internal)?;
        let mut nodes = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            if let Some(json) = self
                .node
                .get_document("nodes", &doc_id)
                .await
                .map_err(internal)?
            {
                if let Ok(p) = map_to_node(&doc_id, &json) {
                    nodes.push(p);
                }
            }
        }
        Ok((
            pb::GetNodesResponse {
                nodes,
                ..Default::default()
            },
            ctx,
        ))
    }

    async fn put_cell(
        &self,
        ctx: Context,
        request: OwnedView<pb::PutCellRequestView<'static>>,
    ) -> Result<(pb::PutCellResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let cell = req
            .cell
            .ok_or_else(|| ConnectError::invalid_argument("cell is required"))?;
        let json = serde_json::to_string(&cell_to_map(&cell))
            .map_err(|e| ConnectError::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("cells", &cell.id, &json)
            .await
            .map_err(internal)?;
        Ok((pb::PutCellResponse::default(), ctx))
    }

    async fn get_cells(
        &self,
        ctx: Context,
        _request: OwnedView<pb::GetCellsRequestView<'static>>,
    ) -> Result<(pb::GetCellsResponse, Context), ConnectError> {
        let doc_ids = self.node.list_documents("cells").await.map_err(internal)?;
        let mut cells = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            if let Some(json) = self
                .node
                .get_document("cells", &doc_id)
                .await
                .map_err(internal)?
            {
                if let Ok(c) = map_to_cell(&doc_id, &json) {
                    cells.push(c);
                }
            }
        }
        Ok((
            pb::GetCellsResponse {
                cells,
                ..Default::default()
            },
            ctx,
        ))
    }

    async fn put_track(
        &self,
        ctx: Context,
        request: OwnedView<pb::PutTrackRequestView<'static>>,
    ) -> Result<(pb::PutTrackResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let track = req
            .track
            .ok_or_else(|| ConnectError::invalid_argument("track is required"))?;
        let json = serde_json::to_string(&track_to_map(&track))
            .map_err(|e| ConnectError::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("tracks", &track.id, &json)
            .await
            .map_err(internal)?;
        Ok((pb::PutTrackResponse::default(), ctx))
    }

    async fn get_tracks(
        &self,
        ctx: Context,
        _request: OwnedView<pb::GetTracksRequestView<'static>>,
    ) -> Result<(pb::GetTracksResponse, Context), ConnectError> {
        let doc_ids = self.node.list_documents("tracks").await.map_err(internal)?;
        let mut tracks = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            if let Some(json) = self
                .node
                .get_document("tracks", &doc_id)
                .await
                .map_err(internal)?
            {
                if let Ok(t) = map_to_track(&doc_id, &json) {
                    tracks.push(t);
                }
            }
        }
        Ok((
            pb::GetTracksResponse {
                tracks,
                ..Default::default()
            },
            ctx,
        ))
    }

    async fn put_command(
        &self,
        ctx: Context,
        request: OwnedView<pb::PutCommandRequestView<'static>>,
    ) -> Result<(pb::PutCommandResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let command = req
            .command
            .ok_or_else(|| ConnectError::invalid_argument("command is required"))?;
        let json = serde_json::to_string(&command_to_map(&command))
            .map_err(|e| ConnectError::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("commands", &command.id, &json)
            .await
            .map_err(internal)?;
        Ok((pb::PutCommandResponse::default(), ctx))
    }

    async fn get_commands(
        &self,
        ctx: Context,
        _request: OwnedView<pb::GetCommandsRequestView<'static>>,
    ) -> Result<(pb::GetCommandsResponse, Context), ConnectError> {
        let doc_ids = self
            .node
            .list_documents("commands")
            .await
            .map_err(internal)?;
        let mut commands = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            if let Some(json) = self
                .node
                .get_document("commands", &doc_id)
                .await
                .map_err(internal)?
            {
                if let Ok(c) = map_to_command(&doc_id, &json) {
                    commands.push(c);
                }
            }
        }
        Ok((
            pb::GetCommandsResponse {
                commands,
                ..Default::default()
            },
            ctx,
        ))
    }

    // --- Subscriptions ---

    async fn subscribe(
        &self,
        ctx: Context,
        request: OwnedView<pb::SubscribeRequestView<'static>>,
    ) -> Result<
        (
            Pin<Box<dyn Stream<Item = Result<pb::DocumentChange, ConnectError>> + Send>>,
            Context,
        ),
        ConnectError,
    > {
        let filter_collections: Vec<String> =
            request.collections.iter().map(|s| s.to_string()).collect();

        let matcher: Option<Matcher> = match request.query.as_option() {
            Some(view) => {
                let owned = view.to_owned_message();
                Some(Matcher::from_proto(&owned).map_err(|e| {
                    ConnectError::invalid_argument(format!("invalid subscription query: {e}"))
                })?)
            }
            None => None,
        };

        // Subscribe to live updates BEFORE fetching the snapshot to avoid
        // missing events that arrive during the snapshot query.
        let rx = self.node.subscribe();

        // Build an initial snapshot for collections named explicitly in the
        // request. Wildcard subscribe (empty collections list) skips the
        // snapshot — there's no index of all collection names in the store.
        let mut snapshot: Vec<Result<pb::DocumentChange, ConnectError>> = vec![];
        for collection in &filter_collections {
            let ids = self
                .node
                .list_documents(collection)
                .await
                .map_err(internal)?;
            for doc_id in ids {
                if let Ok(Some(json)) = self.node.get_document(collection, &doc_id).await {
                    let passes = match &matcher {
                        Some(m) => m.matches_upsert(&json),
                        None => true,
                    };
                    if passes {
                        snapshot.push(Ok(pb::DocumentChange {
                            collection: collection.clone(),
                            doc_id,
                            change_type: pb::ChangeType::CHANGE_TYPE_UPSERT.into(),
                            json_data: Some(json),
                            ..Default::default()
                        }));
                    }
                }
            }
        }

        let filter_collections_live = filter_collections.clone();
        let live_stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(event) => {
                if !event_passes(
                    &filter_collections_live,
                    matcher.as_ref(),
                    &event.collection,
                    event.json_data.as_deref(),
                ) {
                    return None;
                }
                let change_type = match event.change_type {
                    NodeChangeType::Upsert => pb::ChangeType::CHANGE_TYPE_UPSERT,
                    NodeChangeType::Delete => pb::ChangeType::CHANGE_TYPE_DELETE,
                };
                Some(Ok(pb::DocumentChange {
                    collection: event.collection,
                    doc_id: event.doc_id,
                    change_type: change_type.into(),
                    json_data: event.json_data,
                    ..Default::default()
                }))
            }
            Err(_) => None,
        });

        let combined = futures::stream::iter(snapshot).chain(live_stream);
        Ok((Box::pin(combined), ctx))
    }

    // --- Sync Control ---

    async fn start_sync(
        &self,
        ctx: Context,
        _request: OwnedView<pb::StartSyncRequestView<'static>>,
    ) -> Result<(pb::StartSyncResponse, Context), ConnectError> {
        self.node.start_sync().await.map_err(internal)?;
        Ok((pb::StartSyncResponse::default(), ctx))
    }

    async fn stop_sync(
        &self,
        ctx: Context,
        _request: OwnedView<pb::StopSyncRequestView<'static>>,
    ) -> Result<(pb::StopSyncResponse, Context), ConnectError> {
        self.node.stop_sync().await.map_err(internal)?;
        Ok((pb::StopSyncResponse::default(), ctx))
    }

    async fn get_sync_stats(
        &self,
        ctx: Context,
        _request: OwnedView<pb::GetSyncStatsRequestView<'static>>,
    ) -> Result<(pb::GetSyncStatsResponse, Context), ConnectError> {
        let stats = self.node.sync_stats();
        Ok((
            pb::GetSyncStatsResponse {
                sync_active: stats.sync_active,
                connected_peers: stats.connected_peers,
                bytes_sent: stats.bytes_sent,
                bytes_received: stats.bytes_received,
                ..Default::default()
            },
            ctx,
        ))
    }

    // --- Attachments (PRD-006) ---
    //
    // v1 safety default: all four RPCs return Unimplemented until
    // --attachment-root is configured. The real handlers land in a later
    // step; these stubs satisfy the generated trait so the build proceeds
    // while the supporting modules (config / validate / ingest / registry)
    // are written.

    async fn send_attachments(
        &self,
        ctx: Context,
        request: OwnedView<pb::SendAttachmentsRequestView<'static>>,
    ) -> Result<(pb::SendAttachmentsResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let resp = crate::attachments::handlers::send_attachments(&self.node, req).await?;
        Ok((resp, ctx))
    }

    async fn get_attachment_distribution(
        &self,
        ctx: Context,
        request: OwnedView<pb::GetAttachmentDistributionRequestView<'static>>,
    ) -> Result<(pb::GetAttachmentDistributionResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let resp =
            crate::attachments::handlers::get_attachment_distribution(&self.node, req).await?;
        Ok((resp, ctx))
    }

    async fn subscribe_attachment_bundle(
        &self,
        ctx: Context,
        request: OwnedView<pb::SubscribeAttachmentBundleRequestView<'static>>,
    ) -> Result<
        (
            Pin<Box<dyn Stream<Item = Result<pb::AttachmentProgress, ConnectError>> + Send>>,
            Context,
        ),
        ConnectError,
    > {
        let req = request.to_owned_message();
        let stream =
            crate::attachments::handlers::subscribe_attachment_bundle(&self.node, req).await?;
        Ok((stream, ctx))
    }

    async fn cancel_attachment_distribution(
        &self,
        ctx: Context,
        request: OwnedView<pb::CancelAttachmentDistributionRequestView<'static>>,
    ) -> Result<(pb::CancelAttachmentDistributionResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let resp =
            crate::attachments::handlers::cancel_attachment_distribution(&self.node, req).await?;
        Ok((resp, ctx))
    }

    // --- Collection Lifecycle Configuration (peat-node#55 / ADR-016) ---

    async fn set_collection_config(
        &self,
        ctx: Context,
        request: OwnedView<pb::SetCollectionConfigRequestView<'static>>,
    ) -> Result<(pb::SetCollectionConfigResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let cfg = req
            .config
            .ok_or_else(|| ConnectError::invalid_argument("config is required"))?;
        if cfg.collection.is_empty() {
            return Err(ConnectError::invalid_argument(
                "collection name must not be empty",
            ));
        }
        let entry = proto_config_to_entry(cfg)
            .map_err(|e| ConnectError::invalid_argument(e.to_string()))?;
        self.node.set_collection_config(entry).map_err(internal)?;
        Ok((pb::SetCollectionConfigResponse::default(), ctx))
    }

    async fn get_collection_config(
        &self,
        ctx: Context,
        request: OwnedView<pb::GetCollectionConfigRequestView<'static>>,
    ) -> Result<(pb::GetCollectionConfigResponse, Context), ConnectError> {
        let collection = request.collection;
        match self.node.get_collection_config(collection) {
            Some(entry) => Ok((
                pb::GetCollectionConfigResponse {
                    config: buffa::MessageField::some(entry_to_proto_config(entry)),
                    ..Default::default()
                },
                ctx,
            )),
            None => Ok((pb::GetCollectionConfigResponse::default(), ctx)),
        }
    }

    async fn list_collection_configs(
        &self,
        ctx: Context,
        _request: OwnedView<pb::ListCollectionConfigsRequestView<'static>>,
    ) -> Result<(pb::ListCollectionConfigsResponse, Context), ConnectError> {
        let configs = self
            .node
            .list_collection_configs()
            .into_iter()
            .map(entry_to_proto_config)
            .collect();
        Ok((
            pb::ListCollectionConfigsResponse {
                configs,
                ..Default::default()
            },
            ctx,
        ))
    }
}

// --- Proto ↔ JSON conversion helpers ---

fn node_to_map(p: &pb::Node) -> serde_json::Value {
    serde_json::json!({
        "id": p.id,
        "node_type": p.node_type,
        "name": p.name,
        "status": p.status.to_i32(),
        "latitude": p.latitude,
        "longitude": p.longitude,
        "altitude_m": p.altitude_m,
        "readiness": p.readiness,
        "capabilities": p.capabilities,
        "unit_id": p.unit_id,
        "callsign": p.callsign,
    })
}

fn map_to_node(id: &str, json: &str) -> anyhow::Result<pb::Node> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(pb::Node {
        id: id.to_string(),
        node_type: v["node_type"].as_str().unwrap_or_default().to_string(),
        name: v["name"].as_str().unwrap_or_default().to_string(),
        status: buffa::EnumValue::from(v["status"].as_i64().unwrap_or_default() as i32),
        latitude: v["latitude"].as_f64().unwrap_or_default(),
        longitude: v["longitude"].as_f64().unwrap_or_default(),
        altitude_m: v["altitude_m"].as_f64().unwrap_or_default(),
        readiness: v["readiness"].as_f64().unwrap_or_default(),
        capabilities: v["capabilities"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        unit_id: v["unit_id"].as_str().map(|s| s.to_string()),
        callsign: v["callsign"].as_str().map(|s| s.to_string()),
        ..Default::default()
    })
}

fn cell_to_map(c: &pb::Cell) -> serde_json::Value {
    serde_json::json!({
        "id": c.id,
        "name": c.name,
        "status": c.status.to_i32(),
        "node_count": c.node_count,
        "center_latitude": c.center_latitude,
        "center_longitude": c.center_longitude,
        "capabilities": c.capabilities,
        "formation_id": c.formation_id,
        "leader_id": c.leader_id,
    })
}

fn map_to_cell(id: &str, json: &str) -> anyhow::Result<pb::Cell> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(pb::Cell {
        id: id.to_string(),
        name: v["name"].as_str().unwrap_or_default().to_string(),
        status: buffa::EnumValue::from(v["status"].as_i64().unwrap_or_default() as i32),
        node_count: v["node_count"].as_u64().unwrap_or_default() as u32,
        center_latitude: v["center_latitude"].as_f64().unwrap_or_default(),
        center_longitude: v["center_longitude"].as_f64().unwrap_or_default(),
        capabilities: v["capabilities"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
        formation_id: v["formation_id"].as_str().map(|s| s.to_string()),
        leader_id: v["leader_id"].as_str().map(|s| s.to_string()),
        ..Default::default()
    })
}

fn track_to_map(t: &pb::Track) -> serde_json::Value {
    serde_json::json!({
        "id": t.id,
        "source_node": t.source_node,
        "cell_id": t.cell_id,
        "formation_id": t.formation_id,
        "latitude": t.latitude,
        "longitude": t.longitude,
        "altitude_m": t.altitude_m,
        "cep_m": t.cep_m,
        "heading_deg": t.heading_deg,
        "speed_mps": t.speed_mps,
        "classification": t.classification,
        "confidence": t.confidence,
        "category": t.category.to_i32(),
    })
}

fn map_to_track(id: &str, json: &str) -> anyhow::Result<pb::Track> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(pb::Track {
        id: id.to_string(),
        source_node: v["source_node"].as_str().unwrap_or_default().to_string(),
        cell_id: v["cell_id"].as_str().map(|s| s.to_string()),
        formation_id: v["formation_id"].as_str().map(|s| s.to_string()),
        latitude: v["latitude"].as_f64().unwrap_or_default(),
        longitude: v["longitude"].as_f64().unwrap_or_default(),
        altitude_m: v["altitude_m"].as_f64(),
        cep_m: v["cep_m"].as_f64(),
        heading_deg: v["heading_deg"].as_f64(),
        speed_mps: v["speed_mps"].as_f64(),
        classification: v["classification"].as_str().unwrap_or_default().to_string(),
        confidence: v["confidence"].as_f64().unwrap_or_default(),
        category: buffa::EnumValue::from(v["category"].as_i64().unwrap_or_default() as i32),
        ..Default::default()
    })
}

fn command_to_map(c: &pb::Command) -> serde_json::Value {
    serde_json::json!({
        "id": c.id,
        "target_id": c.target_id,
        "command_type": c.command_type,
        "status": c.status.to_i32(),
        "created_at": c.created_at,
        "expires_at": c.expires_at,
        "payload_json": c.payload_json,
    })
}

fn map_to_command(id: &str, json: &str) -> anyhow::Result<pb::Command> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(pb::Command {
        id: id.to_string(),
        target_id: v["target_id"].as_str().unwrap_or_default().to_string(),
        command_type: v["command_type"].as_str().unwrap_or_default().to_string(),
        status: buffa::EnumValue::from(v["status"].as_i64().unwrap_or_default() as i32),
        created_at: v["created_at"].as_i64().unwrap_or_default(),
        expires_at: v["expires_at"].as_i64().unwrap_or_default(),
        payload_json: v["payload_json"].as_str().unwrap_or_default().to_string(),
        ..Default::default()
    })
}

// --- Collection config proto ↔ node type conversions ---

fn entry_to_proto_config(e: CollectionConfigEntry) -> pb::CollectionConfig {
    let policy = match e.deletion_policy {
        StoredDeletionPolicy::SoftDelete => pb::DeletionPolicy::DELETION_POLICY_SOFT_DELETE,
        StoredDeletionPolicy::Tombstone => pb::DeletionPolicy::DELETION_POLICY_TOMBSTONE,
        StoredDeletionPolicy::ImplicitTTL => pb::DeletionPolicy::DELETION_POLICY_IMPLICIT_TTL,
        StoredDeletionPolicy::Immutable => pb::DeletionPolicy::DELETION_POLICY_IMMUTABLE,
    };
    pb::CollectionConfig {
        collection: e.collection,
        deletion_policy: policy.into(),
        soft_delete_ttl_secs: e.soft_delete_ttl_secs,
        tombstone_ttl_secs: e.tombstone_ttl_secs,
        ..Default::default()
    }
}

fn proto_config_to_entry(cfg: pb::CollectionConfig) -> anyhow::Result<CollectionConfigEntry> {
    let policy = match cfg.deletion_policy.to_i32() {
        2 => StoredDeletionPolicy::Tombstone,
        3 => StoredDeletionPolicy::ImplicitTTL,
        4 => StoredDeletionPolicy::Immutable,
        _ => StoredDeletionPolicy::SoftDelete, // 0 = unspecified, 1 = explicit SoftDelete
    };
    Ok(CollectionConfigEntry {
        collection: cfg.collection,
        deletion_policy: policy,
        soft_delete_ttl_secs: cfg.soft_delete_ttl_secs,
        tombstone_ttl_secs: cfg.tombstone_ttl_secs,
    })
}
