//! Connect RPC service implementation for PeatSidecar.
//!
//! Implements the generated `PeatSidecar` trait from connectrpc-build.
//! Supports Connect, gRPC, and gRPC-Web protocols on a single port.

use std::pin::Pin;
use std::sync::Arc;

use buffa::OwnedView;
use connectrpc::{ConnectError, Context};
use futures::stream::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::error;

use crate::node::{ChangeType as NodeChangeType, SidecarNode};
use crate::pb;

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
        self.node
            .connect_peer(request.endpoint_id)
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

    async fn put_platform(
        &self,
        ctx: Context,
        request: OwnedView<pb::PutPlatformRequestView<'static>>,
    ) -> Result<(pb::PutPlatformResponse, Context), ConnectError> {
        let req = request.to_owned_message();
        let platform = req
            .platform
            .ok_or_else(|| ConnectError::invalid_argument("platform is required"))?;
        let json = serde_json::to_string(&platform_to_map(&platform))
            .map_err(|e| ConnectError::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("platforms", &platform.id, &json)
            .await
            .map_err(internal)?;
        Ok((pb::PutPlatformResponse::default(), ctx))
    }

    async fn get_platforms(
        &self,
        ctx: Context,
        _request: OwnedView<pb::GetPlatformsRequestView<'static>>,
    ) -> Result<(pb::GetPlatformsResponse, Context), ConnectError> {
        let doc_ids = self
            .node
            .list_documents("platforms")
            .await
            .map_err(internal)?;
        let mut platforms = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            if let Some(json) = self
                .node
                .get_document("platforms", &doc_id)
                .await
                .map_err(internal)?
            {
                if let Ok(p) = map_to_platform(&doc_id, &json) {
                    platforms.push(p);
                }
            }
        }
        Ok((
            pb::GetPlatformsResponse {
                platforms,
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

    // --- Deployment (P2P Zarf package distribution) ---

    async fn publish_deployment(
        &self,
        ctx: Context,
        request: OwnedView<pb::PublishDeploymentRequestView<'static>>,
    ) -> Result<(pb::PublishDeploymentResponse, Context), ConnectError> {
        // Pitfall 1: materialize the borrowed view because of the map<string,string> field.
        let req = request.to_owned_message();

        // Validate inputs
        if req.target_agent_id.is_empty() {
            return Err(ConnectError::invalid_argument("target_agent_id is required"));
        }
        let path = std::path::Path::new(&req.package_path);
        if !path.exists() {
            return Err(ConnectError::invalid_argument(format!(
                "package_path does not exist: {}",
                req.package_path
            )));
        }

        // Derive a stable name from the filename (best-effort; the sender owns the path).
        let package_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        // Publish blob to Iroh store (BLOB-01; already implemented on SidecarNode).
        let token = self
            .node
            .publish_blob(path, &package_name)
            .await
            .map_err(internal)?;

        // Pitfall 5: always generate a new UUID — no dedup on the sender side.
        let request_id = uuid::Uuid::new_v4().to_string();
        let sender_endpoint_id = self.node.endpoint_addr();

        // Synthesize blob_ticket (Phase 3 parses this; see RESEARCH.md §"What blob_ticket Is").
        let blob_ticket = serde_json::json!({
            "hash": token.hash.as_hex(),
            "size_bytes": token.size_bytes,
            "sender_endpoint_id": sender_endpoint_id,
        })
        .to_string();

        // Pitfall 2: build the typed struct, not a json! macro. serde_json::to_string
        // guarantees the locked schema (DeploymentStatus::Pending → "pending" via serde rename_all).
        let doc = crate::types::DeploymentRequest {
            id: request_id.clone(),
            target_agent_id: req.target_agent_id.clone(),
            package_name,
            package_version: String::new(),
            architecture: String::new(),
            iroh_blob_hash: token.hash.as_hex().to_string(),
            sender_endpoint_id,
            zarf_vars: req.zarf_vars.into_iter().collect(),
            sender_status: crate::types::DeploymentStatus::Pending,
            receiver_status: crate::types::DeploymentStatus::Pending,
            created_at: chrono::Utc::now().timestamp(),
            blob_ticket,
        };

        let json = serde_json::to_string(&doc)
            .map_err(|e| ConnectError::internal(format!("serialization error: {e}")))?;

        self.node
            .put_document("deployment_requests", &request_id, &json)
            .await
            .map_err(internal)?;

        Ok((
            pb::PublishDeploymentResponse {
                request_id,
                ..Default::default()
            },
            ctx,
        ))
    }

    async fn get_deployment_requests(
        &self,
        ctx: Context,
        _request: OwnedView<pb::GetDeploymentRequestsRequestView<'static>>,
    ) -> Result<(pb::GetDeploymentRequestsResponse, Context), ConnectError> {
        let doc_ids = self
            .node
            .list_documents("deployment_requests")
            .await
            .map_err(internal)?;

        let mut requests = Vec::with_capacity(doc_ids.len());
        for doc_id in &doc_ids {
            if let Some(json) = self
                .node
                .get_document("deployment_requests", doc_id)
                .await
                .map_err(internal)?
            {
                match serde_json::from_str::<crate::types::DeploymentRequest>(&json) {
                    Ok(req) => requests.push(deployment_request_to_proto(doc_id, &req)),
                    Err(e) => {
                        error!(doc_id = %doc_id, "failed to deserialize deployment_request: {e}");
                        // Do not abort — skip the malformed doc and continue.
                    }
                }
            }
        }

        Ok((
            pb::GetDeploymentRequestsResponse {
                requests,
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
        let rx = self.node.subscribe();

        let stream = BroadcastStream::new(rx).filter_map(move |result| match result {
            Ok(event) => {
                if !filter_collections.is_empty() && !filter_collections.contains(&event.collection)
                {
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

        Ok((Box::pin(stream), ctx))
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
}

// --- Proto ↔ JSON conversion helpers ---

fn platform_to_map(p: &pb::Platform) -> serde_json::Value {
    serde_json::json!({
        "id": p.id,
        "platform_type": p.platform_type,
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

fn map_to_platform(id: &str, json: &str) -> anyhow::Result<pb::Platform> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(pb::Platform {
        id: id.to_string(),
        platform_type: v["platform_type"].as_str().unwrap_or_default().to_string(),
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
        "platform_count": c.platform_count,
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
        platform_count: v["platform_count"].as_u64().unwrap_or_default() as u32,
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
        "source_platform": t.source_platform,
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
        source_platform: v["source_platform"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
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

fn deployment_request_to_proto(
    id: &str,
    r: &crate::types::DeploymentRequest,
) -> pb::DeploymentRequestDoc {
    pb::DeploymentRequestDoc {
        id: id.to_string(),
        target_agent_id: r.target_agent_id.clone(),
        package_name: r.package_name.clone(),
        package_version: r.package_version.clone(),
        architecture: r.architecture.clone(),
        iroh_blob_hash: r.iroh_blob_hash.clone(),
        sender_endpoint_id: r.sender_endpoint_id.clone(),
        blob_ticket: r.blob_ticket.clone(),
        zarf_vars: r.zarf_vars.clone(),
        // DeploymentStatus → snake_case string via serde_json (Pitfall — don't use format!("{:?}"))
        sender_status: serde_json::to_value(&r.sender_status)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default(),
        receiver_status: serde_json::to_value(&r.receiver_status)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default(),
        created_at: r.created_at,
        ..Default::default()
    }
}
