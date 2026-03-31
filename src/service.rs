//! gRPC service implementation for PeatSidecar.
//!
//! Each RPC method delegates to the underlying `SidecarNode`.

use std::sync::Arc;

use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};
use tracing::error;

use crate::node::{ChangeType as NodeChangeType, SidecarNode};
use crate::proto;
use crate::proto::peat_sidecar_server::PeatSidecar;

/// gRPC service wrapping a SidecarNode.
pub struct PeatSidecarService {
    node: Arc<SidecarNode>,
}

impl PeatSidecarService {
    pub fn new(node: Arc<SidecarNode>) -> Self {
        Self { node }
    }
}

fn internal(e: anyhow::Error) -> Status {
    error!("{e:#}");
    Status::internal(e.to_string())
}

#[tonic::async_trait]
impl PeatSidecar for PeatSidecarService {
    // --- Lifecycle ---

    async fn get_status(
        &self,
        _req: Request<proto::GetStatusRequest>,
    ) -> Result<Response<proto::GetStatusResponse>, Status> {
        let phase = if self.node.is_sync_active() {
            proto::NodePhase::Syncing as i32
        } else {
            proto::NodePhase::Ready as i32
        };

        Ok(Response::new(proto::GetStatusResponse {
            node_id: self.node.node_id().to_string(),
            endpoint_addr: self.node.endpoint_addr(),
            sync_active: self.node.is_sync_active(),
            connected_peers: self.node.connected_peer_count(),
            phase,
        }))
    }

    // --- Peer Management ---

    async fn connect_peer(
        &self,
        req: Request<proto::ConnectPeerRequest>,
    ) -> Result<Response<proto::ConnectPeerResponse>, Status> {
        let inner = req.into_inner();
        self.node
            .connect_peer(&inner.endpoint_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::ConnectPeerResponse {}))
    }

    async fn disconnect_peer(
        &self,
        req: Request<proto::DisconnectPeerRequest>,
    ) -> Result<Response<proto::DisconnectPeerResponse>, Status> {
        let inner = req.into_inner();
        self.node
            .disconnect_peer(&inner.endpoint_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::DisconnectPeerResponse {}))
    }

    async fn list_peers(
        &self,
        _req: Request<proto::ListPeersRequest>,
    ) -> Result<Response<proto::ListPeersResponse>, Status> {
        let peers = self
            .node
            .list_peers()
            .into_iter()
            .map(|p| proto::PeerInfo {
                endpoint_id: p.endpoint_id,
                addresses: p.addresses,
                connected: p.connected,
            })
            .collect();
        Ok(Response::new(proto::ListPeersResponse { peers }))
    }

    // --- Generic Document CRUD ---

    async fn put_document(
        &self,
        req: Request<proto::PutDocumentRequest>,
    ) -> Result<Response<proto::PutDocumentResponse>, Status> {
        let inner = req.into_inner();
        self.node
            .put_document(&inner.collection, &inner.doc_id, &inner.json_data)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::PutDocumentResponse {}))
    }

    async fn get_document(
        &self,
        req: Request<proto::GetDocumentRequest>,
    ) -> Result<Response<proto::GetDocumentResponse>, Status> {
        let inner = req.into_inner();
        let json_data = self
            .node
            .get_document(&inner.collection, &inner.doc_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::GetDocumentResponse { json_data }))
    }

    async fn delete_document(
        &self,
        req: Request<proto::DeleteDocumentRequest>,
    ) -> Result<Response<proto::DeleteDocumentResponse>, Status> {
        let inner = req.into_inner();
        self.node
            .delete_document(&inner.collection, &inner.doc_id)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::DeleteDocumentResponse {}))
    }

    async fn list_documents(
        &self,
        req: Request<proto::ListDocumentsRequest>,
    ) -> Result<Response<proto::ListDocumentsResponse>, Status> {
        let inner = req.into_inner();
        let doc_ids = self
            .node
            .list_documents(&inner.collection)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::ListDocumentsResponse { doc_ids }))
    }

    // --- Typed Collections ---

    async fn put_platform(
        &self,
        req: Request<proto::PutPlatformRequest>,
    ) -> Result<Response<proto::PutPlatformResponse>, Status> {
        let platform = req
            .into_inner()
            .platform
            .ok_or_else(|| Status::invalid_argument("platform is required"))?;
        let json = serde_json::to_string(&platform_to_map(&platform))
            .map_err(|e| Status::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("platforms", &platform.id, &json)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::PutPlatformResponse {}))
    }

    async fn get_platforms(
        &self,
        _req: Request<proto::GetPlatformsRequest>,
    ) -> Result<Response<proto::GetPlatformsResponse>, Status> {
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
        Ok(Response::new(proto::GetPlatformsResponse { platforms }))
    }

    async fn put_cell(
        &self,
        req: Request<proto::PutCellRequest>,
    ) -> Result<Response<proto::PutCellResponse>, Status> {
        let cell = req
            .into_inner()
            .cell
            .ok_or_else(|| Status::invalid_argument("cell is required"))?;
        let json = serde_json::to_string(&cell_to_map(&cell))
            .map_err(|e| Status::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("cells", &cell.id, &json)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::PutCellResponse {}))
    }

    async fn get_cells(
        &self,
        _req: Request<proto::GetCellsRequest>,
    ) -> Result<Response<proto::GetCellsResponse>, Status> {
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
        Ok(Response::new(proto::GetCellsResponse { cells }))
    }

    async fn put_track(
        &self,
        req: Request<proto::PutTrackRequest>,
    ) -> Result<Response<proto::PutTrackResponse>, Status> {
        let track = req
            .into_inner()
            .track
            .ok_or_else(|| Status::invalid_argument("track is required"))?;
        let json = serde_json::to_string(&track_to_map(&track))
            .map_err(|e| Status::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("tracks", &track.id, &json)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::PutTrackResponse {}))
    }

    async fn get_tracks(
        &self,
        _req: Request<proto::GetTracksRequest>,
    ) -> Result<Response<proto::GetTracksResponse>, Status> {
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
        Ok(Response::new(proto::GetTracksResponse { tracks }))
    }

    async fn put_command(
        &self,
        req: Request<proto::PutCommandRequest>,
    ) -> Result<Response<proto::PutCommandResponse>, Status> {
        let command = req
            .into_inner()
            .command
            .ok_or_else(|| Status::invalid_argument("command is required"))?;
        let json = serde_json::to_string(&command_to_map(&command))
            .map_err(|e| Status::internal(format!("serialization error: {e}")))?;
        self.node
            .put_document("commands", &command.id, &json)
            .await
            .map_err(internal)?;
        Ok(Response::new(proto::PutCommandResponse {}))
    }

    async fn get_commands(
        &self,
        _req: Request<proto::GetCommandsRequest>,
    ) -> Result<Response<proto::GetCommandsResponse>, Status> {
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
        Ok(Response::new(proto::GetCommandsResponse { commands }))
    }

    // --- Subscriptions ---

    type SubscribeStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<proto::DocumentChange, Status>> + Send>,
    >;

    async fn subscribe(
        &self,
        req: Request<proto::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let filter_collections: Vec<String> = req.into_inner().collections;
        let rx = self.node.subscribe();

        let stream = BroadcastStream::new(rx).filter_map(move |result| {
            match result {
                Ok(event) => {
                    // Apply collection filter
                    if !filter_collections.is_empty()
                        && !filter_collections.contains(&event.collection)
                    {
                        return None;
                    }
                    let change_type = match event.change_type {
                        NodeChangeType::Upsert => proto::ChangeType::Upsert as i32,
                        NodeChangeType::Delete => proto::ChangeType::Delete as i32,
                    };
                    Some(Ok(proto::DocumentChange {
                        collection: event.collection,
                        doc_id: event.doc_id,
                        change_type,
                        json_data: event.json_data,
                    }))
                }
                Err(_) => None, // Lagged or closed — skip
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }

    // --- Sync Control ---

    async fn start_sync(
        &self,
        _req: Request<proto::StartSyncRequest>,
    ) -> Result<Response<proto::StartSyncResponse>, Status> {
        self.node.start_sync().await.map_err(internal)?;
        Ok(Response::new(proto::StartSyncResponse {}))
    }

    async fn stop_sync(
        &self,
        _req: Request<proto::StopSyncRequest>,
    ) -> Result<Response<proto::StopSyncResponse>, Status> {
        self.node.stop_sync().await.map_err(internal)?;
        Ok(Response::new(proto::StopSyncResponse {}))
    }

    async fn get_sync_stats(
        &self,
        _req: Request<proto::GetSyncStatsRequest>,
    ) -> Result<Response<proto::GetSyncStatsResponse>, Status> {
        let stats = self.node.sync_stats();
        Ok(Response::new(proto::GetSyncStatsResponse {
            sync_active: stats.sync_active,
            connected_peers: stats.connected_peers,
            bytes_sent: stats.bytes_sent,
            bytes_received: stats.bytes_received,
        }))
    }
}

// --- Proto ↔ JSON conversion helpers ---
// These serialize proto messages to JSON maps for CRDT storage and back.
// Using serde_json::Value as the intermediate representation keeps the
// document store schema-agnostic.

fn platform_to_map(p: &proto::Platform) -> serde_json::Value {
    serde_json::json!({
        "id": p.id,
        "platform_type": p.platform_type,
        "name": p.name,
        "status": p.status,
        "latitude": p.latitude,
        "longitude": p.longitude,
        "altitude_m": p.altitude_m,
        "readiness": p.readiness,
        "capabilities": p.capabilities,
        "unit_id": p.unit_id,
        "callsign": p.callsign,
    })
}

fn map_to_platform(id: &str, json: &str) -> anyhow::Result<proto::Platform> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(proto::Platform {
        id: id.to_string(),
        platform_type: v["platform_type"].as_str().unwrap_or_default().to_string(),
        name: v["name"].as_str().unwrap_or_default().to_string(),
        status: v["status"].as_i64().unwrap_or_default() as i32,
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
    })
}

fn cell_to_map(c: &proto::Cell) -> serde_json::Value {
    serde_json::json!({
        "id": c.id,
        "name": c.name,
        "status": c.status,
        "platform_count": c.platform_count,
        "center_latitude": c.center_latitude,
        "center_longitude": c.center_longitude,
        "capabilities": c.capabilities,
        "formation_id": c.formation_id,
        "leader_id": c.leader_id,
    })
}

fn map_to_cell(id: &str, json: &str) -> anyhow::Result<proto::Cell> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(proto::Cell {
        id: id.to_string(),
        name: v["name"].as_str().unwrap_or_default().to_string(),
        status: v["status"].as_i64().unwrap_or_default() as i32,
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
    })
}

fn track_to_map(t: &proto::Track) -> serde_json::Value {
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
        "category": t.category,
    })
}

fn map_to_track(id: &str, json: &str) -> anyhow::Result<proto::Track> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(proto::Track {
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
        category: v["category"].as_i64().unwrap_or_default() as i32,
    })
}

fn command_to_map(c: &proto::Command) -> serde_json::Value {
    serde_json::json!({
        "id": c.id,
        "target_id": c.target_id,
        "command_type": c.command_type,
        "status": c.status,
        "created_at": c.created_at,
        "expires_at": c.expires_at,
        "payload_json": c.payload_json,
    })
}

fn map_to_command(id: &str, json: &str) -> anyhow::Result<proto::Command> {
    let v: serde_json::Value = serde_json::from_str(json)?;
    Ok(proto::Command {
        id: id.to_string(),
        target_id: v["target_id"].as_str().unwrap_or_default().to_string(),
        command_type: v["command_type"].as_str().unwrap_or_default().to_string(),
        status: v["status"].as_i64().unwrap_or_default() as i32,
        created_at: v["created_at"].as_i64().unwrap_or_default(),
        expires_at: v["expires_at"].as_i64().unwrap_or_default(),
        payload_json: v["payload_json"].as_str().unwrap_or_default().to_string(),
    })
}
