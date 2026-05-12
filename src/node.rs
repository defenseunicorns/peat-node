//! SidecarNode — lifecycle wrapper for the Peat mesh participation stack.
//!
//! Follows the same bootstrap pattern as `peat-registry::mesh::node::create_mesh_stack()`:
//! AutomergeStore + IrohEndpoint + MeshSyncTransport + AutomergeSyncCoordinator.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use peat_mesh::security::FormationKey;
use peat_mesh::storage::json_convert::{automerge_to_json, json_to_automerge};
use peat_mesh::storage::{
    AutomergeStore, AutomergeSyncCoordinator, MeshSyncTransport, NetworkedIrohBlobStore,
    SyncProtocolHandler, SyncTransport, CAP_AUTOMERGE_ALPN,
};
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::crypto::StoreCipher;

/// Configuration for the sidecar node.
#[derive(Debug, Clone)]
pub struct SidecarConfig {
    pub node_id: String,
    pub app_id: String,
    pub shared_key: String,
    pub data_dir: PathBuf,
    pub peers: Vec<String>,
    /// Base64-encoded 32-byte AES-256-GCM key for encrypting document content at rest.
    /// When set, all document payloads are encrypted before storage and decrypted on read.
    pub encryption_key: Option<String>,
    /// UDP port to bind the Iroh endpoint on. `None` selects an ephemeral port.
    /// Pin this for deployments where peers need a stable address (e.g. Docker Compose
    /// or any case relying on direct peer-to-peer reachability instead of a relay).
    pub iroh_udp_port: Option<u16>,
}

/// Manages the full Peat mesh stack and exposes operations for the gRPC service.
pub struct SidecarNode {
    node_id: String,
    store: Arc<AutomergeStore>,
    coordinator: Arc<AutomergeSyncCoordinator>,
    sync_transport: Arc<MeshSyncTransport>,
    blob_store: Arc<NetworkedIrohBlobStore>,
    sync_active: Arc<AtomicBool>,
    change_tx: broadcast::Sender<ChangeEvent>,
    cipher: Option<StoreCipher>,
}

/// Internal change event for the broadcast channel.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub collection: String,
    pub doc_id: String,
    pub change_type: ChangeType,
    pub json_data: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum ChangeType {
    Upsert,
    Delete,
}

impl SidecarNode {
    /// Create a new SidecarNode, bootstrapping the full P2P sync stack.
    pub async fn new(config: SidecarConfig) -> anyhow::Result<Self> {
        let automerge_dir = config.data_dir.join("automerge");
        let iroh_dir = config.data_dir.join("iroh");
        tokio::fs::create_dir_all(&automerge_dir).await?;
        tokio::fs::create_dir_all(&iroh_dir).await?;

        // 1. Open Automerge CRDT store
        let store = Arc::new(AutomergeStore::open(&automerge_dir)?);

        // 2. Build Iroh endpoint with memory lookup.
        //
        // `presets::N0DisableRelay` configures the endpoint with n0's DNS/pkarr
        // discovery but `RelayMode::Disabled`. Our subsequent `.address_lookup`
        // overrides the n0 DNS publisher with the in-memory lookup used for
        // explicit `ConnectPeer` peering. Net effect: no dependency on n0's
        // public relay pool. Operators that need NAT traversal for production
        // can opt back into a relay by passing a relay URL via the
        // `ConnectPeer` RPC.
        let memory_lookup = iroh::address_lookup::memory::MemoryLookup::new();
        let mut builder = iroh::Endpoint::builder(iroh::endpoint::presets::N0DisableRelay)
            .address_lookup(memory_lookup.clone());
        if let Some(port) = config.iroh_udp_port {
            let bind_addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse()?;
            builder = builder.bind_addr(bind_addr)?;
        }
        let endpoint = builder.bind().await?;

        info!(
            node_id = %config.node_id,
            endpoint_id = %endpoint.id(),
            "iroh endpoint bound"
        );

        // 3. Derive formation key from shared secret for peer authentication
        let formation_key = FormationKey::from_base64(&config.app_id, &config.shared_key)
            .map_err(|e| anyhow::anyhow!("invalid formation key: {e}"))?;

        // 4. Create sync transport wrapping the Iroh endpoint
        let sync_transport = Arc::new(MeshSyncTransport::new(
            endpoint.clone(),
            formation_key.clone(),
        ));

        // 5. Create sync coordinator
        let coordinator = Arc::new(AutomergeSyncCoordinator::new(
            Arc::clone(&store),
            sync_transport.clone(),
        ));

        // 6. Create sync protocol handler (accepts incoming CRDT sync connections)
        let handler = SyncProtocolHandler::new(
            sync_transport.clone(),
            coordinator.clone(),
            formation_key.clone(),
        );

        // 7. Create networked blob store with sync protocol registered
        let blob_store = NetworkedIrohBlobStore::from_endpoint_with_protocols(
            iroh_dir,
            endpoint,
            memory_lookup,
            vec![(CAP_AUTOMERGE_ALPN, Box::new(handler))],
        )
        .await?;
        // from_endpoint_with_protocols already returns Arc<NetworkedIrohBlobStore>
        let blob_store = blob_store;

        // Initialize optional encryption cipher
        let cipher = match &config.encryption_key {
            Some(key) if !key.is_empty() => {
                let c = StoreCipher::from_base64_key(key)?;
                info!("encryption at rest enabled (AES-256-GCM)");
                Some(c)
            }
            _ => None,
        };

        let (change_tx, _) = broadcast::channel(256);

        // Spawn a task to forward store observer changes to the broadcast channel
        let observer_rx = store.subscribe_to_observer_changes();
        let change_tx_clone = change_tx.clone();
        let store_clone = Arc::clone(&store);
        let cipher_clone = cipher.clone();
        tokio::spawn(async move {
            Self::forward_store_changes(observer_rx, change_tx_clone, store_clone, cipher_clone)
                .await;
        });

        // Spawn a sync loop: when local documents change, push to all peers
        let sync_rx = store.subscribe_to_changes();
        let sync_coordinator = Arc::clone(&coordinator);
        tokio::spawn(async move {
            Self::sync_on_change(sync_rx, sync_coordinator).await;
        });

        Ok(Self {
            node_id: config.node_id,
            store,
            coordinator,
            sync_transport,
            blob_store,
            sync_active: Arc::new(AtomicBool::new(false)),
            change_tx,
            cipher,
        })
    }

    /// React to local document changes by syncing them with all connected peers.
    async fn sync_on_change(
        mut rx: broadcast::Receiver<String>,
        coordinator: Arc<AutomergeSyncCoordinator>,
    ) {
        loop {
            match rx.recv().await {
                Ok(doc_key) => {
                    if let Err(e) = coordinator.sync_document_with_all_peers(&doc_key).await {
                        warn!(doc_key, "sync to peers failed: {e}");
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("sync change listener lagged {n} messages");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    /// Forward Automerge store change notifications to the broadcast channel.
    async fn forward_store_changes(
        mut rx: broadcast::Receiver<String>,
        tx: broadcast::Sender<ChangeEvent>,
        store: Arc<AutomergeStore>,
        cipher: Option<StoreCipher>,
    ) {
        loop {
            match rx.recv().await {
                Ok(key) => {
                    // Keys are "collection:doc_id"
                    if let Some((collection, doc_id)) = key.split_once(':') {
                        // Try to read the current value to include in the change event
                        let raw = match store.get(&key) {
                            Ok(Some(doc)) => automerge_to_json(&doc)
                                .get("value")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            _ => None,
                        };
                        // Decrypt if encrypted
                        let json_data = match raw {
                            Some(v) if crate::crypto::is_encrypted(&v) => match &cipher {
                                Some(c) => match c.decrypt(&v) {
                                    Ok(plain) => Some(plain),
                                    Err(e) => {
                                        warn!(key, "failed to decrypt change event: {e}");
                                        None
                                    }
                                },
                                None => Some(v),
                            },
                            other => other,
                        };
                        let _ = tx.send(ChangeEvent {
                            collection: collection.to_string(),
                            doc_id: doc_id.to_string(),
                            change_type: ChangeType::Upsert,
                            json_data,
                        });
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("store observer lagged {n} messages");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }

    // --- Lifecycle ---

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn endpoint_addr(&self) -> String {
        format!("{}", self.blob_store.endpoint_id())
    }

    pub fn is_sync_active(&self) -> bool {
        self.sync_active.load(Ordering::Relaxed)
    }

    pub fn connected_peer_count(&self) -> u32 {
        self.sync_transport.connected_peers().len() as u32
    }

    // --- Sync Control ---

    pub async fn start_sync(&self) -> anyhow::Result<()> {
        // Sync all documents with all connected peers
        let peers = self.sync_transport.connected_peers();
        for peer_id in peers {
            if let Err(e) = self.coordinator.sync_all_documents_with_peer(peer_id).await {
                warn!(peer = %peer_id, "initial sync failed: {e}");
            }
        }
        self.sync_active.store(true, Ordering::Relaxed);
        info!("sync started");
        Ok(())
    }

    pub async fn stop_sync(&self) -> anyhow::Result<()> {
        self.sync_active.store(false, Ordering::Relaxed);
        info!("sync stopped");
        Ok(())
    }

    pub fn sync_stats(&self) -> SyncStats {
        SyncStats {
            sync_active: self.is_sync_active(),
            connected_peers: self.connected_peer_count(),
            bytes_sent: self.coordinator.total_bytes_sent(),
            bytes_received: self.coordinator.total_bytes_received(),
        }
    }

    // --- Peer Management ---

    /// Connect to a peer by endpoint ID, using direct addresses and/or a relay URL.
    ///
    /// At least one of `addresses` or `relay_url` must be non-empty — without
    /// any reachability hints there is no way to locate the peer. `addresses`
    /// accepts `host:port` (the host is resolved via DNS) or `ip:port`.
    pub async fn connect_peer(
        &self,
        endpoint_id_str: &str,
        addresses: &[String],
        relay_url: &str,
    ) -> anyhow::Result<()> {
        let peer_id: iroh::EndpointId = endpoint_id_str
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid endpoint ID: {e}"))?;

        let has_addresses = addresses.iter().any(|a| !a.is_empty());
        let has_relay = !relay_url.is_empty();
        if !has_addresses && !has_relay {
            return Err(anyhow::anyhow!(
                "ConnectPeer requires at least one of `addresses` or `relay_url` — \
                 the n0 public relay is no longer used by default"
            ));
        }

        let mut peer_addr = iroh::EndpointAddr::new(peer_id);

        for addr_str in addresses {
            if addr_str.is_empty() {
                continue;
            }
            // Resolve via DNS if needed; `lookup_host` handles both "host:port"
            // and "ip:port" forms. Iterate every resolved address — round-robin
            // DNS, dual-stack IPv4/IPv6, and Kubernetes headless services all
            // produce multi-record responses, and dropping all but the first
            // hides reachable paths from Iroh.
            let resolved = tokio::net::lookup_host(addr_str.as_str())
                .await
                .map_err(|e| anyhow::anyhow!("resolve `{addr_str}`: {e}"))?;
            let mut any_added = false;
            for socket in resolved {
                peer_addr = peer_addr.with_ip_addr(socket);
                any_added = true;
            }
            if !any_added {
                return Err(anyhow::anyhow!("no addresses resolved for `{addr_str}`"));
            }
        }

        if has_relay {
            let relay: iroh::RelayUrl = relay_url
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid relay URL `{relay_url}`: {e}"))?;
            peer_addr = peer_addr.with_relay_url(relay);
        }

        self.blob_store.memory_lookup().add_endpoint_info(peer_addr);

        // Connect and authenticate via formation key handshake.
        //
        // Workaround for upstream peat#759: the initiator's `open_bi()`
        // and the acceptor's `accept_bi()` don't always pair on the
        // first attempt during HMAC challenge-response, returning a
        // fast `formation auth failed (code 1)` close. Evidence in
        // peat#759's comment thread is that a single retry succeeds;
        // the race is timing-sensitive on startup. We retry up to
        // `CONNECT_RETRY_ATTEMPTS - 1` times with a small backoff. All
        // failure classes retry — the fast-failure cases (handshake
        // race) cost ~200ms each; genuine unreachable peers still
        // converge to a final error after the per-attempt timeout
        // elapses each loop, which is acceptable for a config that
        // rarely changes after deployment.
        const CONNECT_RETRY_ATTEMPTS: usize = 3;
        const CONNECT_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(200);
        let mut attempt = 0;
        let connection = loop {
            attempt += 1;
            match self.sync_transport.connect_and_authenticate(peer_id).await {
                Ok(c) => break c,
                Err(e) if attempt < CONNECT_RETRY_ATTEMPTS => {
                    warn!(
                        peer = endpoint_id_str,
                        attempt,
                        max_attempts = CONNECT_RETRY_ATTEMPTS,
                        "connect_and_authenticate failed, retrying: {e}"
                    );
                    tokio::time::sleep(CONNECT_RETRY_BACKOFF).await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "connect_and_authenticate failed after {attempt} attempts: {e}"
                    ));
                }
            }
        };

        // Register the connection for CRDT sync
        self.sync_transport
            .start_sync_connection(connection, self.coordinator.clone());

        info!(peer = endpoint_id_str, "connected to peer");
        Ok(())
    }

    pub async fn disconnect_peer(&self, endpoint_id_str: &str) -> anyhow::Result<()> {
        let peer_id: iroh::EndpointId = endpoint_id_str
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid endpoint ID: {e}"))?;
        // Close the QUIC connection (causes the background sync task to exit)
        if let Some(conn) = self.sync_transport.get_connection(&peer_id) {
            conn.close(0u32.into(), b"disconnect requested");
        }
        self.sync_transport.remove_connection(&peer_id);
        self.coordinator.clear_peer_sync_state(peer_id);
        // Yield to let background sync tasks observe the closed connection and clean up
        tokio::task::yield_now().await;
        info!(peer = endpoint_id_str, "disconnected from peer");
        Ok(())
    }

    pub fn list_peers(&self) -> Vec<PeerInfoInternal> {
        self.sync_transport
            .connected_peers()
            .into_iter()
            .map(|id| PeerInfoInternal {
                endpoint_id: id.to_string(),
                addresses: vec![],
                connected: true,
            })
            .collect()
    }

    // --- Document Operations ---
    // Documents are stored as Automerge documents with a single "value" key
    // containing the JSON string.

    pub async fn put_document(
        &self,
        collection: &str,
        doc_id: &str,
        json_data: &str,
    ) -> anyhow::Result<()> {
        // Validate JSON
        let _: serde_json::Value =
            serde_json::from_str(json_data).map_err(|e| anyhow::anyhow!("invalid JSON: {e}"))?;

        let key = format!("{collection}:{doc_id}");

        // Optionally encrypt the payload before storing
        let store_value = match &self.cipher {
            Some(c) => c.encrypt(json_data)?,
            None => json_data.to_string(),
        };

        // Create or update an Automerge document with the (possibly encrypted) value
        let json_value = serde_json::json!({ "value": store_value });
        let existing = self.store.get(&key)?;
        let doc = json_to_automerge(&json_value, existing.as_ref())?;

        self.store.put(&key, &doc)?;

        // Local change notification
        let _ = self.change_tx.send(ChangeEvent {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            change_type: ChangeType::Upsert,
            json_data: Some(json_data.to_string()),
        });

        Ok(())
    }

    pub async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> anyhow::Result<Option<String>> {
        let key = format!("{collection}:{doc_id}");
        match self.store.get(&key)? {
            Some(doc) => {
                let value = automerge_to_json(&doc)
                    .get("value")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                Ok(self.maybe_decrypt(value)?)
            }
            None => Ok(None),
        }
    }

    pub async fn delete_document(&self, collection: &str, doc_id: &str) -> anyhow::Result<()> {
        let key = format!("{collection}:{doc_id}");
        self.store.delete(&key)?;

        let _ = self.change_tx.send(ChangeEvent {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            change_type: ChangeType::Delete,
            json_data: None,
        });

        Ok(())
    }

    pub async fn list_documents(&self, collection: &str) -> anyhow::Result<Vec<String>> {
        let prefix = format!("{collection}:");
        let entries = self.store.scan_prefix(&prefix)?;
        Ok(entries
            .into_iter()
            .filter_map(|(k, _)| k.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect())
    }

    /// Decrypt a value if it's encrypted and a cipher is configured.
    /// Transparently passes through plaintext values (backward compatible).
    fn maybe_decrypt(&self, value: Option<String>) -> anyhow::Result<Option<String>> {
        match value {
            Some(v) if crate::crypto::is_encrypted(&v) => match &self.cipher {
                Some(c) => Ok(Some(c.decrypt(&v)?)),
                None => Ok(Some(v)), // no cipher configured, return as-is
            },
            other => Ok(other),
        }
    }

    /// Subscribe to document changes. Returns a broadcast receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.change_tx.subscribe()
    }

    /// Shutdown the node gracefully.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.blob_store.shutdown().await?;
        Ok(())
    }
}

pub struct SyncStats {
    pub sync_active: bool,
    pub connected_peers: u32,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

pub struct PeerInfoInternal {
    pub endpoint_id: String,
    pub addresses: Vec<String>,
    pub connected: bool,
}
