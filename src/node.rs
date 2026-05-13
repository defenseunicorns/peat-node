//! SidecarNode — lifecycle wrapper for the Peat mesh participation stack.
//!
//! Uses the same peat-protocol stack as peat-ffi to ensure wire-compatible
//! formation handshake and CRDT sync with all peat clients (iOS, Android, etc).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use iroh::PublicKey;
use peat_mesh::config::IrohConfig;
use peat_mesh::storage::json_convert::automerge_to_json;
use peat_mesh::storage::{
    BlobHandle, BlobHash, BlobMetadata, BlobProgress, BlobStore, BlobToken,
    NetworkedIrohBlobStore,
};
use peat_mesh::storage::AutomergeStore;
use peat_protocol::network::IrohTransport;
use peat_protocol::storage::{AutomergeBackend, StorageBackend};
use peat_protocol::sync::automerge::AutomergeIrohBackend;
use peat_protocol::sync::{BackendConfig, DataSyncBackend, TransportConfig};
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
    pub encryption_key: Option<String>,
    /// Enable the deployer task (disabled by default; explicit opt-in on receiver nodes).
    pub enable_deployer: bool,
    /// Directory for blob storage and metadata sidecars. Never under /tmp (K8s memory-backed).
    pub blob_work_dir: PathBuf,
    /// Timeout in seconds for blob download operations.
    pub download_timeout_secs: u64,
}

/// Manages the full Peat mesh stack using peat-protocol for wire compatibility.
pub struct SidecarNode {
    node_id: String,
    store: Arc<AutomergeStore>,
    storage_backend: Arc<AutomergeBackend>,
    sync_backend: Arc<AutomergeIrohBackend>,
    iroh_transport: Arc<IrohTransport>,
    sync_active: Arc<AtomicBool>,
    change_tx: broadcast::Sender<ChangeEvent>,
    cipher: Option<StoreCipher>,
    blob_store: Arc<NetworkedIrohBlobStore>,
    blob_work_dir: PathBuf,
    enable_deployer: bool,
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
    /// Create a new SidecarNode using the peat-protocol stack (same as peat-ffi).
    pub async fn new(config: SidecarConfig) -> anyhow::Result<Self> {
        let storage_path = config.data_dir.clone();
        tokio::fs::create_dir_all(&storage_path).await?;

        // 1. Open Automerge CRDT store (same as peat-ffi)
        let store = Arc::new(AutomergeStore::open(&storage_path)?);

        // 2. Create IrohTransport with mDNS discovery (same as peat-ffi).
        //    Must be created BEFORE the blob store so that endpoint_addr() and
        //    blob store construction can proceed independently.
        let seed = format!("{}/{}", config.app_id, storage_path.display());
        let bind_addr: std::net::SocketAddr = "0.0.0.0:0".parse().unwrap();
        let transport = Arc::new(
            IrohTransport::from_seed_with_discovery_at_addr(&seed, bind_addr)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create transport: {e}"))?,
        );

        let endpoint_addr = transport.endpoint_addr();
        info!(
            node_id = %config.node_id,
            endpoint_id = %hex::encode(transport.endpoint_id().as_bytes()),
            endpoint_addr = ?endpoint_addr,
            "peat-protocol transport bound"
        );

        // D-04 upgrade (BLOB-03): NetworkedIrohBlobStore replaces IrohBlobStore.
        //
        // Architecture note: peat-mesh 0.8.2 uses a separate Iroh endpoint for
        // the blob store (confirmed from peat-protocol AutomergeIrohBackend source,
        // and from NetworkedIrohBlobStore::from_config which creates its own
        // Endpoint via build_endpoint). This differs from the local peat-mesh 0.5.2
        // which had StaticProvider-based endpoint sharing. Using from_config is the
        // correct approach for 0.8.2 — see SUMMARY.md deviation D-01.
        tokio::fs::create_dir_all(&config.blob_work_dir).await?;
        let iroh_config = IrohConfig {
            download_timeout: Duration::from_secs(config.download_timeout_secs),
            ..Default::default()
        };
        let blob_store = NetworkedIrohBlobStore::from_config(
            config.blob_work_dir.clone(),
            &iroh_config,
        )
        .await
        .map_err(|e| anyhow::anyhow!(
            "failed to initialize networked blob store at {}: {e}",
            config.blob_work_dir.display()
        ))?;
        // NetworkedIrohBlobStore::from_config returns Arc<Self> directly.

        // Startup re-import (D-05 / BLOB-02):
        // list_local_blobs() scans .meta.json sidecars in blob_work_dir; for each token we
        // reload the raw bytes from disk into the fresh store so the sender can resume
        // serving blobs after a process restart.
        let existing_blobs = blob_store.list_local_blobs();
        let mut reimported: usize = 0;
        let mut skipped: usize = 0;
        for token in &existing_blobs {
            let blob_file = config.blob_work_dir.join(token.hash.as_hex());
            if !blob_file.exists() {
                tracing::warn!(
                    hash = %token.hash.as_hex(),
                    "sidecar exists but blob file missing; skipping re-import"
                );
                skipped += 1;
                continue;
            }
            match tokio::fs::read(&blob_file).await {
                Ok(content) => {
                    if let Err(e) = blob_store.store_api().add_bytes(content).await {
                        tracing::warn!(hash = %token.hash.as_hex(), "failed to re-import blob: {e}");
                        skipped += 1;
                    } else {
                        reimported += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(hash = %token.hash.as_hex(), "blob file unreadable: {e}");
                    skipped += 1;
                }
            }
        }
        tracing::info!(
            blob_work_dir = %config.blob_work_dir.display(),
            reimported,
            skipped,
            "startup blob re-import complete"
        );

        // 3. Create storage backend with transport (same as peat-ffi)
        let storage_backend = Arc::new(AutomergeBackend::with_transport(
            Arc::clone(&store),
            Arc::clone(&transport),
        ));

        // 4. Create sync backend with formation key auth (same as peat-ffi)
        let sync_backend = Arc::new(AutomergeIrohBackend::new(
            Arc::clone(&storage_backend),
            Arc::clone(&transport),
        ));

        // 5. Initialize sync backend with credentials (same as peat-ffi)
        let backend_config = BackendConfig {
            app_id: config.app_id.clone(),
            persistence_dir: storage_path.clone(),
            shared_key: Some(config.shared_key.clone()),
            transport: TransportConfig::default(),
            extra: std::collections::HashMap::new(),
        };

        sync_backend
            .initialize(backend_config)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to initialize sync backend: {}", e))?;

        info!("sync backend initialized with formation key");

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

        // Spawn a sync loop: when local documents change, sync with all peers
        let sync_rx = store.subscribe_to_changes();
        let sync_for_change = Arc::clone(&sync_backend);
        tokio::spawn(async move {
            Self::sync_on_change(sync_rx, sync_for_change).await;
        });

        Ok(Self {
            node_id: config.node_id,
            store,
            storage_backend,
            sync_backend,
            iroh_transport: transport,
            sync_active: Arc::new(AtomicBool::new(false)),
            change_tx,
            cipher,
            blob_store,
            blob_work_dir: config.blob_work_dir,
            enable_deployer: config.enable_deployer,
        })
    }

    /// React to local document changes by syncing with all connected peers.
    async fn sync_on_change(
        mut rx: broadcast::Receiver<String>,
        sync_backend: Arc<AutomergeIrohBackend>,
    ) {
        loop {
            match rx.recv().await {
                Ok(doc_key) => {
                    if let Err(e) = sync_backend.sync_document(&doc_key).await {
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
                    if let Some((collection, doc_id)) = key.split_once(':') {
                        let raw = match store.get(&key) {
                            Ok(Some(doc)) => automerge_to_json(&doc)
                                .get("value")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string()),
                            _ => None,
                        };
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
        hex::encode(self.iroh_transport.endpoint_id().as_bytes())
    }

    pub fn endpoint_full_addr(&self) -> String {
        let addr = self.iroh_transport.endpoint_addr();
        let id = hex::encode(self.iroh_transport.endpoint_id().as_bytes());
        format!("{id} ({addr:?})")
    }

    pub fn is_sync_active(&self) -> bool {
        self.sync_active.load(Ordering::Relaxed)
    }

    pub fn connected_peer_count(&self) -> u32 {
        self.iroh_transport.peer_count() as u32
    }

    /// Return the directory used for blob storage and .meta.json sidecars.
    ///
    /// Exposed so the watcher (SYNC-01) can resolve local package file paths
    /// of the form `{blob_work_dir}/{hash_hex}` that were produced by
    /// `publish_blob()`.
    pub fn blob_work_dir(&self) -> &std::path::Path {
        &self.blob_work_dir
    }

    // --- Sync Control ---

    pub async fn start_sync(&self) -> anyhow::Result<()> {
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
            bytes_sent: 0,
            bytes_received: 0,
        }
    }

    // --- Peer Management ---

    /// Connect to a peer using the peat-protocol IrohTransport (wire-compatible with peat-ffi).
    pub async fn connect_peer(&self, endpoint_id_str: &str) -> anyhow::Result<()> {
        // Parse "endpoint_id@addr1,addr2" or bare "endpoint_id"
        let (id_str, direct_addrs) = if let Some((id, addrs)) = endpoint_id_str.split_once('@') {
            let addrs: Vec<String> = addrs.split(',').map(|a| a.trim().to_string()).collect();
            (id, addrs)
        } else {
            (endpoint_id_str, vec![])
        };

        // Build PeerInfo matching peat-protocol's expected format
        let peer_info = peat_protocol::network::peer_config::PeerInfo {
            name: format!("peer-{}", &id_str[..8.min(id_str.len())]),
            node_id: id_str.to_string(),
            addresses: direct_addrs.clone(),
            relay_url: None,
        };

        info!(peer = id_str, ?direct_addrs, "connecting to peer via peat-protocol");

        // Use IrohTransport.connect_peer (same code path as peat-ffi)
        let conn_opt = self
            .iroh_transport
            .connect_peer(&peer_info)
            .await
            .map_err(|e| anyhow::anyhow!("connect_peer failed: {e}"))?;

        // If we got a new connection, perform formation handshake
        if let Some(conn) = conn_opt {
            let peer_id = conn.remote_id();

            if let Some(formation_key) = self.sync_backend.formation_key() {
                use peat_protocol::network::perform_initiator_handshake;
                match perform_initiator_handshake(&conn, &formation_key).await {
                    Ok(()) => {
                        self.iroh_transport.emit_peer_connected(peer_id);
                        info!(peer = id_str, "peer connected and authenticated");
                    }
                    Err(e) => {
                        conn.close(1u32.into(), b"authentication failed");
                        self.iroh_transport.disconnect(&peer_id).ok();
                        return Err(anyhow::anyhow!("Formation handshake failed: {e}"));
                    }
                }
            } else {
                self.iroh_transport.emit_peer_connected(peer_id);
                info!(peer = id_str, "peer connected (no formation key)");
            }
        }

        Ok(())
    }

    pub async fn disconnect_peer(&self, endpoint_id_str: &str) -> anyhow::Result<()> {
        let connected = self.iroh_transport.connected_peers();
        for endpoint_id in connected {
            if hex::encode(endpoint_id.as_bytes()) == endpoint_id_str {
                self.iroh_transport
                    .disconnect(&endpoint_id)
                    .map_err(|e| anyhow::anyhow!("disconnect failed: {e}"))?;
                info!(peer = endpoint_id_str, "disconnected from peer");
                return Ok(());
            }
        }
        Err(anyhow::anyhow!("peer not found: {endpoint_id_str}"))
    }

    pub fn list_peers(&self) -> Vec<PeerInfoInternal> {
        self.iroh_transport
            .connected_peers()
            .into_iter()
            .map(|id| PeerInfoInternal {
                endpoint_id: hex::encode(id.as_bytes()),
                addresses: vec![],
                connected: true,
            })
            .collect()
    }

    // --- Document Operations ---
    // Uses raw AutomergeStore with "collection:doc_id" keys.
    // This ensures CRDT sync works (sync operates on raw store keys).
    // peat-ffi uses collection() API which has a different key namespace,
    // so synced docs are read via the raw store on the iOS side too.

    pub async fn put_document(
        &self,
        collection: &str,
        doc_id: &str,
        json_data: &str,
    ) -> anyhow::Result<()> {
        let _: serde_json::Value =
            serde_json::from_str(json_data).map_err(|e| anyhow::anyhow!("invalid JSON: {e}"))?;

        let key = format!("{collection}:{doc_id}");

        let store_value = match &self.cipher {
            Some(c) => c.encrypt(json_data)?,
            None => json_data.to_string(),
        };

        let json_value = serde_json::json!({ "value": store_value });
        let existing = self.store.get(&key)?;
        let doc = peat_mesh::storage::json_convert::json_to_automerge(&json_value, existing.as_ref())?;
        self.store.put(&key, &doc)?;

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

    /// Publish a local file to the Iroh blob store.
    ///
    /// Returns a BlobToken containing the BLAKE3 content hash and size in bytes.
    /// The file is read, hashed, added to the store, its .meta.json sidecar is
    /// written to blob_work_dir, and the raw bytes are exported to
    /// `blob_work_dir/{hash_hex}` so that a subsequent restart can re-import it.
    ///
    /// BLOB-01: content-addressed publish.
    pub async fn publish_blob(
        &self,
        path: &std::path::Path,
        name: &str,
    ) -> anyhow::Result<BlobToken> {
        let metadata = BlobMetadata::with_name(name);
        let token = self.blob_store.create_blob(path, metadata).await?;

        // Per RESEARCH.md Pitfall 3: create_blob writes the sidecar but does NOT
        // export the raw bytes to disk. Export explicitly so the next SidecarNode
        // startup can re-import this blob (BLOB-02). Use blob_work_dir stored on
        // self (fallback option (a) per plan) since IrohBlobStore::blob_dir() may
        // differ between peat-mesh 0.5.2 local source and 0.8.2 crates.io.
        let blob_file = self.blob_work_dir.join(token.hash.as_hex());
        if !blob_file.exists() {
            let bytes = tokio::fs::read(path).await?;
            tokio::fs::write(&blob_file, &bytes).await?;
        }
        Ok(token)
    }

    /// List all blobs known locally (by sidecar scan).
    /// Exposed as a read-only helper for tests and Phase 3 discovery code.
    pub fn list_local_blobs(&self) -> Vec<BlobToken> {
        self.blob_store.list_local_blobs()
    }

    // --- Blob P2P wrapper methods (BLOB-03 / D-04 upgrade) ---

    /// Parse a hex-encoded endpoint ID (32 bytes = 64 hex chars) into an iroh::EndpointId.
    ///
    /// Private helper — shared by add_blob_peer and advertise_blob_for_hash.
    ///
    /// Pattern source: peat-mesh sync_persistence.rs line 335 (verified this session).
    /// Input validation (T-03-01): hex::decode rejects non-hex; explicit 32-byte length
    /// check; PublicKey::from_bytes rejects invalid curve points. Never panics.
    fn parse_endpoint_id_hex(endpoint_id_hex: &str) -> anyhow::Result<iroh::EndpointId> {
        let bytes = hex::decode(endpoint_id_hex)
            .map_err(|e| anyhow::anyhow!("invalid endpoint_id hex '{endpoint_id_hex}': {e}"))?;
        if bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "endpoint_id must be 32 bytes, got {}",
                bytes.len()
            ));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        let public_key = PublicKey::from_bytes(&arr)
            .map_err(|e| anyhow::anyhow!("invalid public key bytes: {e}"))?;
        Ok(public_key)
    }

    /// BLOB-03: Add a peer that this node may want to fetch blobs from.
    ///
    /// Accepts a 64-char hex-encoded endpoint ID (as written to CRDT doc
    /// `sender_endpoint_id` by the sender). Wraps
    /// NetworkedIrohBlobStore::add_peer (iroh_blob_store.rs line 937 in 0.8.2).
    pub async fn add_blob_peer(&self, endpoint_id_hex: &str) -> anyhow::Result<()> {
        let peer = Self::parse_endpoint_id_hex(endpoint_id_hex)?;
        self.blob_store.add_peer(peer).await;
        Ok(())
    }

    /// BLOB-03: Record that a given peer has a specific blob hash.
    ///
    /// Must be called AFTER add_blob_peer — advertise_blob only updates the
    /// in-memory peer-blob index; it does not open a QUIC connection.
    /// Wraps NetworkedIrohBlobStore::advertise_blob (iroh_blob_store.rs line 964 in 0.8.2).
    pub async fn advertise_blob_for_hash(
        &self,
        endpoint_id_hex: &str,
        hash_hex: &str,
    ) -> anyhow::Result<()> {
        let peer = Self::parse_endpoint_id_hex(endpoint_id_hex)?;
        let hash = BlobHash::from_hex(hash_hex);
        self.blob_store.advertise_blob(peer, hash).await;
        Ok(())
    }

    /// BLOB-04: Fetch a blob from a known peer, forwarding BlobProgress events
    /// to the caller-supplied closure.
    ///
    /// Wraps NetworkedIrohBlobStore::fetch_blob (iroh_blob_store.rs line 1015 in 0.8.2).
    /// The progress closure receives: Started -> one Downloading{downloaded:0} -> Completed|Failed.
    /// (NetworkedIrohBlobStore does not surface incremental chunk progress — documented
    /// limitation per RESEARCH.md "Critical API Reference" section.)
    pub async fn fetch_blob_from_peer<F>(
        &self,
        token: &BlobToken,
        progress: F,
    ) -> anyhow::Result<BlobHandle>
    where
        F: FnMut(BlobProgress) + Send + 'static,
    {
        self.blob_store.fetch_blob(token, progress).await
    }

    /// CRDT-03 helper: check whether a blob is already on the local filesystem.
    ///
    /// Used by the Phase 3 discovery loop to skip re-fetching known packages.
    /// Wraps the BlobStore trait method (iroh_blob_store.rs line 1083 in 0.8.2).
    pub fn blob_exists_locally(&self, hash: &BlobHash) -> bool {
        self.blob_store.blob_exists_locally(hash)
    }

    fn maybe_decrypt(&self, value: Option<String>) -> anyhow::Result<Option<String>> {
        match value {
            Some(v) if crate::crypto::is_encrypted(&v) => match &self.cipher {
                Some(c) => Ok(Some(c.decrypt(&v)?)),
                None => Ok(Some(v)),
            },
            other => Ok(other),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.change_tx.subscribe()
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        // IrohTransport handles its own cleanup
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
