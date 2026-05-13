//! SidecarNode — lifecycle wrapper for the Peat mesh participation stack.
//!
//! Bootstrap is delegated to `peat_mesh::sync::AutomergeBackend::with_iroh`,
//! which subsumes the manual `AutomergeStore` + Iroh `Endpoint` +
//! `MeshSyncTransport` + `AutomergeSyncCoordinator` + `SyncProtocolHandler` +
//! `NetworkedIrohBlobStore` wiring this module used to do by hand. Sidecar-
//! specific layers stay here: encryption-at-rest cipher, the change-event
//! broadcast channel that `service.rs::subscribe` consumes, the
//! `connect_peer` retry loop, and the `start_sync`/`stop_sync` flag.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use peat_mesh::storage::json_convert::{automerge_to_json, json_to_automerge};
use peat_mesh::storage::{AutomergeStore, SyncTransport};
use peat_mesh::sync::{AutomergeBackend, AutomergeBackendConfig};
use peat_protocol::storage::file_distribution::IrohFileDistribution;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::attachments::config::AttachmentConfig;
use crate::attachments::registry::{BundleRegistry, RegistryConfig};
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
    /// Attachment distribution (PRD-006). Empty roots → service handlers
    /// return `Unimplemented`. The `IrohFileDistribution` substrate is only
    /// constructed when at least one root is configured.
    pub attachment_config: AttachmentConfig,
}

/// Manages the full Peat mesh stack and exposes operations for the gRPC service.
pub struct SidecarNode {
    node_id: String,
    backend: Arc<AutomergeBackend>,
    sync_active: Arc<AtomicBool>,
    change_tx: broadcast::Sender<ChangeEvent>,
    cipher: Option<StoreCipher>,
    /// PRD-006 attachment configuration. Carried through so handlers can
    /// short-circuit to `Unimplemented` when no `--attachment-root` is
    /// configured.
    attachment_config: AttachmentConfig,
    /// PRD-006 bundle handle table. Always present even when attachments
    /// are disabled (cheap empty registry) so the service layer can hold
    /// an unconditional reference.
    bundle_registry: Arc<BundleRegistry>,
    /// PRD-006 distribution substrate. `Some` iff
    /// `attachment_config.has_roots()` — built from the backend's blob
    /// store + Automerge store, paralleling the rest of the bootstrap.
    file_distribution: Option<Arc<IrohFileDistribution>>,
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
        let iroh_bind_addr = config
            .iroh_udp_port
            .map(|port| -> anyhow::Result<std::net::SocketAddr> {
                Ok(format!("0.0.0.0:{port}").parse()?)
            })
            .transpose()?;

        let backend = AutomergeBackend::with_iroh(AutomergeBackendConfig {
            data_dir: config.data_dir.clone(),
            formation_id: config.app_id.clone(),
            base64_shared_key: config.shared_key.clone(),
            iroh_bind_addr,
        })
        .await?;

        info!(
            node_id = %config.node_id,
            endpoint_id = %backend.blob_store().endpoint_id(),
            "iroh endpoint bound"
        );

        let cipher = match &config.encryption_key {
            Some(key) if !key.is_empty() => {
                let c = StoreCipher::from_base64_key(key)?;
                info!("encryption at rest enabled (AES-256-GCM)");
                Some(c)
            }
            _ => None,
        };

        let (change_tx, _) = broadcast::channel(256);

        // Forward AutomergeStore observer events into our ChangeEvent shape
        // (collection/doc_id split + cipher decrypt) for service.rs::subscribe.
        // The backend spawns its own observer-forwarder for `Node::observe()`,
        // but that emits a different event shape and doesn't decrypt.
        let observer_rx = backend.store().subscribe_to_observer_changes();
        let change_tx_clone = change_tx.clone();
        let store_clone = Arc::clone(backend.store());
        let cipher_clone = cipher.clone();
        tokio::spawn(async move {
            Self::forward_store_changes(observer_rx, change_tx_clone, store_clone, cipher_clone)
                .await;
        });

        // On every local write, push the doc to all connected peers.
        // AutomergeBackend doesn't spawn this — its `SyncEngine::start_sync`
        // is a separate concept (a flag toggle), not the on-change push loop.
        let sync_rx = backend.store().subscribe_to_changes();
        let sync_coordinator = Arc::clone(backend.coordinator());
        tokio::spawn(async move {
            Self::sync_on_change(sync_rx, sync_coordinator).await;
        });

        // PRD-006: bundle handle table is always present (the cheap empty
        // map case when attachments are disabled); FileDistribution is
        // built only when --attachment-root is configured.
        let bundle_registry = Arc::new(BundleRegistry::new(RegistryConfig {
            handle_retention_secs: config.attachment_config.handle_retention_secs,
            max_known_bundles: config.attachment_config.max_known_bundles,
        }));
        let file_distribution = if config.attachment_config.has_roots() {
            Some(Arc::new(IrohFileDistribution::new(
                Arc::clone(backend.blob_store()),
                Arc::clone(backend.store()),
            )))
        } else {
            None
        };

        Ok(Self {
            node_id: config.node_id,
            backend,
            sync_active: Arc::new(AtomicBool::new(false)),
            change_tx,
            cipher,
            attachment_config: config.attachment_config,
            bundle_registry,
            file_distribution,
        })
    }

    /// PRD-006 attachment config — used by handlers to decide whether to
    /// short-circuit to `Unimplemented`.
    pub fn attachment_config(&self) -> &AttachmentConfig {
        &self.attachment_config
    }

    /// PRD-006 bundle handle table.
    pub fn bundle_registry(&self) -> &Arc<BundleRegistry> {
        &self.bundle_registry
    }

    /// PRD-006 distribution substrate. `None` when attachments are
    /// disabled (no `--attachment-root` configured).
    pub fn file_distribution(&self) -> Option<&Arc<IrohFileDistribution>> {
        self.file_distribution.as_ref()
    }

    /// React to local document changes by syncing them with all connected peers.
    async fn sync_on_change(
        mut rx: broadcast::Receiver<String>,
        coordinator: Arc<peat_mesh::storage::AutomergeSyncCoordinator>,
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
        format!("{}", self.backend.blob_store().endpoint_id())
    }

    pub fn is_sync_active(&self) -> bool {
        self.sync_active.load(Ordering::Relaxed)
    }

    pub fn connected_peer_count(&self) -> u32 {
        self.backend.transport().connected_peers().len() as u32
    }

    // --- Sync Control ---

    pub async fn start_sync(&self) -> anyhow::Result<()> {
        // Sync all documents with all connected peers
        let peers = self.backend.transport().connected_peers();
        for peer_id in peers {
            if let Err(e) = self
                .backend
                .coordinator()
                .sync_all_documents_with_peer(peer_id)
                .await
            {
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
            bytes_sent: self.backend.coordinator().total_bytes_sent(),
            bytes_received: self.backend.coordinator().total_bytes_received(),
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

        self.backend
            .blob_store()
            .memory_lookup()
            .add_endpoint_info(peer_addr);

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
            match self
                .backend
                .transport()
                .connect_and_authenticate(peer_id)
                .await
            {
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
        self.backend
            .transport()
            .start_sync_connection(connection, Arc::clone(self.backend.coordinator()));

        info!(peer = endpoint_id_str, "connected to peer");
        Ok(())
    }

    pub async fn disconnect_peer(&self, endpoint_id_str: &str) -> anyhow::Result<()> {
        let peer_id: iroh::EndpointId = endpoint_id_str
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid endpoint ID: {e}"))?;
        // Close the QUIC connection (causes the background sync task to exit)
        if let Some(conn) = self.backend.transport().get_connection(&peer_id) {
            conn.close(0u32.into(), b"disconnect requested");
        }
        self.backend.transport().remove_connection(&peer_id);
        self.backend.coordinator().clear_peer_sync_state(peer_id);
        // Yield to let background sync tasks observe the closed connection and clean up
        tokio::task::yield_now().await;
        info!(peer = endpoint_id_str, "disconnected from peer");
        Ok(())
    }

    pub fn list_peers(&self) -> Vec<PeerInfoInternal> {
        self.backend
            .transport()
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
        let store = self.backend.store();
        let existing = store.get(&key)?;
        let doc = json_to_automerge(&json_value, existing.as_ref())?;

        store.put(&key, &doc)?;

        // `store.put` fires the AutomergeStore observer, which the
        // `forward_store_changes` task re-emits as a `ChangeEvent` on
        // `change_tx`. Emitting a second event directly here would
        // duplicate every local upsert on the broadcast channel and
        // make subscribe-with-filter behavior non-deterministic for
        // counting subscribers. The forwarder is the single source of
        // truth for upsert events — local and remote-sync alike.

        Ok(())
    }

    pub async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> anyhow::Result<Option<String>> {
        let key = format!("{collection}:{doc_id}");
        match self.backend.store().get(&key)? {
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
        self.backend.store().delete(&key)?;

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
        let entries = self.backend.store().scan_prefix(&prefix)?;
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
        self.backend.blob_store().shutdown().await?;
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
