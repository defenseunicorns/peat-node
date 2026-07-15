//! SidecarNode — lifecycle wrapper for the Peat mesh participation stack.
//!
//! Bootstrap is delegated to `peat_mesh::sync::AutomergeBackend::with_iroh`,
//! which subsumes the manual `AutomergeStore` + Iroh `Endpoint` +
//! `MeshSyncTransport` + `AutomergeSyncCoordinator` + `SyncProtocolHandler` +
//! `NetworkedIrohBlobStore` wiring this module used to do by hand. Sidecar-
//! specific layers stay here: encryption-at-rest cipher, the change-event
//! broadcast channel that `service.rs::subscribe` consumes, the
//! `connect_peer` retry loop, and the `start_sync`/`stop_sync` flag.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::crypto::derive_iroh_node_key;
use crate::fanout::FanoutKind;
use base64::Engine as _;
use peat_mesh::discovery::{
    DiscoveryEvent, DiscoveryStrategy, KubernetesDiscovery, KubernetesDiscoveryConfig,
    MdnsDiscovery, PeerInfo,
};
use peat_mesh::qos::GcConfig;
use peat_mesh::storage::json_convert::{automerge_to_json, json_to_automerge};
use peat_mesh::storage::{AutomergeStore, ChangeOrigin, DocChange, SyncTransport, TtlConfig};
use peat_mesh::sync::{AutomergeBackend, AutomergeBackendConfig};
use peat_protocol::storage::file_distribution::{
    DistributionDocument, IrohFileDistribution, NodeTransferStatus,
};
use sha2::{Digest, Sha256};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::attachments::config::AttachmentConfig;
use crate::attachments::registry::{BundleRegistry, RegistryConfig};
use crate::attachments::runtime::BundleRuntimeStore;
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
    /// Deterministic iroh identity seed (peat-node#63 gap-4d). When `Some`, the
    /// iroh endpoint binds this fixed 32-byte secret key so the node's
    /// `EndpointId` is stable across restarts and computable offline by any
    /// holder of the shared key (see [`crate::identity`]). When `None`, iroh
    /// mints a random per-process identity. Derived in `main` from
    /// `(shared_key, node_id)`.
    pub iroh_secret_key: Option<[u8; 32]>,
    /// Blob-download stall threshold. `None` uses peat-mesh's default (30s).
    /// Lower it (e.g. 3-5s) for redundant-peer deployments where an
    /// unreachable preferred peer otherwise costs the full stall on the
    /// first fetch before the peer-health index demotes it (peat-mesh#137).
    pub blob_stall_timeout: Option<Duration>,
    /// Tombstone retention window in hours. `None` → 168 h (7-day default).
    /// Values below 24 h emit a startup warning (ADR-016: tombstone TTL must
    /// be ≥ the longest expected peer offline/partition window). peat-node#136.
    pub tombstone_ttl_hours: Option<u32>,
    /// GC sweep interval in seconds. `None` → 300 s (5-min default). peat-node#136.
    pub gc_interval_secs: Option<u64>,
    /// Max tombstones per GC sweep. `None` → 1000. peat-node#136.
    pub gc_batch_size: Option<usize>,
    /// Attachment distribution (PRD-006). Empty roots → service handlers
    /// return `Unimplemented`. The `IrohFileDistribution` substrate is only
    /// constructed when at least one root is configured.
    pub attachment_config: AttachmentConfig,
    /// Disable mDNS peer discovery. mDNS is on by default so that same-host
    /// nodes (e.g. `docker compose` or bare-metal dev) find each other
    /// automatically. Set to `true` in environments where multicast is
    /// unavailable or undesired (Kubernetes, air-gapped networks). Mirrors
    /// `PeatCredentials::disable_mdns` in `peat-cli`.
    pub disable_mdns: bool,
    // --- Kubernetes peer discovery (peat-node#63) ---
    /// Enable EndpointSlice-based peer discovery for in-cluster deployments.
    /// When `true`, a `KubernetesDiscovery` watcher is started. Requires
    /// `POD_NAME` env var (Kubernetes downward API) and a non-empty `shared_key`
    /// for deterministic iroh keypair derivation; if either is absent, K8s
    /// discovery is skipped with a warn log.
    pub enable_kubernetes_discovery: bool,
    /// Kubernetes namespace to watch. `None` → reads from the service-account
    /// mount (`/var/run/secrets/kubernetes.io/serviceaccount/namespace`), falls
    /// back to `"default"`.
    pub kubernetes_discovery_namespace: Option<String>,
    /// Label selector for EndpointSlice resources. Defaults to `"app=peat-node"`.
    pub kubernetes_discovery_label_selector: String,
    /// Annotation prefix for extracting peer metadata. Defaults to `"peat."`.
    pub kubernetes_discovery_annotation_prefix: String,
    /// EndpointSlice re-list interval in seconds. Defaults to 30.
    pub kubernetes_discovery_interval_secs: u64,
}

impl Default for SidecarConfig {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            app_id: String::new(),
            shared_key: String::new(),
            data_dir: PathBuf::new(),
            peers: Vec::new(),
            encryption_key: None,
            iroh_udp_port: None,
            iroh_secret_key: None,
            blob_stall_timeout: None,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
            attachment_config: AttachmentConfig::default(),
            disable_mdns: false,
            enable_kubernetes_discovery: false,
            kubernetes_discovery_namespace: None,
            kubernetes_discovery_label_selector: "app=peat-node".to_string(),
            kubernetes_discovery_annotation_prefix: "peat.".to_string(),
            kubernetes_discovery_interval_secs: 30,
        }
    }
}

/// Manages the full Peat mesh stack and exposes operations for the gRPC service.
pub struct SidecarNode {
    node_id: String,
    backend: Arc<AutomergeBackend>,
    sync_active: Arc<AtomicBool>,
    change_tx: broadcast::Sender<ChangeEvent>,
    // Wired into the NATS runtime in the next phase plan; exercised here via
    // the crate-private seam before that consumer exists.
    #[allow(dead_code)]
    bridge_change_tx: broadcast::Sender<BridgeChangeEvent>,
    local_revisions: Arc<Mutex<LocalRevisionGuard>>,
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
    /// PRD-006 per-bundle runtime: progress-channel fan-out + per-
    /// distribution state for the subscribe handler. Always present even
    /// when attachments are disabled so service handlers don't have to
    /// branch on Option.
    bundle_runtime: Arc<BundleRuntimeStore>,
    /// **peat-node#91** — registry of peers that have been successfully
    /// connected via [`connect_peer`], keyed by `EndpointId`. The
    /// background reconnect watchdog reads this map every
    /// [`RECONNECT_WATCHDOG_INTERVAL`] and re-establishes any peer whose
    /// connection has dropped out of `MeshSyncTransport::connected_peers()`
    /// (i.e. iroh's idle timeout fired during a network blackout).
    ///
    /// [`disconnect_peer`] removes from this map — an explicit disconnect
    /// is treated as "don't reconnect," distinguishing operator-initiated
    /// teardown from transient link loss.
    registered_peers: Arc<RwLock<HashMap<iroh::EndpointId, PeerRegistration>>>,
    /// Per-collection lifecycle configs (peat-node#55). Stored in memory and
    /// persisted to `data_dir/collection_configs.json` on each write.
    collection_configs: Arc<RwLock<HashMap<String, CollectionConfigEntry>>>,
    collection_configs_path: std::path::PathBuf,
    /// Kept alive so mDNS advertisement and browsing continue for the
    /// node lifetime. `None` when mDNS is disabled or failed to init.
    _mdns: Option<MdnsDiscovery>,
    /// Kept alive so K8s EndpointSlice watching continues for the node
    /// lifetime. `None` when `--enable-kubernetes-discovery` is false or
    /// discovery failed to start.
    _k8s_discovery: Option<KubernetesDiscovery>,
}

/// Address hint captured per [`SidecarNode::connect_peer`] invocation,
/// stored so the auto-reconnect watchdog (peat-node#91) can re-dial the
/// same peer post-blackout without the operator re-issuing the call.
///
/// Carries per-peer backoff state so a permanently-unreachable peer
/// doesn't get dialed every [`RECONNECT_WATCHDOG_INTERVAL`] forever —
/// each consecutive failure doubles the wait up to
/// [`RECONNECT_BACKOFF_MAX`]. A successful dial resets the backoff.
#[derive(Debug, Clone)]
struct PeerRegistration {
    addresses: Vec<String>,
    relay_url: String,
    /// Earliest `Instant` at which the watchdog should next attempt
    /// a re-dial. Set to `Instant::now()` on registration so the first
    /// post-blackout tick fires immediately, then to
    /// `now + backoff` after each failed attempt.
    next_attempt: std::time::Instant,
    /// Current reconnect backoff window. Starts at
    /// [`RECONNECT_BACKOFF_MIN`], doubles on each failure (capped at
    /// [`RECONNECT_BACKOFF_MAX`]), resets on success.
    backoff: Duration,
}

/// How often the reconnect watchdog wakes to check for dropped peers.
/// 5 s is fast enough that a 60 s blackout + 30 s drain window has
/// ample slack for the reconnect roundtrip + backlog drain to complete
/// within the drain budget (peat-node#91 UAT). Tunable here if a future
/// scenario surfaces a different sweet spot.
const RECONNECT_WATCHDOG_INTERVAL: Duration = Duration::from_secs(5);

/// Interval for the periodic peer-status heartbeat log. Coarse on purpose:
/// it's an operator-facing "who am I connected to / who can I target"
/// breadcrumb, not a fast control loop, so it shouldn't compete with the
/// watchdog's 5 s cadence or spam the log.
const PEER_STATUS_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Initial backoff for a freshly-registered peer or a peer whose last
/// dial succeeded. Equal to the watchdog interval so the first retry
/// fires on the very next tick after a transient drop.
const RECONNECT_BACKOFF_MIN: Duration = Duration::from_secs(5);

/// Upper bound on the watchdog's per-peer reconnect backoff. After this
/// many seconds of consecutive failure the watchdog keeps trying at this
/// cadence rather than growing indefinitely. 120 s matches the QA-review
/// suggestion (peat-node#99) — long enough to avoid wasting cycles on a
/// permanently-departed peer, short enough that a 2-minute partition
/// still recovers within one cycle.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(120);

/// Deletion semantics for a collection (peat-node#55 / ADR-016).
///
/// Mirrors `peat_mesh::qos::DeletionPolicy` for the sidecar API surface.
/// Full peat-mesh enforcement per collection requires `AutomergeBackend`
/// to gain a `set_deletion_policy()` surface; until then the policy is
/// persisted and surfaced through the collection-config RPCs so callers can
/// implement application-layer delete routing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum StoredDeletionPolicy {
    SoftDelete,
    Tombstone,
    ImplicitTTL,
    Immutable,
}

/// Per-collection lifecycle configuration persisted to disk (peat-node#55).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionConfigEntry {
    pub collection: String,
    pub deletion_policy: StoredDeletionPolicy,
    /// ADR-016 Tier 1 TTL in seconds. `None` = use mesh default.
    pub soft_delete_ttl_secs: Option<u64>,
    /// ADR-016 Tier 2 tombstone TTL in seconds. `None` = use mesh default (168 h).
    pub tombstone_ttl_secs: Option<u64>,
}

/// Internal change event for the broadcast channel.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub collection: String,
    pub doc_id: String,
    pub change_type: ChangeType,
    pub json_data: Option<String>,
}

/// Private transport-origin event consumed only by the native NATS bridge.
///
/// This deliberately does not extend [`ChangeEvent`]: client subscriptions
/// retain their existing local/remote/delete behavior and public shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BridgeChangeEvent {
    pub collection: String,
    pub doc_id: String,
    pub remote_peer_id: String,
    pub json_data: String,
}

const BRIDGE_CHANGE_CAPACITY: usize = 256;
const LOCAL_REVISION_CAPACITY: usize = 4096;
const MAX_REVISION_HEADS: usize = 64;
const REVISION_DIGEST_DOMAIN: &[u8] = b"peat-node:local-revision:v1";

/// Fixed-width, non-evicting journal of locally authored revisions.
///
/// The retained digest payload is exactly 4096 * 32 = 131,072 bytes plus
/// fixed metadata. It retains neither document keys nor Automerge head
/// vectors. Exhaustion is sticky and fail-closed until process restart.
struct LocalRevisionGuard {
    slots: Box<[[u8; 32]]>,
    len: usize,
    exhausted: bool,
}

impl LocalRevisionGuard {
    fn new() -> Self {
        Self {
            slots: vec![[0; 32]; LOCAL_REVISION_CAPACITY].into_boxed_slice(),
            len: 0,
            exhausted: false,
        }
    }

    fn digest_revision<'a>(
        &mut self,
        key: &str,
        heads: impl ExactSizeIterator<Item = &'a [u8]>,
    ) -> Option<[u8; 32]> {
        // Automerge 0.9.0 get_heads() has already collected and sorted every
        // current head before this check. This limit bounds only our digest
        // iteration and retained state, not that inherited temporary Vec.
        let head_count = heads.len();
        if self.exhausted || head_count > MAX_REVISION_HEADS {
            self.exhausted = true;
            return None;
        }

        let mut digest = Sha256::new();
        digest.update((REVISION_DIGEST_DOMAIN.len() as u64).to_be_bytes());
        digest.update(REVISION_DIGEST_DOMAIN);
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key.as_bytes());
        digest.update((head_count as u64).to_be_bytes());
        for head in heads {
            digest.update((head.len() as u64).to_be_bytes());
            digest.update(head);
        }
        Some(digest.finalize().into())
    }

    fn record<'a>(&mut self, key: &str, heads: impl ExactSizeIterator<Item = &'a [u8]>) -> bool {
        let Some(digest) = self.digest_revision(key, heads) else {
            return false;
        };
        if self.slots[..self.len].contains(&digest) {
            return true;
        }
        if self.len == self.slots.len() {
            self.exhausted = true;
            return false;
        }
        self.slots[self.len] = digest;
        self.len += 1;
        true
    }

    fn is_local<'a>(&mut self, key: &str, heads: impl ExactSizeIterator<Item = &'a [u8]>) -> bool {
        let Some(digest) = self.digest_revision(key, heads) else {
            return true;
        };
        self.slots[..self.len].contains(&digest)
    }
}

/// Read-only facade over the node's document store.
///
/// The backing `Arc<AutomergeStore>` is intentionally private and this type
/// implements no `Deref`, `AsRef`, or `Borrow`, so callers cannot recover a
/// mutable store handle.
///
#[derive(Clone)]
pub struct DocumentStoreReader {
    store: Arc<AutomergeStore>,
}

impl DocumentStoreReader {
    pub fn get(&self, key: &str) -> anyhow::Result<Option<serde_json::Value>> {
        self.store
            .get(key)
            .map(|doc| doc.map(|doc| automerge_to_json(&doc)))
    }

    pub fn scan_prefix(&self, prefix: &str) -> anyhow::Result<Vec<(String, serde_json::Value)>> {
        self.store.scan_prefix(prefix).map(|entries| {
            entries
                .into_iter()
                .map(|(key, doc)| (key, automerge_to_json(&doc)))
                .collect()
        })
    }

    pub fn keys_with_prefix(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        self.store.keys_with_prefix(prefix)
    }

    pub fn subscribe_to_observer_changes(&self) -> broadcast::Receiver<String> {
        self.store.subscribe_to_observer_changes()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ChangeType {
    Upsert,
    Delete,
}

/// Fixed, source-free classifications returned by bridge create operations.
///
/// Ingress uses these variants to decide whether an operation is permanent or
/// retryable without exposing payload text, encryption details, or store
/// error chains through bridge diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateBridgeDocumentError {
    AlreadyExists,
    InvalidInput,
    Encryption,
    Conversion,
    StoreRead,
    StoreWrite,
}

impl std::fmt::Display for CreateBridgeDocumentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::AlreadyExists => "bridge document already exists",
            Self::InvalidInput => "bridge document input is invalid",
            Self::Encryption => "bridge document encryption failed",
            Self::Conversion => "bridge document conversion failed",
            Self::StoreRead => "bridge document store read failed",
            Self::StoreWrite => "bridge document store write failed",
        })
    }
}

impl std::error::Error for CreateBridgeDocumentError {}

#[derive(Clone, Copy)]
enum DocumentWriteMode {
    Upsert,
    CreateOnly,
}

enum DocumentWriteError {
    AlreadyExists,
    InvalidInput(serde_json::Error),
    Encryption(anyhow::Error),
    Conversion(anyhow::Error),
    StoreRead(anyhow::Error),
    StoreWrite(anyhow::Error),
}

impl DocumentWriteError {
    fn classification(&self) -> CreateBridgeDocumentError {
        match self {
            Self::AlreadyExists => CreateBridgeDocumentError::AlreadyExists,
            Self::InvalidInput(_) => CreateBridgeDocumentError::InvalidInput,
            Self::Encryption(_) => CreateBridgeDocumentError::Encryption,
            Self::Conversion(_) => CreateBridgeDocumentError::Conversion,
            Self::StoreRead(_) => CreateBridgeDocumentError::StoreRead,
            Self::StoreWrite(_) => CreateBridgeDocumentError::StoreWrite,
        }
    }

    fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::AlreadyExists => anyhow::anyhow!("document already exists"),
            Self::InvalidInput(error) => anyhow::anyhow!("invalid JSON: {error}"),
            Self::Encryption(error)
            | Self::Conversion(error)
            | Self::StoreRead(error)
            | Self::StoreWrite(error) => error,
        }
    }
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

        let ttl_config = config.tombstone_ttl_hours.map(|hours| TtlConfig {
            tombstone_ttl_hours: hours,
            ..TtlConfig::default()
        });

        let gc_config = if config.gc_interval_secs.is_some() || config.gc_batch_size.is_some() {
            Some(GcConfig {
                gc_interval: config
                    .gc_interval_secs
                    .map(std::time::Duration::from_secs)
                    .unwrap_or_else(|| GcConfig::default().gc_interval),
                tombstone_batch_size: config
                    .gc_batch_size
                    .unwrap_or_else(|| GcConfig::default().tombstone_batch_size),
                ..GcConfig::default()
            })
        } else {
            None
        };

        let mut backend_cfg = AutomergeBackendConfig::default();
        backend_cfg.data_dir = config.data_dir.clone();
        backend_cfg.formation_id = config.app_id.clone();
        backend_cfg.base64_shared_key = config.shared_key.clone();
        backend_cfg.iroh_bind_addr = iroh_bind_addr;
        backend_cfg.download_stall_timeout = config.blob_stall_timeout;
        // peat-node already encrypts at a higher layer via `StoreCipher`
        // (see `forward_store_changes` below), so leave the peat-mesh-level
        // cipher as None for now.
        backend_cfg.cipher = None;
        backend_cfg.ttl_config = ttl_config;
        backend_cfg.gc_config = gc_config;
        // Deterministic iroh identity (peat-node#63 gap-4d). `Some` → the
        // endpoint binds a fixed keypair seeded from (shared_key, node_id) so
        // the EndpointId is stable across restarts and computable offline by
        // peers; `None` → iroh's random per-process identity. See
        // [`crate::identity`].
        backend_cfg.iroh_secret_key = config.iroh_secret_key;

        // peat-node#63 — deterministic iroh keypair for K8s peer discovery.
        // All pods in the same formation derive their iroh SecretKey from
        // HKDF-SHA256(ikm=shared_key, info="iroh:"+pod_name) so any peer can
        // compute any other pod's EndpointId from its pod name alone.
        // Requires POD_NAME env var (Kubernetes downward API) and a non-empty
        // shared_key. Absent either, we skip derivation and let iroh generate
        // a random key — K8s discovery will still start but won't be able to
        // dial discovered pods (connect_peer will fail to parse the pod name
        // as an iroh EndpointId and skip them), so a warn is logged.
        let pod_name: Option<String> = std::env::var("POD_NAME").ok().filter(|s| !s.is_empty());
        if config.enable_kubernetes_discovery {
            // Decision extracted into `resolve_k8s_identity` (pure, tested) so
            // the (POD_NAME, shared_key) matrix is covered without a cluster.
            match resolve_k8s_identity(pod_name.as_deref(), &config.shared_key) {
                K8sIdentity::Derived(seed) => {
                    backend_cfg.iroh_secret_key = Some(seed);
                    info!(
                        pod_name = pod_name.as_deref().unwrap_or_default(),
                        "K8s discovery: deterministic iroh keypair derived"
                    );
                }
                K8sIdentity::MissingPodName => {
                    warn!(
                        "K8s discovery enabled but POD_NAME env var is not set — \
                         iroh keypair will be random; peer discovery will not work"
                    );
                }
                K8sIdentity::EmptySharedKey => {
                    warn!(
                        "K8s discovery enabled but shared_key is empty — \
                         iroh keypair will be random; peer discovery will not work"
                    );
                }
                K8sIdentity::InvalidSharedKey => {
                    return Err(anyhow::anyhow!("shared_key base64 decode failed"));
                }
            }
        }

        let backend = AutomergeBackend::with_iroh(backend_cfg).await?;

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
        let (bridge_change_tx, _) = broadcast::channel(BRIDGE_CHANGE_CAPACITY);
        let local_revisions = Arc::new(Mutex::new(LocalRevisionGuard::new()));

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

        // On every change (local or sync-received), push the doc to
        // all connected peers — except the source peer when the change
        // arrived via sync, so we don't echo back to it.
        //
        // We deliberately subscribe to the origin-tagged channel
        // (`subscribe_to_changes_with_origin`) instead of the local-
        // only `subscribe_to_changes`. The latter never fires on
        // sync-received writes, which breaks transitive gossip: when
        // peat-node-b receives a doc from peer A, b's local-only
        // channel stays silent, b never fans out to its other peers,
        // and an observer connected to b never sees the change.
        // peat-mesh documents this channel as the gossip-driver
        // contract (peat-mesh#891 / #907).
        // QoS-priority relay fanout (peat-node#138; mirrors peat-mesh#247 /
        // ADR-0013). The listener enqueues each change non-blockingly; a
        // single worker drains highest-QoS-first and performs the fanout, so a
        // latency-sensitive document preempts a lower-priority backlog instead
        // of being head-of-line-blocked behind it in the inline loop.
        let sync_rx = backend.store().subscribe_to_changes_with_origin();
        let bridge_rx = backend.store().subscribe_to_changes_with_origin();
        let bridge_store = Arc::clone(backend.store());
        let bridge_tx = bridge_change_tx.clone();
        let bridge_cipher = cipher.clone();
        let bridge_local_revisions = Arc::clone(&local_revisions);
        tokio::spawn(async move {
            Self::forward_bridge_changes(
                bridge_rx,
                bridge_tx,
                bridge_store,
                bridge_cipher,
                bridge_local_revisions,
            )
            .await;
        });
        let fanout = crate::fanout::PriorityFanout::new();
        tokio::spawn(Arc::clone(&fanout).run(
            Arc::clone(backend.coordinator()),
            Arc::clone(backend.transport()),
        ));
        tokio::spawn(async move {
            Self::sync_on_change(sync_rx, fanout).await;
        });

        // PRD-006: bundle handle table is always present (the cheap empty
        // map case when attachments are disabled); FileDistribution is
        // built only when --attachment-root is configured.
        let bundle_registry = Arc::new(BundleRegistry::new(RegistryConfig {
            handle_retention_secs: config.attachment_config.handle_retention_secs,
            max_known_bundles: config.attachment_config.max_known_bundles,
        }));
        // Construct the distribution substrate when either send
        // (roots) or receive (inbox) is configured: the receive-side
        // watcher (#68) is owned by `IrohFileDistribution` and a
        // receive-only node still needs the instance (its in-memory
        // `distributions` map stays empty — nothing to self-skip).
        let file_distribution = if config.attachment_config.has_roots()
            || config.attachment_config.inbox_path.is_some()
        {
            Some(Arc::new(IrohFileDistribution::new(
                Arc::clone(backend.blob_store()),
                Arc::clone(backend.store()),
            )))
        } else {
            None
        };

        // PRD-006 retention eviction. Without this, terminal bundles
        // linger until LRU pressure or process restart removes them —
        // making the --attachment-handle-retention-secs knob inert. The
        // tick interval scales with retention so tests that set short
        // retention windows observe eviction promptly, while the default
        // 24h retention only sweeps once a minute.
        // Guard covers both send-side (roots) and receive-side (inbox):
        // receive-only nodes also accumulate terminal bundle handles and
        // need time-based eviction just as senders do.
        if (config.attachment_config.has_roots() || config.attachment_config.inbox_path.is_some())
            && config.attachment_config.handle_retention_secs > 0
        {
            let registry = Arc::clone(&bundle_registry);
            let retention_secs = config.attachment_config.handle_retention_secs;
            let interval_secs = (retention_secs / 2).clamp(1, 60) as u64;
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // Discard the immediate-first tick — there's nothing to
                // evict on startup.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    registry.evict_expired();
                }
            });
        }

        // PRD-006 v1.1 receive-side watcher. Spawned only when
        // --attachment-inbox is configured. Pulls blobs from
        // distribution documents targeting this node's iroh endpoint
        // and writes them to the inbox.
        if let Some(ref inbox_path) = config.attachment_config.inbox_path {
            let endpoint_short = backend.blob_store().endpoint_id().fmt_short().to_string();
            let sink = std::sync::Arc::new(crate::attachments::inbox::FilesystemInboxSink::new(
                inbox_path.clone(),
            ));
            let raw_poll_secs = config.attachment_config.inbox_poll_secs;
            if raw_poll_secs == 0 {
                warn!(
                    "PEAT_NODE_ATTACHMENT_INBOX_POLL_SECS=0 is not supported; \
                     clamped to 1 — set a value ≥1 to suppress this warning"
                );
            }
            file_distribution
                .as_ref()
                .expect("file_distribution is Some when inbox_path is configured")
                .start_receive_watcher(
                    endpoint_short,
                    sink,
                    std::time::Duration::from_secs(raw_poll_secs.max(1) as u64),
                );
        }

        let registered_peers: Arc<RwLock<HashMap<iroh::EndpointId, PeerRegistration>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // peat-node#91 — auto-reconnect watchdog. Iroh's idle timeout
        // (default ~30 s) closes peer connections during a network
        // blackout. The sync coordinator drains its backlog over a
        // healthy connection but has no mechanism to re-establish one
        // post-blackout — the underlying `MeshSyncTransport` doesn't
        // own a `ReconnectionManager`. This watchdog fills that gap by
        // periodically comparing the live-connection set against the
        // registry of peers the operator originally asked to connect
        // to, and re-dialing any that have dropped out.
        //
        // Design decision (peat-node#100): the watchdog lives here rather
        // than inside MeshSyncTransport or peat-mesh because the
        // operator-intent registry (which peers should be auto-reconnected
        // vs. left dropped) is a peat-node concept — it's populated by the
        // gRPC ConnectPeer / DisconnectPeer calls, which are above the
        // transport layer. Pushing the watchdog into MeshSyncTransport would
        // require exposing that policy downward. The alternative (factoring
        // a shared ReconnectionManager into peat-mesh) is tracked in #100
        // as a future option if a second consumer duplicates this pattern.
        //
        // The watchdog holds `Weak` references rather than `Arc` so
        // it exits cleanly when `SidecarNode` is dropped — important
        // for integration tests that spin nodes up and down.
        {
            let registered = Arc::downgrade(&registered_peers);
            let backend_weak = Arc::downgrade(&backend);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(RECONNECT_WATCHDOG_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                // Discard the immediate-first tick — there's nothing to
                // reconnect on startup; first user-initiated connect_peer
                // calls populate the registry.
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let Some(registered) = registered.upgrade() else {
                        // SidecarNode was dropped; exit watchdog.
                        debug!("reconnect watchdog: SidecarNode dropped, exiting");
                        break;
                    };
                    let Some(backend) = backend_weak.upgrade() else {
                        debug!("reconnect watchdog: backend dropped, exiting");
                        break;
                    };

                    let live: std::collections::HashSet<iroh::EndpointId> =
                        backend.transport().connected_peers().into_iter().collect();

                    // Per-peer backoff: only consider a peer ready to dial
                    // if `next_attempt` has elapsed. This keeps the
                    // permanently-unreachable-peer cost bounded (peat-node
                    // #99 QA finding) without sacrificing the fast-recovery
                    // path for transient drops, where `backoff` starts at
                    // RECONNECT_BACKOFF_MIN (= watchdog interval).
                    let now = std::time::Instant::now();
                    let dead: Vec<(iroh::EndpointId, PeerRegistration)> = {
                        let registered = registered.read().unwrap_or_else(|e| e.into_inner());
                        registered
                            .iter()
                            .filter(|(id, reg)| !live.contains(*id) && reg.next_attempt <= now)
                            .map(|(id, reg)| (*id, reg.clone()))
                            .collect()
                    };

                    if dead.is_empty() {
                        continue;
                    }

                    for (peer_id, reg) in dead {
                        info!(
                            peer = %peer_id,
                            backoff_secs = reg.backoff.as_secs(),
                            "auto-reconnect: peer not in live set, re-dialing"
                        );
                        let dial_result = Self::dial_and_attach(&backend, peer_id, &reg).await;
                        // Update the registry's per-peer backoff state.
                        // On success: reset backoff so a future drop is
                        // re-tried immediately on the next tick. On
                        // failure: double up to RECONNECT_BACKOFF_MAX and
                        // schedule the next attempt accordingly. If the
                        // operator explicitly disconnected during the dial,
                        // the entry will be missing — skip silently.
                        // Capture the updated backoff inside the write lock
                        // so the error-path log doesn't need a second
                        // lock acquisition.
                        let next_backoff = {
                            let mut guard = registered.write().unwrap_or_else(|e| e.into_inner());
                            if let Some(entry) = guard.get_mut(&peer_id) {
                                match &dial_result {
                                    Ok(()) => {
                                        entry.backoff = RECONNECT_BACKOFF_MIN;
                                        entry.next_attempt = now;
                                        None
                                    }
                                    Err(_) => {
                                        entry.backoff =
                                            (entry.backoff * 2).min(RECONNECT_BACKOFF_MAX);
                                        entry.next_attempt = now + entry.backoff;
                                        Some(entry.backoff)
                                    }
                                }
                            } else {
                                None
                            }
                        };
                        match dial_result {
                            Ok(()) => info!(peer = %peer_id, "auto-reconnect succeeded"),
                            Err(e) => warn!(
                                peer = %peer_id,
                                "auto-reconnect failed (next attempt in {:?}): {e}",
                                next_backoff.unwrap_or(RECONNECT_BACKOFF_MIN)
                            ),
                        }
                    }
                }
            });
        }

        // Periodic peer-status heartbeat. Operators diagnosing sync or
        // attachment problems need a single line answering "who is this node
        // actually connected to, and which peers can it target?". Two sets
        // matter and they can legitimately differ:
        //   * `connected_peers()` (transport) — live CRDT-sync connections.
        //   * `known_peers()` (blob store) — peers THIS node dialed. This is
        //     the exact set `resolve_targets` uses for distribution targeting
        //     and `fetch_blob` uses to locate blob providers, so it's the set
        //     that governs whether an attachment can be delivered.
        // A peer in `known` but not `connected` means a dial is failing; a
        // receiver missing from a sender's `known` set is why a synced
        // distribution doc never turns into a delivered file. Logged at info
        // so it shows under the default filter. Holds a `Weak` ref so it exits
        // cleanly when the node is dropped (mirrors the reconnect watchdog).
        {
            let backend_weak = Arc::downgrade(&backend);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(PEER_STATUS_LOG_INTERVAL);
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    ticker.tick().await;
                    let Some(backend) = backend_weak.upgrade() else {
                        debug!("peer-status logger: backend dropped, exiting");
                        break;
                    };
                    let connected: Vec<String> = backend
                        .transport()
                        .connected_peers()
                        .into_iter()
                        .map(|id| id.fmt_short().to_string())
                        .collect();
                    let known: Vec<String> = backend
                        .blob_store()
                        .known_peers()
                        .await
                        .into_iter()
                        .map(|id| id.fmt_short().to_string())
                        .collect();
                    info!(
                        connected_count = connected.len(),
                        known_count = known.len(),
                        connected_peers = ?connected,
                        known_peers = ?known,
                        "peer status"
                    );
                }
            });
        }

        // ── mDNS peer discovery ──────────────────────────────────────────────
        // On by default; `disable_mdns: true` (or `--disable-mdns`) opts out.
        // In environments without multicast (Kubernetes, most containers) init
        // fails with a warn and the node continues without local discovery.
        let mdns = if config.disable_mdns {
            None
        } else {
            match MdnsDiscovery::new() {
                Err(e) => {
                    warn!("mDNS init failed (no local discovery): {e}");
                    None
                }
                Ok(mut m) => {
                    if let Err(e) = m.start().await {
                        warn!("mDNS start failed: {e}");
                        None
                    } else {
                        let ep = backend.transport().endpoint();
                        let eid = ep.id().to_string();
                        let port = ep
                            .bound_sockets()
                            .into_iter()
                            .find(|s| s.is_ipv4())
                            .map(|s| s.port())
                            .unwrap_or(0);
                        if port > 0 {
                            // Advertise our REAL LAN interface addresses so peers on
                            // OTHER hosts can discover and dial us. peat-mesh's
                            // `advertise` enables mdns-sd address auto-detection, which
                            // publishes every non-loopback interface address (and keeps
                            // them current). The previous `advertise_with_addr(127.0.0.1)`
                            // only ever reached nodes on the same host. `port` is still
                            // carried in the TXT record as a fallback for resolvers that
                            // yield no A records.
                            let mut meta = std::collections::HashMap::new();
                            meta.insert("port".to_string(), port.to_string());
                            meta.insert("formation_id".to_string(), config.app_id.clone());
                            match m.advertise(&eid, port, Some(meta)) {
                                Ok(()) => {
                                    info!("mDNS: advertising endpoint {eid} on LAN (port {port})")
                                }
                                Err(e) => warn!("mDNS advertise failed: {e}"),
                            }
                        }
                        if let Ok(rx) = m.event_stream() {
                            let backend_arc = Arc::clone(&backend);
                            let formation_id = config.app_id.clone();
                            tokio::spawn(async move {
                                Self::mdns_watcher(backend_arc, rx, formation_id).await;
                            });
                        }
                        Some(m)
                    }
                }
            }
        };

        // ── Kubernetes EndpointSlice peer discovery ──────────────────────────
        // peat-node#63. Only active when --enable-kubernetes-discovery is set.
        // Uses HKDF-SHA256-derived iroh keys so pods can dial each other by
        // pod name; see the deterministic keypair derivation above.
        let k8s_discovery = if config.enable_kubernetes_discovery {
            let k8s_cfg = KubernetesDiscoveryConfig {
                namespace: config.kubernetes_discovery_namespace.clone(),
                label_selector: config.kubernetes_discovery_label_selector.clone(),
                annotation_prefix: config.kubernetes_discovery_annotation_prefix.clone(),
                poll_interval: Duration::from_secs(config.kubernetes_discovery_interval_secs),
            };
            let mut discovery = KubernetesDiscovery::new(k8s_cfg);
            match discovery.start().await {
                Err(e) => {
                    warn!(
                        "K8s discovery start failed (running outside a cluster?): {e}; \
                         continuing without K8s peer discovery"
                    );
                    None
                }
                Ok(()) => {
                    if let Ok(rx) = discovery.event_stream() {
                        let backend_arc = Arc::clone(&backend);
                        let shared_key_bytes = if config.shared_key.is_empty() {
                            Vec::new()
                        } else {
                            // Non-empty shared_key was already validated as
                            // base64 during backend construction
                            // (`FormationKey::from_base64`), so this cannot
                            // fail here. `.expect` (not `.unwrap_or_default`)
                            // so a future regression in that upstream
                            // validation panics loudly rather than silently
                            // deriving empty-IKM seeds that fail every dial.
                            base64::engine::general_purpose::STANDARD
                                .decode(&config.shared_key)
                                .expect("shared_key base64 validated during backend construction")
                        };
                        let our_pod = pod_name.clone();
                        tokio::spawn(async move {
                            Self::k8s_discovery_watcher(backend_arc, rx, shared_key_bytes, our_pod)
                                .await;
                        });
                    }
                    info!("K8s peer discovery started");
                    Some(discovery)
                }
            }
        } else {
            None
        };

        // Load per-collection lifecycle configs (peat-node#55).
        let collection_configs_path = config.data_dir.join("collection_configs.json");
        let collection_configs: HashMap<String, CollectionConfigEntry> =
            if collection_configs_path.exists() {
                match std::fs::read_to_string(&collection_configs_path)
                    .map_err(anyhow::Error::from)
                    .and_then(|s| serde_json::from_str(&s).map_err(anyhow::Error::from))
                {
                    Ok(map) => map,
                    Err(e) => {
                        warn!(
                            path = %collection_configs_path.display(),
                            "collection_configs.json is unreadable or corrupt — \
                             starting with empty config (persisted policies lost): {e}"
                        );
                        HashMap::new()
                    }
                }
            } else {
                HashMap::new()
            };

        Ok(Self {
            node_id: config.node_id,
            backend,
            sync_active: Arc::new(AtomicBool::new(false)),
            change_tx,
            bridge_change_tx,
            local_revisions,
            cipher,
            attachment_config: config.attachment_config,
            bundle_registry,
            file_distribution,
            bundle_runtime: Arc::new(BundleRuntimeStore::new()),
            registered_peers,
            collection_configs: Arc::new(RwLock::new(collection_configs)),
            collection_configs_path,
            _mdns: mdns,
            _k8s_discovery: k8s_discovery,
        })
    }

    /// Background task: connect to mDNS peers as they announce.
    /// Mirrors `MeshSession::mdns_watcher` in `peat-cli/src/join.rs`.
    async fn mdns_watcher(
        backend: Arc<AutomergeBackend>,
        mut rx: tokio::sync::mpsc::Receiver<DiscoveryEvent>,
        formation_id: String,
    ) {
        let our_id = backend.transport().endpoint().id().to_string();
        let mut auth_failed: std::collections::HashSet<String> = std::collections::HashSet::new();
        while let Some(event) = rx.recv().await {
            let peer = match event {
                DiscoveryEvent::PeerFound(p) | DiscoveryEvent::PeerUpdated(p) => p,
                DiscoveryEvent::PeerLost(_) => continue,
            };
            if peer.node_id == our_id || auth_failed.contains(&peer.node_id) {
                continue;
            }
            if let Some(fid) = peer.metadata.get("formation_id") {
                if fid != &formation_id {
                    continue;
                }
            }
            let already = backend
                .transport()
                .connected_peers()
                .iter()
                .any(|id| id.to_string() == peer.node_id);
            if already {
                continue;
            }
            for addr in Self::mdns_peer_addresses(&peer) {
                let registration = PeerRegistration {
                    addresses: vec![addr.to_string()],
                    relay_url: String::new(),
                    next_attempt: std::time::Instant::now(),
                    backoff: RECONNECT_BACKOFF_MIN,
                };
                let peer_id_str = &peer.node_id;
                let Ok(peer_id) = peer_id_str.parse::<iroh::EndpointId>() else {
                    continue;
                };
                match Self::dial_and_attach(&backend, peer_id, &registration).await {
                    Ok(()) => {
                        info!(peer = %peer.node_id, %addr, "mDNS: connected to peer");
                        let pid = backend
                            .transport()
                            .connected_peers()
                            .into_iter()
                            .find(|id| id.to_string() == peer.node_id);
                        if let Some(pid) = pid {
                            let _ = backend
                                .coordinator()
                                .sync_all_documents_with_peer(pid)
                                .await;
                        }
                        break;
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("formation")
                            || msg.contains("mismatch")
                            || msg.contains("auth")
                        {
                            auth_failed.insert(peer.node_id.clone());
                        }
                        warn!(peer = %peer.node_id, %addr, "mDNS: connect failed: {e}");
                    }
                }
            }
        }
    }

    /// Background task: connect to K8s-discovered pods as they appear in
    /// EndpointSlices (peat-node#63).
    ///
    /// Each pod's iroh `EndpointId` is derived deterministically from
    /// `HKDF-SHA256(shared_key_bytes, "iroh:" + pod_name)` — the same formula
    /// used to set `AutomergeBackendConfig::iroh_secret_key` at startup, so the
    /// derived endpoint ID matches the peer's actual endpoint ID.
    async fn k8s_discovery_watcher(
        backend: Arc<AutomergeBackend>,
        mut rx: tokio::sync::mpsc::Receiver<DiscoveryEvent>,
        shared_key_bytes: Vec<u8>,
        our_pod_name: Option<String>,
    ) {
        while let Some(event) = rx.recv().await {
            let peer = match event {
                DiscoveryEvent::PeerFound(p) | DiscoveryEvent::PeerUpdated(p) => p,
                DiscoveryEvent::PeerLost(_) => continue,
            };
            // Decision extracted into `k8s_dial_decision` (pure, tested): skip
            // self, skip no-addresses, skip already-connected, else dial.
            let connected = backend.transport().connected_peers();
            let peer_id = match k8s_dial_decision(
                &peer,
                our_pod_name.as_deref(),
                &shared_key_bytes,
                &connected,
            ) {
                K8sDialDecision::SkipSelf | K8sDialDecision::SkipAlreadyConnected => continue,
                K8sDialDecision::SkipNoAddresses => {
                    debug!(pod = %peer.node_id, "K8s discovery: no addresses, skipping");
                    continue;
                }
                K8sDialDecision::Dial(peer_id) => peer_id,
            };
            for addr in &peer.addresses {
                let registration = PeerRegistration {
                    addresses: vec![addr.to_string()],
                    relay_url: String::new(),
                    next_attempt: std::time::Instant::now(),
                    backoff: RECONNECT_BACKOFF_MIN,
                };
                match Self::dial_and_attach(&backend, peer_id, &registration).await {
                    Ok(()) => {
                        info!(
                            pod = %peer.node_id,
                            %addr,
                            endpoint_id = %peer_id.fmt_short(),
                            "K8s discovery: connected to peer"
                        );
                        let _ = backend
                            .coordinator()
                            .sync_all_documents_with_peer(peer_id)
                            .await;
                        break;
                    }
                    Err(e) => {
                        warn!(pod = %peer.node_id, %addr, "K8s discovery: connect failed: {e}");
                    }
                }
            }
        }
    }

    fn mdns_peer_addresses(peer: &PeerInfo) -> Vec<std::net::SocketAddr> {
        if !peer.addresses.is_empty() {
            return peer.addresses.clone();
        }
        if let Some(port_str) = peer.metadata.get("port") {
            if let Ok(port) = port_str.parse::<u16>() {
                warn!(
                    peer = %peer.node_id,
                    "mDNS: addresses empty, using metadata port fallback 127.0.0.1:{port}"
                );
                return vec![std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    port,
                )];
            }
        }
        debug!(peer = %peer.node_id, "mDNS: no usable address for peer");
        vec![]
    }

    /// PRD-006 per-bundle runtime store (progress channels + per-
    /// distribution state for subscribe).
    pub fn bundle_runtime(&self) -> &Arc<BundleRuntimeStore> {
        &self.bundle_runtime
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
    pub(crate) fn file_distribution(&self) -> Option<&Arc<IrohFileDistribution>> {
        self.file_distribution.as_ref()
    }

    /// PRD-006 ingest target — the iroh blob store the backend holds.
    /// Exposed for the attachment handlers; otherwise prefer the
    /// higher-level mesh operations.
    pub fn blob_store(&self) -> &peat_mesh::storage::NetworkedIrohBlobStore {
        self.backend.blob_store()
    }

    /// Read-only access to documents held by the backend.
    ///
    /// Exposed for the attachment subsystem's inbox watcher (which writes
    /// receiver-side `NodeTransferStatus` into the `file_distributions`
    /// collection so the sender's `IrohFileDistribution` progress watcher
    /// sees real cross-peer state — see `attachments/inbox.rs`), and for
    /// integration tests that need to read the local document state
    /// directly rather than through the gRPC surface.
    ///
    /// Former raw-store mutation is intentionally a compile error:
    ///
    /// ```compile_fail
    /// fn raw_mutation_is_unavailable(reader: &peat_node::node::DocumentStoreReader) {
    ///     reader.delete("frames:one").unwrap();
    ///     reader.put("frames:one", panic!("type is irrelevant")).unwrap();
    /// }
    /// ```
    pub fn document_store(&self) -> DocumentStoreReader {
        DocumentStoreReader {
            store: Arc::clone(self.backend.store()),
        }
    }

    /// Typed attachment-document read for integration and attachment code.
    pub fn read_attachment_distribution(
        &self,
        distribution_id: &str,
    ) -> anyhow::Result<Option<DistributionDocument>> {
        peat_protocol::storage::read_distribution_document(
            self.backend.store().as_ref(),
            distribution_id,
        )
    }

    /// Narrow attachment mutation used by the discovery-grace promoter.
    ///
    /// The only collection this can mutate is peat-mesh's canonical
    /// `file_distributions`, which bridge configuration reserves exactly.
    pub(crate) fn write_attachment_node_status(
        &self,
        distribution_id: &str,
        node_id: &str,
        status: &NodeTransferStatus,
    ) -> anyhow::Result<()> {
        peat_protocol::storage::write_receiver_node_status(
            self.backend.store().as_ref(),
            distribution_id,
            node_id,
            status,
        )
    }

    /// The short-form id of this node's iroh endpoint, the same string the
    /// sender's `IrohFileDistribution::resolve_targets` produces in the
    /// distribution document's `target_nodes` and the receiver writes back
    /// in `node_statuses`. Cached lookup; cheap.
    pub fn endpoint_short_id(&self) -> String {
        self.backend
            .blob_store()
            .endpoint_id()
            .fmt_short()
            .to_string()
    }

    /// React to document changes (local writes AND sync-received writes) by
    /// enqueuing them onto the QoS-priority relay fanout queue (peat-node#138).
    /// The queue's worker performs the actual fanout, draining
    /// highest-QoS-first; this listener stays non-blocking so the change
    /// broadcast is drained promptly and fanout *ordering* is decided by
    /// priority rather than arrival order.
    ///
    /// Local writes fan to every peer ([`FanoutKind::AllPeers`]); a
    /// remote-origin change fans to every peer **except** its source
    /// ([`FanoutKind::ExcludeSource`]) — echo suppression, the peat-mesh#239
    /// gossip-amplification guard, preserved through the queue. The source is
    /// matched by `EndpointId::to_string()` per peat-mesh's transport-agnostic
    /// contract.
    async fn sync_on_change(
        mut rx: broadcast::Receiver<DocChange>,
        fanout: Arc<crate::fanout::PriorityFanout>,
    ) {
        loop {
            match rx.recv().await {
                Ok(DocChange { key, origin }) => {
                    let kind = match origin {
                        ChangeOrigin::Local => FanoutKind::AllPeers,
                        ChangeOrigin::Remote(source) => FanoutKind::ExcludeSource(source),
                    };
                    fanout.enqueue(&key, kind);
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("sync change listener lagged {n} messages");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // Broadcast closed (shutdown): let the worker drain and exit.
                    fanout.close();
                    break;
                }
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
                        // Read the current doc and extract a JSON string for the event.
                        // Two storage formats co-exist (peat-node#7):
                        //   - encrypted: {"value":"<ENC:v1:...>"} — extract & decrypt
                        //   - structured: direct Automerge map — serialize as JSON
                        let raw = match store.get(&key) {
                            Ok(Some(doc)) => {
                                let j = automerge_to_json(&doc);
                                if let Some(s) = j
                                    .get("value")
                                    .and_then(|v| v.as_str())
                                    .filter(|s| crate::crypto::is_encrypted(s))
                                {
                                    Some(s.to_string())
                                } else {
                                    serde_json::to_string(&j).ok()
                                }
                            }
                            _ => None,
                        };
                        // Decrypt if the raw value carries an ENC prefix.
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

    /// Forward only proven remote, present, non-local revisions to the bridge.
    ///
    /// Pinned peat-mesh exposes `DocChange { key, origin }`, not an atomic
    /// origin+snapshot event. The shared per-key lock therefore closes the
    /// notification/reread race: local writes record their completed exact
    /// heads before unlock, while this path captures and classifies the current
    /// snapshot under the same lock. Superseded remote events may be dropped;
    /// a later local snapshot can never be attributed as remote.
    async fn forward_bridge_changes(
        mut rx: broadcast::Receiver<DocChange>,
        tx: broadcast::Sender<BridgeChangeEvent>,
        store: Arc<AutomergeStore>,
        cipher: Option<StoreCipher>,
        local_revisions: Arc<Mutex<LocalRevisionGuard>>,
    ) {
        loop {
            match rx.recv().await {
                Ok(DocChange {
                    key,
                    origin: ChangeOrigin::Remote(remote_peer_id),
                }) => {
                    let snapshot = {
                        let _key_guard = store.lock_doc(&key);
                        let doc = match store.get(&key) {
                            Ok(Some(doc)) => doc,
                            Ok(None) => continue,
                            Err(_) => {
                                warn!("bridge snapshot read failed");
                                continue;
                            }
                        };
                        let heads = doc.get_heads();
                        let locally_authored = local_revisions
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .is_local(&key, heads.iter().map(AsRef::as_ref));
                        drop(heads);
                        if locally_authored {
                            continue;
                        }
                        automerge_to_json(&doc)
                    };

                    let Some((collection, doc_id)) = key.split_once(':') else {
                        warn!("bridge snapshot key classification failed");
                        continue;
                    };
                    let json_data = if let Some(encrypted) = snapshot
                        .get("value")
                        .and_then(|value| value.as_str())
                        .filter(|value| crate::crypto::is_encrypted(value))
                    {
                        let Some(cipher) = &cipher else {
                            warn!("bridge snapshot decrypt unavailable");
                            continue;
                        };
                        match cipher.decrypt(encrypted) {
                            Ok(plaintext) => plaintext,
                            Err(_) => {
                                warn!("bridge snapshot decrypt failed");
                                continue;
                            }
                        }
                    } else {
                        match serde_json::to_string(&snapshot) {
                            Ok(json) => json,
                            Err(_) => {
                                warn!("bridge snapshot conversion failed");
                                continue;
                            }
                        }
                    };

                    let _ = tx.send(BridgeChangeEvent {
                        collection: collection.to_owned(),
                        doc_id: doc_id.to_owned(),
                        remote_peer_id,
                        json_data,
                    });
                }
                Ok(DocChange {
                    origin: ChangeOrigin::Local,
                    ..
                }) => {}
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!(dropped_count = count, "bridge change listener lagged");
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

    /// The first IP-bound UDP port the iroh endpoint is listening on, if any.
    ///
    /// Used by integration tests that bind to port 0 to discover the OS-
    /// assigned port without racing on a hardcoded number. Returns `None` if
    /// the endpoint reports no IP-transport addresses (e.g. relay-only).
    pub fn bound_udp_port(&self) -> Option<u16> {
        self.backend
            .blob_store()
            .endpoint()
            .bound_sockets()
            .into_iter()
            .next()
            .map(|sa| sa.port())
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
    ///
    /// The peer is recorded in [`Self::registered_peers`] so the
    /// auto-reconnect watchdog can re-dial it if the iroh idle timeout
    /// fires during a network blackout (peat-node#91). The address
    /// hints passed here are reused verbatim by the watchdog.
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

        let registration = PeerRegistration {
            addresses: addresses.to_vec(),
            relay_url: relay_url.to_string(),
            // Just registered + successful dial below means the
            // first watchdog tick should re-check immediately if the
            // connection drops. The two-step pattern (dial then
            // insert) keeps these defaults coherent with the
            // dial-already-succeeded invariant the insert below
            // upholds.
            next_attempt: std::time::Instant::now(),
            backoff: RECONNECT_BACKOFF_MIN,
        };

        Self::dial_and_attach(&self.backend, peer_id, &registration).await?;

        // Record the address hints AFTER the dial succeeds so the watchdog
        // doesn't try to reconnect peers that never connected in the first
        // place. peat-node#91 — the auto-reconnect path keys on this map.
        self.registered_peers
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(peer_id, registration);

        info!(peer = endpoint_id_str, "connected to peer");
        Ok(())
    }

    /// Inner dial-and-attach used by both [`Self::connect_peer`] and the
    /// auto-reconnect watchdog. Resolves addresses, authenticates the
    /// formation handshake, wires the connection into the sync transport,
    /// and registers the peer with the blob-store peer index.
    ///
    /// Does **not** touch `registered_peers` — callers decide whether the
    /// peer should be eligible for future auto-reconnects (the public
    /// `connect_peer` inserts; the watchdog re-uses an existing entry).
    async fn dial_and_attach(
        backend: &Arc<AutomergeBackend>,
        peer_id: iroh::EndpointId,
        registration: &PeerRegistration,
    ) -> anyhow::Result<()> {
        let mut peer_addr = iroh::EndpointAddr::new(peer_id);

        for addr_str in &registration.addresses {
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

        if !registration.relay_url.is_empty() {
            let relay: iroh::RelayUrl = registration.relay_url.parse().map_err(|e| {
                anyhow::anyhow!("invalid relay URL `{}`: {e}", registration.relay_url)
            })?;
            peer_addr = peer_addr.with_relay_url(relay);
        }

        backend
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
        const CONNECT_RETRY_BACKOFF: Duration = Duration::from_millis(200);
        let mut attempt = 0;
        let connection = loop {
            attempt += 1;
            match backend.transport().connect_and_authenticate(peer_id).await {
                Ok(c) => break c,
                Err(e) if attempt < CONNECT_RETRY_ATTEMPTS => {
                    warn!(
                        peer = %peer_id,
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
        backend
            .transport()
            .start_sync_connection(connection, Arc::clone(backend.coordinator()));

        // PRD-006: register the peer with the blob store so it shows up
        // in `known_peers()`. `IrohFileDistribution::resolve_targets`
        // reads from this list for `AllNodes` scope, and the receive-
        // side fetch path in `NetworkedIrohBlobStore::fetch_blob`
        // iterates it when the BlobPeerIndex doesn't yet know which
        // peer holds a given blob. The two lists (iroh connection /
        // blob-store peer index) are tracked separately upstream —
        // before peat-node 0.3.4 only the former was populated, which
        // silently broke the attachment-delivery end-to-end path
        // (target_nodes resolved to `[]` for AllNodes distributions
        // unless the operator called add_peer through a private API).
        backend.blob_store().add_peer(peer_id).await;
        // Clear any stale health record from a prior session so this peer
        // starts at Neutral rather than inheriting a pre-blackout Unhealthy
        // rank. Known-peers and BlobPeerIndex entries are preserved — only
        // the health verdict is reset.
        backend.blob_store().reset_peer_health(&peer_id).await;

        Ok(())
    }

    pub async fn disconnect_peer(&self, endpoint_id_str: &str) -> anyhow::Result<()> {
        let peer_id: iroh::EndpointId = endpoint_id_str
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid endpoint ID: {e}"))?;
        // peat-node#91 — remove from the auto-reconnect registry FIRST.
        // An explicit disconnect is the operator saying "stop talking to
        // this peer," distinguishing intentional teardown from transient
        // link loss. Doing this before closing the QUIC connection avoids
        // a race where the watchdog tick observes the dead connection
        // before the registry entry is gone and re-dials the peer the
        // operator just asked to disconnect.
        self.registered_peers
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&peer_id);
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

    /// **Test-only.** Forcibly close the underlying QUIC connection to a
    /// peer *without* removing it from the auto-reconnect registry —
    /// i.e. the moral equivalent of iroh's idle timeout firing during a
    /// network blackout. Used by `tests/auto_reconnect_test.rs` to drive
    /// the peat-node#91 reproducing test deterministically; production
    /// code paths should use [`Self::disconnect_peer`] which also
    /// unregisters the peer.
    ///
    /// Hidden from rustdoc because it intentionally bypasses the
    /// registry invariant `disconnect_peer` upholds.
    #[doc(hidden)]
    pub async fn simulate_idle_timeout_for_test(
        &self,
        endpoint_id_str: &str,
    ) -> anyhow::Result<()> {
        let peer_id: iroh::EndpointId = endpoint_id_str
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid endpoint ID: {e}"))?;
        if let Some(conn) = self.backend.transport().get_connection(&peer_id) {
            conn.close(0u32.into(), b"simulated idle timeout (test)");
        }
        self.backend.transport().remove_connection(&peer_id);
        self.backend.coordinator().clear_peer_sync_state(peer_id);
        tokio::task::yield_now().await;
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
    // Unencrypted documents are stored as structured Automerge maps (field-level
    // CRDT — peat-node#7). Encrypted documents use a {"value":"<ciphertext>"}
    // wrapper because the ciphertext is an opaque string, not a JSON map.

    pub async fn put_document(
        &self,
        collection: &str,
        doc_id: &str,
        json_data: &str,
    ) -> anyhow::Result<()> {
        self.write_document(collection, doc_id, json_data, DocumentWriteMode::Upsert)
            .map_err(DocumentWriteError::into_anyhow)?;

        // `store.put` fires the AutomergeStore observer, which the
        // `forward_store_changes` task re-emits as a `ChangeEvent` on
        // `change_tx`. Emitting a second event directly here would
        // duplicate every local upsert on the broadcast channel and
        // make subscribe-with-filter behavior non-deterministic for
        // counting subscribers. The forwarder is the single source of
        // truth for upsert events — local and remote-sync alike.

        Ok(())
    }

    /// Persist one immutable bridge envelope through the canonical store path.
    ///
    /// This is a Rust-only API for native bridge ingress. It deliberately does
    /// not emit a change directly: the successful `store.put` below remains
    /// the single source for observer events and local-origin mesh fanout.
    pub async fn create_bridge_document(
        &self,
        collection: &str,
        doc_id: &str,
        envelope_json: &str,
    ) -> Result<(), CreateBridgeDocumentError> {
        self.write_document(
            collection,
            doc_id,
            envelope_json,
            DocumentWriteMode::CreateOnly,
        )
        .map_err(|error| error.classification())
    }

    fn write_document(
        &self,
        collection: &str,
        doc_id: &str,
        json_data: &str,
        mode: DocumentWriteMode,
    ) -> Result<(), DocumentWriteError> {
        let parsed: serde_json::Value =
            serde_json::from_str(json_data).map_err(DocumentWriteError::InvalidInput)?;

        let key = format!("{collection}:{doc_id}");
        let store = self.backend.store();
        let _key_guard = store.lock_doc(&key);
        let existing = store.get(&key).map_err(DocumentWriteError::StoreRead)?;
        if matches!(mode, DocumentWriteMode::CreateOnly) && existing.is_some() {
            return Err(DocumentWriteError::AlreadyExists);
        }
        let conversion_base = match mode {
            DocumentWriteMode::Upsert => existing.as_ref(),
            DocumentWriteMode::CreateOnly => None,
        };

        let doc = match &self.cipher {
            Some(cipher) => {
                // Ciphertext is opaque — wrap in {"value":"<ciphertext>"} so
                // json_to_automerge has a map root to work with.
                let ciphertext = cipher
                    .encrypt(json_data)
                    .map_err(DocumentWriteError::Encryption)?;
                let wrapped = serde_json::json!({ "value": ciphertext });
                json_to_automerge(&wrapped, conversion_base)
                    .map_err(DocumentWriteError::Conversion)?
            }
            None => {
                // Write JSON directly as structured Automerge fields for
                // field-level CRDT merging (peat-node#7).
                json_to_automerge(&parsed, conversion_base)
                    .map_err(DocumentWriteError::Conversion)?
            }
        };

        store
            .put(&key, &doc)
            .map_err(DocumentWriteError::StoreWrite)?;
        let heads = doc.get_heads();
        self.local_revisions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .record(&key, heads.iter().map(AsRef::as_ref));
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
                let json = automerge_to_json(&doc);
                // Two doc shapes co-exist in the same store (peat-node#7):
                //
                //   - Encrypted: {"value":"<ENC:v1:...>"} — extract the
                //     inner string and decrypt.
                //   - Structured (unencrypted gRPC writes and all peat-cli
                //     writes): direct Automerge map fields. Serialize to
                //     JSON and return as-is.
                if let Some(s) = json
                    .get("value")
                    .and_then(|v| v.as_str())
                    .filter(|s| crate::crypto::is_encrypted(s))
                {
                    Ok(self.maybe_decrypt(Some(s.to_string()))?)
                } else {
                    Ok(Some(serde_json::to_string(&json)?))
                }
            }
            None => Ok(None),
        }
    }

    pub async fn delete_document(&self, collection: &str, doc_id: &str) -> anyhow::Result<()> {
        let key = format!("{collection}:{doc_id}");
        let store = self.backend.store();
        let _key_guard = store.lock_doc(&key);
        store.delete(&key)?;

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

    #[allow(dead_code)]
    pub(crate) fn subscribe_bridge_changes(&self) -> broadcast::Receiver<BridgeChangeEvent> {
        self.bridge_change_tx.subscribe()
    }

    // --- Collection Lifecycle Configuration (peat-node#55 / ADR-016) ---

    pub fn set_collection_config(&self, entry: CollectionConfigEntry) -> anyhow::Result<()> {
        let mut configs = self
            .collection_configs
            .write()
            .unwrap_or_else(|e| e.into_inner());
        configs.insert(entry.collection.clone(), entry);
        let json = serde_json::to_string_pretty(&*configs)?;
        std::fs::write(&self.collection_configs_path, json)?;
        Ok(())
    }

    pub fn get_collection_config(&self, collection: &str) -> Option<CollectionConfigEntry> {
        self.collection_configs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(collection)
            .cloned()
    }

    pub fn list_collection_configs(&self) -> Vec<CollectionConfigEntry> {
        self.collection_configs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Shutdown the node gracefully.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.backend.blob_store().shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nats_bridge::config::{BridgeConfig, BridgeConfigIssueKind};
    use peat_mesh::storage::IROH_DISTRIBUTION_COLLECTION;

    async fn test_node(encrypted: bool) -> (tempfile::TempDir, SidecarNode) {
        let dir = tempfile::tempdir().unwrap();
        let encryption_key =
            encrypted.then(|| base64::engine::general_purpose::STANDARD.encode([0x5au8; 32]));
        let node = SidecarNode::new(SidecarConfig {
            node_id: "bridge-test".to_owned(),
            app_id: "bridge-test".to_owned(),
            data_dir: dir.path().to_path_buf(),
            encryption_key,
            disable_mdns: true,
            ..Default::default()
        })
        .await
        .unwrap();
        (dir, node)
    }

    fn put_remote(node: &SidecarNode, key: &str, json: &str, peer: &str) {
        let value: serde_json::Value = serde_json::from_str(json).unwrap();
        let doc = if let Some(cipher) = &node.cipher {
            let encrypted = cipher.encrypt(json).unwrap();
            json_to_automerge(&serde_json::json!({ "value": encrypted }), None).unwrap()
        } else {
            json_to_automerge(&value, None).unwrap()
        };
        let store = node.backend.store();
        let _guard = store.lock_doc(key);
        store
            .put_with_origin(key, &doc, ChangeOrigin::Remote(peer.to_owned()))
            .unwrap();
    }

    async fn expect_no_bridge_event(rx: &mut broadcast::Receiver<BridgeChangeEvent>) {
        assert!(tokio::time::timeout(Duration::from_millis(75), rx.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn bridge_change_remote_plain_and_encrypted_upserts_are_exact() {
        for encrypted in [false, true] {
            let (_dir, node) = test_node(encrypted).await;
            let mut rx = node.subscribe_bridge_changes();
            let json = r#"{"kind":"peat.nats-bridge","version":1,"subject":"vision.summary","source_node_id":"other","payload":" {\"frame\":1} "}"#;
            put_remote(&node, "frames:remote-1", json, "peer-immediate");
            let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(event.collection, "frames");
            assert_eq!(event.doc_id, "remote-1");
            assert_eq!(event.remote_peer_id, "peer-immediate");
            if encrypted {
                assert_eq!(event.json_data, json);
            } else {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&event.json_data).unwrap(),
                    serde_json::from_str::<serde_json::Value>(json).unwrap()
                );
            }
        }
    }

    #[tokio::test]
    async fn bridge_change_local_mutations_and_all_deletes_are_excluded() {
        let (_dir, node) = test_node(false).await;
        let mut rx = node.subscribe_bridge_changes();
        node.put_document("frames", "local", r#"{"local":true}"#)
            .await
            .unwrap();
        node.create_bridge_document("frames", "ingress", r#"{"ingress":true}"#)
            .await
            .unwrap();
        node.delete_document("frames", "local").await.unwrap();
        {
            let store = node.backend.store();
            let key = "frames:remote-delete";
            let _guard = store.lock_doc(key);
            store
                .delete_with_origin(key, ChangeOrigin::Remote("peer-a".to_owned()))
                .unwrap();
        }
        expect_no_bridge_event(&mut rx).await;
    }

    #[tokio::test]
    async fn bridge_change_queued_remote_then_local_overwrite_never_emits_local_snapshot() {
        let (_dir, node) = test_node(false).await;
        let mut rx = node.subscribe_bridge_changes();
        let key = "frames:raced";
        {
            // The store lock is the deterministic pause gate: the Remote
            // notification is queued, but the forwarder cannot capture until
            // the mediated local revision has been recorded before unlock.
            let store = node.backend.store();
            let _pause = store.lock_doc(key);
            store
                .put_with_origin(
                    key,
                    &json_to_automerge(&serde_json::json!({"remote": true}), None).unwrap(),
                    ChangeOrigin::Remote("peer-a".to_owned()),
                )
                .unwrap();
            let local = json_to_automerge(&serde_json::json!({"local": true}), None).unwrap();
            store.put(key, &local).unwrap();
            let heads = local.get_heads();
            node.local_revisions
                .lock()
                .unwrap()
                .record(key, heads.iter().map(AsRef::as_ref));
        }
        expect_no_bridge_event(&mut rx).await;
    }

    #[tokio::test]
    async fn bridge_change_queued_remote_tombstone_then_local_put_is_suppressed() {
        let (_dir, node) = test_node(false).await;
        let mut rx = node.subscribe_bridge_changes();
        let key = "frames:tombstone-race";
        {
            let store = node.backend.store();
            let _pause = store.lock_doc(key);
            store
                .delete_with_origin(key, ChangeOrigin::Remote("peer-a".to_owned()))
                .unwrap();
            let local = json_to_automerge(&serde_json::json!({"local": true}), None).unwrap();
            store.put(key, &local).unwrap();
            let heads = local.get_heads();
            node.local_revisions
                .lock()
                .unwrap()
                .record(key, heads.iter().map(AsRef::as_ref));
        }
        expect_no_bridge_event(&mut rx).await;
    }

    #[tokio::test]
    async fn bridge_change_remote_capture_first_preserves_clone_before_local_last() {
        let (_dir, node) = test_node(false).await;
        let mut rx = node.subscribe_bridge_changes();
        put_remote(&node, "frames:ordered", r#"{"remote":true}"#, "peer-a");
        let remote = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        node.put_document("frames", "ordered", r#"{"local":true}"#)
            .await
            .unwrap();
        assert_eq!(remote.json_data, r#"{"remote":true}"#);
        expect_no_bridge_event(&mut rx).await;
    }

    #[tokio::test]
    async fn bridge_change_local_first_remote_last_emits_exact_remote_state() {
        let (_dir, node) = test_node(false).await;
        let mut rx = node.subscribe_bridge_changes();
        node.put_document("frames", "ordered", r#"{"local":true}"#)
            .await
            .unwrap();
        put_remote(&node, "frames:ordered", r#"{"remote":true}"#, "peer-a");
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.json_data, r#"{"remote":true}"#);
    }

    #[test]
    fn bridge_change_revision_guard_is_fixed_non_evicting_and_fail_closed() {
        let mut guard = LocalRevisionGuard::new();
        assert_eq!(guard.slots.len(), LOCAL_REVISION_CAPACITY);
        assert_eq!(std::mem::size_of_val(&*guard.slots), 131_072);
        let head = [7u8; 32];
        for index in 0..LOCAL_REVISION_CAPACITY {
            assert!(guard.record(&format!("frames:{index}"), std::iter::once(head.as_slice())));
        }
        assert_eq!(guard.len, LOCAL_REVISION_CAPACITY);
        assert!(!guard.record("frames:overflow", std::iter::once(head.as_slice())));
        assert!(guard.exhausted);
        assert!(guard.is_local("never-seen", std::iter::once(head.as_slice())));
    }

    #[test]
    fn bridge_change_revision_digest_frames_inputs_and_caps_post_get_heads_work() {
        let mut guard = LocalRevisionGuard::new();
        let heads_64 = [[1u8; 32]; MAX_REVISION_HEADS];
        assert!(guard.record("ab", heads_64.iter().map(|head| head.as_slice())));
        assert!(guard.is_local("ab", heads_64.iter().map(|head| head.as_slice())));
        assert!(!guard.is_local("a", heads_64.iter().map(|head| head.as_slice())));

        let heads_65 = [[2u8; 32]; MAX_REVISION_HEADS + 1];
        assert!(!guard.record("over", heads_65.iter().map(|head| head.as_slice())));
        assert!(guard.exhausted);
        // Pinned Automerge 0.9.0 get_heads() has already allocated and sorted
        // its complete Vec before this 65-head fail-closed check. This asserts
        // retained guard state only; it makes no transient allocation claim.
        assert_eq!(guard.len, 1);
    }

    #[tokio::test]
    async fn bridge_change_broadcast_is_bounded_and_recovers_after_lag() {
        let (_dir, node) = test_node(false).await;
        let mut rx = node.subscribe_bridge_changes();
        for index in 0..=BRIDGE_CHANGE_CAPACITY {
            node.bridge_change_tx
                .send(BridgeChangeEvent {
                    collection: "frames".to_owned(),
                    doc_id: index.to_string(),
                    remote_peer_id: "peer".to_owned(),
                    json_data: "{}".to_owned(),
                })
                .unwrap();
        }
        assert!(matches!(
            rx.recv().await,
            Err(broadcast::error::RecvError::Lagged(1))
        ));
        assert!(rx.recv().await.is_ok());
    }

    #[tokio::test]
    async fn bridge_change_reader_facade_supports_reads_without_raw_store_extraction() {
        let (_dir, node) = test_node(false).await;
        let mut observer = node.document_store().subscribe_to_observer_changes();
        node.put_document("frames", "one", r#"{"value":1}"#)
            .await
            .unwrap();
        assert_eq!(observer.recv().await.unwrap(), "frames:one");
        assert_eq!(
            node.document_store().get("frames:one").unwrap(),
            Some(serde_json::json!({"value": 1}))
        );
        assert_eq!(
            node.document_store()
                .keys_with_prefix("frames:")
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            node.document_store().scan_prefix("frames:").unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn bridge_change_public_subscription_keeps_remote_upsert_semantics() {
        let (_dir, node) = test_node(false).await;
        let mut public_rx = node.subscribe();
        let mut bridge_rx = node.subscribe_bridge_changes();
        put_remote(
            &node,
            "frames:public-contract",
            r#"{"remote":true}"#,
            "peer-a",
        );

        let public = tokio::time::timeout(Duration::from_secs(2), public_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(public.collection, "frames");
        assert_eq!(public.doc_id, "public-contract");
        assert!(matches!(public.change_type, ChangeType::Upsert));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(public.json_data.as_deref().unwrap())
                .unwrap(),
            serde_json::json!({"remote": true})
        );

        let private = tokio::time::timeout(Duration::from_secs(2), bridge_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(private.remote_peer_id, "peer-a");
    }

    #[test]
    fn bridge_change_diagnostics_are_fixed_and_payload_safe() {
        let source = include_str!("node.rs");
        let diagnostics: Vec<_> = source
            .lines()
            .filter(|line| {
                line.contains("bridge snapshot") || line.contains("bridge change listener")
            })
            .collect();
        assert!(!diagnostics.is_empty());
        for line in diagnostics {
            for forbidden in ["json_data", "remote_peer_id", "ciphertext", "{key}", "%key"] {
                assert!(
                    !line.contains(forbidden),
                    "unsafe bridge diagnostic: {line}"
                );
            }
        }
    }

    #[tokio::test]
    async fn bridge_change_reserved_collection_blocks_envelope_shaped_attachment_adversary() {
        let (_dir, node) = test_node(false).await;
        let key = format!("{IROH_DISTRIBUTION_COLLECTION}:adversary");
        let value = serde_json::json!({
            "kind": "peat.nats-bridge",
            "version": 1,
            "subject": "vision.summary",
            "source_node_id": "remote",
            "payload": "{\"frame\":1}",
            "blob_hash": "attachment-like-extra-field"
        });
        let doc = json_to_automerge(&value, None).unwrap();
        node.backend.store().put(&key, &doc).unwrap();
        assert!(node.backend.store().get(&key).unwrap().is_some());

        let error = BridgeConfig::from_raw(
            Some("nats://127.0.0.1:4222"),
            &[format!("vision.summary={IROH_DISTRIBUTION_COLLECTION}")],
        )
        .expect_err("internal attachment collection must not create a runtime");
        assert_eq!(
            error.issues()[0].kind,
            BridgeConfigIssueKind::ReservedCollection
        );
        assert!(BridgeConfig::from_raw(
            Some("nats://127.0.0.1:4222"),
            &["vision.summary=File_Distributions".to_owned()],
        )
        .is_ok());
    }

    #[test]
    fn bridge_change_source_inventory_keeps_raw_mutation_private_and_reserved() {
        let node_source = include_str!("node.rs");
        let handler_source = include_str!("attachments/handlers.rs");
        assert!(!node_source.contains(&["pub fn ", "backend("].concat()));
        assert!(!node_source.contains(&["pub fn ", "raw_store("].concat()));
        assert!(!node_source.contains(&["pub fn ", "file_distribution("].concat()));
        assert!(!handler_source.contains("document_store().put"));
        assert!(!handler_source.contains("document_store().delete"));
        assert!(node_source.contains("write_attachment_node_status"));
        assert_eq!(IROH_DISTRIBUTION_COLLECTION, "file_distributions");
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

// ── Kubernetes-discovery decision helpers ───────────────────────────────────
//
// Pure functions extracted from `SidecarNode::new` and `k8s_discovery_watcher`
// so the highest-risk discovery logic — the (POD_NAME, shared_key) startup
// matrix and the per-peer dial decision — is hermetically testable without a
// kube-apiserver (peat-node#63 QA follow-up).

/// Outcome of resolving the deterministic iroh-identity inputs for K8s
/// discovery from the `(POD_NAME, shared_key)` pair.
#[derive(Debug, PartialEq, Eq)]
enum K8sIdentity {
    /// Both inputs present and valid — the derived 32-byte iroh secret seed.
    Derived([u8; 32]),
    /// `POD_NAME` env var absent or empty.
    MissingPodName,
    /// `shared_key` empty.
    EmptySharedKey,
    /// `shared_key` present but not valid base64.
    InvalidSharedKey,
}

/// Resolve the deterministic iroh seed. Precedence matches the original
/// inline matrix: derive only when both a non-empty pod name and a non-empty
/// shared key are present; otherwise classify the missing/invalid input.
fn resolve_k8s_identity(pod_name: Option<&str>, shared_key: &str) -> K8sIdentity {
    match (pod_name, shared_key) {
        (Some(pn), sk) if !pn.is_empty() && !sk.is_empty() => {
            match base64::engine::general_purpose::STANDARD.decode(sk) {
                Ok(bytes) => K8sIdentity::Derived(derive_iroh_node_key(&bytes, pn)),
                Err(_) => K8sIdentity::InvalidSharedKey,
            }
        }
        (None, _) | (Some(""), _) => K8sIdentity::MissingPodName,
        _ => K8sIdentity::EmptySharedKey,
    }
}

/// Dial decision for a single discovered K8s peer.
#[derive(Debug, PartialEq, Eq)]
enum K8sDialDecision {
    /// The peer is this node — don't dial ourselves.
    SkipSelf,
    /// EndpointSlice carried no addresses yet.
    SkipNoAddresses,
    /// Already connected to this peer.
    SkipAlreadyConnected,
    /// Dial the peer at its (deterministically derived) endpoint id.
    Dial(iroh::EndpointId),
}

/// Decide whether/whom to dial for a discovered peer. The peer's endpoint id is
/// derived from `(shared_key_bytes, peer.node_id)` — the same derivation every
/// node uses for its own identity, so the derived id matches the peer's actual
/// wire identity.
fn k8s_dial_decision(
    peer: &PeerInfo,
    our_pod_name: Option<&str>,
    shared_key_bytes: &[u8],
    connected: &[iroh::EndpointId],
) -> K8sDialDecision {
    if our_pod_name == Some(peer.node_id.as_str()) {
        return K8sDialDecision::SkipSelf;
    }
    if peer.addresses.is_empty() {
        return K8sDialDecision::SkipNoAddresses;
    }
    let peer_id =
        iroh::SecretKey::from_bytes(&derive_iroh_node_key(shared_key_bytes, &peer.node_id))
            .public();
    if connected.contains(&peer_id) {
        return K8sDialDecision::SkipAlreadyConnected;
    }
    K8sDialDecision::Dial(peer_id)
}

#[cfg(test)]
mod k8s_discovery_tests {
    use super::*;
    use std::net::SocketAddr;

    // base64 of 32 bytes of 0x2a — a valid full-length formation secret.
    const KEY_B64: &str = "KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio=";
    fn key_bytes() -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(KEY_B64)
            .unwrap()
    }
    fn peer(node_id: &str, addrs: &[&str]) -> PeerInfo {
        PeerInfo::new(
            node_id.to_string(),
            addrs
                .iter()
                .map(|a| a.parse::<SocketAddr>().unwrap())
                .collect(),
        )
    }
    fn endpoint_for(node_id: &str) -> iroh::EndpointId {
        iroh::SecretKey::from_bytes(&derive_iroh_node_key(&key_bytes(), node_id)).public()
    }

    // ── startup (POD_NAME, shared_key) matrix ──
    #[test]
    fn resolve_identity_derives_with_both_inputs() {
        match resolve_k8s_identity(Some("pod-a"), KEY_B64) {
            K8sIdentity::Derived(seed) => {
                assert_eq!(seed, derive_iroh_node_key(&key_bytes(), "pod-a"))
            }
            other => panic!("expected Derived, got {other:?}"),
        }
    }
    #[test]
    fn resolve_identity_missing_pod_name() {
        assert_eq!(
            resolve_k8s_identity(None, KEY_B64),
            K8sIdentity::MissingPodName
        );
        assert_eq!(
            resolve_k8s_identity(Some(""), KEY_B64),
            K8sIdentity::MissingPodName
        );
    }
    #[test]
    fn resolve_identity_empty_shared_key() {
        assert_eq!(
            resolve_k8s_identity(Some("pod-a"), ""),
            K8sIdentity::EmptySharedKey
        );
    }
    #[test]
    fn resolve_identity_invalid_base64() {
        assert_eq!(
            resolve_k8s_identity(Some("pod-a"), "not!valid!base64!"),
            K8sIdentity::InvalidSharedKey
        );
    }

    // ── per-peer dial decision ──
    #[test]
    fn dial_skips_self() {
        let p = peer("me", &["127.0.0.1:5000"]);
        assert_eq!(
            k8s_dial_decision(&p, Some("me"), &key_bytes(), &[]),
            K8sDialDecision::SkipSelf
        );
    }
    #[test]
    fn dial_skips_no_addresses() {
        let p = peer("other", &[]);
        assert_eq!(
            k8s_dial_decision(&p, Some("me"), &key_bytes(), &[]),
            K8sDialDecision::SkipNoAddresses
        );
    }
    #[test]
    fn dial_skips_already_connected() {
        let p = peer("other", &["127.0.0.1:5000"]);
        let connected = vec![endpoint_for("other")];
        assert_eq!(
            k8s_dial_decision(&p, Some("me"), &key_bytes(), &connected),
            K8sDialDecision::SkipAlreadyConnected
        );
    }
    #[test]
    fn dial_targets_new_peer_with_derived_id() {
        let p = peer("other", &["127.0.0.1:5000"]);
        assert_eq!(
            k8s_dial_decision(&p, Some("me"), &key_bytes(), &[]),
            K8sDialDecision::Dial(endpoint_for("other"))
        );
    }
}
