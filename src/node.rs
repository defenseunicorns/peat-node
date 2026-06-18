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
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::fanout::FanoutKind;
use peat_mesh::discovery::{DiscoveryEvent, DiscoveryStrategy, MdnsDiscovery, PeerInfo};
use peat_mesh::qos::GcConfig;
use peat_mesh::storage::json_convert::{automerge_to_json, json_to_automerge};
use peat_mesh::storage::{AutomergeStore, ChangeOrigin, DocChange, SyncTransport, TtlConfig};
use peat_mesh::sync::{AutomergeBackend, AutomergeBackendConfig};
use peat_protocol::storage::file_distribution::IrohFileDistribution;
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
            cipher,
            attachment_config: config.attachment_config,
            bundle_registry,
            file_distribution,
            bundle_runtime: Arc::new(BundleRuntimeStore::new()),
            registered_peers,
            collection_configs: Arc::new(RwLock::new(collection_configs)),
            collection_configs_path,
            _mdns: mdns,
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
    pub fn file_distribution(&self) -> Option<&Arc<IrohFileDistribution>> {
        self.file_distribution.as_ref()
    }

    /// PRD-006 ingest target — the iroh blob store the backend holds.
    /// Exposed for the attachment handlers; otherwise prefer the
    /// higher-level mesh operations.
    pub fn blob_store(&self) -> &peat_mesh::storage::NetworkedIrohBlobStore {
        self.backend.blob_store()
    }

    /// The Automerge document store the backend holds.
    ///
    /// Exposed for the attachment subsystem's inbox watcher (which writes
    /// receiver-side `NodeTransferStatus` into the `file_distributions`
    /// collection so the sender's `IrohFileDistribution` progress watcher
    /// sees real cross-peer state — see `attachments/inbox.rs`), and for
    /// integration tests that need to read the local document state
    /// directly rather than through the gRPC surface.
    pub fn document_store(&self) -> &Arc<AutomergeStore> {
        self.backend.store()
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
        let parsed: serde_json::Value =
            serde_json::from_str(json_data).map_err(|e| anyhow::anyhow!("invalid JSON: {e}"))?;

        let key = format!("{collection}:{doc_id}");
        let store = self.backend.store();
        let existing = store.get(&key)?;

        let doc = match &self.cipher {
            Some(c) => {
                // Ciphertext is opaque — wrap in {"value":"<ciphertext>"} so
                // json_to_automerge has a map root to work with.
                let ciphertext = c.encrypt(json_data)?;
                let wrapped = serde_json::json!({ "value": ciphertext });
                json_to_automerge(&wrapped, existing.as_ref())?
            }
            None => {
                // Write user JSON directly as structured Automerge fields for
                // field-level CRDT merging (peat-node#7).
                json_to_automerge(&parsed, existing.as_ref())?
            }
        };

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
