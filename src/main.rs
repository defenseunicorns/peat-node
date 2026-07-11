//! peat-node — Local CRDT mesh node exposing a Connect RPC API.
//!
//! Runs as a standalone binary, Kubernetes sidecar, or systemd service alongside
//! applications (e.g., UDS Remote Agent). Participates in a P2P CRDT mesh and
//! exposes it over Connect RPC / gRPC / gRPC-Web.
//!
//! Optionally watches a co-located UDS Remote Agent and syncs its state
//! to the mesh for cross-cluster visibility.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use connectrpc::Router;
use tracing::{error, info, warn};

use peat_node::attachments::config::{AttachmentConfig, AttachmentPriorityCli};
use peat_node::identity;
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;
use peat_node::watcher;

#[derive(Parser, Debug, Clone)]
#[command(name = "peat-node", about = "Peat CRDT mesh node with Connect RPC API")]
struct Args {
    /// Log the full resolved configuration at startup (secrets redacted).
    #[arg(long, env = "PEAT_NODE_PRINT_CONFIG", default_value = "false")]
    print_config: bool,

    /// Optional subcommand. With none, peat-node runs the mesh node (default).
    #[command(subcommand)]
    command: Option<Command>,

    /// Listen address. Use "unix:///path/to/sock" for Unix socket or
    /// "tcp://0.0.0.0:50051" for TCP. Default: tcp://0.0.0.0:50051
    #[arg(long, env = "PEAT_NODE_LISTEN", default_value = "tcp://0.0.0.0:50051")]
    listen: String,

    /// Persistent data directory.
    #[arg(long, env = "PEAT_NODE_DATA_DIR", default_value = "/data/peat-node")]
    data_dir: PathBuf,

    /// Node identifier. Defaults to a random UUID.
    #[arg(long, env = "PEAT_NODE_NODE_ID")]
    node_id: Option<String>,

    /// Formation/application identifier for group authentication.
    #[arg(long, env = "PEAT_NODE_APP_ID", default_value = "peat-default")]
    app_id: String,

    /// Base64-encoded 32-byte shared key for formation authentication.
    #[arg(long, env = "PEAT_NODE_SHARED_KEY", default_value = "")]
    shared_key: String,

    /// Base64-encoded 32-byte AES-256-GCM key for encrypting document content at rest.
    #[arg(long, env = "PEAT_NODE_ENCRYPTION_KEY")]
    encryption_key: Option<String>,

    /// Peers to connect to on startup, in `endpoint_id@host:port` form.
    /// The `@host:port` suffix is required (the n0 public relay is no longer
    /// used by default, so a bare endpoint ID has no way to locate the peer).
    /// One peer per entry; pass `--peer` repeatedly or comma-separate in
    /// `PEAT_NODE_PEERS` to register multiple peers. For more than one
    /// reachable address per peer, use the `ConnectPeer` RPC at runtime —
    /// the comma in this flag separates peers, not addresses within a peer.
    /// Example: `aa11..@10.0.0.5:51071,bb22..@peer-b.svc:51071`
    #[arg(long, env = "PEAT_NODE_PEERS", value_delimiter = ',')]
    peer: Vec<String>,

    /// Disable mDNS peer discovery. mDNS is on by default so same-host nodes
    /// find each other automatically. Set this flag (or
    /// `PEAT_NODE_DISABLE_MDNS=true`) in environments where multicast is
    /// unavailable or undesired (Kubernetes, most containers). Mirrors
    /// `disable_mdns` in `peat-cli` credentials.
    #[arg(long, env = "PEAT_NODE_DISABLE_MDNS", default_value = "false")]
    disable_mdns: bool,

    /// Auto-start sync on boot.
    #[arg(long, env = "PEAT_NODE_AUTO_SYNC", default_value = "true")]
    auto_sync: bool,

    /// Bind the Iroh QUIC endpoint to a specific UDP port. Default: ephemeral.
    /// Pin this for deployments where peers reach this node via a stable
    /// host:port (e.g. Docker Compose, fleet-managed sidecars). The n0 public
    /// relay is disabled by default — peers must be reachable directly or via
    /// an explicit `--relay-url` passed to `ConnectPeer`.
    #[arg(long, env = "PEAT_NODE_IROH_UDP_PORT")]
    iroh_udp_port: Option<u16>,

    /// Blob-download stall threshold, in seconds. A blob fetch attempt that
    /// makes no progress for this long is abandoned and the next known peer
    /// is tried. Default: peat-mesh's 30s. Lower it (e.g. 3-5) for
    /// redundant-peer deployments (dual-C2) where an unreachable preferred
    /// peer otherwise costs the full 30s on the first fetch before the
    /// peer-health index demotes it (peat-mesh#137). A live transfer never
    /// trips this — the watchdog resets on progress.
    #[arg(long, env = "PEAT_NODE_BLOB_STALL_TIMEOUT_SECS")]
    blob_stall_timeout_secs: Option<u64>,

    // --- Tombstone / GC config (peat-node#136) ---
    /// Tombstone retention window in hours. Tombstones are kept for at least
    /// this long before being reaped; peers dark for longer than this risk
    /// resurrecting deleted documents on reconnect (ADR-016 invariant).
    /// Default: 168 h (7 days) — the conservative DDIL floor. Values below
    /// 24 h produce a startup warning. Set lower only for test environments
    /// with bounded partition windows.
    #[arg(long, env = "PEAT_NODE_TOMBSTONE_TTL_HOURS")]
    tombstone_ttl_hours: Option<u32>,

    /// Garbage-collection interval, in seconds. Controls how often the
    /// background GC task sweeps for expired tombstones and implicit-TTL
    /// documents. Default: 300 s (5 min).
    #[arg(long, env = "PEAT_NODE_GC_INTERVAL_SECS")]
    gc_interval_secs: Option<u64>,

    /// Maximum number of tombstones processed per GC sweep. Default: 1000.
    /// Lower this on memory-constrained edge nodes to reduce peak GC
    /// allocation.
    #[arg(long, env = "PEAT_NODE_GC_BATCH_SIZE")]
    gc_batch_size: Option<usize>,

    // --- Agent Watcher ---
    /// Local UDS Remote Agent address to watch. If not set, the watcher is disabled.
    /// Example: http://localhost:8080
    #[arg(long, env = "PEAT_NODE_AGENT_ADDR")]
    agent_addr: Option<String>,

    /// Agent poll interval in seconds.
    #[arg(long, env = "PEAT_NODE_AGENT_POLL_INTERVAL", default_value = "10")]
    agent_poll_interval: u64,

    // --- Agent Watcher TLS ---
    /// Path to PEM-encoded client certificate for mTLS to the agent.
    #[arg(long, env = "PEAT_NODE_AGENT_TLS_CERT")]
    agent_tls_cert: Option<PathBuf>,

    /// Path to PEM-encoded client private key for mTLS to the agent.
    #[arg(long, env = "PEAT_NODE_AGENT_TLS_KEY")]
    agent_tls_key: Option<PathBuf>,

    /// Path to PEM-encoded CA certificate for verifying the agent's server certificate.
    #[arg(long, env = "PEAT_NODE_AGENT_TLS_CA")]
    agent_tls_ca: Option<PathBuf>,

    // --- Attachment Distribution (PRD-006) ---
    //
    // Safety default: with no `--attachment-root` entries, the four
    // attachment RPCs return `Unimplemented`. Operators must consciously opt
    // in by naming the roots that may be read.
    /// Allowlisted attachment root, in `name=path` form. Repeatable; comma-
    /// separated in `PEAT_NODE_ATTACHMENT_ROOT`. Each `path` must exist and
    /// be a directory at startup; it is canonicalised once and stored as the
    /// canonical form. Example: `outbox=/var/lib/peat/outbox,media=/var/lib/peat/media`.
    #[arg(
        long = "attachment-root",
        env = "PEAT_NODE_ATTACHMENT_ROOT",
        value_delimiter = ','
    )]
    attachment_root: Vec<String>,

    /// Per-file size cap (bytes). Files larger than this are rejected
    /// `ResourceExhausted` at validation.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_MAX_FILE_BYTES",
        default_value_t = peat_node::attachments::config::DEFAULT_MAX_FILE_BYTES
    )]
    attachment_max_file_bytes: u64,

    /// Per-request total-bytes cap. Bundles whose `Σ size_bytes` exceeds
    /// this are rejected `ResourceExhausted`.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_MAX_BUNDLE_BYTES",
        default_value_t = peat_node::attachments::config::DEFAULT_MAX_BUNDLE_BYTES
    )]
    attachment_max_bundle_bytes: u64,

    /// Per-request file-count cap. Bundles with more files than this are
    /// rejected `ResourceExhausted`.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_MAX_FILES_PER_BUNDLE",
        default_value_t = peat_node::attachments::config::DEFAULT_MAX_FILES_PER_BUNDLE
    )]
    attachment_max_files_per_bundle: u32,

    /// Cap on `NodeListScope.node_ids.len()`. Over-cap requests rejected
    /// `ResourceExhausted`.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_MAX_NODE_LIST_LEN",
        default_value_t = peat_node::attachments::config::DEFAULT_MAX_NODE_LIST_LEN
    )]
    attachment_max_node_list_len: u32,

    /// In-flight distribution cap. Requests beyond this are rejected
    /// `ResourceExhausted` unless `--attachment-queue-when-full` is set.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_MAX_CONCURRENT_DISTRIBUTIONS",
        default_value_t = peat_node::attachments::config::DEFAULT_MAX_CONCURRENT_DISTRIBUTIONS
    )]
    attachment_max_concurrent_distributions: u32,

    /// If true, accept and queue requests beyond the in-flight cap; else
    /// reject `ResourceExhausted`.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_QUEUE_WHEN_FULL",
        default_value_t = peat_node::attachments::config::DEFAULT_QUEUE_WHEN_FULL
    )]
    attachment_queue_when_full: bool,

    /// Default `AttachmentPriority` when caller leaves it `UNSPECIFIED`.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_DEFAULT_PRIORITY",
        value_enum,
        default_value_t = AttachmentPriorityCli::Routine
    )]
    attachment_default_priority: AttachmentPriorityCli,

    /// Grace window (seconds) for unknown node IDs in `NodeListScope` before
    /// they are marked `FAILED` in per-node status. A node may not yet be
    /// known to this peat-node at request time.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_DISCOVERY_GRACE_SECS",
        default_value_t = peat_node::attachments::config::DEFAULT_DISCOVERY_GRACE_SECS
    )]
    attachment_discovery_grace_secs: u32,

    /// How long terminal bundles' handle tables are retained for `bundle_id`
    /// lookups, `SubscribeAttachmentBundle` late-attach, and `AlreadyExists`
    /// enforcement. `0` disables retention entirely (no idempotency, no
    /// late-subscribe — discouraged).
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_HANDLE_RETENTION_SECS",
        default_value_t = peat_node::attachments::config::DEFAULT_HANDLE_RETENTION_SECS
    )]
    attachment_handle_retention_secs: u32,

    /// Hard cap on handle-table size. LRU eviction kicks in before the
    /// retention window expires when exceeded. Bounds memory growth on
    /// long-running edge nodes.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_MAX_KNOWN_BUNDLES",
        default_value_t = peat_node::attachments::config::DEFAULT_MAX_KNOWN_BUNDLES
    )]
    attachment_max_known_bundles: u32,

    /// Receive-side attachment inbox (PRD-006 v1.1). When set, peat-node
    /// spawns a background watcher that polls the synced
    /// `file_distributions` collection, fetches any blob whose
    /// distribution doc targets this node's iroh endpoint, and writes
    /// the bytes to `{inbox}/{relative_path}`, mirroring the sender's
    /// outbox layout (a sender-supplied name that is absolute or contains
    /// `..` falls back to `{inbox}/{distribution_id}.bin`). Unset
    /// (default) disables receive-side delivery — peers still see the
    /// sender's distribution doc via Automerge sync but no auto-pull
    /// happens. Created if missing.
    #[arg(long, env = "PEAT_NODE_ATTACHMENT_INBOX")]
    attachment_inbox: Option<PathBuf>,

    /// Inbox watcher poll interval in seconds. Smaller = faster
    /// delivery, more `file_distributions` scans. Default 1s.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_INBOX_POLL_SECS",
        default_value_t = peat_node::attachments::config::DEFAULT_INBOX_POLL_SECS
    )]
    attachment_inbox_poll_secs: u32,

    /// Enable the send-side outbox watcher: auto-distribute (AllNodes scope)
    /// any stable new file dropped into an `--attachment-root`, with no
    /// `SendAttachments` call. Off by default. Requires at least one root.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_OUTBOX_WATCH",
        default_value_t = false
    )]
    attachment_outbox_watch: bool,

    /// Outbox watcher poll interval in seconds. Default 2s.
    #[arg(
        long,
        env = "PEAT_NODE_ATTACHMENT_OUTBOX_POLL_SECS",
        default_value_t = peat_node::attachments::config::DEFAULT_OUTBOX_POLL_SECS
    )]
    attachment_outbox_poll_secs: u32,

    // --- Zenoh Bridge ---
    /// Zenoh endpoint(s) to connect to. Empty = multicast scouting.
    /// Example: tcp/192.168.1.1:7447
    #[cfg(feature = "zenoh-bridge")]
    #[arg(long, env = "PEAT_NODE_ZENOH_CONNECT", value_delimiter = ',')]
    zenoh_connect: Vec<String>,

    /// Zenoh key expression(s) to subscribe to. Presence enables the bridge.
    /// Example: local/**
    #[cfg(feature = "zenoh-bridge")]
    #[arg(long, env = "PEAT_NODE_ZENOH_SUBSCRIBE", value_delimiter = ',')]
    zenoh_subscribe: Vec<String>,

    /// Collection prefix for Zenoh-ingested documents. Default: "zenoh".
    #[cfg(feature = "zenoh-bridge")]
    #[arg(long, env = "PEAT_NODE_ZENOH_PREFIX", default_value = "zenoh")]
    zenoh_prefix: String,

    /// Minimum interval (milliseconds) between writes for the same Zenoh key.
    /// Samples arriving faster are dropped (latest-wins). Default: 1000.
    #[cfg(feature = "zenoh-bridge")]
    #[arg(
        long,
        env = "PEAT_NODE_ZENOH_MIN_WRITE_INTERVAL_MS",
        default_value = "1000"
    )]
    zenoh_min_write_interval_ms: u64,

    // --- Kubernetes peer discovery (peat-node#63) ---
    /// Enable Kubernetes EndpointSlice-based peer discovery for in-cluster
    /// deployments. Requires the `POD_NAME` env var (set via Kubernetes
    /// downward API) and a non-empty `--shared-key`. Disable mDNS with
    /// `--disable-mdns` in the same deployment. Default: false (off).
    #[arg(
        long,
        env = "PEAT_NODE_ENABLE_KUBERNETES_DISCOVERY",
        default_value = "false"
    )]
    enable_kubernetes_discovery: bool,

    /// Kubernetes namespace to watch for EndpointSlice resources. Default:
    /// reads from the service-account namespace mount, falls back to `default`.
    #[arg(long, env = "PEAT_NODE_DISCOVERY_NAMESPACE")]
    discovery_namespace: Option<String>,

    /// Label selector for EndpointSlice resources. Default: `app=peat-node`.
    #[arg(
        long,
        env = "PEAT_NODE_DISCOVERY_LABEL_SELECTOR",
        default_value = "app=peat-node"
    )]
    discovery_label_selector: String,

    /// Annotation prefix for peer metadata in EndpointSlice annotations.
    /// Default: `peat.`.
    #[arg(
        long,
        env = "PEAT_NODE_DISCOVERY_ANNOTATION_PREFIX",
        default_value = "peat."
    )]
    discovery_annotation_prefix: String,

    /// EndpointSlice re-list interval in seconds. Default: 30.
    #[arg(long, env = "PEAT_NODE_DISCOVERY_INTERVAL_SECS", default_value = "30")]
    discovery_interval_secs: u64,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// Print the deterministic iroh `EndpointId` for a `(shared-key, node-id)`
    /// pair, offline — no node boot, no network, no access to the peer.
    ///
    /// A node's identity is `HKDF-SHA256(shared_key, "iroh:" + node_id)`, so any
    /// holder of the shared key can compute any node's `EndpointId` from its
    /// `node_id` alone. Use this to fill in `PEAT_NODE_PEERS` entries
    /// (`<endpoint_id>@host:port`) for peers you can't reach to query.
    DeriveId {
        /// Base64-encoded shared key (same value as the peer's
        /// `PEAT_NODE_SHARED_KEY`).
        #[arg(long, env = "PEAT_NODE_SHARED_KEY")]
        shared_key: String,
        /// The peer's node id (its `PEAT_NODE_NODE_ID`).
        #[arg(long, env = "PEAT_NODE_NODE_ID")]
        node_id: String,
    },
}

/// Keys among `vars` that match `prefix` and have an empty value — the set to
/// treat as unset. Pure so the empty-env normalization is unit-testable without
/// touching the process environment.
fn empty_prefixed_env_keys<I>(vars: I, prefix: &str) -> Vec<String>
where
    I: IntoIterator<Item = (String, String)>,
{
    vars.into_iter()
        .filter(|(k, v)| k.starts_with(prefix) && v.is_empty())
        .map(|(k, _)| k)
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                // Include the whole sync stack at info by default, not just
                // peat-node itself. `peat_protocol` carries the attachment
                // send/receive watcher logs (targeting + blob fetch) — without
                // it a failed delivery is invisible. `iroh=warn` surfaces QUIC
                // dial / connection failures (the usual reason a peer never
                // enters `known_peers`) without the info-level packet spam.
                // Override the whole thing with `RUST_LOG`.
                .unwrap_or_else(|_| {
                    "peat_node=info,peat_mesh=info,peat_protocol=info,iroh=warn".into()
                }),
        )
        .init();

    // Version banner at the top of the logs: peat-node's own version plus the
    // resolved versions of the core dependency stack (captured from Cargo.lock
    // by build.rs). Lets an operator confirm exactly which build + mesh/protocol
    // RC a container is running from the first log line.
    info!(
        version = env!("CARGO_PKG_VERSION"),
        peat_mesh = env!("PEAT_MESH_VERSION"),
        peat_protocol = env!("PEAT_PROTOCOL_VERSION"),
        peat_schema = env!("PEAT_SCHEMA_VERSION"),
        "peat-node version + dependency stack"
    );

    // Treat any empty `PEAT_NODE_*` env var as unset before clap parses.
    // Compose/Helm routinely inject empty-string env vars for "disabled"
    // optional settings (e.g. `PEAT_NODE_ATTACHMENT_INBOX=""`); clap otherwise
    // rejects those with "a value is required for '--…' but none was supplied"
    // and the node crash-loops on startup. Empty is never a meaningful value
    // for any of our vars, so dropping them lets the Option/default args
    // resolve normally.
    for key in empty_prefixed_env_keys(std::env::vars(), "PEAT_NODE_") {
        // SAFETY (2021 edition: this call is safe): runs at the very top of
        // main before any spawned task reads the environment.
        std::env::remove_var(&key);
    }

    let args = Args::parse();

    // Offline subcommands short-circuit before any mesh bootstrap.
    if let Some(Command::DeriveId {
        shared_key,
        node_id,
    }) = &args.command
    {
        let endpoint_id = identity::derive_endpoint_id(shared_key, node_id)?;
        // Print only the id to stdout so it's pipe/`$(...)`-friendly.
        println!("{endpoint_id}");
        return Ok(());
    }

    // `node_id` is explicit only when the operator set it; otherwise it's a
    // fresh random UUID, which makes deterministic identity impossible (a new
    // id every boot). Track that so we can warn below.
    let node_id_explicit = args.node_id.is_some();
    let node_id = args
        .node_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    info!(
        node_id = %node_id,
        listen = %args.listen,
        data_dir = %args.data_dir.display(),
        agent_addr = ?args.agent_addr,
        "starting peat-node"
    );

    // Full resolved-configuration dump (opt-in via --print-config /
    // PEAT_NODE_PRINT_CONFIG). Secrets are redacted before logging.
    if args.print_config {
        let mut redacted = args.clone();
        redacted.shared_key = "<redacted>".to_string();
        if redacted.encryption_key.is_some() {
            redacted.encryption_key = Some("<redacted>".to_string());
        }
        info!("resolved configuration (PEAT_NODE_PRINT_CONFIG):\n{redacted:#?}");
    }

    tokio::fs::create_dir_all(&args.data_dir).await?;

    // Deterministic iroh identity (peat-node#63 gap-4d): seed the keypair from
    // (shared_key, node_id) so the EndpointId is stable across restarts and
    // computable offline by peers. Empty shared key → None → iroh's random
    // per-process identity (pre-feature behaviour).
    let iroh_secret_key = identity::derive_iroh_secret_seed(&args.shared_key, &node_id)?;
    // In Kubernetes-discovery mode the deterministic identity is (re)derived
    // inside SidecarNode::new from POD_NAME, so a missing PEAT_NODE_NODE_ID is
    // not a problem there — only warn for the static-peering path.
    if iroh_secret_key.is_some() && !node_id_explicit && !args.enable_kubernetes_discovery {
        warn!(
            node_id = %node_id,
            "PEAT_NODE_NODE_ID is not set — using a random per-boot node id, so this \
             node's iroh EndpointId will change on every restart and peers cannot \
             pre-compute it. Set a stable PEAT_NODE_NODE_ID for deterministic peering."
        );
    }

    // Build the attachment configuration. Canonicalises every --attachment-root
    // and fails fast on bad inputs (missing dir, duplicate name, malformed name).
    let attachment_config = AttachmentConfig::from_raw(
        &args.attachment_root,
        args.attachment_inbox.clone(),
        args.attachment_max_file_bytes,
        args.attachment_max_bundle_bytes,
        args.attachment_max_files_per_bundle,
        args.attachment_max_node_list_len,
        args.attachment_max_concurrent_distributions,
        args.attachment_queue_when_full,
        args.attachment_default_priority,
        args.attachment_discovery_grace_secs,
        args.attachment_handle_retention_secs,
        args.attachment_max_known_bundles,
        args.attachment_inbox_poll_secs,
        args.attachment_outbox_watch,
        args.attachment_outbox_poll_secs,
    )?;
    if attachment_config.has_roots() {
        let names: Vec<&str> = attachment_config.roots.keys().map(String::as_str).collect();
        info!(
            roots = ?names,
            max_file_bytes = attachment_config.max_file_bytes,
            max_bundle_bytes = attachment_config.max_bundle_bytes,
            max_files_per_bundle = attachment_config.max_files_per_bundle,
            max_concurrent = attachment_config.max_concurrent_distributions,
            default_priority = attachment_config.default_priority.as_str(),
            "attachment distribution enabled"
        );
    } else {
        info!("attachment distribution disabled — no --attachment-root configured");
    }

    // Bootstrap the mesh node
    let config = SidecarConfig {
        node_id: node_id.clone(),
        app_id: args.app_id,
        shared_key: args.shared_key,
        data_dir: args.data_dir,
        peers: args.peer.clone(),
        encryption_key: args.encryption_key,
        iroh_udp_port: args.iroh_udp_port,
        iroh_secret_key,
        disable_mdns: args.disable_mdns,
        blob_stall_timeout: args.blob_stall_timeout_secs.map(Duration::from_secs),
        tombstone_ttl_hours: args.tombstone_ttl_hours,
        gc_interval_secs: args.gc_interval_secs,
        gc_batch_size: args.gc_batch_size,
        attachment_config,
        enable_kubernetes_discovery: args.enable_kubernetes_discovery,
        kubernetes_discovery_namespace: args.discovery_namespace,
        kubernetes_discovery_label_selector: args.discovery_label_selector,
        kubernetes_discovery_annotation_prefix: args.discovery_annotation_prefix,
        kubernetes_discovery_interval_secs: args.discovery_interval_secs,
    };

    let node = Arc::new(SidecarNode::new(config).await?);

    // Send-side outbox watcher (opt-in): auto-distribute files dropped into the
    // configured roots, the symmetric counterpart to the receive-side inbox
    // watcher. Spawned here (not in SidecarNode::new) because it drives the
    // gRPC SendAttachments path, which needs the constructed Arc<SidecarNode>.
    {
        let acfg = node.attachment_config();
        if acfg.outbox_watch {
            if acfg.has_roots() {
                let roots = acfg.roots.clone();
                let poll = std::time::Duration::from_secs(acfg.outbox_poll_secs.max(1) as u64);
                let watch_node = Arc::clone(&node);
                tokio::spawn(async move {
                    peat_node::attachments::outbox::run(watch_node, roots, poll).await;
                });
            } else {
                warn!(
                    "PEAT_NODE_ATTACHMENT_OUTBOX_WATCH is set but no --attachment-root is \
                     configured — outbox watcher not started (nothing to watch)"
                );
            }
        }
    }

    // Initial peers in `endpoint_id@host:port` form, one per entry. The
    // outer `,` in `PEAT_NODE_PEERS` separates peers (handled by clap's
    // `value_delimiter`); multiple addresses for one peer go through the
    // `ConnectPeer` RPC at runtime.
    for peer_spec in &args.peer {
        if peer_spec.is_empty() {
            continue;
        }
        let Some((endpoint_id, addr)) = peer_spec.split_once('@') else {
            error!(
                peer = peer_spec,
                "ignoring --peer: expected `endpoint_id@host:port` form (the n0 \
                 public relay is no longer used; a bare endpoint ID has no way \
                 to locate the peer)"
            );
            continue;
        };
        let addresses = vec![addr.to_string()];
        if let Err(e) = node.connect_peer(endpoint_id, &addresses, "").await {
            error!(peer = peer_spec, "failed to connect to peer: {e}");
        }
    }

    // Auto-start sync if configured
    if args.auto_sync {
        node.start_sync().await?;
    }

    // Start agent watcher if configured
    if let Some(agent_addr) = args.agent_addr {
        let watcher_config = watcher::WatcherConfig {
            agent_addr,
            poll_interval: Duration::from_secs(args.agent_poll_interval),
            node_id: node_id.clone(),
            tls: watcher::TlsConfig {
                cert: args.agent_tls_cert,
                key: args.agent_tls_key,
                ca_cert: args.agent_tls_ca,
            },
        };
        let watcher_node = Arc::clone(&node);
        tokio::spawn(async move {
            watcher::run(watcher_config, watcher_node).await;
        });
    }

    // Start Zenoh bridge if configured
    #[cfg(feature = "zenoh-bridge")]
    if !args.zenoh_subscribe.is_empty() {
        let zenoh_config = peat_node::zenoh_bridge::ZenohBridgeConfig {
            connect: args.zenoh_connect,
            subscribe: args.zenoh_subscribe,
            prefix: args.zenoh_prefix,
            node_id: node_id.clone(),
            min_write_interval: std::time::Duration::from_millis(args.zenoh_min_write_interval_ms),
        };
        let zenoh_node = Arc::clone(&node);
        tokio::spawn(async move {
            peat_node::zenoh_bridge::run(zenoh_config, zenoh_node).await;
        });
    }

    // Build the Connect RPC service (handles Connect + gRPC + gRPC-Web)
    let service = Arc::new(PeatSidecarService::new(Arc::clone(&node)));
    let router = service.register(Router::new());

    // Parse listen address and start server
    if let Some(path) = args.listen.strip_prefix("unix://") {
        let uds_path = PathBuf::from(path);
        if uds_path.exists() {
            tokio::fs::remove_file(&uds_path).await?;
        }
        if let Some(parent) = uds_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        info!(path = %uds_path.display(), "listening on Unix socket");

        let listener = tokio::net::UnixListener::bind(&uds_path)?;
        let connect_service = connectrpc::ConnectRpcService::new(router);

        loop {
            let (stream, _) = listener.accept().await?;
            let svc = connect_service.clone();
            tokio::spawn(async move {
                let io = hyper_util::rt::TokioIo::new(stream);
                let _ = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                )
                .serve_connection(
                    io,
                    hyper::service::service_fn(move |req| {
                        let mut s = svc.clone();
                        async move { tower::Service::call(&mut s, req).await }
                    }),
                )
                .await;
            });
        }
    } else {
        // TCP
        let addr_str = args.listen.strip_prefix("tcp://").unwrap_or(&args.listen);
        let addr: std::net::SocketAddr = addr_str.parse()?;

        info!(%addr, "listening on TCP (Connect + gRPC + gRPC-Web)");

        connectrpc::Server::new(router)
            .serve(addr)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    info!("peat-node stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::empty_prefixed_env_keys;

    #[test]
    fn empty_prefixed_env_keys_selects_only_empty_matching_prefix() {
        let vars = vec![
            ("PEAT_NODE_ATTACHMENT_INBOX".to_string(), "".to_string()), // empty + prefix -> drop
            ("PEAT_NODE_SHARED_KEY".to_string(), "abc".to_string()),    // non-empty -> keep
            ("PEAT_NODE_AGENT_ADDR".to_string(), "".to_string()),       // empty + prefix -> drop
            ("OTHER_VAR".to_string(), "".to_string()), // empty but wrong prefix -> keep
            ("PATH".to_string(), "/usr/bin".to_string()), // unrelated -> keep
        ];
        let mut got = empty_prefixed_env_keys(vars, "PEAT_NODE_");
        got.sort();
        assert_eq!(
            got,
            vec!["PEAT_NODE_AGENT_ADDR", "PEAT_NODE_ATTACHMENT_INBOX"]
        );
    }

    #[test]
    fn empty_prefixed_env_keys_empty_when_none_match() {
        let vars = vec![(
            "PEAT_NODE_LISTEN".to_string(),
            "tcp://0.0.0.0:50051".to_string(),
        )];
        assert!(empty_prefixed_env_keys(vars, "PEAT_NODE_").is_empty());
    }
}
