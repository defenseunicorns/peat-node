//! peat-node — Local CRDT mesh node exposing a Connect RPC API.
//!
//! Runs as a standalone binary, Kubernetes sidecar, or systemd service alongside
//! applications (e.g., UDS Remote Agent). Participates in a P2P CRDT mesh and
//! exposes it over Connect RPC / gRPC / gRPC-Web.
//!
//! Optionally watches a co-located UDS Remote Agent and syncs its state
//! to the mesh for cross-cluster visibility.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use connectrpc::Router;
use tracing::{error, info};

use peat_node::attachments::config::{AttachmentConfig, AttachmentPriorityCli};
use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;
use peat_node::watcher;

#[derive(Parser, Debug)]
#[command(name = "peat-node", about = "Peat CRDT mesh node with Connect RPC API")]
struct Args {
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
    /// the bytes to `{inbox}/{distribution_id}/{filename}`. Unset
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "peat_node=info,peat_mesh=info".into()),
        )
        .init();

    let args = Args::parse();
    let node_id = args
        .node_id
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    info!(
        node_id = %node_id,
        listen = %args.listen,
        data_dir = %args.data_dir.display(),
        agent_addr = ?args.agent_addr,
        "starting peat-node"
    );

    tokio::fs::create_dir_all(&args.data_dir).await?;

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
        disable_mdns: args.disable_mdns,
        blob_stall_timeout: args.blob_stall_timeout_secs.map(Duration::from_secs),
        tombstone_ttl_hours: args.tombstone_ttl_hours,
        gc_interval_secs: args.gc_interval_secs,
        gc_batch_size: args.gc_batch_size,
        attachment_config,
    };

    let node = Arc::new(SidecarNode::new(config).await?);

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
