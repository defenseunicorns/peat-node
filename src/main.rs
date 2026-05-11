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

    /// Peer endpoint IDs to connect to on startup.
    #[arg(long, env = "PEAT_NODE_PEERS", value_delimiter = ',')]
    peer: Vec<String>,

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

    // Bootstrap the mesh node
    let config = SidecarConfig {
        node_id: node_id.clone(),
        app_id: args.app_id,
        shared_key: args.shared_key,
        data_dir: args.data_dir,
        peers: args.peer.clone(),
        encryption_key: args.encryption_key,
        iroh_udp_port: args.iroh_udp_port,
    };

    let node = Arc::new(SidecarNode::new(config).await?);

    // Initial peers via CLI/env are best-effort and require either a relay
    // URL or out-of-band knowledge of the peer's direct address — neither of
    // which the simple `--peer <endpoint_id>` flag carries. For real
    // bootstrapping use the `ConnectPeer` RPC at runtime with explicit
    // addresses. Kept here for backwards compatibility — will log an error
    // explaining the requirement.
    for peer_id in &args.peer {
        if peer_id.is_empty() {
            continue;
        }
        if let Err(e) = node.connect_peer(peer_id, &[], "").await {
            error!(peer = peer_id, "failed to connect to peer: {e}");
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
