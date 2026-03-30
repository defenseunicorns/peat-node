//! peat-sidecar — Peat mesh participant exposing a gRPC API.
//!
//! Designed to run as a Kubernetes sidecar container alongside Go applications
//! (e.g., UDS Remote Agent). The sidecar bootstraps a full CRDT mesh node
//! and exposes its capabilities over gRPC (Unix socket or TCP).
//!
//! Optionally watches a co-located UDS Remote Agent and syncs its state
//! to the mesh for cross-cluster visibility.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tonic::transport::Server;
use tracing::{error, info};

use peat_sidecar::node::{SidecarConfig, SidecarNode};
use peat_sidecar::proto::peat_sidecar_server::PeatSidecarServer;
use peat_sidecar::service::PeatSidecarService;
use peat_sidecar::watcher;

#[derive(Parser, Debug)]
#[command(name = "peat-sidecar", about = "Peat mesh sidecar with gRPC API")]
struct Args {
    /// Listen address. Use "unix:///path/to/sock" for Unix socket or
    /// "tcp://0.0.0.0:50051" for TCP. Default: tcp://0.0.0.0:50051
    #[arg(long, env = "PEAT_SIDECAR_LISTEN", default_value = "tcp://0.0.0.0:50051")]
    listen: String,

    /// Persistent data directory.
    #[arg(long, env = "PEAT_SIDECAR_DATA_DIR", default_value = "/data/peat-sidecar")]
    data_dir: PathBuf,

    /// Node identifier. Defaults to a random UUID.
    #[arg(long, env = "PEAT_SIDECAR_NODE_ID")]
    node_id: Option<String>,

    /// Formation/application identifier for group authentication.
    #[arg(long, env = "PEAT_SIDECAR_APP_ID", default_value = "peat-default")]
    app_id: String,

    /// Base64-encoded 32-byte shared key for formation authentication.
    #[arg(long, env = "PEAT_SIDECAR_SHARED_KEY", default_value = "")]
    shared_key: String,

    /// Peer endpoint IDs to connect to on startup.
    #[arg(long, env = "PEAT_SIDECAR_PEERS", value_delimiter = ',')]
    peer: Vec<String>,

    /// Auto-start sync on boot.
    #[arg(long, env = "PEAT_SIDECAR_AUTO_SYNC", default_value = "true")]
    auto_sync: bool,

    // --- Agent Watcher ---

    /// Local UDS Remote Agent address to watch. If not set, the watcher is disabled.
    /// Example: http://localhost:8080
    #[arg(long, env = "PEAT_SIDECAR_AGENT_ADDR")]
    agent_addr: Option<String>,

    /// Agent poll interval in seconds.
    #[arg(long, env = "PEAT_SIDECAR_AGENT_POLL_INTERVAL", default_value = "10")]
    agent_poll_interval: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "peat_sidecar=info,peat_mesh=info".into()),
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
        "starting peat-sidecar"
    );

    tokio::fs::create_dir_all(&args.data_dir).await?;

    // Bootstrap the mesh node
    let config = SidecarConfig {
        node_id: node_id.clone(),
        app_id: args.app_id,
        shared_key: args.shared_key,
        data_dir: args.data_dir,
        peers: args.peer.clone(),
    };

    let node = Arc::new(SidecarNode::new(config).await?);

    // Connect to initial peers
    for peer_id in &args.peer {
        if peer_id.is_empty() {
            continue;
        }
        if let Err(e) = node.connect_peer(peer_id).await {
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
        };
        let watcher_node = Arc::clone(&node);
        tokio::spawn(async move {
            watcher::run(watcher_config, watcher_node).await;
        });
    }

    let service = PeatSidecarService::new(Arc::clone(&node));

    // Parse listen address and start server
    if let Some(path) = args.listen.strip_prefix("unix://") {
        // Unix domain socket
        let uds_path = PathBuf::from(path);
        if uds_path.exists() {
            tokio::fs::remove_file(&uds_path).await?;
        }
        if let Some(parent) = uds_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        info!(path = %uds_path.display(), "listening on Unix socket");

        let uds = tokio::net::UnixListener::bind(&uds_path)?;
        let uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

        Server::builder()
            .add_service(PeatSidecarServer::new(service))
            .serve_with_incoming(uds_stream)
            .await?;
    } else {
        // TCP
        let addr_str = args
            .listen
            .strip_prefix("tcp://")
            .unwrap_or(&args.listen);
        let addr: std::net::SocketAddr = addr_str.parse()?;

        info!(%addr, "listening on TCP");

        Server::builder()
            .add_service(PeatSidecarServer::new(service))
            .serve(addr)
            .await?;
    }

    info!("peat-sidecar stopped");
    Ok(())
}
