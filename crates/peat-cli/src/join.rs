//! Shared join / auth / sync prelude per peat-node ADR-001.
//!
//! Bootstraps the same `peat_mesh::sync::AutomergeBackend` that `peat-node`
//! itself uses, then dials each peer specified in the credentials bundle
//! with formation-key authentication. The backend hangs off an
//! `Arc<AutomergeBackend>` so subcommands can pass the session around
//! cheaply.
//!
//! Lifecycle: `MeshSession` owns the data directory its Automerge store lives
//! in. When a persistent `data_dir` is provided (via `--data-dir` flag or the
//! credentials bundle), the directory is left on disk after the CLI exits so
//! the next invocation can resume from the same state. Without a `data_dir`
//! the session falls back to a `TempDir` that is cleaned up on drop — documents
//! only survive if they sync to a connected peer before exit.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use peat_mesh::discovery::{DiscoveryEvent, DiscoveryStrategy, MdnsDiscovery, PeerInfo};
use peat_mesh::storage::SyncTransport;
use peat_mesh::sync::{AutomergeBackend, AutomergeBackendConfig};
use tempfile::TempDir;

use crate::cli::CliError;
use crate::creds::{expand_data_dir, PeatCredentials};

/// Options derived from the parsed `CommonArgs` that affect how the join
/// prelude runs.
#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// Per-peer connect timeout (also the budget the caller gets back when
    /// no peer answers).
    pub timeout: Duration,
    /// Caller-supplied identity. `None` → ephemeral UUID.
    pub as_id: Option<String>,
    /// Persist the Automerge store here. `None` → ephemeral TempDir.
    /// Overrides `data_dir` in the credentials bundle when both are set.
    pub data_dir: Option<PathBuf>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            as_id: None,
            data_dir: None,
        }
    }
}

/// Backing store for a `MeshSession` — either a self-cleaning `TempDir` or a
/// caller-supplied persistent path.
enum DataDir {
    Ephemeral(TempDir),
    Persistent(PathBuf),
}

impl DataDir {
    fn path(&self) -> &Path {
        match self {
            DataDir::Ephemeral(d) => d.path(),
            DataDir::Persistent(p) => p.as_path(),
        }
    }
}

/// Settle window after the join + per-peer sync_all kick to let the peer's
/// reciprocal catch-up sync drain into the CLI's local store. Loopback and
/// LAN handshakes typically finish well under this; very slow links may
/// need more (and individual read commands poll on top — see query.rs).
const POST_JOIN_SETTLE: Duration = Duration::from_millis(1000);

/// A live mesh participant. Holds the backend and the directory its store
/// lives in. Dropping a persistent session leaves the directory intact;
/// dropping an ephemeral session removes it.
pub struct MeshSession {
    backend: Arc<AutomergeBackend>,
    node_id: String,
    _data_dir: DataDir,
    // Kept alive so mDNS advertisement and browsing continue for the
    // session lifetime. None when mDNS is disabled or failed to init.
    _mdns: Option<MdnsDiscovery>,
}

impl MeshSession {
    /// Bootstrap the backend, then dial each peer in the credentials bundle.
    ///
    /// Returns `Ok(MeshSession)` if at least one configured peer connected
    /// (or if no peers were configured — useful for read-from-self in tests).
    /// Returns `Err` if peers were configured but none could be reached.
    pub async fn open(creds: PeatCredentials, opts: SessionOptions) -> Result<Self, CliError> {
        // Resolve data dir: CLI flag > creds bundle > ephemeral TempDir.
        let data_dir: DataDir = if let Some(p) = opts.data_dir.clone() {
            create_private_dir(&p)?;
            DataDir::Persistent(p)
        } else if let Some(raw) = &creds.data_dir {
            let p = expand_data_dir(raw)?;
            create_private_dir(&p)?;
            DataDir::Persistent(p)
        } else {
            DataDir::Ephemeral(tempfile::tempdir().map_err(|e| {
                CliError::Generic(format!("could not create ephemeral data dir: {e}"))
            })?)
        };

        // Stable actor identity for persistent stores: derive from
        // `data_dir/identity` so the same actor ID is reused across
        // invocations. With an ephemeral store a fresh UUID is fine because
        // the store is thrown away on exit anyway.
        // Automerge actors accumulate per-actor change history; reusing the
        // same actor across invocations keeps the Automerge document lean.
        let node_id = opts.as_id.clone().unwrap_or_else(|| {
            if let DataDir::Persistent(p) = &data_dir {
                let id_file = p.join("identity");
                if let Ok(id) = std::fs::read_to_string(&id_file) {
                    let id = id.trim().to_string();
                    if !id.is_empty() {
                        return id;
                    }
                }
                let fresh = uuid::Uuid::new_v4().to_string();
                let _ = std::fs::write(&id_file, &fresh);
                fresh
            } else {
                uuid::Uuid::new_v4().to_string()
            }
        });

        tracing::debug!(
            node_id = %node_id,
            data_dir = %data_dir.path().display(),
            persistent = matches!(data_dir, DataDir::Persistent(_)),
            "bootstrapping peat-cli backend"
        );

        let mut backend_cfg = AutomergeBackendConfig::default();
        backend_cfg.data_dir = data_dir.path().to_path_buf();
        backend_cfg.formation_id = creds.app_id.clone();
        backend_cfg.base64_shared_key = creds.shared_key.clone();
        // CLI is a transient client; let iroh pick an ephemeral UDP port.
        backend_cfg.iroh_bind_addr = None;
        // At-rest cipher is handled at the peat-node layer for now.
        // The CLI's tempdir-backed store is short-lived enough that
        // omitting it is safe for Phase 2.
        backend_cfg.cipher = None;
        // CLI uses peat-mesh's default stall threshold (peat-mesh#137).
        backend_cfg.download_stall_timeout = None;
        let backend = AutomergeBackend::with_iroh(backend_cfg)
            .await
            .map_err(|e| {
                let msg = format!("{e:#}");
                // redb holds an exclusive file lock on the store while open.
                // "Cannot acquire lock" means another peat process is running
                // against the same data_dir.
                if msg.contains("Cannot acquire lock") || msg.contains("Database already open") {
                    CliError::Generic(format!(
                        "backend bootstrap: {msg}\n\
                     hint: the local store at {} is locked — another `peat` \
                     process is likely running. Stop it and retry.",
                        data_dir.path().display()
                    ))
                } else {
                    CliError::Generic(format!("backend bootstrap: {msg}"))
                }
            })?;

        // ── mDNS peer discovery ────────────────────────────────────────────
        // Start mDNS before explicit peer connects so the daemon begins
        // collecting peer announcements while we dial. `disable_mdns` in
        // the creds bundle opts out — useful in containers where multicast
        // is unavailable.
        let (mut mdns, mdns_rx) = if creds.disable_mdns {
            (None, None)
        } else {
            match MdnsDiscovery::new() {
                Err(e) => {
                    tracing::warn!("mDNS init failed (no local discovery): {e}");
                    (None, None)
                }
                Ok(mut m) => {
                    if let Err(e) = m.start().await {
                        tracing::warn!("mDNS start failed: {e}");
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
                            // Advertise with a concrete loopback address so
                            // same-host peers can connect directly. Also embed
                            // the port in the TXT metadata as a fallback for
                            // the case where cross-process mDNS resolution
                            // returns an empty address list (query responses
                            // don't always attach the A record; the port from
                            // the SRV record is always present).
                            let loopback = std::net::SocketAddr::new(
                                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                                port,
                            );
                            let mut meta = std::collections::HashMap::new();
                            meta.insert("port".to_string(), port.to_string());
                            // Embed formation_id so peers can filter before
                            // attempting a connection. Avoids connecting to
                            // peers from a different formation (e.g. test
                            // processes running alongside the user's session).
                            meta.insert("formation_id".to_string(), creds.app_id.clone());
                            match m.advertise_with_addr(&eid, loopback, Some(meta)) {
                                Ok(()) => tracing::debug!("mDNS: advertising on 127.0.0.1:{port}"),
                                Err(e) => tracing::warn!("mDNS advertise (loopback) failed: {e}"),
                            }
                        }
                    }
                    let rx = m.event_stream().ok();
                    (Some(m), rx)
                }
            }
        };
        // Keep mdns alive in the session; mut only needed for event_stream above.
        let mdns: Option<MdnsDiscovery> = mdns.take();

        let mut connected: usize = 0;
        for spec in &creds.peers {
            match connect_peer(&backend, spec, opts.timeout).await {
                Ok(()) => {
                    connected += 1;
                    tracing::info!(peer = %spec, "connected");
                }
                Err(e) => {
                    tracing::warn!(peer = %spec, "peer connection failed: {e}");
                }
            }
        }

        if !creds.peers.is_empty() && connected == 0 {
            return Err(CliError::Generic(format!(
                "no peers reachable (configured: {})",
                creds.peers.len()
            )));
        }

        // Kick off initial sync per connected peer. `start_sync_connection`
        // wired the transport above; this asks each peer for the documents
        // they have so the CLI's local store gets populated. Mirrors
        // `peat-node` src/node.rs::start_sync. Errors are logged but don't
        // fail the join — partial sync is still useful to subsequent
        // commands.
        for peer_id in backend.transport().connected_peers() {
            if let Err(e) = backend
                .coordinator()
                .sync_all_documents_with_peer(peer_id)
                .await
            {
                tracing::warn!(peer = %peer_id, "initial sync_all_documents failed: {e}");
            }
        }

        // Spawn the same on-change pusher peat-node runs: when the local
        // store accepts a write (via `create` / `update` / `delete`), push
        // it to every currently-connected peer. Without this the CLI's
        // writes stay local — `--wait-for-sync` would only block on a
        // local timer, with nothing actually flowing across the wire.
        // Task is owned by the MeshSession's tokio runtime; it terminates
        // when the broadcast channel closes (i.e. when the backend is
        // dropped on session drop).
        Self::spawn_on_change_pusher(&backend);

        // Brief settle window: let the peer's reciprocal sync drain in AND
        // give mDNS time to collect announcements from other peat processes
        // on the same host (loopback round-trip ≪ 10ms; full window = 1s).
        tokio::time::sleep(POST_JOIN_SETTLE).await;

        // ── Connect to mDNS-discovered peers ─────────────────────────────
        // By now mDNS has had the full settle window to receive peer
        // announcements. Connect to any we found that aren't already wired.
        if let Some(ref m) = mdns {
            let our_id = backend.transport().endpoint().id().to_string();
            let already: std::collections::HashSet<String> = backend
                .transport()
                .connected_peers()
                .iter()
                .map(|id| id.to_string())
                .collect();

            let discovered = m.discovered_peers().await;
            tracing::debug!("mDNS: {} peer(s) discovered after settle", discovered.len());
            for peer in discovered {
                if peer.node_id == our_id || already.contains(&peer.node_id) {
                    continue;
                }
                // Skip peers from a different formation. Old peat versions
                // that don't embed formation_id are still attempted.
                if let Some(fid) = peer.metadata.get("formation_id") {
                    if fid != &creds.app_id {
                        continue;
                    }
                }
                'addr: for addr in Self::peer_addresses(&peer) {
                    let spec = format!("{}@{}", peer.node_id, addr);
                    match connect_peer(&backend, &spec, opts.timeout).await {
                        Ok(()) => {
                            tracing::info!(
                                peer = %peer.node_id, %addr,
                                "mDNS: connected to peer"
                            );
                            // Immediate sync so this peer's existing docs
                            // are available to the current command.
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
                            break 'addr;
                        }
                        Err(e) => {
                            tracing::warn!(
                                peer = %peer.node_id, %addr,
                                "mDNS: connect failed: {e}"
                            );
                        }
                    }
                }
            }
        }

        // ── Background mDNS watcher ───────────────────────────────────────
        // Connects to peers that announce AFTER the initial settle window —
        // e.g., a `peat create` that starts after `peat observe` is already
        // running. The task terminates when the receiver closes (mdns dropped).
        if let Some(rx) = mdns_rx {
            let backend_arc = Arc::clone(&backend);
            let timeout = opts.timeout;
            let formation_id = creds.app_id.clone();
            tokio::spawn(async move {
                Self::mdns_watcher(backend_arc, rx, timeout, formation_id).await;
            });
        }

        Ok(Self {
            backend,
            node_id,
            _data_dir: data_dir,
            _mdns: mdns,
        })
    }

    /// Return the effective address list for an mDNS peer.
    ///
    /// Cross-process mDNS queries sometimes produce a `ServiceResolved` with
    /// empty `addresses` because the A record arrives in a separate packet
    /// after the SRV record. When that happens, fall back to constructing
    /// `127.0.0.1:{port}` from the `port` TXT metadata we embed at
    /// advertisement time — reliable for same-host peers.
    fn peer_addresses(peer: &PeerInfo) -> Vec<std::net::SocketAddr> {
        if !peer.addresses.is_empty() {
            return peer.addresses.clone();
        }
        // A record wasn't in the initial response. Use the embedded port.
        if let Some(port_str) = peer.metadata.get("port") {
            if let Ok(port) = port_str.parse::<u16>() {
                tracing::warn!(
                    peer = %peer.node_id,
                    "mDNS: addresses empty, using metadata port fallback 127.0.0.1:{port}"
                );
                return vec![std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    port,
                )];
            }
        }
        tracing::debug!(peer = %peer.node_id, "mDNS: no usable address for peer");
        vec![]
    }

    /// Background task: connect to mDNS peers as they announce themselves
    /// after the initial settle window.
    async fn mdns_watcher(
        backend: Arc<AutomergeBackend>,
        mut rx: tokio::sync::mpsc::Receiver<DiscoveryEvent>,
        timeout: Duration,
        formation_id: String,
    ) {
        let our_id = backend.transport().endpoint().id().to_string();
        // Peers that failed with a permanent error (wrong formation key).
        // Formation ID mismatch is not transient — skip indefinitely.
        let mut auth_failed: std::collections::HashSet<String> = std::collections::HashSet::new();
        while let Some(event) = rx.recv().await {
            let peer = match event {
                DiscoveryEvent::PeerFound(p) | DiscoveryEvent::PeerUpdated(p) => p,
                DiscoveryEvent::PeerLost(_) => continue,
            };
            if peer.node_id == our_id || auth_failed.contains(&peer.node_id) {
                continue;
            }
            // Skip peers advertising a different formation_id without
            // attempting a connection. Each test run creates a new endpoint
            // ID, so auth_failed alone can't suppress the deluge.
            if let Some(fid) = peer.metadata.get("formation_id") {
                if fid != &formation_id {
                    continue;
                }
            }
            let already_connected = backend
                .transport()
                .connected_peers()
                .iter()
                .any(|id| id.to_string() == peer.node_id);
            if already_connected {
                continue;
            }
            for addr in Self::peer_addresses(&peer) {
                let spec = format!("{}@{}", peer.node_id, addr);
                match connect_peer(&backend, &spec, timeout).await {
                    Ok(()) => {
                        tracing::debug!(peer = %peer.node_id, %addr, "mDNS watcher: connected to peer");
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
                            // Permanent: different formation key — never retry.
                            auth_failed.insert(peer.node_id.clone());
                        }
                        tracing::debug!(peer = %peer.node_id, %addr, "mDNS watcher connect failed: {e}");
                    }
                }
            }
        }
    }

    fn spawn_on_change_pusher(backend: &Arc<AutomergeBackend>) {
        let mut rx = backend.store().subscribe_to_changes();
        let coord = Arc::clone(backend.coordinator());
        tokio::spawn(async move {
            while let Ok(key) = rx.recv().await {
                if let Err(e) = coord.sync_document_with_all_peers(&key).await {
                    tracing::warn!(key = %key, "on_change_pusher: sync failed: {e}");
                }
            }
        });
    }

    pub fn backend(&self) -> &Arc<AutomergeBackend> {
        &self.backend
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

/// Create `path` (and any parents) with restricted permissions.
///
/// On Unix the leaf directory is created with mode `0700` so the local
/// Automerge store is not world-readable on shared hosts. On other platforms
/// `create_dir_all` is used without further restriction.
fn create_private_dir(path: &Path) -> Result<(), CliError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        // Create parents with default permissions, then the leaf with 0700.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                CliError::Generic(format!("could not create data_dir {}: {e}", path.display()))
            })?;
        }
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .map_err(|e| {
                CliError::Generic(format!("could not create data_dir {}: {e}", path.display()))
            })?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(path).map_err(|e| {
        CliError::Generic(format!("could not create data_dir {}: {e}", path.display()))
    })?;
    Ok(())
}

/// Split an `endpoint_id@host:port` spec into its two parts and parse the
/// endpoint id. Factored out for unit testing so we don't need a live
/// backend to exercise the parsing.
fn parse_peer_spec(spec: &str) -> Result<(iroh::EndpointId, &str), CliError> {
    let (peer_id_str, addr_str) = spec.split_once('@').ok_or_else(|| {
        CliError::Malformed(format!(
            "peer spec `{spec}`: expected `endpoint_id@host:port` form"
        ))
    })?;
    let peer_id: iroh::EndpointId = peer_id_str
        .parse()
        .map_err(|e| CliError::Malformed(format!("peer id `{peer_id_str}`: {e}")))?;
    Ok((peer_id, addr_str))
}

/// Dial a single peer with formation-key authentication and wire it into
/// the sync transport. Mirrors `src/node.rs::dial_and_attach` in peat-node,
/// minus the auto-reconnect watchdog (a short-lived CLI doesn't need it).
async fn connect_peer(
    backend: &Arc<AutomergeBackend>,
    spec: &str,
    timeout: Duration,
) -> Result<(), CliError> {
    let (peer_id, addr_str) = parse_peer_spec(spec)?;

    let mut peer_addr = iroh::EndpointAddr::new(peer_id);
    let mut any_added = false;
    let resolved = tokio::net::lookup_host(addr_str).await.map_err(|e| {
        CliError::Generic(format!("resolve `{addr_str}` for peer `{peer_id}`: {e}"))
    })?;
    // Filter to IPv4 only. Docker / k3d / generic bridge networks
    // routinely advertise both A and AAAA records for service-name
    // hostnames, but the IPv6 half is often non-routable across the
    // bridge (no SLAAC, no neighbour discovery into the container).
    // Iroh's `EndpointAddr` accepts all candidates and races them in
    // parallel, but its hold-time on a dead IPv6 candidate eats the
    // whole 30 s QUIC handshake budget before falling back — which
    // is precisely the failure shape we see on PR #114's Quickstart
    // Path A (CLI in peat-node-a → peat-node-b dual-stack resolve
    // → 3 × 30 s retries, all timed out). Restricting to IPv4 here
    // is the same simplification the compose example's bootstrap
    // script implicitly relies on by handing peat-node a single
    // `peat-node-a:51071` hint that resolves IPv4-first under
    // Docker's embedded DNS.
    for socket in resolved {
        if !socket.is_ipv4() {
            tracing::debug!(peer = %peer_id, ?socket, "skipping non-IPv4 candidate");
            continue;
        }
        peer_addr = peer_addr.with_ip_addr(socket);
        any_added = true;
    }
    if !any_added {
        return Err(CliError::Generic(format!(
            "no IPv4 addresses resolved for `{addr_str}` (got only IPv6 candidates?)"
        )));
    }

    // Diagnostic: log the exact resolved IP(s) we're handing iroh.
    // PR #114's last failing run on the post-#205 fix showed iroh
    // receiving `ip_addresses=[172.18.0.2:51071]` but peat-node-b's
    // sidecar never seeing any inbound — open question is whether
    // tokio's resolver in peat-node-a's container resolved
    // `peat-node-b` to peat-node-b's actual IP, or to a sibling /
    // local-container IP. This info-level log makes the resolved
    // socket(s) visible at CI-default logging so we don't need
    // RUST_LOG=debug to read them.
    tracing::info!(
        peer = %peer_id,
        peer_addr_id = %peer_addr.id,
        peer_addr_addrs = ?peer_addr.addrs,
        peer_addr_relay = ?peer_addr.relay_urls().collect::<Vec<_>>(),
        spec = %spec,
        "dialing peer with fully-populated EndpointAddr (peat-mesh#205 path)"
    );

    // Retry loop covers peat#759's HMAC challenge-response race that
    // peat-node's `dial_and_attach` already papers over with the
    // same shape. Each attempt gets the full caller-supplied
    // timeout; the race typically fails fast (~200ms) when it loses,
    // so the overall budget rarely exceeds the per-attempt timeout
    // in practice.
    //
    // We call `connect_and_authenticate_with_addr(peer_addr)` —
    // shipped on peat-mesh's `fix-205-connect-with-addr` branch
    // (peat-mesh#206 / closes peat-mesh#205). Passing the full
    // `EndpointAddr` (with the resolved IPv4 socket) bypasses iroh's
    // `address_lookup` chain entirely: no DNS attempt, no chain-
    // dispatch race, direct UDP dial.
    const CONNECT_RETRY_ATTEMPTS: usize = 3;
    const CONNECT_RETRY_BACKOFF: Duration = Duration::from_millis(200);
    let mut attempt = 0;
    let connection = loop {
        attempt += 1;
        let result = tokio::time::timeout(
            timeout,
            backend
                .transport()
                .connect_and_authenticate_with_addr(peer_addr.clone()),
        )
        .await;
        match result {
            Ok(Ok(c)) => break c,
            Ok(Err(e)) if attempt < CONNECT_RETRY_ATTEMPTS => {
                tracing::warn!(
                    peer = %peer_id,
                    attempt,
                    max_attempts = CONNECT_RETRY_ATTEMPTS,
                    "connect_and_authenticate_with_addr failed, retrying: {e}"
                );
                tokio::time::sleep(CONNECT_RETRY_BACKOFF).await;
            }
            Ok(Err(e)) => {
                return Err(CliError::Generic(format!(
                    "connect/auth to `{peer_id}` failed after {attempt} attempts: {e}"
                )));
            }
            Err(_) if attempt < CONNECT_RETRY_ATTEMPTS => {
                tracing::warn!(
                    peer = %peer_id,
                    attempt,
                    max_attempts = CONNECT_RETRY_ATTEMPTS,
                    "connect to `{peer_id}` timed out after {timeout:?}, retrying"
                );
                tokio::time::sleep(CONNECT_RETRY_BACKOFF).await;
            }
            Err(_) => {
                return Err(CliError::Generic(format!(
                    "connect to `{peer_id}` timed out after {timeout:?} (attempt {attempt})"
                )));
            }
        }
    };

    backend
        .transport()
        .start_sync_connection(connection, Arc::clone(backend.coordinator()));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_spec_without_at_sign() {
        let err = parse_peer_spec("not-an-endpoint-spec").unwrap_err();
        assert_eq!(err.exit_code(), 4); // Malformed
        assert!(err.to_string().contains("endpoint_id@host:port"));
    }

    #[test]
    fn rejects_unparseable_endpoint_id() {
        let err = parse_peer_spec("notanid@10.0.0.5:4242").unwrap_err();
        assert_eq!(err.exit_code(), 4);
        assert!(err.to_string().contains("peer id"));
    }
}
