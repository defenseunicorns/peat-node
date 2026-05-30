//! Shared join / auth / sync prelude per peat-node ADR-001.
//!
//! Bootstraps the same `peat_mesh::sync::AutomergeBackend` that `peat-node`
//! itself uses, then dials each peer specified in the credentials bundle
//! with formation-key authentication. The backend hangs off an
//! `Arc<AutomergeBackend>` so subcommands can pass the session around
//! cheaply.
//!
//! Lifecycle: `MeshSession` owns an ephemeral `TempDir` that backs the
//! Automerge store on disk. Dropping the session drops the tempdir, which
//! cleans up after the CLI invocation. The CLI is a short-lived
//! "observer" node (ADR-001 §"Node posture per command"); no persistent
//! state survives.

use std::sync::Arc;
use std::time::Duration;

use peat_mesh::storage::SyncTransport;
use peat_mesh::sync::{AutomergeBackend, AutomergeBackendConfig};
use tempfile::TempDir;

use crate::cli::CliError;
use crate::creds::PeatCredentials;

/// Options derived from the parsed `CommonArgs` that affect how the join
/// prelude runs.
#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// Per-peer connect timeout (also the budget the caller gets back when
    /// no peer answers).
    pub timeout: Duration,
    /// Caller-supplied identity. `None` → ephemeral UUID.
    pub as_id: Option<String>,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            as_id: None,
        }
    }
}

/// Settle window after the join + per-peer sync_all kick to let the peer's
/// reciprocal catch-up sync drain into the CLI's local store. Loopback and
/// LAN handshakes typically finish well under this; very slow links may
/// need more (and individual read commands poll on top — see query.rs).
const POST_JOIN_SETTLE: Duration = Duration::from_millis(1000);

/// A live mesh participant. Holds the backend plus the tempdir its store
/// lives in so cleanup is RAII.
pub struct MeshSession {
    backend: Arc<AutomergeBackend>,
    node_id: String,
    // RAII: dropping this removes the on-disk store.
    _data_dir: TempDir,
}

impl MeshSession {
    /// Bootstrap the backend, then dial each peer in the credentials bundle.
    ///
    /// Returns `Ok(MeshSession)` if at least one configured peer connected
    /// (or if no peers were configured — useful for read-from-self in tests).
    /// Returns `Err` if peers were configured but none could be reached.
    pub async fn open(creds: PeatCredentials, opts: SessionOptions) -> Result<Self, CliError> {
        let node_id = opts
            .as_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let data_dir = tempfile::tempdir()
            .map_err(|e| CliError::Generic(format!("could not create ephemeral data dir: {e}")))?;

        tracing::debug!(
            node_id = %node_id,
            data_dir = %data_dir.path().display(),
            "bootstrapping peat-cli backend"
        );

        let backend = AutomergeBackend::with_iroh(AutomergeBackendConfig {
            data_dir: data_dir.path().to_path_buf(),
            formation_id: creds.app_id.clone(),
            base64_shared_key: creds.shared_key.clone(),
            // CLI is a transient client; let Iroh pick an ephemeral UDP port.
            iroh_bind_addr: None,
            // At-rest cipher is handled at the peat-node layer for now
            // (matches the rc.26 comment in peat-node/src/node.rs). The CLI's
            // tempdir-backed store is short-lived enough that omitting it is
            // safe for Phase 2; revisit if persistent state is added.
            cipher: None,
        })
        .await
        .map_err(|e| CliError::Generic(format!("backend bootstrap: {e}")))?;

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

        // Brief settle window for the peer's reciprocal sync to drain into
        // our local store. peat-mesh doesn't surface a "sync caught up"
        // signal; this fixed window is the v1 heuristic — long enough for
        // loopback / LAN, short enough that interactive CLI feels snappy.
        // Subcommands that need stronger guarantees layer additional
        // polling on top (see query.rs).
        tokio::time::sleep(POST_JOIN_SETTLE).await;

        Ok(Self {
            backend,
            node_id,
            _data_dir: data_dir,
        })
    }

    fn spawn_on_change_pusher(backend: &Arc<AutomergeBackend>) {
        let mut rx = backend.store().subscribe_to_changes();
        let coord = Arc::clone(backend.coordinator());
        tokio::spawn(async move {
            while let Ok(key) = rx.recv().await {
                if let Err(e) = coord.sync_document_with_all_peers(&key).await {
                    tracing::warn!(key = %key, "sync_document_with_all_peers failed: {e}");
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
    for socket in resolved {
        peer_addr = peer_addr.with_ip_addr(socket);
        any_added = true;
    }
    if !any_added {
        return Err(CliError::Generic(format!(
            "no addresses resolved for `{addr_str}`"
        )));
    }

    backend
        .blob_store()
        .memory_lookup()
        .add_endpoint_info(peer_addr);

    let connection = tokio::time::timeout(
        timeout,
        backend.transport().connect_and_authenticate(peer_id),
    )
    .await
    .map_err(|_| {
        CliError::Generic(format!(
            "connect to `{peer_id}` timed out after {timeout:?}"
        ))
    })?
    .map_err(|e| CliError::Generic(format!("connect/auth to `{peer_id}`: {e}")))?;

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
