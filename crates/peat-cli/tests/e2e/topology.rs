//! Test topology helper for peat-cli end-to-end scenarios (ADR-001 Phase 5).
//!
//! Spins up a real `AutomergeBackend` in-process to act as the "remote peer"
//! the spawned `peat` binary connects to via formation-key authentication
//! over loopback Iroh. Each TestPeer owns its own tempdir + ephemeral
//! formation key; tests that need isolation just construct independent
//! peers.
//!
//! Behavior coverage: the harness exercises the full join handshake +
//! sync transport — not a mock. The same code paths peat-node uses in
//! production move data between the in-process backend and the
//! subprocess-spawned CLI.

#![allow(dead_code)] // Each scenario uses a subset of these helpers.

use peat_mesh::storage::SyncTransport;
use peat_mesh::sync::{AutomergeBackend, AutomergeBackendConfig};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

const TEST_APP_ID: &str = "peat-cli-e2e";

/// 32-byte zero key, base64-encoded. Real deployments rotate a securely
/// generated key; e2e tests just need a valid-shape constant so every
/// TestPeer in a run shares the same formation.
pub fn test_formation_key() -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode([0u8; 32])
}

/// A live AutomergeBackend bound to a loopback Iroh endpoint, ready to
/// accept incoming peer connections from a spawned `peat` subprocess.
pub struct TestPeer {
    pub backend: Arc<AutomergeBackend>,
    pub endpoint_id: iroh::EndpointId,
    pub udp_port: u16,
    pub formation_key_b64: String,
    pub app_id: String,
    _data_dir: TempDir,
}

impl TestPeer {
    /// Boot a peer on a kernel-assigned ephemeral UDP port. `Iroh` picks
    /// the port; we read it back via the bound endpoint.
    pub async fn start() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let formation_key_b64 = test_formation_key();
        let app_id = TEST_APP_ID.to_string();

        let backend = AutomergeBackend::with_iroh(AutomergeBackendConfig {
            data_dir: dir.path().to_path_buf(),
            formation_id: app_id.clone(),
            base64_shared_key: formation_key_b64.clone(),
            // None → kernel-assigned ephemeral port on a loopback Iroh socket.
            iroh_bind_addr: None,
            cipher: None,
        })
        .await
        .expect("AutomergeBackend bootstrap");

        let endpoint_id = backend.blob_store().endpoint_id();
        let udp_port = backend
            .blob_store()
            .endpoint()
            .bound_sockets()
            .into_iter()
            .next()
            .map(|s| s.port())
            .expect("at least one bound UDP socket");

        // Production peat-node spawns two background tasks that the test
        // peer needs to mirror so a freshly-connected CLI subprocess sees
        // sync flow:
        //
        //   1. on-change push: when a local `store.put` fires the observer,
        //      push the change to every currently-connected peer.
        //   2. on-connect catch-up: when a new peer appears in
        //      `connected_peers`, push the peer's full doc set so the
        //      newcomer's first read sees state.
        //
        // Without (2) a CLI that joins AFTER docs are seeded sees an
        // empty store. peat-mesh#235's `sync_all_documents_with_peer`
        // is the API; production peat-node binds it to start_sync().
        Self::spawn_on_change_pusher(&backend);
        Self::spawn_on_connect_catchup(&backend);

        Self {
            backend,
            endpoint_id,
            udp_port,
            formation_key_b64,
            app_id,
            _data_dir: dir,
        }
    }

    fn spawn_on_change_pusher(backend: &Arc<AutomergeBackend>) {
        let mut rx = backend.store().subscribe_to_changes();
        let coord = Arc::clone(backend.coordinator());
        tokio::spawn(async move {
            while let Ok(key) = rx.recv().await {
                let _ = coord.sync_document_with_all_peers(&key).await;
            }
        });
    }

    fn spawn_on_connect_catchup(backend: &Arc<AutomergeBackend>) {
        let backend = Arc::clone(backend);
        tokio::spawn(async move {
            let mut seen: HashSet<iroh::EndpointId> = HashSet::new();
            loop {
                let current: HashSet<iroh::EndpointId> =
                    backend.transport().connected_peers().into_iter().collect();
                for new_peer in current.difference(&seen) {
                    let _ = backend
                        .coordinator()
                        .sync_all_documents_with_peer(*new_peer)
                        .await;
                }
                seen = current;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }

    /// Write a credentials YAML that points the spawned CLI back at this
    /// peer's endpoint over the loopback interface.
    pub fn write_creds(&self, path: &Path) -> std::io::Result<()> {
        let yaml = format!(
            "app_id: {app_id}\n\
             shared_key: {key}\n\
             peers:\n  - {peer_id}@127.0.0.1:{port}\n",
            app_id = self.app_id,
            key = self.formation_key_b64,
            peer_id = self.endpoint_id,
            port = self.udp_port,
        );
        std::fs::write(path, yaml)
    }

    /// Convenience: write a creds.yaml into a fresh tempdir and return the
    /// path. Caller owns the tempdir for the duration of the test.
    pub fn creds_tempfile(&self, dir: &TempDir) -> PathBuf {
        let path = dir.path().join("creds.yaml");
        self.write_creds(&path).expect("write creds.yaml");
        path
    }
}
