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

use peat_mesh::storage::{ChangeOrigin, SyncTransport};
use peat_mesh::sync::{AutomergeBackend, AutomergeBackendConfig};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command as TokioCommand};
use tokio::time::timeout;

pub const TEST_APP_ID_CONST: &str = "peat-cli-e2e";
const TEST_APP_ID: &str = TEST_APP_ID_CONST;

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
    // Background tasks spawned by start(). Aborted on drop so they don't
    // accumulate across test runs and starve the tokio runtime.
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for TestPeer {
    fn drop(&mut self) {
        // Fallback: abort tasks when the peer is dropped without calling stop().
        // Tests should call stop().await instead for deterministic cleanup.
        self.abort_tasks();
    }
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
        //   1. transitive-gossip push: when a doc changes locally OR
        //      lands here via sync from another peer, push the change
        //      to every connected peer EXCEPT the source. The
        //      origin-tagged channel (peat#891/#907) carries the
        //      source attribution so the relay never echoes a remote
        //      change back to its sender.
        //   2. on-connect catch-up: when a new peer appears in
        //      `connected_peers`, push the peer's full doc set so the
        //      newcomer's first read sees state.
        //
        // Without (1)'s remote-origin half a CLI subprocess authoring a
        // write would reach the test peer but never propagate to a
        // second CLI subprocess subscribed via `peat observe`. Without
        // (2) a CLI that joins AFTER docs are seeded sees an empty
        // store.
        let tasks = vec![
            Self::spawn_transitive_gossip_pusher(&backend),
            Self::spawn_on_connect_catchup(&backend),
        ];

        Self {
            backend,
            endpoint_id,
            udp_port,
            formation_key_b64,
            app_id,
            _data_dir: dir,
            _tasks: tasks,
        }
    }

    fn spawn_transitive_gossip_pusher(
        backend: &Arc<AutomergeBackend>,
    ) -> tokio::task::JoinHandle<()> {
        let mut rx = backend.store().subscribe_to_changes_with_origin();
        let coord = Arc::clone(backend.coordinator());
        let backend = Arc::clone(backend);
        tokio::spawn(async move {
            while let Ok(change) = rx.recv().await {
                // peat-mesh rc.29 also fires this channel on tombstone-
                // driven deletes (peat-mesh#202). Distinguish insert/
                // update from delete by reading the store — a `None`
                // return means the key is gone, so the relay needs to
                // use the tombstone push channel rather than the
                // Automerge document sync channel.
                let is_tombstone = matches!(backend.store().get(&change.key), Ok(None));

                match change.origin {
                    ChangeOrigin::Local => {
                        if is_tombstone {
                            // Push tombstones to every connected peer.
                            // The tombstone channel is global-per-peer
                            // (not per-key), so one call covers any
                            // recent tombstones in flight.
                            for peer in backend.transport().connected_peers() {
                                let _ = coord.send_tombstones_to_peer(peer).await;
                            }
                        } else {
                            // Local doc write: relay to every peer
                            // (per-peer sync state makes redundant
                            // pushes a no-op).
                            let _ = coord.sync_document_with_all_peers(&change.key).await;
                        }
                    }
                    ChangeOrigin::Remote(source) => {
                        // Sync-received write: relay to every peer
                        // except the source. The `ChangeOrigin` contract
                        // (peat-mesh `automerge_store.rs` doc comment)
                        // pins the stringification to
                        // `EndpointId::to_string()` on the Iroh
                        // transport, so direct string equality is the
                        // sanctioned suppression test.
                        for peer in backend.transport().connected_peers() {
                            if peer.to_string() == source {
                                continue;
                            }
                            if is_tombstone {
                                let _ = coord.send_tombstones_to_peer(peer).await;
                            } else {
                                let _ = coord.sync_document_with_peer(&change.key, peer).await;
                            }
                        }
                    }
                }
            }
        })
    }

    fn spawn_on_connect_catchup(backend: &Arc<AutomergeBackend>) -> tokio::task::JoinHandle<()> {
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
        })
    }

    /// Shut down the peer cleanly: abort background tasks, await their
    /// cancellation, then drop the backend.
    ///
    /// Must be called at the end of every serial e2e test. Without this,
    /// aborting the tasks only SCHEDULES cancellation — the tasks still hold
    /// `Arc<AutomergeBackend>` until they actually complete. If they haven't
    /// completed by the time the next test starts, the backend stays alive and
    /// peat-mesh's internal sync_task (which exits when the backend drops)
    /// keeps running. After 30+ serial tests, dozens of leaked sync_tasks
    /// starve the tokio runtime and cause sync to time out.
    pub async fn stop(mut self) {
        let tasks = std::mem::take(&mut self._tasks);
        for task in tasks {
            task.abort();
            let _ = task.await; // JoinError on abort is expected and fine
        }
        // backend field drops here → Arc count → 0 → backend drops →
        // store channel closes → internal sync_task exits naturally.
    }

    /// Signal all background tasks to cancel (synchronous, fire-and-forget).
    /// Prefer `stop()` at the end of each test for deterministic cleanup.
    pub fn abort_tasks(&self) {
        for task in &self._tasks {
            task.abort();
        }
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

/// Spawn a long-lived `peat` subprocess with piped stdout/stderr. The
/// caller drives the returned `Child` (typically by reading stdout via
/// [`await_stdout_contains`]); dropping it sends SIGKILL via
/// `kill_on_drop`, so test failures cannot leak observer processes.
///
/// This is the second-binary half of the multi-process topology: paired
/// with [`run_peat`] (the foreground subprocess via `assert_cmd`), tests
/// can drive scenarios where two real `peat` binary instances run
/// concurrently against the same [`TestPeer`] rendezvous and exchange
/// data over real Iroh QUIC (no in-process shortcut).
pub fn spawn_peat_streaming(creds: &Path, args: &[&str]) -> Child {
    let peat_path = assert_cmd::cargo::cargo_bin("peat");
    let mut owned: Vec<String> = vec![
        "--creds".into(),
        creds.to_string_lossy().into_owned(),
        "--timeout".into(),
        "15s".into(),
    ];
    owned.extend(args.iter().map(|s| (*s).to_string()));

    TokioCommand::new(peat_path)
        .env("RUST_LOG", "peat_cli=warn")
        .args(owned)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn peat (streaming)")
}

/// Non-panicking variant: reads until `needle` is seen, returning `Ok(seen)`,
/// or returns `Err(seen)` after the caller's timeout fires. Designed for use
/// with `tokio::time::timeout` so the caller can do post-mortem diagnosis.
pub async fn await_stdout_contains_no_panic(child: &mut Child, needle: &str) -> String {
    let stdout = child.stdout.take().expect("piped stdout");
    let mut reader = BufReader::new(stdout).lines();
    let mut seen = String::new();
    loop {
        match reader.next_line().await {
            Ok(Some(line)) => {
                seen.push_str(&line);
                seen.push('\n');
                if line.contains(needle) {
                    return seen;
                }
            }
            Ok(None) | Err(_) => {
                // EOF or error — won't see needle; caller's timeout handles it.
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Read lines from `child.stdout` until one contains `needle` or
/// `deadline` elapses. Panics with the accumulated stdout on miss so the
/// test report shows what the observer actually emitted.
///
/// Takes ownership of the child's stdout handle (consumes
/// `child.stdout`) — call once per child.
pub async fn await_stdout_contains(child: &mut Child, needle: &str, deadline: Duration) -> String {
    let stdout = child.stdout.take().expect("piped stdout");
    let mut reader = BufReader::new(stdout).lines();
    let start = Instant::now();
    let mut seen = String::new();
    loop {
        let remaining = deadline.checked_sub(start.elapsed()).unwrap_or_default();
        if remaining.is_zero() {
            panic!("did not see `{needle}` on subprocess stdout within {deadline:?}\nseen so far:\n{seen}");
        }
        match timeout(remaining, reader.next_line()).await {
            Ok(Ok(Some(line))) => {
                seen.push_str(&line);
                seen.push('\n');
                if line.contains(needle) {
                    return seen;
                }
            }
            Ok(Ok(None)) => {
                panic!("subprocess stdout closed before seeing `{needle}`\nseen:\n{seen}")
            }
            Ok(Err(e)) => panic!("read subprocess stdout: {e}"),
            Err(_) => panic!("did not see `{needle}` within {deadline:?}\nseen so far:\n{seen}"),
        }
    }
}
