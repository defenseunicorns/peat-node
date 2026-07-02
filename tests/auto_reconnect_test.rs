//! Reproducing test for **peat-node#91** — auto-reconnect after blackout.
//!
//! ## What pre-fix behavior this captures
//!
//! Before the watchdog landed, `SidecarNode::connect_peer` populated the
//! `MeshSyncTransport` connection table but had no mechanism to
//! re-establish a connection if iroh's QUIC idle timeout (~30 s default)
//! fired during a network blackout. The sidecar would silently lose all
//! peers and never re-dial them, even after the underlying network
//! recovered. Operators had to issue a fresh `ConnectPeer` RPC per peer
//! to recover, which is what the `RECONNECT_ON_RESTORE` workaround in
//! peat-sim emulated.
//!
//! ## What this test exercises
//!
//! 1. Boot two `SidecarNode`s on OS-assigned UDP ports, connect B → A.
//! 2. Force-close the underlying QUIC connection from B's side using
//!    `simulate_idle_timeout_for_test` — this bypasses `disconnect_peer`,
//!    so the auto-reconnect registry retains its entry for A. This is the
//!    moral equivalent of iroh's idle timeout firing.
//! 3. Confirm the connection vanished from `connected_peers()`.
//! 4. Wait long enough for the watchdog to tick
//!    (`RECONNECT_WATCHDOG_INTERVAL` is 5 s in production, plus dial RTT).
//! 5. Assert the connection re-appeared without any further operator
//!    action — i.e. the watchdog re-dialed using the stored
//!    `PeerRegistration`.
//!
//! Pre-fix this test fails at step 5 because no reconnect logic exists;
//! post-fix it passes within ~6–10 s of step 2.

use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use tempfile::TempDir;

/// A booted node bundled with the `TempDir` that owns its data directory.
/// Holding the `TempDir` here means the directory is RAII-cleaned when the
/// test scope ends — `dir.keep()` (the previous shape) would have leaked
/// one tempdir per `boot` call per test run.
struct BootedNode {
    node: Arc<SidecarNode>,
    _dir: TempDir,
}

/// Boot a `SidecarNode` on an OS-assigned UDP port (`iroh_udp_port =
/// Some(0)`). Using port 0 avoids fixed-port collisions when `cargo test`
/// runs binaries in parallel or when another local process happens to
/// hold the port — flagged as a flake source in peat-node#99's QA review.
async fn boot(node_id: &str) -> BootedNode {
    let dir = TempDir::new().unwrap();
    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: node_id.to_string(),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.path().to_path_buf(),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: Some(0),
            iroh_secret_key: None,
            attachment_config: Default::default(),
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
            ..Default::default()
        })
        .await
        .unwrap(),
    );
    BootedNode { node, _dir: dir }
}

#[tokio::test(flavor = "multi_thread")]
async fn watchdog_redials_peer_after_simulated_idle_timeout_peat_node_91() {
    let node_a = boot("auto-reconnect-A").await;
    let node_b = boot("auto-reconnect-B").await;

    let endpoint_a = node_a.node.endpoint_addr();
    let port_a = node_a
        .node
        .bound_udp_port()
        .expect("node A must report a bound UDP port");

    node_b
        .node
        .connect_peer(&endpoint_a, &[format!("127.0.0.1:{port_a}")], "")
        .await
        .expect("initial connect_peer must succeed");

    // Give iroh a brief moment to fully settle the connection.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        node_b.node.connected_peer_count(),
        1,
        "B should see exactly 1 peer (A) after initial connect",
    );

    // Simulate iroh's idle-timeout: the underlying QUIC connection
    // drops, but the auto-reconnect registry retains its entry for A
    // (because the operator did NOT call disconnect_peer).
    node_b
        .node
        .simulate_idle_timeout_for_test(&endpoint_a)
        .await
        .expect("simulated idle timeout must succeed");
    assert_eq!(
        node_b.node.connected_peer_count(),
        0,
        "B should see 0 peers immediately after simulated idle timeout",
    );

    // The watchdog ticks every 5 s; wait long enough for at least one
    // tick + a dial RTT. Poll instead of sleeping a fixed window so the
    // test exits as soon as recovery is observable.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut recovered = false;
    while tokio::time::Instant::now() < deadline {
        if node_b.node.connected_peer_count() >= 1 {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        recovered,
        "B should auto-reconnect to A within 15 s of simulated idle timeout \
         (peat-node#91); connected_peer_count={}",
        node_b.node.connected_peer_count()
    );
}
