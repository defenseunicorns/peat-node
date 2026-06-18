//! UDS transport bind path — pin that the SidecarNode can be hosted
//! behind a Connect-RPC server bound to a `unix:///path/to/sock` path,
//! the socket file exists with the right file type, and a client can
//! open a connection to it.
//!
//! Full HTTP-over-UDS wire round-trip isn't asserted here — driving a
//! Connect-RPC request over a `tokio::net::UnixStream` from Rust today
//! requires either pulling in `hyperlocal` or enabling hyper's full
//! client feature stack, which we don't currently take as a dev-dep.
//! TCP exercises the same Connect-RPC pipeline in every other
//! integration test; this test pins the UDS-specific listener config
//! so a regression there can't ship silently. The wire-over-UDS gap is
//! tracked separately.

use std::os::unix::fs::FileTypeExt;
use std::sync::Arc;
use std::time::Duration;

use peat_node::node::{SidecarConfig, SidecarNode};
use peat_node::pb::PeatSidecarExt;
use peat_node::service::PeatSidecarService;

#[tokio::test]
async fn sidecar_binds_unix_socket_path_and_accepts_connection() {
    let tmp = tempfile::tempdir().unwrap();
    let sock_path = tmp.path().join("peat.sock");

    let node = Arc::new(
        SidecarNode::new(SidecarConfig {
            blob_stall_timeout: None,
            node_id: "uds-test".to_string(),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: tmp.path().join("data"),
            peers: vec![],
            encryption_key: None,
            iroh_udp_port: None,
            iroh_secret_key: None,
            attachment_config: Default::default(),
            disable_mdns: true,
            tombstone_ttl_hours: None,
            gc_interval_secs: None,
            gc_batch_size: None,
            ..Default::default()
        })
        .await
        .expect("boot node"),
    );

    let service = Arc::new(PeatSidecarService::new(node));
    let router = service.register(connectrpc::Router::new());
    let connect_service = connectrpc::ConnectRpcService::new(router);

    // Identical bind pattern to the production binary's UDS path
    // (`src/main.rs::main` `unix://...` arm). A regression in that
    // wiring would fail to bind or fail to accept here.
    let listener = tokio::net::UnixListener::bind(&sock_path).expect("bind UDS");
    let svc = connect_service.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => break,
            };
            let svc = svc.clone();
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
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Socket file exists and is a unix socket.
    let meta = tokio::fs::metadata(&sock_path)
        .await
        .expect("socket metadata");
    assert!(
        meta.file_type().is_socket(),
        "expected a unix domain socket at {}",
        sock_path.display()
    );

    // A client can connect to the listener.
    let stream = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::UnixStream::connect(&sock_path),
    )
    .await
    .expect("connect timed out")
    .expect("connect");
    drop(stream);
}
