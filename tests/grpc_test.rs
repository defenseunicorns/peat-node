//! End-to-end functional test: boots a full gRPC server and exercises
//! the API through a tonic client, validating both plaintext and encrypted modes.

use std::time::Duration;
use tonic::transport::Channel;

use peat_sidecar::node::{SidecarConfig, SidecarNode};
use peat_sidecar::proto::peat_sidecar_client::PeatSidecarClient;
use peat_sidecar::proto::peat_sidecar_server::PeatSidecarServer;
use peat_sidecar::proto::{
    DeleteDocumentRequest, GetDocumentRequest, GetStatusRequest, GetSyncStatsRequest,
    ListDocumentsRequest, PutDocumentRequest,
};
use peat_sidecar::service::PeatSidecarService;

async fn boot_grpc_server(
    port: u16,
    encryption_key: Option<String>,
) -> PeatSidecarClient<Channel> {
    let dir = tempfile::tempdir().unwrap();
    let node = std::sync::Arc::new(
        SidecarNode::new(SidecarConfig {
            node_id: format!("grpc-test-{port}"),
            app_id: "test".to_string(),
            shared_key: String::new(),
            data_dir: dir.keep(),
            peers: vec![],
            encryption_key,
        })
        .await
        .unwrap(),
    );

    let service = PeatSidecarService::new(node);
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PeatSidecarServer::new(service))
            .serve(addr)
            .await
            .unwrap();
    });

    // Wait for server to be ready
    tokio::time::sleep(Duration::from_millis(500)).await;

    let channel = Channel::from_shared(format!("http://127.0.0.1:{port}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    PeatSidecarClient::new(channel)
}

#[tokio::test]
async fn grpc_full_crud_plaintext() {
    let mut client = boot_grpc_server(50071, None).await;

    // Status
    let status = client
        .get_status(GetStatusRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(status.node_id, "grpc-test-50071");
    assert!(!status.endpoint_addr.is_empty());

    // Put
    client
        .put_document(PutDocumentRequest {
            collection: "test".into(),
            doc_id: "doc-1".into(),
            json_data: r#"{"hello":"world"}"#.into(),
        })
        .await
        .unwrap();

    // Get
    let doc = client
        .get_document(GetDocumentRequest {
            collection: "test".into(),
            doc_id: "doc-1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(doc.json_data.as_deref(), Some(r#"{"hello":"world"}"#));

    // List
    let list = client
        .list_documents(ListDocumentsRequest {
            collection: "test".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.doc_ids, vec!["doc-1"]);

    // Delete
    client
        .delete_document(DeleteDocumentRequest {
            collection: "test".into(),
            doc_id: "doc-1".into(),
        })
        .await
        .unwrap();

    let doc = client
        .get_document(GetDocumentRequest {
            collection: "test".into(),
            doc_id: "doc-1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(doc.json_data, None);

    // Sync stats
    let stats = client
        .get_sync_stats(GetSyncStatsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.connected_peers, 0);
}

#[tokio::test]
async fn grpc_full_crud_encrypted() {
    use base64::Engine;
    let key = base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]);
    let mut client = boot_grpc_server(50072, Some(key)).await;

    // Put encrypted
    client
        .put_document(PutDocumentRequest {
            collection: "secure".into(),
            doc_id: "secret-1".into(),
            json_data: r#"{"classified":"top-secret"}"#.into(),
        })
        .await
        .unwrap();

    // Get decrypts transparently
    let doc = client
        .get_document(GetDocumentRequest {
            collection: "secure".into(),
            doc_id: "secret-1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        doc.json_data.as_deref(),
        Some(r#"{"classified":"top-secret"}"#)
    );

    // Overwrite
    client
        .put_document(PutDocumentRequest {
            collection: "secure".into(),
            doc_id: "secret-1".into(),
            json_data: r#"{"classified":"updated"}"#.into(),
        })
        .await
        .unwrap();

    let doc = client
        .get_document(GetDocumentRequest {
            collection: "secure".into(),
            doc_id: "secret-1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        doc.json_data.as_deref(),
        Some(r#"{"classified":"updated"}"#)
    );

    // List still works
    let list = client
        .list_documents(ListDocumentsRequest {
            collection: "secure".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list.doc_ids, vec!["secret-1"]);

    // Delete
    client
        .delete_document(DeleteDocumentRequest {
            collection: "secure".into(),
            doc_id: "secret-1".into(),
        })
        .await
        .unwrap();

    let doc = client
        .get_document(GetDocumentRequest {
            collection: "secure".into(),
            doc_id: "secret-1".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(doc.json_data, None);
}
