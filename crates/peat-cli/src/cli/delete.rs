//! `peat delete` — tombstone a document (ADR-001 §Write semantics, ADR-034).

use clap::Args;
use peat_mesh::qos::Tombstone;
use peat_mesh::storage::SyncTransport;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cli::query::parse_target;
use crate::cli::writes::POST_WRITE_SYNC_WAIT;
use crate::cli::{parse_timeout, CliError, CommonArgs};
use crate::creds;
use crate::join::{MeshSession, SessionOptions};

#[derive(Debug, Args)]
pub struct DeleteArgs {
    /// Target as `<COLLECTION>/<DOC_ID>`.
    pub target: String,

    /// Block until at least one peer has acknowledged.
    #[arg(long)]
    pub wait_for_sync: bool,
}

pub async fn run(args: DeleteArgs, common: CommonArgs) -> Result<(), CliError> {
    let (collection, doc_id) = parse_target(&args.target)?;
    let doc_id = doc_id.ok_or_else(|| {
        CliError::Malformed(format!(
            "delete requires `<collection>/<doc_id>`; got `{}`",
            args.target
        ))
    })?;

    let creds = creds::load(common.creds.as_deref())?;
    let timeout = parse_timeout(&common.timeout)?;
    let session = MeshSession::open(
        creds,
        SessionOptions {
            timeout,
            as_id: common.as_id.clone(),
        },
    )
    .await?;

    // Lamport: proper distributed Lamport state isn't exposed by peat-mesh
    // at this surface; v1 uses wall-clock nanoseconds since epoch as a
    // monotonically-increasing-per-node proxy. Two CLI invocations on the
    // same node will not collide unless they fire in the same nanosecond.
    let lamport = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);

    let tombstone = Tombstone::new(doc_id, collection, session.node_id(), lamport);
    session
        .backend()
        .store()
        .put_tombstone(&tombstone)
        .map_err(|e| CliError::Generic(format!("put_tombstone: {e}")))?;

    // peat-mesh stores tombstones in a separate table and does NOT fire
    // the document-changes observer, so the join-prelude on-change pusher
    // won't see this write. Explicitly push tombstones to each connected
    // peer; this is what peat-node's delete path will eventually do once
    // the cross-peer tombstone protocol is wired in src/node.rs (today
    // peat-node's delete is local-only too). For the CLI we do it inline.
    for peer_id in session.backend().transport().connected_peers() {
        if let Err(e) = session
            .backend()
            .coordinator()
            .send_tombstones_to_peer(peer_id)
            .await
        {
            tracing::warn!(peer = %peer_id, "send_tombstones_to_peer failed: {e}");
        }
    }

    if args.wait_for_sync {
        tokio::time::sleep(POST_WRITE_SYNC_WAIT).await;
    }

    println!("tombstone:{collection}/{doc_id}");
    Ok(())
}
