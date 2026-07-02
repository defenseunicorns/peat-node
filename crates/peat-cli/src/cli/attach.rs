//! `peat attach` — distribute and receive file attachments over the mesh.
//!
//! Three subcommands:
//!
//! * `peat attach send <file>` — ingest a file into the mesh blob store and
//!   create a distribution document targeting connected peers.
//! * `peat attach status <dist-id>` — read current transfer status for a
//!   distribution from the synced Automerge store.
//! * `peat attach watch [--inbox <dir>]` — start a receive watcher that polls
//!   for incoming distribution documents targeting this node and writes each
//!   blob to `<inbox>/<relative-path>` (mirroring the outbox layout). Runs until `SIGINT`
//!   unless `--dist-id` is given, in which case it exits once that specific
//!   distribution has been delivered.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

use async_trait::async_trait;
use clap::{Args, Subcommand};
use peat_mesh::storage::blob_traits::{BlobMetadata, BlobStore};
use peat_mesh::storage::SyncTransport;
use peat_protocol::storage::{
    read_distribution_document, DistributionDocument, DistributionScope, FileDistribution,
    IrohFileDistribution, ReceiveSink, TransferPriority,
};

use crate::cli::{parse_timeout, CliError, CommonArgs};
use crate::creds;
use crate::join::{MeshSession, SessionOptions};

#[derive(Debug, Args)]
pub struct AttachArgs {
    #[command(subcommand)]
    pub command: AttachCommand,
}

#[derive(Debug, Subcommand)]
pub enum AttachCommand {
    /// Distribute a file to peers in the mesh.
    Send(SendArgs),
    /// Show the current transfer status of a distribution.
    Status(StatusArgs),
    /// Watch for incoming distributions and write them to an inbox directory.
    Watch(WatchArgs),
}

#[derive(Debug, Args)]
pub struct SendArgs {
    /// Path to the file to distribute.
    pub file: PathBuf,

    /// Distribution scope: `all` (default), `nodes:id1,id2`, or `formation:id`.
    #[arg(long, default_value = "all", value_name = "SCOPE")]
    pub scope: String,

    /// Transfer priority: `critical`, `high`, `normal` (default), or `low`.
    #[arg(long, default_value = "normal", value_name = "LEVEL")]
    pub priority: String,

    /// Block until all target nodes confirm receipt (or --timeout expires).
    #[arg(long)]
    pub wait: bool,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    /// Distribution ID returned by `peat attach send`.
    pub dist_id: String,
}

#[derive(Debug, Args)]
pub struct WatchArgs {
    /// Directory to write received files into.
    #[arg(long, default_value = "inbox", value_name = "PATH")]
    pub inbox: PathBuf,

    /// Exit once this specific distribution has been delivered to the inbox.
    /// Without this flag, watch runs until interrupted (SIGINT).
    #[arg(long, value_name = "DIST-ID")]
    pub dist_id: Option<String>,
}

pub async fn run(args: AttachArgs, common: CommonArgs) -> Result<(), CliError> {
    match args.command {
        AttachCommand::Send(a) => run_send(a, common).await,
        AttachCommand::Status(a) => run_status(a, common).await,
        AttachCommand::Watch(a) => run_watch(a, common).await,
    }
}

async fn run_send(args: SendArgs, common: CommonArgs) -> Result<(), CliError> {
    let scope = parse_scope(&args.scope)?;
    let priority = parse_priority(&args.priority)?;
    let timeout = parse_timeout(&common.timeout)?;

    let file_path = args
        .file
        .canonicalize()
        .map_err(|e| CliError::Generic(format!("cannot access `{}`: {e}", args.file.display())))?;
    let filename = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("attachment")
        .to_string();

    let creds = creds::load(common.creds.as_deref())?;
    let session = MeshSession::open(
        creds,
        SessionOptions {
            timeout,
            as_id: common.as_id.clone(),
            data_dir: common.data_dir.clone(),
        },
    )
    .await?;

    // Register connected peers with the blob store so AllNodes scope can
    // resolve them into target_nodes. peat-node does this in dial_and_attach;
    // peat-cli's connect_peer calls start_sync_connection only, so we do
    // it here before resolve_targets runs inside distribute().
    for peer_id in session.backend().transport().connected_peers() {
        session.backend().blob_store().add_peer(peer_id).await;
    }

    let file_dist = IrohFileDistribution::new(
        session.backend().blob_store().clone(),
        session.backend().store().clone(),
    );

    let metadata = BlobMetadata::with_name(filename);
    let token = session
        .backend()
        .blob_store()
        .create_blob(&file_path, metadata)
        .await
        .map_err(|e| CliError::Generic(format!("create_blob: {e}")))?;

    let handle = file_dist
        .distribute(&token, scope, priority)
        .await
        .map_err(|e| CliError::Generic(format!("distribute: {e}")))?;

    if args.wait {
        let status = file_dist
            .wait_for_completion(&handle, timeout)
            .await
            .map_err(|e| CliError::Generic(format!("wait_for_completion: {e}")))?;
        let out = serde_json::json!({
            "distribution_id": handle.distribution_id,
            "status": "complete",
            "completed": status.completed,
            "failed": status.failed,
            "total_targets": status.total_targets,
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    } else {
        let out = serde_json::json!({
            "distribution_id": handle.distribution_id,
            "blob_hash": handle.blob_hash.as_hex(),
            "scope": args.scope,
            "priority": args.priority,
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
    }

    Ok(())
}

async fn run_status(args: StatusArgs, common: CommonArgs) -> Result<(), CliError> {
    let timeout = parse_timeout(&common.timeout)?;
    let creds = creds::load(common.creds.as_deref())?;
    let session = MeshSession::open(
        creds,
        SessionOptions {
            timeout,
            as_id: common.as_id.clone(),
            data_dir: common.data_dir.clone(),
        },
    )
    .await?;

    let doc = read_distribution_document(session.backend().store(), &args.dist_id)
        .map_err(|e| CliError::Generic(format!("read distribution: {e}")))?;

    match doc {
        Some(d) => {
            let out = serde_json::to_string_pretty(&d)
                .map_err(|e| CliError::Generic(format!("serialize status: {e}")))?;
            println!("{out}");
        }
        None => {
            return Err(CliError::Generic(format!(
                "distribution `{}` not found in local store \
                 (try connecting with --creds and --timeout to sync it first)",
                args.dist_id
            )));
        }
    }

    Ok(())
}

async fn run_watch(args: WatchArgs, common: CommonArgs) -> Result<(), CliError> {
    let timeout = parse_timeout(&common.timeout)?;
    let creds = creds::load(common.creds.as_deref())?;
    let session = MeshSession::open(
        creds,
        SessionOptions {
            timeout,
            as_id: common.as_id.clone(),
            data_dir: common.data_dir.clone(),
        },
    )
    .await?;

    // Register connected peers in the blob store so the receive watcher can
    // fetch blobs from them. Mirrors the same call in run_send.
    for peer_id in session.backend().transport().connected_peers() {
        session.backend().blob_store().add_peer(peer_id).await;
    }

    std::fs::create_dir_all(&args.inbox)
        .map_err(|e| CliError::Generic(format!("create inbox `{}`: {e}", args.inbox.display())))?;

    let file_dist = IrohFileDistribution::new(
        session.backend().blob_store().clone(),
        session.backend().store().clone(),
    );

    // The receive watcher matches distributions against this node's short
    // endpoint id (the fmt_short() of the Iroh QUIC key). This is the same
    // id the sender populates target_nodes with when it calls known_peers().
    let own_short_id = session
        .backend()
        .blob_store()
        .endpoint_id()
        .fmt_short()
        .to_string();

    if let Some(dist_id) = &args.dist_id {
        // Attach a Notify to the sink so deliver() wakes us the instant the
        // target distribution lands — no filesystem polling needed.
        let notify = Arc::new(Notify::new());
        let sink: Arc<dyn ReceiveSink> = Arc::new(InboxSink::new_with_notify(
            args.inbox.clone(),
            dist_id.clone(),
            Arc::clone(&notify),
        ));
        // Create the notified future BEFORE starting the watcher so a
        // delivery that races the task spawn doesn't drop the permit.
        let notified = notify.notified();
        file_dist.start_receive_watcher(own_short_id, sink, Duration::from_secs(1));
        notified.await;
        let out = serde_json::json!({
            "distribution_id": dist_id,
            "status": "delivered",
            "inbox": args.inbox.display().to_string(),
        });
        println!("{}", serde_json::to_string_pretty(&out).unwrap());
        Ok(())
    } else {
        let sink: Arc<dyn ReceiveSink> = Arc::new(InboxSink::new(args.inbox.clone()));
        file_dist.start_receive_watcher(own_short_id, sink, Duration::from_secs(1));
        tokio::signal::ctrl_c()
            .await
            .map_err(|e| CliError::Generic(format!("signal handler: {e}")))?;
        Err(CliError::Interrupted)
    }
}

fn parse_scope(s: &str) -> Result<DistributionScope, CliError> {
    match s {
        "all" => Ok(DistributionScope::AllNodes),
        _ if s.starts_with("nodes:") => {
            let ids: Vec<String> = s["nodes:".len()..]
                .split(',')
                .filter(|id| !id.is_empty())
                .map(|id| id.to_string())
                .collect();
            if ids.is_empty() {
                return Err(CliError::Malformed(
                    "nodes: scope requires at least one node id (e.g. nodes:abc,def)".into(),
                ));
            }
            Ok(DistributionScope::Nodes { node_ids: ids })
        }
        _ if s.starts_with("formation:") => {
            let id = s["formation:".len()..].to_string();
            if id.is_empty() {
                return Err(CliError::Malformed(
                    "formation: scope requires a formation id (e.g. formation:alpha-cell)".into(),
                ));
            }
            Ok(DistributionScope::Formation { formation_id: id })
        }
        _ => Err(CliError::Malformed(format!(
            "unrecognised scope `{s}`: expected `all`, `nodes:id1,id2`, or `formation:id`"
        ))),
    }
}

fn parse_priority(s: &str) -> Result<TransferPriority, CliError> {
    match s {
        "critical" => Ok(TransferPriority::Critical),
        "high" => Ok(TransferPriority::High),
        "normal" => Ok(TransferPriority::Normal),
        "low" => Ok(TransferPriority::Low),
        _ => Err(CliError::Malformed(format!(
            "unrecognised priority `{s}`: expected `critical`, `high`, `normal`, or `low`"
        ))),
    }
}

/// Inbox receive sink: writes each delivered blob to `{inbox_root}/{relative_path}`,
/// mirroring the sender's outbox layout (so `outbox/sub/report.pdf` lands at
/// `inbox/sub/report.pdf`) via a tmp-then-rename pair. Re-delivery of the same
/// path overwrites (latest-wins). Faithfully mirrors the `FilesystemInboxSink` in
/// `peat-node::attachments::inbox` — including the path-traversal guard and the
/// post-write size validation; keep the two in lockstep.
///
/// When constructed with [`InboxSink::new_with_notify`], fires the provided
/// `Notify` after the target distribution's blob is successfully renamed into
/// place, allowing `run_watch --dist-id` to wake immediately rather than
/// polling the filesystem.
struct InboxSink {
    inbox_root: PathBuf,
    /// If set, `deliver()` fires this notify once the named distribution lands.
    delivery_signal: Option<(String, Arc<Notify>)>,
}

impl InboxSink {
    fn new(inbox_root: PathBuf) -> Self {
        Self {
            inbox_root,
            delivery_signal: None,
        }
    }

    fn new_with_notify(inbox_root: PathBuf, target_dist_id: String, notify: Arc<Notify>) -> Self {
        Self {
            inbox_root,
            delivery_signal: Some((target_dist_id, notify)),
        }
    }
}

#[async_trait]
impl ReceiveSink for InboxSink {
    /// "Already delivered" gate keyed on the mirrored path
    /// (`{inbox}/{relative_path}`): a restarted watcher doesn't re-fetch a
    /// delivery whose target already holds a regular file of the declared size.
    async fn already_delivered(&self, doc: &DistributionDocument) -> bool {
        let rel = inbox_relpath(&doc.blob_metadata)
            .unwrap_or_else(|| PathBuf::from(format!("{}.bin", doc.distribution_id)));
        match tokio::fs::metadata(self.inbox_root.join(rel)).await {
            Ok(md) => md.is_file() && md.len() == doc.blob_size,
            Err(_) => false,
        }
    }

    async fn deliver(&self, doc: &DistributionDocument, blob_path: &Path) -> anyhow::Result<()> {
        let rel = inbox_relpath(&doc.blob_metadata)
            .unwrap_or_else(|| PathBuf::from(format!("{}.bin", doc.distribution_id)));
        let target = self.inbox_root.join(&rel);

        // Create the (possibly nested) parent dir and stage the tmp file in it
        // so the publishing rename is atomic on the same filesystem.
        let parent = target
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.inbox_root.clone());
        tokio::fs::create_dir_all(&parent).await?;
        let fname = target
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("blob");
        let tmp = parent.join(format!(".{fname}.partial"));

        tokio::fs::copy(blob_path, &tmp).await?;

        // Post-write size validation before the publishing rename — matches
        // FilesystemInboxSink. iroh already content-verifies the blob on fetch,
        // so a size match confirms a complete, untruncated local write. On
        // mismatch, drop the tmp and bail so the watcher retries rather than
        // publishing a short file.
        let written = tokio::fs::metadata(&tmp).await?.len();
        if written != doc.blob_size {
            let _ = tokio::fs::remove_file(&tmp).await;
            anyhow::bail!(
                "inbox write size mismatch for {} (dist {}): wrote {written} bytes, \
                 expected {} — leaving for retry",
                rel.display(),
                doc.distribution_id,
                doc.blob_size
            );
        }
        tokio::fs::rename(&tmp, &target).await?;
        if let Some((target_id, notify)) = &self.delivery_signal {
            if doc.distribution_id == *target_id {
                notify.notify_one();
            }
        }
        Ok(())
    }
}

/// Resolve the sender-provided `blob_metadata.name` into a safe path **relative
/// to the inbox root**, preserving subdirectories so the inbox mirrors the
/// sender's layout (`outbox/sub/demo.txt` → `inbox/sub/demo.txt`). Faithfully
/// mirrors `peat-node::attachments::inbox::inbox_relpath`.
///
/// Path-traversal guard: the sender controls `name`, so only `Normal` path
/// components are accepted. Any absolute path, `..`, root, or drive-prefix
/// component returns `None`, and the caller falls back to a flat
/// `<distribution_id>.bin` at the inbox root — a malicious or malformed name
/// can never write outside the inbox. Returns `None` for a missing/empty name
/// or one that sanitises to nothing.
fn inbox_relpath(metadata: &BlobMetadata) -> Option<PathBuf> {
    use std::path::Component;

    let raw = metadata.name.as_deref()?;
    if raw.is_empty() {
        return None;
    }
    let mut safe = PathBuf::new();
    for comp in Path::new(raw).components() {
        match comp {
            Component::Normal(c) => safe.push(c),
            Component::CurDir => {} // "." — ignore
            // ".." (ParentDir), "/" (RootDir), or a Windows prefix — reject the
            // whole name rather than try to repair it.
            _ => return None,
        }
    }
    if safe.as_os_str().is_empty() {
        None
    } else {
        Some(safe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_all() {
        assert!(matches!(
            parse_scope("all"),
            Ok(DistributionScope::AllNodes)
        ));
    }

    #[test]
    fn scope_nodes() {
        let s = parse_scope("nodes:a,b,c").unwrap();
        let DistributionScope::Nodes { node_ids } = s else {
            panic!("wrong variant");
        };
        assert_eq!(node_ids, ["a", "b", "c"]);
    }

    #[test]
    fn scope_formation() {
        let s = parse_scope("formation:alpha-cell").unwrap();
        let DistributionScope::Formation { formation_id } = s else {
            panic!("wrong variant");
        };
        assert_eq!(formation_id, "alpha-cell");
    }

    #[test]
    fn scope_invalid() {
        assert_eq!(parse_scope("unknown").unwrap_err().exit_code(), 4);
    }

    #[test]
    fn scope_nodes_empty_list() {
        assert_eq!(parse_scope("nodes:").unwrap_err().exit_code(), 4);
    }

    #[test]
    fn scope_formation_empty_id() {
        assert_eq!(parse_scope("formation:").unwrap_err().exit_code(), 4);
    }

    #[test]
    fn priority_all() {
        assert!(matches!(
            parse_priority("critical"),
            Ok(TransferPriority::Critical)
        ));
        assert!(matches!(parse_priority("high"), Ok(TransferPriority::High)));
        assert!(matches!(
            parse_priority("normal"),
            Ok(TransferPriority::Normal)
        ));
        assert!(matches!(parse_priority("low"), Ok(TransferPriority::Low)));
    }

    #[test]
    fn priority_invalid() {
        assert_eq!(parse_priority("unknown").unwrap_err().exit_code(), 4);
    }

    fn doc_with(distribution_id: &str, blob_size: u64, name: Option<&str>) -> DistributionDocument {
        DistributionDocument {
            distribution_id: distribution_id.to_string(),
            blob_hash: "deadbeef".to_string(),
            blob_size,
            blob_metadata: BlobMetadata {
                name: name.map(String::from),
                content_type: None,
                custom: std::collections::HashMap::new(),
            },
            scope: DistributionScope::AllNodes,
            priority: TransferPriority::Normal,
            target_nodes: vec![],
            started_at: chrono::Utc::now(),
            status: "distributing".to_string(),
            cancelled_at: None,
            collection: None,
            node_statuses: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn inbox_relpath_preserves_subdirs() {
        assert_eq!(
            inbox_relpath(&BlobMetadata::with_name("report.pdf".to_string())),
            Some(PathBuf::from("report.pdf"))
        );
        assert_eq!(
            inbox_relpath(&BlobMetadata::with_name("sub/dir/report.pdf".to_string())),
            Some(PathBuf::from("sub/dir/report.pdf"))
        );
    }

    #[test]
    fn inbox_relpath_rejects_traversal_and_absolute() {
        // Sender-controlled name must never escape the inbox — reject so the
        // caller uses the `<distribution_id>.bin` fallback.
        assert_eq!(
            inbox_relpath(&BlobMetadata::with_name("../../etc/passwd".to_string())),
            None
        );
        assert_eq!(
            inbox_relpath(&BlobMetadata::with_name("/etc/passwd".to_string())),
            None
        );
        assert_eq!(
            inbox_relpath(&BlobMetadata::with_name("a/../../b".to_string())),
            None
        );
    }

    #[test]
    fn inbox_relpath_none_for_missing_or_empty() {
        let none = BlobMetadata {
            name: None,
            content_type: None,
            custom: std::collections::HashMap::new(),
        };
        assert_eq!(inbox_relpath(&none), None);
        assert_eq!(inbox_relpath(&BlobMetadata::with_name(String::new())), None);
    }

    /// Regression guard for #173 / the peat-cli↔peat-node inbox-layout
    /// divergence: `deliver()` must mirror the sender's relative path, NOT nest
    /// the file under a `{distribution_id}/` directory like the pre-#173 sink.
    #[tokio::test]
    async fn deliver_mirrors_relative_path_not_distribution_id_dir() {
        let inbox = tempfile::tempdir().unwrap();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), b"hello world").unwrap();
        let sink = InboxSink::new(inbox.path().to_path_buf());
        let doc = doc_with("dist-Y", 11, Some("sub/dir/greeting.txt"));

        sink.deliver(&doc, src.path()).await.unwrap();

        let landed = inbox.path().join("sub").join("dir").join("greeting.txt");
        assert_eq!(std::fs::read(&landed).unwrap(), b"hello world");
        // The old layout nested under {distribution_id}/ — assert it does NOT.
        assert!(
            !inbox.path().join("dist-Y").exists(),
            "delivery must not nest under a distribution_id directory"
        );
        assert!(sink.already_delivered(&doc).await);

        // Re-deliver overwrites (latest-wins); no partial left behind.
        sink.deliver(&doc, src.path()).await.unwrap();
        assert_eq!(std::fs::read(&landed).unwrap(), b"hello world");
        assert!(!inbox
            .path()
            .join("sub")
            .join("dir")
            .join(".greeting.txt.partial")
            .exists());
    }

    #[tokio::test]
    async fn deliver_traversal_name_falls_back_inside_inbox() {
        let inbox = tempfile::tempdir().unwrap();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), b"x").unwrap();
        let sink = InboxSink::new(inbox.path().to_path_buf());
        // A hostile name resolves to the flat fallback inside the inbox.
        let doc = doc_with("dist-evil", 1, Some("../../../../tmp/pwned"));
        sink.deliver(&doc, src.path()).await.unwrap();
        assert!(inbox.path().join("dist-evil.bin").is_file());
    }

    #[tokio::test]
    async fn deliver_size_mismatch_bails_without_publishing() {
        let inbox = tempfile::tempdir().unwrap();
        let src = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(src.path(), b"hello world").unwrap(); // 11 bytes
        let sink = InboxSink::new(inbox.path().to_path_buf());
        let doc = doc_with("dist-Z", 99, Some("x.txt")); // declares 99
        assert!(sink.deliver(&doc, src.path()).await.is_err());
        assert!(
            !inbox.path().join("x.txt").exists(),
            "a short file must not be published"
        );
    }
}
