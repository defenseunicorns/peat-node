//! `peat create` — strict-create a new document in a collection
//! (ADR-001 §"Write semantics" → "Idempotency on `create`").

use clap::Args;
use peat_mesh::storage::json_convert::json_to_automerge;
use peat_mesh::storage::SyncTransport;
use serde_json::Value;
use std::path::PathBuf;

use crate::cli::writes::{
    apply_proto3_defaults, apply_sets, is_registered_collection, read_from,
    validate_against_schema, POST_WRITE_SYNC_WAIT,
};
use crate::cli::{parse_timeout, CliError, CommonArgs};
use crate::creds;
use crate::join::{MeshSession, SessionOptions};

#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("input").required(true).args(["from", "set"]))]
pub struct CreateArgs {
    /// Target as `<COLLECTION>` or `<COLLECTION>/<DOC_ID>`.
    /// If `<DOC_ID>` is omitted a UUID is generated.
    pub target: String,

    /// Read document content from file. Use `-` for stdin.
    #[arg(long, value_name = "PATH", conflicts_with = "set")]
    pub from: Option<PathBuf>,

    /// Build document from `path=value` pairs (repeatable).
    #[arg(long, value_name = "PATH=VALUE")]
    pub set: Vec<String>,

    /// Validate and prepare the operation; do not submit.
    #[arg(long)]
    pub dry_run: bool,

    /// Block until at least one peer has acknowledged.
    #[arg(long)]
    pub wait_for_sync: bool,

    /// Skip schema validation (emits warning to stderr).
    #[arg(long)]
    pub no_validate: bool,
}

pub async fn run(args: CreateArgs, common: CommonArgs) -> Result<(), CliError> {
    if args.no_validate {
        tracing::warn!("--no-validate set; skipping schema validation");
    }

    let (collection, doc_id_opt) = crate::cli::query::parse_target(&args.target)?;
    // doc_id from the target path, or a generated UUID if only a collection
    // was given (e.g. `peat create capabilities --set name=foo`).
    let doc_id = doc_id_opt
        .map(str::to_string)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let key = format!("{collection}:{doc_id}");

    let mut json_value: Value = match (args.from.as_deref(), args.set.as_slice()) {
        (Some(path), _) => read_from(path)?,
        (None, sets) if !sets.is_empty() => apply_sets(Value::Object(Default::default()), sets)?,
        _ => unreachable!("clap ArgGroup requires --from or --set"),
    };

    // For registered peat-schema types the `id` field equals the store key's
    // doc id by convention, and the type validator requires it. Fill it from
    // the target doc id so the operator doesn't have to repeat it in `--set`.
    // Only for registered collections — arbitrary collections don't get a
    // spurious `id`. If the document already carries an `id` that disagrees
    // with the target doc id (via `--set id=…` or a `--from` file), that's a
    // contradiction: the stored key would say one thing and the doc another.
    // Reject it rather than silently storing a mismatch.
    if is_registered_collection(collection) {
        if let Value::Object(ref mut map) = json_value {
            match map.get("id") {
                Some(Value::String(existing)) if existing != &doc_id => {
                    return Err(CliError::Malformed(format!(
                        "document `id` ({existing}) does not match target doc id ({doc_id}); \
                         omit `id` or make it match `{collection}/{doc_id}`"
                    )));
                }
                Some(_) => {} // present and matching (or non-string — validation will catch)
                None => {
                    map.insert("id".to_string(), Value::String(doc_id.clone()));
                }
            }
        }
    }

    // For registered peat-schema types, underlay proto3 zero-defaults for
    // every field the user did not specify (peat-node#112). Without this,
    // `peat create capabilities --set id=cap-1 --set name=thermal` fails
    // prost's strict deserialize on sibling-field absence. User-supplied
    // fields always win.
    let json_value = apply_proto3_defaults(collection, json_value);

    // Schema gate (ADR-001 §"Write semantics" → "Validation"). Runs against
    // the peat-schema type registry: known collections enforce the
    // typed-message shape and field constraints; unknown collections accept
    // structurally. `--no-validate` skips the gate with the warning above.
    if !args.no_validate {
        validate_against_schema(collection, &json_value)?;
    }

    if args.dry_run {
        // Print the would-be operation in canonical JSON and exit 0 without
        // joining the mesh.
        let op = serde_json::json!({
            "op": "create",
            "key": key,
            "doc": json_value,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&op)
                .map_err(|e| CliError::Generic(format!("serialize: {e}")))?
        );
        return Ok(());
    }

    let creds = creds::load(common.creds.as_deref())?;
    let timeout = parse_timeout(&common.timeout)?;
    let session = MeshSession::open(
        creds,
        SessionOptions {
            timeout,
            as_id: common.as_id.clone(),
            data_dir: common.data_dir.clone(),
        },
    )
    .await?;

    // Strict-create: error if the document already exists. ADR-001 maps this
    // to exit 4 (Malformed) — caller explicitly asked for create, not upsert.
    if session
        .backend()
        .store()
        .get(&key)
        .map_err(|e| CliError::Generic(format!("read `{key}`: {e}")))?
        .is_some()
    {
        return Err(CliError::Malformed(format!(
            "document `{key}` already exists; use `update` for upsert semantics"
        )));
    }

    let doc = json_to_automerge(&json_value, None)
        .map_err(|e| CliError::Generic(format!("build automerge doc: {e}")))?;
    session
        .backend()
        .store()
        .put(&key, &doc)
        .map_err(|e| CliError::Generic(format!("put `{key}`: {e}")))?;

    if args.wait_for_sync {
        // Push inline on the main task so the on-change-pusher background
        // task cannot race the CLI exit and drop mid-send.
        if let Err(e) = session
            .backend()
            .coordinator()
            .sync_document_with_all_peers(&key)
            .await
        {
            tracing::warn!(key = %key, "sync_document_with_all_peers failed: {e}");
        }
        // Round-trip confirmation: sync_document_with_all_peers writes to
        // the QUIC send buffer but does not wait for peer ACK. A subsequent
        // sync_all_documents_with_peer request/response cycle only completes
        // after the peer has processed the connection's prior data, confirming
        // the create was received before the CLI exits.
        for peer_id in session.backend().transport().connected_peers() {
            if let Err(e) = session
                .backend()
                .coordinator()
                .sync_all_documents_with_peer(peer_id)
                .await
            {
                tracing::warn!(peer = %peer_id, "post-create sync round-trip failed: {e}");
            }
        }
        tokio::time::sleep(POST_WRITE_SYNC_WAIT).await;
    }

    println!("{key}");
    Ok(())
}
