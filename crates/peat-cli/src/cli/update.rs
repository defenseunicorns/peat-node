//! `peat update` — apply a delta to an existing document, or create it if
//! missing (ADR-001 §Write semantics → `update` semantics).
//!
//! `--set <path>=<value>` walks the JSON shape via `apply_sets`. `--from
//! <PATH>` reads a full proposed document and computes a minimal Automerge
//! delta against the stored state via `AutomergeStore::diff` →
//! `apply_delta` (peat-mesh#187), preserving the ADR-021 "create once,
//! evolve through deltas" invariant — the existing operation history on
//! the doc survives the round-trip-edit pattern.

use clap::Args;
use peat_mesh::storage::json_convert::{automerge_to_json, json_to_automerge};
use peat_mesh::storage::AutomergeStore;
use serde_json::Value;
use std::path::PathBuf;

use crate::cli::query::parse_target;
use crate::cli::writes::{
    apply_proto3_defaults, apply_sets, read_from, validate_against_schema, POST_WRITE_SYNC_WAIT,
};
use crate::cli::{parse_timeout, CliError, CommonArgs};
use crate::creds;
use crate::join::{MeshSession, SessionOptions};

#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("input").required(true).args(["from", "set"]))]
pub struct UpdateArgs {
    /// Target as `<COLLECTION>/<DOC_ID>`.
    pub target: String,

    /// Read full document content (delta computed from current). Use `-` for stdin.
    #[arg(long, value_name = "PATH", conflicts_with = "set")]
    pub from: Option<PathBuf>,

    /// Surgical field updates as `path=value` (repeatable).
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

pub async fn run(args: UpdateArgs, common: CommonArgs) -> Result<(), CliError> {
    if args.no_validate {
        tracing::warn!("--no-validate set; skipping schema validation");
    }

    let (collection, doc_id) = parse_target(&args.target)?;
    let doc_id = doc_id.ok_or_else(|| {
        CliError::Malformed(format!(
            "update requires `<collection>/<doc_id>`; got `{}`",
            args.target
        ))
    })?;
    let key = format!("{collection}:{doc_id}");

    // `--from` is parsed *before* the join prelude so a bad path or
    // malformed JSON fails fast (exit 4) without a mesh handshake.
    let from_doc: Option<Value> = match args.from.as_deref() {
        Some(path) => Some(read_from(path)?),
        None => None,
    };

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

    let existing = session
        .backend()
        .store()
        .get(&key)
        .map_err(|e| CliError::Generic(format!("read `{key}`: {e}")))?;

    // Build the proposed JSON shape. `--from` replaces wholesale; `--set`
    // overlays onto the current doc (or an empty object if missing).
    let updated = match from_doc {
        Some(doc) => doc,
        None => {
            let base = existing
                .as_ref()
                .map(automerge_to_json)
                .unwrap_or_else(|| Value::Object(Default::default()));
            apply_sets(base, &args.set)?
        }
    };

    // For registered peat-schema types, underlay proto3 zero-defaults
    // (peat-node#112). When `existing` is `Some`, `base` already carries
    // every field, so the underlay is a no-op — defaults only fill the
    // upsert-on-missing case where the user's `--set` overlay started
    // from an empty object.
    let updated = apply_proto3_defaults(collection, updated);

    // Schema gate (ADR-001 §"Write semantics" → "Validation"). Validate
    // the *post-update* shape against the registry. Unknown collections
    // are accepted structurally; known types must satisfy field
    // constraints. `--no-validate` skips with the warning above.
    if !args.no_validate {
        validate_against_schema(collection, &updated)?;
    }

    if args.dry_run {
        let op = serde_json::json!({
            "op": if existing.is_some() { "update" } else { "create-via-update" },
            "key": key,
            "doc": updated,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&op)
                .map_err(|e| CliError::Generic(format!("serialize: {e}")))?
        );
        return Ok(());
    }

    match existing {
        Some(current) => {
            // Round-trip-edit (ADR-001 Phase 4b, peat-mesh#187): compute
            // a minimal delta from `current` → `proposed` and apply it,
            // preserving ADR-021's "create once, evolve through deltas"
            // invariant. `json_to_automerge(.., Some(&current))` evolves
            // the existing doc's history; `AutomergeStore::diff` then
            // extracts only the new changes.
            let proposed = json_to_automerge(&updated, Some(&current))
                .map_err(|e| CliError::Generic(format!("build automerge doc: {e}")))?;
            let delta = AutomergeStore::diff(&current, &proposed);
            session
                .backend()
                .store()
                .apply_delta(&key, &delta)
                .map_err(|e| CliError::Generic(format!("apply_delta `{key}`: {e}")))?;
        }
        None => {
            // Upsert semantics (ADR-001): missing doc → initial creation,
            // not recreation. There is no prior history to delta against,
            // so `put` is the correct path here — ADR-021's invariant
            // names this as the lone exception.
            let doc = json_to_automerge(&updated, None)
                .map_err(|e| CliError::Generic(format!("build automerge doc: {e}")))?;
            session
                .backend()
                .store()
                .put(&key, &doc)
                .map_err(|e| CliError::Generic(format!("put `{key}`: {e}")))?;
        }
    }

    if args.wait_for_sync {
        tokio::time::sleep(POST_WRITE_SYNC_WAIT).await;
    }

    println!("{key}");
    Ok(())
}
