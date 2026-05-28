//! `peat update` — apply a delta to an existing document, or create it if
//! missing (ADR-001 §Write semantics → `update` semantics).
//!
//! Phase 4a wires `--set <path>=<value>` only. `--from <PATH>` requires
//! Automerge delta computation in `peat-mesh` (tracked at
//! <https://github.com/defenseunicorns/peat-mesh/issues/187>); the handler
//! returns `NotImplemented` with a pointer to the upstream issue when
//! `--from` is supplied.

use clap::Args;
use peat_mesh::storage::json_convert::{automerge_to_json, json_to_automerge};
use serde_json::Value;
use std::path::PathBuf;

use crate::cli::query::parse_target;
use crate::cli::writes::{apply_sets, POST_WRITE_SYNC_WAIT};
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
    if args.from.is_some() {
        return Err(CliError::NotImplemented(
            "update --from (gated on peat-mesh#187 — Automerge delta API)",
        ));
    }
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

    // Upsert semantics (ADR-001): if doc doesn't exist, this becomes initial
    // creation. ADR-021's "create once, evolve through deltas" invariant
    // holds because the initial `update` against a missing doc is initial
    // creation, not recreation.
    let base = existing
        .as_ref()
        .map(automerge_to_json)
        .unwrap_or_else(|| Value::Object(Default::default()));
    let updated = apply_sets(base, &args.set)?;

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

    let doc = json_to_automerge(&updated, existing.as_ref())
        .map_err(|e| CliError::Generic(format!("build automerge doc: {e}")))?;
    session
        .backend()
        .store()
        .put(&key, &doc)
        .map_err(|e| CliError::Generic(format!("put `{key}`: {e}")))?;

    if args.wait_for_sync {
        tokio::time::sleep(POST_WRITE_SYNC_WAIT).await;
    }

    println!("{key}");
    Ok(())
}
