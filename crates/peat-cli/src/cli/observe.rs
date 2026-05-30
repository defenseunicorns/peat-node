//! `peat observe` — subscribe and stream document changes until interrupted
//! (ADR-001 §Lifecycle, §"Sync mode mapping").
//!
//! Phase 3 caveat: `--mode` is parsed but currently effective behavior is
//! latest-only — `peat-mesh` does not yet expose mode-bound subscription
//! at the `subscribe_to_observer_changes` surface. Operators see a stderr
//! warning when they pass a non-default mode. The flag stays in place so
//! scripts don't have to change when the upstream API grows the binding.

use clap::{Args, ValueEnum};
use std::io::Write;
use tokio::sync::broadcast::error::RecvError;

use crate::cli::output::{render_observe_deleted, render_observe_event};
use crate::cli::query::parse_target;
use crate::cli::{parse_timeout, CliError, CommonArgs};
use crate::creds;
use crate::join::{MeshSession, SessionOptions};

#[derive(Debug, Args)]
#[command(group = clap::ArgGroup::new("scope").required(true).args(["target", "all_collections"]))]
pub struct ObserveArgs {
    /// Target as `<COLLECTION>` or `<COLLECTION>/<DOC_ID>`. Mutually exclusive with `--all-collections`.
    pub target: Option<String>,

    /// Observe every collection reachable with the supplied credentials.
    /// Equivalent to subscribing to the full mesh changes stream.
    #[arg(
        long = "all-collections",
        visible_alias = "all",
        conflicts_with = "target"
    )]
    pub all_collections: bool,

    /// Sync mode (maps to ADR-019 sync modes).
    #[arg(long, value_enum, default_value_t = SyncMode::LatestOnly)]
    pub mode: SyncMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SyncMode {
    /// Stream current-state updates only.
    LatestOnly,
    /// Tail recent history then live updates.
    Windowed,
    /// Every delta — forensics, debugging, CDC.
    FullHistory,
}

pub async fn run(args: ObserveArgs, common: CommonArgs) -> Result<(), CliError> {
    // `--all-collections` skips per-collection prefix filtering — every
    // observer event reaches the renderer. Authorization is formation-key-
    // only today (peat#941 deferred), so the bundle's read scope is the
    // entire store.
    let (prefix, target_key) = if args.all_collections {
        (String::new(), None)
    } else {
        let target = args
            .target
            .as_deref()
            .expect("ArgGroup `scope` guarantees target when all_collections is false");
        let (collection, doc_id) = parse_target(target)?;
        (
            format!("{collection}:"),
            doc_id.map(|id| format!("{collection}:{id}")),
        )
    };

    if args.mode != SyncMode::LatestOnly {
        tracing::warn!(
            mode = ?args.mode,
            "--mode {:?} is parsed but currently behaves as latest-only; \
             peat-mesh does not yet bind subscription QoS at this surface",
            args.mode
        );
    }

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

    let store = session.backend().store().clone();
    let mut rx = store.subscribe_to_observer_changes();

    // tokio::signal::ctrl_c is a Future that resolves on first SIGINT.
    // We multiplex it with the broadcast receiver via select.
    let mut sigint = Box::pin(tokio::signal::ctrl_c());

    loop {
        tokio::select! {
            // SIGINT → 130 per ADR shell integration discipline.
            _ = &mut sigint => return Err(CliError::Interrupted),

            event = rx.recv() => {
                let key = match event {
                    Ok(k) => k,
                    Err(RecvError::Lagged(n)) => {
                        tracing::warn!(dropped = n, "observer lagged");
                        continue;
                    }
                    Err(RecvError::Closed) => break,
                };

                if let Some(target) = &target_key {
                    if &key != target { continue; }
                } else if !key.starts_with(&prefix) {
                    continue;
                }

                let render = match store.get(&key) {
                    // Live doc: render it.
                    Ok(Some(d)) => render_observe_event(&key, &d, common.output),
                    // Document is gone between event emission and our read
                    // (tombstoned). Emit a structurally distinct "deleted"
                    // record so CDC consumers see deletes, not just upserts.
                    // ADR-034 metadata refinement is a follow-up; for v1
                    // we emit a minimal `deleted: true` envelope.
                    Ok(None) => render_observe_deleted(&key, common.output),
                    Err(e) => {
                        tracing::warn!(key = %key, error = %e, "store read failed");
                        continue;
                    }
                };

                match render {
                    Ok(()) => {}
                    // ADR-001 §"Shell integration discipline": pipe-close
                    // exits cleanly. Propagate so main.rs maps it to
                    // status 0.
                    Err(CliError::BrokenPipe) => return Err(CliError::BrokenPipe),
                    Err(e) => return Err(e),
                }

                // Flush so streamed records reach the consumer immediately
                // instead of waiting for stdio buffer to fill. Ignore the
                // pipe-close case here too.
                let _ = std::io::stdout().flush();
            }
        }
    }

    Ok(())
}
