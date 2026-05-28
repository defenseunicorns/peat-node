//! `peat query` — fetch current materialized state and exit (ADR-001 §Lifecycle).

use clap::Args;
use std::time::{Duration, Instant};

use crate::cli::output::render_query;
use crate::cli::{parse_timeout, CliError, CommonArgs};
use crate::creds;
use crate::join::{MeshSession, SessionOptions};

#[derive(Debug, Args)]
pub struct QueryArgs {
    /// Target as `<COLLECTION>` or `<COLLECTION>/<DOC_ID>`.
    pub target: String,

    /// Cap the number of records emitted.
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,
}

/// Polling interval for "wait for sync to populate" after connect. Tighter
/// than the timeout so we don't block longer than necessary when sync
/// completes quickly.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Minimum settle window before the first read attempt. Gives `sync_all_documents_with_peer`
/// a head start so the very first scan isn't racing the handshake.
const INITIAL_SETTLE: Duration = Duration::from_millis(250);

pub async fn run(args: QueryArgs, common: CommonArgs) -> Result<(), CliError> {
    let (collection, doc_id) = parse_target(&args.target)?;

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

    tokio::time::sleep(INITIAL_SETTLE).await;

    let store = session.backend().store();
    // Poll for state: peat-mesh doesn't surface a per-peer "sync drained"
    // signal, so we re-read every POLL_INTERVAL up to --timeout. As soon as
    // we see ANY matching state, return it. Empty-after-timeout is also a
    // valid result (the target genuinely has no matching documents).
    let deadline = Instant::now() + timeout;
    loop {
        let docs = match doc_id {
            Some(id) => {
                let key = format!("{collection}:{id}");
                match store
                    .get(&key)
                    .map_err(|e| CliError::Generic(format!("read `{key}`: {e}")))?
                {
                    Some(doc) => vec![(key, doc)],
                    None => Vec::new(),
                }
            }
            None => {
                let prefix = format!("{collection}:");
                let mut entries = store
                    .scan_prefix(&prefix)
                    .map_err(|e| CliError::Generic(format!("scan `{prefix}`: {e}")))?;
                if let Some(n) = args.limit {
                    entries.truncate(n);
                }
                entries
            }
        };

        if !docs.is_empty() || Instant::now() >= deadline {
            return render_query(&docs, common.output);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Split a target spec into `(collection, optional_doc_id)`. The grammar is
/// the minimum the ADR demands: a single `/` separator. Trailing slashes and
/// double slashes are malformed.
///
/// Shared with `observe` — same target grammar.
pub(crate) fn parse_target(s: &str) -> Result<(&str, Option<&str>), CliError> {
    if s.is_empty() {
        return Err(CliError::Malformed("target is empty".into()));
    }
    match s.split_once('/') {
        Some((_, "")) => Err(CliError::Malformed(format!(
            "target `{s}`: trailing slash without doc id"
        ))),
        Some(("", _)) => Err(CliError::Malformed(format!(
            "target `{s}`: leading slash without collection"
        ))),
        Some((_, d)) if d.contains('/') => Err(CliError::Malformed(format!(
            "target `{s}`: only one slash allowed"
        ))),
        Some((c, d)) => Ok((c, Some(d))),
        None => Ok((s, None)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_collection_only() {
        assert_eq!(parse_target("contacts").unwrap(), ("contacts", None));
    }
    #[test]
    fn parses_collection_with_doc_id() {
        assert_eq!(
            parse_target("contacts/c-1").unwrap(),
            ("contacts", Some("c-1"))
        );
    }
    #[test]
    fn rejects_empty() {
        assert_eq!(parse_target("").unwrap_err().exit_code(), 4);
    }
    #[test]
    fn rejects_trailing_slash() {
        assert_eq!(parse_target("contacts/").unwrap_err().exit_code(), 4);
    }
    #[test]
    fn rejects_leading_slash() {
        assert_eq!(parse_target("/c-1").unwrap_err().exit_code(), 4);
    }
    #[test]
    fn rejects_two_slashes() {
        assert_eq!(parse_target("a/b/c").unwrap_err().exit_code(), 4);
    }
}
