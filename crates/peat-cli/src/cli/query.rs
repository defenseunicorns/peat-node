//! `peat query` — fetch current materialized state and exit (ADR-001 §Lifecycle).

use clap::Args;
use std::time::Duration;

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

/// Settle window after `MeshSession::open` returns, giving the initial sync
/// from the connected peer(s) time to populate the local store before we
/// snapshot it. peat-mesh today doesn't surface a "sync drained" signal —
/// this fixed window is the v1 heuristic; revisit when the upstream API grows
/// a per-peer drain marker.
const POST_CONNECT_SETTLE: Duration = Duration::from_millis(500);

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

    tokio::time::sleep(POST_CONNECT_SETTLE).await;

    let store = session.backend().store();
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

    render_query(&docs, common.output)
}

/// Split a target spec into `(collection, optional_doc_id)`. The grammar is
/// the minimum the ADR demands: a single `/` separator. Trailing slashes and
/// double slashes are malformed.
fn parse_target(s: &str) -> Result<(&str, Option<&str>), CliError> {
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
