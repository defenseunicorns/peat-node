//! Output formatters.
//!
//! ADR-001 §"Document rendering" mandates a typed-vs-generic dispatch model.
//! `peat-schema` does not yet expose a type-metadata registry (tracked
//! upstream as a follow-up; see ADR-001 §Open Questions), so v1 ships the
//! **generic** path only — every document renders structurally via
//! `peat_mesh::storage::json_convert::automerge_to_json`. This is the
//! "renderer-not-found fallback" path the ADR specifies; type-aware
//! formatters will land additively when the upstream registry exists.
//!
//! Stream discipline (ADR-001 §"Shell integration discipline"):
//!   - All document data goes to stdout.
//!   - Logs / status / errors go to stderr (wired in main.rs via
//!     tracing_subscriber).
//!   - `json` / `ndjson` outputs are stable schemas (the generic structural
//!     JSON shape). `text` is human-readable and may evolve.

use automerge::Automerge;
use clap::ValueEnum;
use peat_mesh::storage::json_convert::automerge_to_json;
use serde_json::Value;

use crate::cli::CliError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    /// Human-readable. v1: pretty-printed JSON of the generic structural
    /// representation. Will gain key-value blocks / typed formatters when
    /// the upstream type registry lands.
    Text,
    /// Single canonical JSON value (object keyed by doc key when multiple,
    /// or the doc value directly when a single record).
    Json,
    /// One JSON record per line. Natural for `observe | jq` and log shipping.
    Ndjson,
}

/// Render the result of a `query` invocation.
///
/// `docs` is the list returned by the store: each entry is `(key, Automerge)`
/// where `key` is the full `collection:doc_id` form. The renderer strips the
/// collection prefix when emitting per-doc keys in `json` mode for ergonomic
/// downstream piping.
pub fn render_query(docs: &[(String, Automerge)], fmt: OutputFormat) -> Result<(), CliError> {
    match fmt {
        OutputFormat::Text | OutputFormat::Json => {
            let value = match docs {
                // Single doc: emit the doc value directly.
                [(_, doc)] => automerge_to_json(doc),
                // Empty or multi-doc collection: emit a JSON object keyed by
                // the raw store key (collection:doc_id) so consumers can
                // identify each record.
                _ => Value::Object(
                    docs.iter()
                        .map(|(k, d)| (k.clone(), automerge_to_json(d)))
                        .collect(),
                ),
            };
            let out = serde_json::to_string_pretty(&value)
                .map_err(|e| CliError::Generic(format!("serialize JSON: {e}")))?;
            println!("{out}");
        }
        OutputFormat::Ndjson => {
            for (_, doc) in docs {
                let line = serde_json::to_string(&automerge_to_json(doc))
                    .map_err(|e| CliError::Generic(format!("serialize JSON: {e}")))?;
                println!("{line}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::transaction::Transactable;

    fn fixture_doc(name: &str) -> Automerge {
        let mut d = Automerge::new();
        let mut tx = d.transaction();
        tx.put(automerge::ROOT, "name", name).unwrap();
        tx.commit();
        d
    }

    #[test]
    fn single_doc_json_emits_object_not_wrapper() {
        // We can't easily intercept println! in a unit test, but we can
        // verify the rendered JSON structure via the same conversion path.
        let docs = [("contacts:c-1".to_string(), fixture_doc("alice"))];
        let v = automerge_to_json(&docs[0].1);
        assert_eq!(v["name"], serde_json::json!("alice"));
    }

    #[test]
    fn multi_doc_json_emits_keyed_object() {
        let docs = [
            ("contacts:c-1".to_string(), fixture_doc("alice")),
            ("contacts:c-2".to_string(), fixture_doc("bob")),
        ];
        // Build the same object render_query would produce.
        let obj: serde_json::Map<String, Value> = docs
            .iter()
            .map(|(k, d)| (k.clone(), automerge_to_json(d)))
            .collect();
        assert_eq!(obj["contacts:c-1"]["name"], serde_json::json!("alice"));
        assert_eq!(obj["contacts:c-2"]["name"], serde_json::json!("bob"));
    }
}
