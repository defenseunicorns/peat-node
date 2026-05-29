//! Output formatters.
//!
//! ADR-001 §"Document rendering" mandates a typed-vs-generic dispatch
//! model. The peat-schema runtime type registry (peat#946/#947) provides
//! the typed path; this module wires it in for `text` mode of `query`:
//!
//! - **Known collection** (`for_collection(key.collection_prefix)`
//!   returns a `TypeDescriptor`) → typed render using
//!   `TypeDescriptor.fields` for label / order / format dispatch
//!   (`FieldFormat::Percentage`, `Position`, `Enum`, `List`, etc.).
//! - **Unknown collection** → generic structural JSON via
//!   `automerge_to_json` (the ADR's "renderer-not-found fallback" path).
//!
//! `json` and `ndjson` output formats are the **stable contract surface**
//! for downstream scripts — they always emit the generic structural JSON
//! shape regardless of registry hits. ADR-001 §"Document rendering" calls
//! this out so consumers of `peat … --output json` see the same shape
//! whether or not the document's type happens to be registered.
//!
//! Stream discipline (ADR-001 §"Shell integration discipline"):
//!   - All document data goes to stdout.
//!   - Logs / status / errors go to stderr (wired in main.rs via
//!     tracing_subscriber).
//!   - `text` is human-readable and may evolve; `json` / `ndjson` are
//!     stable.

use automerge::Automerge;
use clap::ValueEnum;
use peat_mesh::storage::json_convert::automerge_to_json;
use peat_schema::type_registry::{BuiltinRegistry, FieldFormat, TypeDescriptor, TypeRegistry};
use serde_json::Value;

use crate::cli::CliError;

/// Extract the collection name from a store key of the form
/// `collection:doc_id`. Returns `None` if the key has no separator.
fn collection_of(key: &str) -> Option<&str> {
    key.split_once(':').map(|(c, _)| c)
}

/// Format a single field value per its `FieldFormat` hint. Falls back to
/// the verbatim JSON rendering when the value shape doesn't match the
/// hint (defensive — operators see *something* rather than nothing).
fn format_field_value(value: Option<&Value>, fmt: &FieldFormat) -> String {
    let v = match value {
        None | Some(Value::Null) => return "—".to_string(),
        Some(v) => v,
    };
    match fmt {
        FieldFormat::Text => match v {
            Value::String(s) => s.clone(),
            _ => v.to_string(),
        },
        FieldFormat::Number { unit } => match (v.as_f64(), unit) {
            (Some(n), Some(u)) => format!("{n} {u}"),
            (Some(n), None) => format!("{n}"),
            _ => v.to_string(),
        },
        FieldFormat::Percentage => match v.as_f64() {
            Some(n) => format!("{:.1}%", n * 100.0),
            None => v.to_string(),
        },
        FieldFormat::Boolean => match v.as_bool() {
            Some(b) => b.to_string(),
            None => v.to_string(),
        },
        FieldFormat::Timestamp => {
            // Timestamps come through as the proto3 Timestamp message
            // shape: {"seconds": …, "nanos": …}. v1 emits the raw JSON;
            // a richer RFC 3339 conversion can layer in later.
            serde_json::to_string(v).unwrap_or_else(|_| v.to_string())
        }
        FieldFormat::Position => match v.as_object() {
            Some(obj) => {
                let lat = obj.get("latitude").and_then(|x| x.as_f64()).unwrap_or(0.0);
                let lon = obj.get("longitude").and_then(|x| x.as_f64()).unwrap_or(0.0);
                let alt = obj.get("altitude").and_then(|x| x.as_f64()).unwrap_or(0.0);
                format!("{lat:.4}°, {lon:.4}°, {alt:.0}m")
            }
            None => v.to_string(),
        },
        FieldFormat::Enum { variants } => match v.as_u64() {
            Some(idx) => variants
                .get(idx as usize)
                .map(|s| (*s).to_string())
                .unwrap_or_else(|| format!("unknown({idx})")),
            None => v.to_string(),
        },
        FieldFormat::Nested { .. } => {
            // v1: pretty-print as JSON. Recursing through the registry to
            // a nested TypeDescriptor's `fields` is a richer follow-on.
            serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
        }
        FieldFormat::List { item_format } => match v.as_array() {
            Some(arr) if arr.is_empty() => "(empty)".to_string(),
            Some(arr) => {
                let parts: Vec<String> = arr
                    .iter()
                    .map(|x| format_field_value(Some(x), item_format))
                    .collect();
                parts.join(", ")
            }
            None => v.to_string(),
        },
        FieldFormat::JsonString => match v.as_str() {
            Some(s) => {
                if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                    serde_json::to_string(&parsed).unwrap_or_else(|_| s.to_string())
                } else {
                    s.to_string()
                }
            }
            None => v.to_string(),
        },
        FieldFormat::BlobRef => {
            // Renderer-only summary: don't dereference. v1 just dumps the
            // metadata as JSON; a future format pass can pull size/hash
            // into a "<blob:N sha256:…>" form.
            serde_json::to_string(v).unwrap_or_else(|_| v.to_string())
        }
        // FieldFormat is `#[non_exhaustive]` so we wildcard-fall-through to
        // a verbatim rendering for any future variant we don't recognise.
        _ => v.to_string(),
    }
}

/// Build a typed-render string for one document given its descriptor.
/// Format: type name on a line, then `<label> : <value>` pairs in
/// canonical field order, right-aligned label column.
fn render_typed_doc(doc_json: &Value, desc: &TypeDescriptor) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out, "{}", desc.name);
    let label_width = desc.fields.iter().map(|f| f.label.len()).max().unwrap_or(0);
    let obj = doc_json.as_object();
    for field in &desc.fields {
        let value = obj.and_then(|o| o.get(field.name));
        let formatted = format_field_value(value, &field.format);
        let label = field.label;
        let _ = writeln!(out, "  {label:>label_width$} : {formatted}");
    }
    out
}

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

/// Render a single observe event — one document change emitted on each
/// `subscribe_to_observer_changes` signal. Uses ndjson semantics regardless
/// of the configured `OutputFormat` for the streaming subcommands, because
/// stream consumers want one-record-per-line. `text` mode adds a leading
/// `key=` prefix for human readability; `json` produces the bare JSON value;
/// `ndjson` adds the key as a top-level field for `jq` selection.
pub fn render_observe_event(key: &str, doc: &Automerge, fmt: OutputFormat) -> Result<(), CliError> {
    let doc_json = automerge_to_json(doc);
    let line = match fmt {
        OutputFormat::Text => {
            let body = serde_json::to_string(&doc_json)
                .map_err(|e| CliError::Generic(format!("serialize JSON: {e}")))?;
            format!("{key} {body}")
        }
        OutputFormat::Json => serde_json::to_string(&doc_json)
            .map_err(|e| CliError::Generic(format!("serialize JSON: {e}")))?,
        OutputFormat::Ndjson => {
            let mut obj = serde_json::Map::with_capacity(2);
            obj.insert("key".into(), Value::String(key.to_string()));
            obj.insert("doc".into(), doc_json);
            serde_json::to_string(&Value::Object(obj))
                .map_err(|e| CliError::Generic(format!("serialize JSON: {e}")))?
        }
    };
    write_stream_line(&line)
}

/// Render a "deleted" event observed via the change stream: a key fired the
/// changes observer but the document is gone (tombstoned between event and
/// our read). Emits a structurally distinct record so CDC consumers see
/// deletes, not just upserts.
pub fn render_observe_deleted(key: &str, fmt: OutputFormat) -> Result<(), CliError> {
    let line = match fmt {
        OutputFormat::Text => format!("{key} <deleted>"),
        OutputFormat::Json => serde_json::to_string(&serde_json::json!({"deleted": true}))
            .map_err(|e| CliError::Generic(format!("serialize JSON: {e}")))?,
        OutputFormat::Ndjson => {
            let mut obj = serde_json::Map::with_capacity(2);
            obj.insert("key".into(), Value::String(key.to_string()));
            obj.insert("deleted".into(), Value::Bool(true));
            serde_json::to_string(&Value::Object(obj))
                .map_err(|e| CliError::Generic(format!("serialize JSON: {e}")))?
        }
    };
    write_stream_line(&line)
}

/// Single point that writes a line to stdout for the streaming subcommands.
/// `writeln!` (not `println!`) so we can intercept BrokenPipe instead of
/// panicking, and map it to the dedicated [`CliError::BrokenPipe`] variant
/// the observe loop and main.rs treat as a clean exit per ADR-001 §"Shell
/// integration discipline".
fn write_stream_line(line: &str) -> Result<(), CliError> {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    match writeln!(handle, "{line}") {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Err(CliError::BrokenPipe),
        Err(e) => Err(CliError::Generic(format!("stdout write: {e}"))),
    }
}

/// Render the result of a `query` invocation.
///
/// `docs` is the list returned by the store: each entry is `(key, Automerge)`
/// where `key` is the full `collection:doc_id` form.
///
/// In `text` mode, when the document's collection resolves to a known
/// `peat-schema` type, the typed renderer dispatches off `TypeDescriptor.fields`
/// — field labels, ordering, format hints. Unknown collections fall through to
/// the structurally-faithful generic JSON walk. `json` and `ndjson` modes are
/// always the structural JSON form regardless of registry hits — they're the
/// stable contract surface for downstream scripts (ADR-001 §"Document
/// rendering" → "Output formats").
pub fn render_query(docs: &[(String, Automerge)], fmt: OutputFormat) -> Result<(), CliError> {
    let registry = BuiltinRegistry::with_peat_schema_types();
    match fmt {
        OutputFormat::Text => {
            // Try typed rendering per doc when the collection is known.
            // Single-doc + known type → typed render directly.
            // Anything else → fall through to structural JSON.
            if let [(key, doc)] = docs {
                if let Some(desc) = collection_of(key).and_then(|c| registry.for_collection(c)) {
                    let json = automerge_to_json(doc);
                    let rendered = render_typed_doc(&json, desc);
                    print!("{rendered}");
                    return Ok(());
                }
            }
            // Multi-doc OR unknown collection → existing structural JSON
            // path. (Multi-doc typed rendering is a follow-on; the issue
            // is producing readable multi-doc output, not a registry gap.)
            let value = match docs {
                [(_, doc)] => automerge_to_json(doc),
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
        OutputFormat::Json => {
            // Stable structural contract — no typed dispatch.
            let value = match docs {
                [(_, doc)] => automerge_to_json(doc),
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

    // --- Typed-renderer dispatch tests (peat#946 / peat#947 adoption) ---

    #[test]
    fn collection_of_splits_on_first_colon() {
        assert_eq!(collection_of("capabilities:cap-1"), Some("capabilities"));
        assert_eq!(collection_of("node-states:n-1"), Some("node-states"));
        // Multi-colon: only the first separator counts; doc_id may have colons.
        assert_eq!(collection_of("contacts:tenant:a:c-1"), Some("contacts"));
        assert_eq!(collection_of("no-colon-here"), None);
    }

    #[test]
    fn format_percentage_renders_with_unit() {
        assert_eq!(
            format_field_value(Some(&serde_json::json!(0.95)), &FieldFormat::Percentage),
            "95.0%"
        );
        assert_eq!(
            format_field_value(Some(&serde_json::json!(0.0)), &FieldFormat::Percentage),
            "0.0%"
        );
    }

    #[test]
    fn format_number_with_unit_appends_suffix() {
        assert_eq!(
            format_field_value(
                Some(&serde_json::json!(10.5)),
                &FieldFormat::Number { unit: Some("m/s") }
            ),
            "10.5 m/s"
        );
        assert_eq!(
            format_field_value(
                Some(&serde_json::json!(42)),
                &FieldFormat::Number { unit: None }
            ),
            "42"
        );
    }

    #[test]
    fn format_enum_resolves_variant_by_index() {
        let variants: &[&str] = &["Unspecified", "Sensor", "Compute"];
        let fmt = FieldFormat::Enum { variants };
        assert_eq!(
            format_field_value(Some(&serde_json::json!(1)), &fmt),
            "Sensor"
        );
        assert_eq!(
            format_field_value(Some(&serde_json::json!(2)), &fmt),
            "Compute"
        );
        // Out-of-range index surfaces "unknown(N)" so renderers never silently
        // drop unrecognised enum values.
        assert_eq!(
            format_field_value(Some(&serde_json::json!(99)), &fmt),
            "unknown(99)"
        );
    }

    #[test]
    fn format_position_renders_lat_lon_alt() {
        let pos = serde_json::json!({
            "latitude": 40.7128,
            "longitude": -74.0060,
            "altitude": 10.0,
        });
        let s = format_field_value(Some(&pos), &FieldFormat::Position);
        assert!(s.contains("40.7128°"), "got {s}");
        assert!(s.contains("-74.0060°"), "got {s}");
        assert!(s.contains("10m"), "got {s}");
    }

    #[test]
    fn format_list_joins_items() {
        let v = serde_json::json!(["alpha", "beta", "gamma"]);
        let fmt = FieldFormat::List {
            item_format: Box::new(FieldFormat::Text),
        };
        assert_eq!(format_field_value(Some(&v), &fmt), "alpha, beta, gamma");
        // Empty list gets a sentinel rather than a confusing blank.
        let empty = serde_json::json!([]);
        assert_eq!(format_field_value(Some(&empty), &fmt), "(empty)");
    }

    #[test]
    fn format_null_is_em_dash() {
        // Null / None values render as an em dash so operators see "field
        // is absent" rather than the literal "null".
        assert_eq!(format_field_value(None, &FieldFormat::Text), "—");
        assert_eq!(
            format_field_value(Some(&serde_json::Value::Null), &FieldFormat::Percentage),
            "—"
        );
    }

    #[test]
    fn render_typed_doc_emits_capability_with_labels() {
        // Round-trip: build a Capability-shaped JSON, render through the
        // registry's descriptor, assert the typed labels + format hints
        // show up in the output.
        let registry = BuiltinRegistry::with_peat_schema_types();
        let desc = registry.for_collection("capabilities").unwrap();
        let doc = serde_json::json!({
            "id": "cap-1",
            "name": "thermal-sensor",
            "capability_type": 1,
            "confidence": 0.95,
            "metadata_json": "{}",
            "registered_at": null,
        });
        let rendered = render_typed_doc(&doc, desc);
        assert!(rendered.starts_with("Capability\n"), "got: {rendered}");
        assert!(rendered.contains("ID : cap-1"), "got: {rendered}");
        assert!(rendered.contains("Type : Sensor"), "got: {rendered}");
        assert!(rendered.contains("Confidence : 95.0%"), "got: {rendered}");
        // Null registered_at renders as em dash.
        assert!(rendered.contains("Registered : —"), "got: {rendered}");
    }
}
