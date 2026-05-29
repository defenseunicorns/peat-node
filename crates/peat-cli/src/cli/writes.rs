//! Shared helpers for the write subcommands (`create` / `update` / `delete`).
//!
//! Input parsing: `--from <PATH>` (file or stdin) and `--set path=value`
//! collapse here so the per-subcommand handlers stay focused on their
//! distinct semantics. Type coercion on `--set` values is intentionally
//! narrow — bool / null / number / string only. Arrays and nested objects
//! come from `--from`.

use peat_schema::type_registry::{BuiltinRegistry, TypeRegistry};
use serde_json::{Map, Value};
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use crate::cli::CliError;

/// Validate a proposed JSON document against the type registered for
/// `collection` in `peat-schema`. Returns `Ok(())` when:
///
/// - the collection has no registered type (unknown shape — caller decides
///   whether to accept structurally), or
/// - the registered type's validator accepts the JSON.
///
/// Returns `Err(CliError::Malformed)` (exit 4 per ADR-001) when the
/// registered validator rejects the JSON. The error message carries the
/// type name and the underlying validation error.
///
/// Callers gate this on the `--no-validate` flag — when set, skip the
/// call entirely and emit a stderr warning.
pub fn validate_against_schema(collection: &str, value: &Value) -> Result<(), CliError> {
    // Construct the builtin registry per call. Cost is one HashMap build
    // (~5 entries today). Cheap relative to the rest of the write path.
    // Cache via OnceLock if it becomes hot.
    let registry = BuiltinRegistry::with_peat_schema_types();
    let Some(desc) = registry.for_collection(collection) else {
        // Unknown collection → accept structurally (per ADR-001 §"Write
        // semantics" → "Validation": application-defined types are
        // accepted without validation).
        return Ok(());
    };
    (desc.validate_json)(value).map_err(|e| {
        CliError::Malformed(format!(
            "schema validation failed for {} document: {e}",
            desc.name
        ))
    })
}

/// Fixed-period wait that approximates ADR-001 `--wait-for-sync` semantics.
/// peat-mesh does not yet surface a per-write "acknowledged by N peers"
/// signal — this gives the local sync coordinator a budget to push the
/// new op to connected peers before the CLI exits. Real ack tracking
/// lands when upstream exposes it.
pub const POST_WRITE_SYNC_WAIT: Duration = Duration::from_millis(750);

/// Read the `--from` argument: a path, or `-` for stdin.
pub fn read_from(path: &Path) -> Result<Value, CliError> {
    let raw = if path.as_os_str() == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| CliError::Generic(format!("read stdin: {e}")))?;
        buf
    } else {
        std::fs::read_to_string(path)
            .map_err(|e| CliError::Malformed(format!("read `{}`: {e}", path.display())))?
    };
    serde_json::from_str(&raw).map_err(|e| {
        CliError::Malformed(format!(
            "parse `{}` as JSON: {e}",
            if path.as_os_str() == "-" {
                "<stdin>".to_string()
            } else {
                path.display().to_string()
            }
        ))
    })
}

/// Build a JSON object from `--set path=value` pairs applied to the given
/// starting value. Paths are dot-separated; intermediate objects auto-create.
///
/// Value coercion order: `null` → null, `true`/`false` → bool, integer → i64,
/// float → f64, fallback → string.
pub fn apply_sets(mut base: Value, sets: &[String]) -> Result<Value, CliError> {
    for s in sets {
        apply_set(&mut base, s)?;
    }
    Ok(base)
}

fn apply_set(value: &mut Value, expr: &str) -> Result<(), CliError> {
    let (path, raw_val) = expr
        .split_once('=')
        .ok_or_else(|| CliError::Malformed(format!("--set `{expr}`: expected `path=value`")))?;
    if path.is_empty() {
        return Err(CliError::Malformed(format!("--set `{expr}`: empty path")));
    }
    let parts: Vec<&str> = path.split('.').collect();
    if parts.iter().any(|p| p.is_empty()) {
        return Err(CliError::Malformed(format!(
            "--set `{expr}`: empty path segment"
        )));
    }
    set_path(value, &parts, coerce(raw_val))
}

fn set_path(value: &mut Value, parts: &[&str], new_val: Value) -> Result<(), CliError> {
    // Promote a non-object root into an object so we have somewhere to put a
    // path. Top-level scalar update via path doesn't make sense for v1.
    if !value.is_object() {
        *value = Value::Object(Map::new());
    }
    let map = value.as_object_mut().unwrap();
    if let [last] = parts {
        map.insert((*last).to_string(), new_val);
        return Ok(());
    }
    let (head, tail) = parts.split_first().unwrap();
    let entry = map
        .entry((*head).to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    set_path(entry, tail, new_val)
}

fn coerce(raw: &str) -> Value {
    match raw {
        "null" => return Value::Null,
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    if let Ok(n) = raw.parse::<i64>() {
        return Value::from(n);
    }
    if let Ok(n) = raw.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(n) {
            return Value::Number(num);
        }
    }
    Value::String(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_one_top_level_field() {
        let out = apply_sets(json!({}), &["name=alice".into()]).unwrap();
        assert_eq!(out, json!({"name": "alice"}));
    }

    #[test]
    fn set_dotted_path_creates_intermediate_objects() {
        let out = apply_sets(json!({}), &["position.lat=40.7128".into()]).unwrap();
        assert_eq!(out, json!({"position": {"lat": 40.7128}}));
    }

    #[test]
    fn set_overrides_existing_field() {
        let out = apply_sets(json!({"name": "alice"}), &["name=bob".into()]).unwrap();
        assert_eq!(out, json!({"name": "bob"}));
    }

    #[test]
    fn set_coerces_typed_values() {
        let out = apply_sets(
            json!({}),
            &[
                "n=42".into(),
                "f=2.5".into(),
                "b=true".into(),
                "x=null".into(),
                "s=hello".into(),
            ],
        )
        .unwrap();
        assert_eq!(
            out,
            json!({"n": 42, "f": 2.5, "b": true, "x": null, "s": "hello"})
        );
    }

    #[test]
    fn set_rejects_missing_equals() {
        let err = apply_sets(json!({}), &["just-a-path".into()]).unwrap_err();
        assert_eq!(err.exit_code(), 4);
    }

    #[test]
    fn set_rejects_empty_path() {
        let err = apply_sets(json!({}), &["=value".into()]).unwrap_err();
        assert_eq!(err.exit_code(), 4);
    }

    #[test]
    fn set_rejects_empty_segment() {
        let err = apply_sets(json!({}), &["a..b=v".into()]).unwrap_err();
        assert_eq!(err.exit_code(), 4);
    }

    #[test]
    fn set_replaces_non_object_root() {
        // If the base is a scalar (rare; would happen if someone read a doc
        // whose root is a scalar — unusual for Peat), apply_sets promotes it
        // to an object rather than panicking.
        let out = apply_sets(json!("scalar"), &["name=alice".into()]).unwrap();
        assert_eq!(out, json!({"name": "alice"}));
    }
}
