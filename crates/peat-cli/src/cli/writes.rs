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

/// Apply proto3 zero-defaults for every field of a registered peat-schema
/// type, then merge the caller's value on top (caller wins per top-level
/// field). For unknown collections the value is returned unchanged.
///
/// Closes peat-node#112: `--set` partial payloads on registered types
/// were failing prost's strict `Deserialize` derive with "missing field"
/// errors because prost-generated impls have no `#[serde(default)]`. The
/// fix is to give prost the wire zero for every field, then layer the
/// operator's `--set` overlay on top so partial intent survives.
///
/// The defaults come from `TypeDescriptor::proto3_zero()` (peat#953 /
/// peat#954, shipped in peat-schema rc.21). The descriptor exposes
/// the canonical wire zero for the type, generated from the same
/// codegen path as the validator — so the round-trip property
/// (`proto3_zero` ⊆ `validate_json::Ok`) holds by construction and
/// can't drift.
pub fn apply_proto3_defaults(collection: &str, mut value: Value) -> Value {
    let registry = BuiltinRegistry::with_peat_schema_types();
    let Some(desc) = registry.for_collection(collection) else {
        return value;
    };
    let Value::Object(ref mut user) = value else {
        // Caller handed us a non-object (e.g. they `--from`-loaded a
        // scalar). Validation will reject; nothing for us to merge.
        return value;
    };
    let Value::Object(default_map) = desc.proto3_zero() else {
        // `proto3_zero` is contracted to return a JSON object for
        // every registered type; treat anything else as a no-op merge
        // rather than panicking — validation will surface the misshape.
        return Value::Object(user.clone());
    };
    for (k, default_v) in default_map {
        user.entry(k).or_insert(default_v);
    }
    value
}

/// Brief settle window used inside `--wait-for-sync` after the
/// `MeshSession::close()` graceful shutdown. The QUIC CONNECTION_CLOSE
/// guarantees delivery; this residual gives the peer's tokio runtime a
/// moment to apply the change to its store before the CLI returns.
pub const POST_WRITE_SYNC_WAIT: Duration = Duration::from_millis(250);

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
    fn defaults_underlay_keeps_user_set_values_winning() {
        let user = json!({"name": "thermal", "confidence": 0.5});
        let merged = apply_proto3_defaults("capabilities", user);
        let obj = merged.as_object().unwrap();
        assert_eq!(obj["id"], json!(""), "default fills missing field");
        assert_eq!(obj["name"], json!("thermal"), "user value wins");
        assert_eq!(obj["confidence"], json!(0.5), "user value wins on numerics");
        assert_eq!(
            obj["registered_at"],
            json!(null),
            "optional message defaults to null"
        );
    }

    #[test]
    fn defaults_underlay_is_no_op_for_unknown_collection() {
        let user = json!({"name": "alice"});
        let merged = apply_proto3_defaults("contacts", user.clone());
        assert_eq!(merged, user, "unknown collections pass through unchanged");
    }

    #[test]
    fn defaults_plus_min_required_validates_for_capability() {
        let user = json!({"id": "cap-1", "name": "thermal"});
        let merged = apply_proto3_defaults("capabilities", user);
        validate_against_schema("capabilities", &merged).expect("min Capability validates");
    }

    #[test]
    fn defaults_plus_min_required_validates_for_node_config() {
        let user = json!({
            "id": "node-1",
            "platform_type": "rover",
            "comm_range_m": 1500.0,
            "max_speed_mps": 12.0,
        });
        let merged = apply_proto3_defaults("node-configs", user);
        validate_against_schema("node-configs", &merged).expect("min NodeConfig validates");
    }

    #[test]
    fn defaults_plus_min_required_validates_for_cell_config() {
        let user = json!({"id": "cc-1", "min_size": 2, "max_size": 8});
        let merged = apply_proto3_defaults("cell-configs", user);
        validate_against_schema("cell-configs", &merged).expect("min CellConfig validates");
    }

    #[test]
    fn defaults_alone_validate_for_node_state_and_cell_state() {
        // NodeState + CellState have no required scalar fields; pure
        // defaults are a valid document.
        let ns = apply_proto3_defaults("node-states", Value::Object(Default::default()));
        validate_against_schema("node-states", &ns).expect("default NodeState validates");
        let cs = apply_proto3_defaults("cell-states", Value::Object(Default::default()));
        validate_against_schema("cell-states", &cs).expect("default CellState validates");
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
