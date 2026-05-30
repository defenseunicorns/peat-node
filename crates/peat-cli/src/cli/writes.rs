//! Shared helpers for the write subcommands (`create` / `update` / `delete`).
//!
//! Input parsing: `--from <PATH>` (file or stdin) and `--set path=value`
//! collapse here so the per-subcommand handlers stay focused on their
//! distinct semantics. Type coercion on `--set` values is intentionally
//! narrow — bool / null / number / string only. Arrays and nested objects
//! come from `--from`.

use peat_schema::type_registry::{BuiltinRegistry, TypeRegistry};
use serde_json::{json, Map, Value};
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
/// Why a hardcoded per-collection table rather than a generic
/// `FieldFormat`-driven default function: `FieldFormat::JsonString` is
/// ambiguous — `Capability::metadata_json` (real `string`, default `""`)
/// and `NodeConfig::operator_binding` (real `optional HumanMachinePair`,
/// default `null`) both carry the same `JsonString` format in the
/// rc.19 registry. A descriptor-driven default would be wrong for one
/// or the other; an explicit per-type table is correct for both. The
/// table is small (5 types) and stable, and `defaults_match_proto3_round_trip`
/// in this module's tests catches drift against the registry's
/// validators.
///
/// Long-term shape: `peat-schema` exposes a per-type `proto3_zero()` on
/// `TypeDescriptor` and this function becomes registry-driven. Tracked
/// as a follow-up at the bottom of [peat-node#112].
///
/// [peat-node#112]: https://github.com/defenseunicorns/peat-node/issues/112
pub fn apply_proto3_defaults(collection: &str, mut value: Value) -> Value {
    let Some(defaults) = proto3_defaults_for(collection) else {
        return value;
    };
    let Value::Object(ref mut user) = value else {
        // Caller handed us a non-object (e.g. they `--from`-loaded a
        // scalar). Validation will reject; nothing for us to merge.
        return value;
    };
    let Value::Object(default_map) = defaults else {
        return Value::Object(user.clone());
    };
    for (k, default_v) in default_map {
        user.entry(k).or_insert(default_v);
    }
    value
}

/// Per-collection proto3 zero map for the 5 builtin peat-schema types
/// (peat-schema rc.19). Returns `None` for unknown collections.
fn proto3_defaults_for(collection: &str) -> Option<Value> {
    match collection {
        "capabilities" => Some(json!({
            "id": "",
            "name": "",
            "capability_type": 0,
            "confidence": 0.0,
            "metadata_json": "",
            "registered_at": null,
        })),
        "node-configs" => Some(json!({
            "id": "",
            "platform_type": "",
            "capabilities": [],
            "comm_range_m": 0.0,
            "max_speed_mps": 0.0,
            "operator_binding": null,
            "created_at": null,
        })),
        "node-states" => Some(json!({
            "position": null,
            "fuel_minutes": 0,
            "health": 0,
            "phase": 0,
            "cell_id": null,
            "zone_id": null,
            "timestamp": null,
        })),
        "cell-configs" => Some(json!({
            "id": "",
            "max_size": 0,
            "min_size": 0,
            "created_at": null,
        })),
        "cell-states" => Some(json!({
            "config": null,
            "leader_id": null,
            "members": [],
            "capabilities": [],
            "platoon_id": null,
            "timestamp": null,
        })),
        _ => None,
    }
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
    fn defaults_pure_pass_prost_deserialize_for_every_registered_type() {
        // Drift catcher (peat-node#112): proves each per-collection
        // proto3-defaults map round-trips through prost's Deserialize.
        // If peat-schema adds a required field to one of the 5 builtin
        // types and this crate's `proto3_defaults_for` lags, this test
        // fails with prost's "missing field" error pointing at the new
        // field — the same error operators would otherwise see when
        // running `peat create <collection> --set ...`.
        for collection in [
            "capabilities",
            "node-configs",
            "node-states",
            "cell-configs",
            "cell-states",
        ] {
            let defaults = apply_proto3_defaults(collection, Value::Object(Default::default()));
            // Call validate_json directly so prost's deserialize error,
            // not the validator's "MissingField", surfaces if the
            // defaults are incomplete.
            let registry = BuiltinRegistry::with_peat_schema_types();
            let desc = registry
                .for_collection(collection)
                .unwrap_or_else(|| panic!("{collection} not registered"));
            // Validators reject empty required strings (e.g. id), but
            // that's an `Err(MissingField)` from the validator AFTER a
            // successful prost deserialize. Drift would surface as an
            // `Err(InvalidValue("could not deserialise as …"))` from
            // the deserialize step — the substring is the signature.
            if let Err(err) = (desc.validate_json)(&defaults) {
                let msg = format!("{err}");
                assert!(
                    !msg.contains("could not deserialise"),
                    "defaults for `{collection}` are out of sync with the registry: {msg}"
                );
            }
        }
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
