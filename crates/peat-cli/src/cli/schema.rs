//! `peat schema` — inspect the peat-schema type registry without joining
//! the mesh (ADR-001 §Command surface, schema-discovery follow-on).
//!
//! Two sub-subcommands:
//!   - `peat schema list` — every registered type, one row per type.
//!   - `peat schema describe <TYPE>` — full field-level shape for one
//!     type, addressed by canonical collection (e.g. `capabilities`)
//!     or canonical id (e.g. `peat.capability.v1.Capability`).
//!
//! Both are local commands: no creds, no mesh handshake, no transport.
//! The registry is built in-process from `BuiltinRegistry::with_peat_schema_types()`
//! — the operator gets a deterministic answer offline.

use clap::{Args, Subcommand};
use peat_schema::type_registry::{
    BuiltinRegistry, FieldFormat, TypeDescriptor, TypeId, TypeRegistry,
};
use serde_json::{json, Value};

use crate::cli::output::OutputFormat;
use crate::cli::CliError;

#[derive(Debug, Args)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub sub: SchemaSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum SchemaSubcommand {
    /// List every type the CLI knows about.
    List,
    /// Describe one type's fields, addressed by collection name or canonical id.
    Describe {
        /// Collection name (`capabilities`) or canonical id (`peat.capability.v1.Capability`).
        target: String,
    },
}

pub fn run(args: SchemaArgs, output: OutputFormat) -> Result<(), CliError> {
    let registry = BuiltinRegistry::with_peat_schema_types();
    match args.sub {
        SchemaSubcommand::List => render_list(&registry, output),
        SchemaSubcommand::Describe { target } => render_describe(&registry, &target, output),
    }
}

/// Resolve `target` to a descriptor by trying the collection-name map
/// first, then the canonical-id map. The two namespaces are disjoint in
/// the rc.21 registry (collections are kebab-case, ids are dotted FQNs).
fn resolve<'a>(registry: &'a BuiltinRegistry, target: &str) -> Option<&'a TypeDescriptor> {
    registry
        .for_collection(target)
        .or_else(|| registry.get(&TypeId::new(target)))
}

fn render_list(registry: &BuiltinRegistry, output: OutputFormat) -> Result<(), CliError> {
    let mut entries: Vec<&TypeDescriptor> = registry.iter().collect();
    // Stable order for scripts. `iter()` returns HashMap order otherwise.
    entries.sort_by(|a, b| a.id.as_str().cmp(b.id.as_str()));

    match output {
        OutputFormat::Json => {
            let arr: Value = entries.iter().map(|d| descriptor_json(d)).collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&arr)
                    .map_err(|e| CliError::Generic(format!("serialize: {e}")))?
            );
        }
        OutputFormat::Ndjson => {
            for d in &entries {
                let line = serde_json::to_string(&descriptor_json(d))
                    .map_err(|e| CliError::Generic(format!("serialize: {e}")))?;
                println!("{line}");
            }
        }
        OutputFormat::Text => {
            // Column-aligned table: collection, type name, version, id.
            // No external column-formatting dep; this matches the
            // typed-render style of `query --output text`.
            let coll_w = entries
                .iter()
                .map(|d| d.canonical_collection.as_deref().unwrap_or("-").len())
                .max()
                .unwrap_or(0)
                .max("COLLECTION".len());
            let name_w = entries
                .iter()
                .map(|d| d.name.len())
                .max()
                .unwrap_or(0)
                .max("TYPE".len());
            println!(
                "{coll:<coll_w$}  {name:<name_w$}  {ver:<7}  ID",
                coll = "COLLECTION",
                name = "TYPE",
                ver = "VERSION"
            );
            for d in &entries {
                let coll = d.canonical_collection.as_deref().unwrap_or("-");
                println!(
                    "{coll:<coll_w$}  {name:<name_w$}  {ver:<7}  {id}",
                    name = d.name,
                    ver = d.version,
                    id = d.id.as_str()
                );
            }
        }
    }
    Ok(())
}

fn render_describe(
    registry: &BuiltinRegistry,
    target: &str,
    output: OutputFormat,
) -> Result<(), CliError> {
    let desc = resolve(registry, target).ok_or_else(|| {
        CliError::Malformed(format!(
            "no registered type matches `{target}` (try `peat schema list`)"
        ))
    })?;

    match output {
        OutputFormat::Json | OutputFormat::Ndjson => {
            let v = descriptor_json(desc);
            let s = match output {
                OutputFormat::Json => serde_json::to_string_pretty(&v),
                _ => serde_json::to_string(&v),
            }
            .map_err(|e| CliError::Generic(format!("serialize: {e}")))?;
            println!("{s}");
        }
        OutputFormat::Text => {
            println!("{} ({})", desc.name, desc.version);
            println!("  id:         {}", desc.id.as_str());
            println!(
                "  collection: {}",
                desc.canonical_collection.as_deref().unwrap_or("-")
            );
            println!("  fields:");
            let label_w = desc.fields.iter().map(|f| f.label.len()).max().unwrap_or(0);
            let name_w = desc.fields.iter().map(|f| f.name.len()).max().unwrap_or(0);
            for f in &desc.fields {
                println!(
                    "    {label:<label_w$}  {name:<name_w$}  {fmt}",
                    label = f.label,
                    name = f.name,
                    fmt = format_field_format(&f.format),
                );
            }
        }
    }
    Ok(())
}

/// Stable JSON shape for one type descriptor. The `--output json` /
/// `--output ndjson` contract for `schema list` and `schema describe`.
fn descriptor_json(desc: &TypeDescriptor) -> Value {
    json!({
        "id": desc.id.as_str(),
        "name": desc.name,
        "version": desc.version,
        "collection": desc.canonical_collection,
        "fields": desc.fields.iter().map(|f| json!({
            "name": f.name,
            "label": f.label,
            "format": format_field_format(&f.format),
        })).collect::<Vec<_>>(),
    })
}

/// Short string representation of a `FieldFormat`. The `FieldFormat`
/// enum is `#[non_exhaustive]`, so the catchall keeps us building if
/// upstream adds a variant — surfaced as `unknown` in the meantime.
fn format_field_format(fmt: &FieldFormat) -> String {
    match fmt {
        FieldFormat::Text => "text".into(),
        FieldFormat::Number { unit: Some(u) } => format!("number[{u}]"),
        FieldFormat::Number { unit: None } => "number".into(),
        FieldFormat::Percentage => "percentage".into(),
        FieldFormat::Boolean => "boolean".into(),
        FieldFormat::Timestamp => "timestamp".into(),
        FieldFormat::Position => "position".into(),
        FieldFormat::Enum { variants } => format!("enum[{}]", variants.join("|")),
        FieldFormat::Nested { nested_type_id } => format!("nested[{}]", nested_type_id.as_str()),
        FieldFormat::List { item_format } => format!("list[{}]", format_field_format(item_format)),
        FieldFormat::JsonString => "json-string".into(),
        FieldFormat::BlobRef => "blob-ref".into(),
        _ => "unknown".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_by_collection_name() {
        let r = BuiltinRegistry::with_peat_schema_types();
        let d = resolve(&r, "capabilities").expect("capabilities resolves");
        assert_eq!(d.name, "Capability");
    }

    #[test]
    fn resolves_by_canonical_id() {
        let r = BuiltinRegistry::with_peat_schema_types();
        let d = resolve(&r, "peat.capability.v1.Capability").expect("id resolves");
        assert_eq!(d.canonical_collection.as_deref(), Some("capabilities"));
    }

    #[test]
    fn unknown_target_returns_none() {
        let r = BuiltinRegistry::with_peat_schema_types();
        assert!(resolve(&r, "definitely-not-a-thing").is_none());
    }

    #[test]
    fn descriptor_json_has_stable_keys() {
        let r = BuiltinRegistry::with_peat_schema_types();
        let d = resolve(&r, "capabilities").unwrap();
        let v = descriptor_json(d);
        let obj = v.as_object().unwrap();
        for key in ["id", "name", "version", "collection", "fields"] {
            assert!(obj.contains_key(key), "missing key `{key}` in {v}");
        }
        assert!(v["fields"].is_array());
    }

    #[test]
    fn format_renderer_covers_every_variant_used_by_builtins() {
        // Drive `format_field_format` across every field of every
        // registered type. The peat-schema upstream introducing a new
        // FieldFormat variant would surface here as `"unknown"` —
        // catches it before operators see it on their `schema describe`
        // output.
        let r = BuiltinRegistry::with_peat_schema_types();
        for d in r.iter() {
            for f in &d.fields {
                let rendered = format_field_format(&f.format);
                assert_ne!(
                    rendered, "unknown",
                    "unrecognised FieldFormat on {}::{}: {:?}",
                    d.name, f.name, f.format
                );
            }
        }
    }
}
