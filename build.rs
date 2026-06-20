use std::fs;

/// Extract a dependency's resolved version from `Cargo.lock` so peat-node can
/// log its dependency stack at startup. Returns "unknown" if not found.
fn locked_version(lock: &str, crate_name: &str) -> String {
    let needle = format!("name = \"{crate_name}\"");
    if let Some(idx) = lock.find(&needle) {
        // Within a `[[package]]` block the `version = "..."` line follows the
        // `name = "..."` line, so the first one after the match is ours.
        if let Some(line) = lock[idx..]
            .lines()
            .find(|l| l.trim_start().starts_with("version = "))
        {
            if let Some(v) = line.split('"').nth(1) {
                return v.to_string();
            }
        }
    }
    "unknown".to_string()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    connectrpc_build::Config::new()
        .files(&["proto/sidecar.proto"])
        .includes(&["proto/"])
        .include_file("_connectrpc.rs")
        .compile()?;

    // Surface the resolved versions of the core dependency stack as
    // compile-time env vars so peat-node can print them in its startup banner.
    let lock = fs::read_to_string("Cargo.lock").unwrap_or_default();
    for (crate_name, env_var) in [
        ("peat-mesh", "PEAT_MESH_VERSION"),
        ("peat-protocol", "PEAT_PROTOCOL_VERSION"),
        ("peat-schema", "PEAT_SCHEMA_VERSION"),
    ] {
        println!(
            "cargo:rustc-env={env_var}={}",
            locked_version(&lock, crate_name)
        );
    }

    println!("cargo:rerun-if-changed=proto/sidecar.proto");
    println!("cargo:rerun-if-changed=Cargo.lock");
    Ok(())
}
