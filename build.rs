fn main() -> Result<(), Box<dyn std::error::Error>> {
    connectrpc_build::Config::new()
        .files(&["proto/sidecar.proto"])
        .includes(&["proto/"])
        .include_file("_connectrpc.rs")
        .compile()?;

    println!("cargo:rerun-if-changed=proto/sidecar.proto");
    Ok(())
}
