fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true) // useful for integration tests
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/sidecar.proto"], &["proto/"])?;

    println!("cargo:rerun-if-changed=proto/sidecar.proto");
    Ok(())
}
