fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a vendored protoc so the build works on machines that don't have
    // protoc in $PATH. This is critical for CI and hermetic Docker builds.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        // Build both sides — server drives the daemon, client drives
        // the bundled `examples/subscribe.rs` demo and ships the types
        // terminals can depend on if they vendor this crate.
        .build_client(true)
        .build_server(true)
        .compile_protos(&["../../proto/oi.proto"], &["../../proto"])?;
    println!("cargo:rerun-if-changed=../../proto/oi.proto");
    Ok(())
}
