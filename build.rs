fn main() -> std::io::Result<()> {
    prost_build::Config::new()
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(
            &["service-protocol/dev/restate/service/protocol.proto"],
            &["service-protocol"],
        )?;
    Ok(())
}
