// Thanks! https://github.com/tokio-rs/console/blob/5f6faa22d944735c2b8c312cac03b35a4ab228ef/console-api/tests/bootstrap.rs
// MIT License

use std::{path::PathBuf, process::Command};

#[test]
fn bootstrap() {
    let root_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out_dir = root_dir.join("src/service_protocol/generated");

    if let Err(error) = prost_build::Config::new()
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[allow(dead_code)]")
        .enum_attribute(".", "#[allow(clippy::enum_variant_names)]")
        .enum_attribute("protocol.NotificationTemplate.id", "#[derive(Eq, Hash)]")
        .out_dir(out_dir.clone())
        .compile_protos(
            &[root_dir.join("service-protocol/dev/restate/service/protocol.proto")],
            &[root_dir.join("service-protocol")],
        )
    {
        panic!("failed to compile `console-api` protobuf: {error}");
    }

    let status = Command::new("git")
        .arg("diff")
        .arg("--exit-code")
        .arg("--")
        .arg(out_dir)
        .status();
    match status {
        Ok(status) if !status.success() => panic!("You should commit the protobuf files"),
        Err(error) => panic!("failed to run `git diff`: {error}"),
        Ok(_) => {}
    }
}
