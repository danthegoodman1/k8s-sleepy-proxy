fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_prost_build::configure()
        .skip_debug([".sleepypods.controlplane.v1.CertificateBundle"])
        .type_attribute(
            ".sleepypods.controlplane.v1.PersistentVolumeSourceTemplate.kind",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile_protos(
            &["proto/sleepypods/controlplane/v1/control_plane.proto"],
            &["proto"],
        )?;

    Ok(())
}
