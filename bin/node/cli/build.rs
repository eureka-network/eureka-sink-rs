fn main() {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .build_client(true)
        .compile(
            &["../../../proto/sepana/ingest/v1/ingest.proto"],
            &["../../../proto/"],
        )
        .unwrap();
}
