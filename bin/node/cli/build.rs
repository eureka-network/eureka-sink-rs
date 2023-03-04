fn main() {
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .build_client(true)
        .compile(
            &["../../../proto/eureka/ingest/v1/records.proto"],
            &["../../../proto/"],
        )
        .unwrap();
}
