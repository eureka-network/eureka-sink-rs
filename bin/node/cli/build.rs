fn main() {
    tonic_build::configure()
        .build_client(true)
        .compile(
            &["../../../proto/sepana/ingest/v1/records.proto"],
            &["../../../proto/"],
        )
        .unwrap();
}
