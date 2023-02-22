fn main() {
    tonic_build::configure()
        .build_client(true)
        .compile(
            &[
                "substreams/proto/sf/substreams/v1/substreams.proto",
                "substreams/proto/sf/substreams/v1/package.proto",
            ],
            &["substreams/proto"],
        )
        .unwrap();
}
