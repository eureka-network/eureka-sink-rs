const PROTO_SRC: &'static str = "proto";
const PROTO_DST: &'static str = "proto/imported";
const PROTOS: &[&str] = &[
    "sf/substreams/v1/substreams.proto",
    "sf/substreams/v1/package.proto",
    "sf/substreams/v1/modules.proto",
    "sf/substreams/v1/clock.proto",
];

fn main() {
    let mut downloader =
        git_download::repo("https://github.com/streamingfast/substreams").branch_name("v0.2.0");

    for proto in PROTOS {
        downloader = downloader.add_file(
            format!("{}/{}", PROTO_SRC, proto),
            format!("{}/{}", PROTO_DST, proto),
        );
    }
    downloader.exec().unwrap();

    tonic_build::configure()
        .build_client(true)
        .compile(
            &PROTOS
                .iter()
                .map(|proto| format!("{}/{}", PROTO_DST, proto))
                .collect::<Vec<_>>(),
            &[PROTO_DST],
        )
        .unwrap();
}
