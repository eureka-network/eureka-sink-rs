fn main() -> std::io::Result<()> {
    prost_build::compile_protos(
        &["../../proto/sepana/ingest/v1/ingest.proto"],
        &["../../proto/"],
    )?;
    Ok(())
}
