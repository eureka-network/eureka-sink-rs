fn main() -> std::io::Result<()> {
    prost_build::compile_protos(
        &["../../../../proto/eureka/ingest/v1/records.proto"],
        &["../../../../proto/"],
    )?;
    Ok(())
}
