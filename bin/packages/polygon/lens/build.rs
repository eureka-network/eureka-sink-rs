use anyhow::{Ok, Result};
use std::env;
use std::path::Path;
use substreams_ethereum::Abigen;

fn main() -> Result<()> {
    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("abi");

    Abigen::new("lens-events", "abi/Events.json")?
        .generate()?
        .write_to_file(Path::new(&dest_path).join("lens_events.rs"))?;

    prost_build::compile_protos(
        &["../../../../proto/eureka/ingest/v1/records.proto"],
        &["../../../../proto/"],
    )?;

    Ok(())
}
