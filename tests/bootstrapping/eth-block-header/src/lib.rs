pub mod ingest {
    include!(concat!(env!("OUT_DIR"), "/sepana.ingest.v1.rs"));
}
use ingest::{IngestOperation, IngestOperations};
use serde_json::json;
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;

#[substreams::handlers::map]
fn ingest(block: eth::Block) -> Result<IngestOperations, substreams::errors::Error> {
    let header = block.header.as_ref().unwrap();
    Ok(IngestOperations {
        operations: vec![IngestOperation {
            key: Hex(&header.parent_hash).to_string(),
            value: json!({
                "number": block.number,
                "hash": Hex(&block.hash).to_string(),
                "parent_hash": Hex(&header.parent_hash).to_string(),
                "timestamp": header.timestamp.as_ref().unwrap().to_string()
            })
            .to_string(),
            block: block.number,
            ordinal: 0,
            r#type: ingest::ingest_operation::Type::Insert.into(),
        }],
    })
}
