pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/eureka.ingest.v1.rs"));
}
use pb::{Field, RecordChange, RecordChanges};
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;

#[substreams::handlers::map]
fn block_meta(block: eth::Block) -> Result<RecordChanges, substreams::errors::Error> {
    let header = block.header.as_ref().unwrap();
    Ok(RecordChanges {
        record_changes: vec![RecordChange {
            record: "eth-block-header".to_string(),
            id: Hex(&block.hash).to_string(),
            ordinal: 0,
            operation: pb::record_change::Operation::Create.into(),
            fields: vec![
                Field {
                    name: "number".to_string(),
                    new_value: Some(pb::Value {
                        typed: Some(pb::value::Typed::Uint64(block.number)),
                    }),
                    old_value: None,
                },
                Field {
                    name: "hash".to_string(),
                    new_value: Some(pb::Value {
                        typed: Some(pb::value::Typed::String(Hex(&block.hash).to_string())),
                    }),
                    old_value: None,
                },
                Field {
                    name: "parent_hash".to_string(),
                    new_value: Some(pb::Value {
                        typed: Some(pb::value::Typed::String(
                            Hex(&header.parent_hash).to_string(),
                        )),
                    }),
                    old_value: None,
                },
                Field {
                    name: "timestamp".to_string(),
                    new_value: Some(pb::Value {
                        typed: Some(pb::value::Typed::String(
                            header.timestamp.as_ref().unwrap().to_string(),
                        )),
                    }),
                    old_value: None,
                },
            ],
        }],
    })
}
