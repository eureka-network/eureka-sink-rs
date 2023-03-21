pub mod abi {
    include!(concat!(env!("OUT_DIR"), "/abi/lens_events.rs"));
}
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/eureka.ingest.v1.rs"));
}
mod parser;
use pb::{
    record_change::Operation, value::Typed, Field, OffchainData, OffchainDataContent,
    OffchainDataRecords, RecordChange, RecordChanges, Value,
};
use substreams::scalar::BigInt;
use substreams::{hex, Hex};
use substreams_ethereum::pb::eth::v2 as eth;

pub const LENS_HUB_PROXY: [u8; 20] = hex!("Db46d1Dc155634FbC732f92E853b10B288AD5a1d");

#[substreams::handlers::map]
pub fn map_posts(block: eth::Block) -> Result<RecordChanges, substreams::errors::Error> {
    use abi::events::PostCreated;
    let record_changes: Result<Vec<_>, _> = block
        .events::<PostCreated>(&[&LENS_HUB_PROXY])
        .map(|(event, log)| {
            let record = "lens_posts".to_string();
            Ok(RecordChange {
                record: record.clone(),
                id: get_post_id(&event.profile_id, &event.pub_id),
                ordinal: log.ordinal(),
                operation: Operation::Create.into(),
                fields: vec![
                    Field {
                        name: "profile_id".to_string(),
                        new_value: Some(Value {
                            typed: Some(Typed::String(
                                Hex(&event.profile_id.to_signed_bytes_le()).to_string(),
                            )),
                        }),
                        old_value: None,
                    },
                    Field {
                        name: "content_uri".to_string(),
                        new_value: Some(Value {
                            typed: Some(Typed::Offchaindata(OffchainData {
                                uri: event.content_uri,
                                handler: "parse_offchain_data".to_string(),
                                max_retries: 3,
                                wait_before_retry: 5,
                            })),
                        }),
                        old_value: None,
                    },
                    Field {
                        name: "timestamp".to_string(),
                        new_value: Some(Value {
                            typed: Some(Typed::Uint64(event.timestamp.to_u64())),
                        }),
                        old_value: None,
                    },
                ],
            })
        })
        .collect();
    Ok(RecordChanges {
        record_changes: record_changes?,
    })
}

#[substreams::handlers::map]
pub fn parse_offchain_data(
    content: OffchainDataContent,
) -> Result<OffchainDataRecords, substreams::errors::Error> {
    match parser::parse_content(&content) {
        Ok(v) => Ok(v),
        Err(_) => Ok(OffchainDataRecords {
            uri: content.uri,
            manifest: content.manifest,
            records: Vec::new(),
        }),
    }
}

fn get_post_id(profile_id: &BigInt, pub_id: &BigInt) -> String {
    format!(
        "{}-{}",
        Hex(profile_id.to_signed_bytes_le()).to_string(),
        Hex(pub_id.to_signed_bytes_le()).to_string()
    )
}
