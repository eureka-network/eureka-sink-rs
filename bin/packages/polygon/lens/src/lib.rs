pub mod abi {
    include!(concat!(env!("OUT_DIR"), "/abi/lens_events.rs"));
}
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/eureka.ingest.v1.rs"));
}
use pb::{value::Typed, Field, RecordChange, RecordChanges, Value};
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
            Ok(RecordChange {
                record: "lens_posts".to_string(),
                id: get_post_id(&event.profile_id, &event.pub_id),
                ordinal: log.ordinal(),
                operation: pb::record_change::Operation::Create.into(),
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
                            typed: Some(Typed::String(event.content_uri)),
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

fn get_post_id(profile_id: &BigInt, pub_id: &BigInt) -> String {
    format!(
        "{}-{}",
        Hex(profile_id.to_signed_bytes_le()).to_string(),
        Hex(pub_id.to_signed_bytes_le()).to_string()
    )
}
