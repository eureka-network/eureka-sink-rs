pub mod ingest {
    include!(concat!(env!("OUT_DIR"), "/sepana.ingest.v1.rs"));
}

use ingest::{IngestOperation, IngestOperations};
use num_bigint;
use serde_json::json;
use substreams::scalar::BigInt;
use substreams::store::{StoreGet, StoreGetRaw, StoreNew, StoreSet, StoreSetRaw};
use substreams::{store, Hex};
use substreams_ethereum::pb::eth as pbeth;

#[substreams::handlers::store]
fn store_balance(block: pbeth::v2::Block, output: store::StoreSetRaw) {
    for transaction in &block.transaction_traces {
        for call in &transaction.calls {
            for balance_change in &call.balance_changes {
                let new_value = balance_change
                    .new_value
                    .as_ref()
                    .map(|value| {
                        num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, &value.bytes)
                            .into()
                    })
                    .unwrap_or(BigInt::zero());

                output.set(
                    transaction.end_ordinal,
                    format!("Address:{}", Hex(&balance_change.address).to_string()),
                    &new_value.to_string(),
                )
            }
        }
    }
}

#[substreams::handlers::map]
fn ingest(
    block: pbeth::v2::Block,
    store: store::StoreGetRaw,
) -> Result<IngestOperations, substreams::errors::Error> {
    let mut operations = Vec::new();
    for transaction in &block.transaction_traces {
        for call in &transaction.calls {
            for balance_change in &call.balance_changes {
                let new_value = balance_change
                    .new_value
                    .as_ref()
                    .map(|value| {
                        num_bigint::BigInt::from_bytes_be(num_bigint::Sign::Plus, &value.bytes)
                            .into()
                    })
                    .unwrap_or(BigInt::zero());

                let key = Hex(&balance_change.address).to_string();
                if let Some(_) = store.get_first(format!("Address:{}", key)) {
                    // update
                    operations.push(IngestOperation {
                        key: key.clone(),
                        value: json!({ "account": key, "balance": new_value.to_string() })
                            .to_string(),
                        block: block.number,
                        ordinal: transaction.end_ordinal,
                        r#type: ingest::ingest_operation::Type::Update.into(),
                    });
                } else {
                    // insert
                    operations.push(IngestOperation {
                        key: key.clone(),
                        value: json!({ "account": key, "balance": new_value.to_string() })
                            .to_string(),
                        block: block.number,
                        ordinal: transaction.end_ordinal,
                        r#type: ingest::ingest_operation::Type::Insert.into(),
                    });
                }
            }
        }
    }
    Ok(IngestOperations { operations })
}
