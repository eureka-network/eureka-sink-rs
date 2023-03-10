mod block_commit;
mod p_adic_representations;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/eureka.ingest.v1.rs"));
}
use pb::{Field, RecordChange, RecordChanges};
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;
use plonky2::hash::{merkle_tree::MerkleTree, poseidon::PoseidonHash};
use plonky2::field::goldilocks_field::GoldilocksField;

use crate::block_commit::BlockCommitment;

pub type F = GoldilocksField;
pub type Digest = [F; 4];

pub struct EventsCommitment(
    pub MerkleTree<F, PoseidonHash>
);

#[substreams::handlers::map]
fn extract_events(block: eth::Block) -> Result<RecordChanges, substreams::errors::Error> {
    
    let block_commitment = BlockCommitment{ block: block.clone()};
    block_commitment.commit_events();

    Ok(RecordChanges {
        record_changes: vec![RecordChange{
            record: "commmits".to_string(),
            id: "1".to_string(),
            operation: pb::record_change::Operation::Create.into(),
            ordinal: 0,
            fields: vec![Field{
                name: "name".to_string(),
                new_value: Some(pb::Value{
                    typed: Some(pb::value::Typed::String("test".to_string()))
                }),
                old_value: None
            }]
        }]
    })
    // for event in block.events()  
    // let header = block.header.as_ref().unwrap();
    // Ok(RecordChanges {
    //     record_changes: vec![RecordChange {
    //         record: "eth_blockheaders".to_string(),
    //         id: Hex(&block.hash).to_string(),
    //         ordinal: 0,
    //         operation: pb::record_change::Operation::Create.into(),
    //         fields: vec![
    //             Field {
    //                 name: "number".to_string(),
    //                 new_value: Some(pb::Value {
    //                     typed: Some(pb::value::Typed::Uint64(block.number)),
    //                 }),
    //                 old_value: None,
    //             },
    //             Field {
    //                 name: "hash".to_string(),
    //                 new_value: Some(pb::Value {
    //                     typed: Some(pb::value::Typed::String(Hex(&block.hash).to_string())),
    //                 }),
    //                 old_value: None,
    //             },
    //             Field {
    //                 name: "parent_hash".to_string(),
    //                 new_value: Some(pb::Value {
    //                     typed: Some(pb::value::Typed::String(
    //                         Hex(&header.parent_hash).to_string(),
    //                     )),
    //                 }),
    //                 old_value: None,
    //             },
    //             Field {
    //                 name: "timestamp".to_string(),
    //                 new_value: Some(pb::Value {
    //                     typed: Some(pb::value::Typed::String(
    //                         header.timestamp.as_ref().unwrap().to_string(),
    //                     )),
    //                 }),
    //                 old_value: None,
    //             },
    //         ],
    //     }],
    // })
}
