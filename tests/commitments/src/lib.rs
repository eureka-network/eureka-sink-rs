mod block_commit;
mod p_adic_representations;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/eureka.ingest.v1.rs"));
}
use pb::{Field, RecordChange, RecordChanges};
use plonky2::{
    field::{goldilocks_field::GoldilocksField, types::PrimeField64},
    hash::{merkle_tree::MerkleTree, poseidon::PoseidonHash},
};
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;

use crate::block_commit::BlockCommitment;

const DOMAIN_SEPARATION_LABEL: &str = "tests.commitments.BLOCK_COMMITMENT";

pub type F = GoldilocksField;
pub type Digest = [F; 4];

pub struct EventsCommitment(pub MerkleTree<F, PoseidonHash>);

impl EventsCommitment {
    fn new(data: Vec<Vec<F>>) -> Self {
        Self(MerkleTree::new(data, 0))
    }

    pub fn tree_height(&self) -> usize {
        self.0.leaves.len().trailing_zeros() as usize
    }

    pub fn get_inner(&self) -> &MerkleTree<F, PoseidonHash> {
        &self.0
    }
}

#[substreams::handlers::map]
fn extract_events(block: eth::Block) -> Result<RecordChanges, substreams::errors::Error> {
    let mut block_commitment = BlockCommitment::new(block.clone());
    block_commitment.commit_events().map_err(|_| {
        substreams::errors::Error::Unexpected("Failed to commit to block events".to_string())
    })?;

    Ok(RecordChanges {
        record_changes: block_commitment
            .encoded_logs()
            .into_iter()
            .map(|log| {
                let mut le_log_address = [0u8; 32];
                log.address().to_little_endian(&mut le_log_address);

                let log_topics = log.topics();

                let mut topic0 = [0u8; 32];
                log_topics[0].to_little_endian(&mut topic0);
                let mut topic1 = [0u8; 32];
                log_topics[1].to_little_endian(&mut topic1);
                let mut topic2 = [0u8; 32];
                log_topics[2].to_little_endian(&mut topic2);
                let mut topic3 = [0u8; 32];
                log_topics[3].to_little_endian(&mut topic3);
                let mut topic4 = [0u8; 32];
                log_topics[4].to_little_endian(&mut topic4);

                RecordChange {
                    record: "commmits".to_string(),
                    id: format!("{}<{}>", DOMAIN_SEPARATION_LABEL, Hex(&block.hash)),
                    operation: pb::record_change::Operation::Create.into(),
                    ordinal: 0,
                    fields: vec![
                        Field {
                            name: "blocknumber".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::Uint64(block.number)),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "blockhash".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(Hex(&block.hash).to_string())),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "txhash".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(
                                    Hex(&log.tx_hash()).to_string(),
                                )),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "txindex".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::Uint32(log.tx_index())),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "logindex".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::Uint32(log.log_index())),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "address".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(
                                    Hex(&le_log_address).to_string(),
                                )),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "topic0".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(Hex(&topic0).to_string())),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "topic1".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(Hex(&topic1).to_string())),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "topic2".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(Hex(&topic2).to_string())),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "topic3".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(Hex(&topic3).to_string())),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "topic4".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(Hex(&topic4).to_string())),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "data".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(
                                    Hex(&log
                                        .data()
                                        .iter()
                                        .flat_map(|u| {
                                            let mut slice = [0u8; 32];
                                            u.to_little_endian(&mut slice);
                                            slice
                                        })
                                        .collect::<Vec<u8>>())
                                    .to_string(),
                                )),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "addressF".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(
                                    Hex(&log
                                        .field_encoding_address()
                                        .iter()
                                        .flat_map(|f| f.to_canonical_u64().to_le_bytes())
                                        .collect::<Vec<u8>>())
                                    .to_string(),
                                )),
                            }),
                            old_value: None,
                        },
                        Field {
                            name: "commitment".to_string(),
                            new_value: Some(pb::Value {
                                typed: Some(pb::value::Typed::String(
                                    // our MerkleTree has cap == 0, therefore,
                                    // the only elements in its cap corresponde to vec![root]
                                    Hex(&block_commitment.events_commitment_root()).to_string(),
                                )),
                            }),
                            old_value: None,
                        },
                    ],
                }
            })
            .collect(),
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
