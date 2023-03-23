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

    let num_logs = block.logs().map(|x| x).collect::<Vec<_>>().len();
    let encoded_logs = block_commitment.encoded_logs();

    let mut record_changes = vec![];

    for log in encoded_logs {
        let mut le_log_address = [0u8; 32];
        log.address.to_little_endian(&mut le_log_address);

        let mut field_topics = vec![];
        'inner: for (i, topic) in log.topics.iter().enumerate() {
            if i > 4 {
                break 'inner;
            }
            let mut le_topic = [0u8; 32];
            topic.to_little_endian(&mut le_topic);
            field_topics.push(Field {
                name: format!("topic{}", i),
                new_value: Some(pb::Value {
                    typed: Some(pb::value::Typed::String(Hex(&le_topic).to_string())),
                }),
                old_value: None,
            });
        }

        let mut fields = vec![
            Field {
                name: "blocknumber".to_string(),
                new_value: Some(pb::Value {
                    typed: Some(pb::value::Typed::Uint64(block.number)),
                }),
                old_value: None,
            },
            Field {
                name: "num_logs".to_string(),
                new_value: Some(pb::Value {
                    typed: Some(pb::value::Typed::Uint64(num_logs.try_into().unwrap())),
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
                    typed: Some(pb::value::Typed::String(Hex(&log.tx_hash).to_string())),
                }),
                old_value: None,
            },
            Field {
                name: "txindex".to_string(),
                new_value: Some(pb::Value {
                    typed: Some(pb::value::Typed::Uint32(log.tx_index)),
                }),
                old_value: None,
            },
            Field {
                name: "logindex".to_string(),
                new_value: Some(pb::Value {
                    typed: Some(pb::value::Typed::Uint32(log.log_index)),
                }),
                old_value: None,
            },
            Field {
                name: "address".to_string(),
                new_value: Some(pb::Value {
                    typed: Some(pb::value::Typed::String(Hex(&le_log_address).to_string())),
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
            Field {
                name: "data".to_string(),
                new_value: Some(pb::Value {
                    typed: Some(pb::value::Typed::String(
                        Hex(&log
                            .data
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
        ];

        fields.extend(field_topics);

        record_changes.push(RecordChange {
            record: "eth_events".to_string(),
            id: format!(
                "{}<{}_{}_{}_{}>",
                DOMAIN_SEPARATION_LABEL,
                Hex(&block.hash),
                Hex(&log.tx_hash),
                log.tx_index,
                log.log_index
            ),
            operation: pb::record_change::Operation::Create.into(),
            ordinal: 0,
            fields,
        })
    }

    Ok(RecordChanges { record_changes })
}
