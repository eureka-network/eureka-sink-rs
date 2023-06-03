mod block_commit;
mod p_adic_representations;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/eureka.ingest.v1.rs"));
}
use anyhow;
use ethereum_types::U256;
use pb::{Field, RecordChange, RecordChanges};
use plonky2::{
    field::{goldilocks_field::GoldilocksField, types::PrimeField64},
    hash::{
        hash_types::HashOut, merkle_proofs::verify_merkle_proof, merkle_tree::MerkleTree,
        poseidon::PoseidonHash,
    },
    plonk::{circuit_builder::CircuitBuilder, circuit_data::CircuitConfig},
};
use substreams::{hex, Hex};
use substreams_ethereum::pb::eth::v2 as eth;

use crate::{
    block_commit::{BlockCommitment, EncodedLog},
    p_adic_representations::goldilocks_adic_representation,
};

// todo: don't be an idiot and have explicit addresses here, currently: lens hub proxy contract
pub const SELECT_ADDRESS: [u8; 20] = hex!("Db46d1Dc155634FbC732f92E853b10B288AD5a1d");
const DOMAIN_SEPARATION_LABEL: &str = "tests.commitments.BLOCK_COMMITMENT";
const D: usize = 2;

pub type F = GoldilocksField;
pub type Digest = [F; 4];

// todo: we changed this to include encoded logs; update code elsewhere
// reason: we need events to be hashed as one atomic unit before adding to the tree
pub struct EventsCommitment {
    pub merkle_tree: Option(MerkleTree<F, PoseidonHash>),
    pub encoded_logs: Vec<EncodedLog>,
};

impl EventsCommitment {
    fn new(data: Vec<EncodedLog>) -> Self {
        Self(
            merkle_tree: None,
            encoded_logs: data
        )
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

/// The current function aim is to build a circuit to check event selection
/// from a specific address (addr). We need to verify the following steps:
///
/// 1. Loop over k encoded events (encoded as `Vec<F>`); and compute the following:
///    1.a. which events select for the address
/// 2. For each event, `e`, check if `is_equal(e.address[j]`, addr[j])` for j=2..7 the address elements
/// 3. build the merkle trie of all the encoded events, and assert it is equal to PI `C_n`
/// 4.
fn select_address_events_over_logs(
    encoded_logs: Vec<Vec<F>>,
    pi_root: HashOut<F>,
    pi_select_address: [F; 5],
    commitment_tree: MerkleTree<F, PoseidonHash>,
) -> Result<CircuitBuilder<F, D>, anyhow::Error> {
    // let's try to select events based on address
    let select_address: U256 = U256::from_little_endian(&SELECT_ADDRESS);
    let goldilock_select_address: [F; 5] = goldilocks_adic_representation(select_address);

    // build circuit
    let mut circuit_builder =
        CircuitBuilder::<F, D>::new(CircuitConfig::standard_recursion_config());

    // for loop over all events (a naive approach)
    // for each event:
    //   - check the five address field elements are equal
    //   - XOR check the path ? or can we prove the whole tree in one pass
    //   - if address matches then add to new vector
    //   - (optional? desired?) build new tree of selected events

    // get targets for current selected address
    let select_address_targets = circuit_builder.add_virtual_targets(5_usize);
    circuit_builder.register_public_inputs(&select_address_targets);

    // for each log event address that matches select_address, add it to the circuit
    // builder, as targets
    for (leaf_index, encoded_log) in encoded_logs.iter().enumerate() {
        let current_event_address = encoded_log.address;
        // add virtual targets for current event address
        let current_event_address_targets = circuit_builder.add_virtual_targets(5_usize);
        if select_address == current_event_address {
            // in the case of equality between addresses, register public inputs for the
            // current event address targets
            circuit_builder.register_public_inputs(&current_event_address_targets);
            // for each byte, check equality in place
            (0..5).for_each(|i| {
                circuit_builder.connect(select_address_targets[i], current_event_address_targets[i])
            });
            // for selected events (i.e., addresses matching selected address), verify these
            // against the event commitment tree previously generated
            let proof = commitment_tree.prove(leaf_index);

            if let Err(e) = verify_merkle_proof(
                encoded_log.goldilock_encoding.clone(),
                leaf_index,
                pi_root,
                &proof,
            ) {
                return Err(anyhow::anyhow!(
                    "Failed to verify merkle proof for event with index {} with error {}",
                    leaf_index,
                    e
                ));
            }
        }
    }

    Ok(circuit_builder)
}

fn circuit_select_address_events_over_logs(
    builder: &mut CircuitBuilder,
    events: Vec<EncodedLog>,
    pi_events_root: HashOut<F>,
    pi_select_address: Vec<F>,
) -> (HashOut<F>, usize, Vec<EncodedLog>) {
    // extract the length of the events vec
    let k = events.len();

    // specify 5 targets for `pi_select_address`
    let select_address_targets = builder.add_virtual_targets(5_usize);

    // precompute which events are selected
    let mut selected_events = Vec<(usize, EncodedLog)>::new();
    let mut unseleceted_events = Vec<(usize, EncodedLog)>::new();

    // specify true bool targets for later usage
    let truet = builder._true();

    for (index, event) in events.enumerate() {
        if event.field_encoding_address() == pi_select_address {
            selected_events.push((index + 1, event));
        } else {
            unseleceted_events.push((index + 1, event));
        }
    }

    // now prove that each selected event addresses matched on the original address
    for (index, selected_event) in selected_events {
        let select_event_address = selected_event.field_encoding_address();
        // add the corresponding 5 targets to this event to the builder
        let select_event_targets = builder.add_virtual_targets(5_usize);

        // address is defined by 5 field elements, and they all need to equal the selection address
        let mut bool_cummulative_target = builder.is_equal(select_address_targets[0], select_event_targets[0]);
        for i in 1..5 {
            let bool_target = builder.is_equal(select_address_targets[i], select_event_targets[i]);
            bool_cummulative_target = builder.and(bool_cummulative_target, bool_target);
        }
        builder.connect(truet, bool_cummulative_target);
    }
    
    // and we prove that for the not selected events the addresses do not match on the original address
    for (index, unselected_event) in unseleceted_events {
        let unselect_event_address = unselected_event.field_encoding_address();
        // add the corresponding 5 targets to this event to the builder
        let unselect_event_targets = builder.add_virtual_targets(5_usize);

        // address is defined by 5 field elements, and combined they should not match the selection address
        let mut bool_cummulative_target = builder.is_equal(select_address_targets[0], unselect_event_targets[0]);
        for i in 1..5 {
            let bool_target = builder.is_equal(select_address_targets[i], unselect_event_targets[i]);
            bool_cummulative_target = builder.and(bool_cummulative_target, bool_target);
        }
        // assert that at least one of the 5 targets do not match 
        let let_me_be_true = builder.not(bool_cummulative_target);
        builder.connect(truet, bool_cummulative_target);
    }


    // build wires that connect `events` with `pi_events_root`

    // register public inputs
    builder.register_public_inputs(select_address_targets);


    // todos:
    // - build tree of all events
    //   - connect root to PI pi_root C_n
    //   - each event must be hashed before being put into the tree
    // - build tree of the selected events
    //   - connect the root to PI E_n
}
