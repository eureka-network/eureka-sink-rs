use substreams_ethereum::{block_view::LogView, pb::eth::v2 as pb};
// use plonky2::plonk::circuit_builder::CircuitBuilder;
// use plonky2::iop::witness::{PartialWitness, WitnessWrite};
use anyhow::{anyhow, Error};
use ethereum_types::U256;
use plonky2::field::goldilocks_field::GoldilocksField;
use plonky2::field::types::Field;
use plonky2::hash::{merkle_tree::MerkleTree, poseidon::PoseidonHash};
use plonky2::iop::witness::PartialWitness;
use plonky2::plonk::circuit_builder::CircuitBuilder;
use plonky2::plonk::config::Hasher;
use plonky2::util::serialization::Buffer;

use crate::{p_adic_representations::goldilocks_adic_representation, Digest, EventsCommitment, F};
const U256_BYTES: usize = 32;

pub struct BlockCommitment {
    block: pb::Block,
    events_commitment: Option<EventsCommitment>,
}

impl BlockCommitment {
    pub fn new(block: pb::Block) -> Self {
        Self {
            block,
            events_commitment: None,
        }
    }

    pub fn commit_events(&mut self) {
        let block = self.block.clone();
        // log:
        // txIndex  F (int)
        // logIndex F (int)
        // address  20 bytes
        // topics   0-4 * 32 bytes
        //          [F;4]
        // data     bytes (this we can hash because we wont compute over it)
        let mut logs: Vec<Vec<F>> = vec![];
        for log_view in block.logs() {
            // [`TransactionReceipt`] also contains a field `logs`
            let tx_index = F::from_canonical_u32(log_view.receipt.transaction.index);
            let log_index = F::from_canonical_u32(log_view.log.index);
            // TODO: For now we encode 20-byte addresses as U256,
            // further optimizations can be made
            let u256_address = U256::from_little_endian(&log_view.log.address);
            let goldilocks_address = goldilocks_adic_representation(u256_address);
            let u256_topics = log_view
                .log
                .topics
                .iter()
                .map(|t| U256::from_little_endian(t))
                .collect::<Vec<U256>>();
            let goldilocks_topics = u256_topics
                .iter()
                .map(|t| goldilocks_adic_representation(t.clone()).to_vec())
                .collect::<Vec<Vec<F>>>();
            // TODO: check this
            let u256_log_data = (1..(log_view.log.data.len() / U256_BYTES))
                .map(|i| U256::from_little_endian(&log_view.log.data[i..(i + U256_BYTES)]))
                .collect::<Vec<U256>>();
            let goldilocks_log_data = u256_log_data
                .iter()
                .map(|u| goldilocks_adic_representation(u.clone()))
                .flatten()
                .collect::<Vec<F>>();

            let hash_data = PoseidonHash::hash_no_pad(&goldilocks_log_data);
            let mut log_event = vec![tx_index, log_index];

            log_event.extend(goldilocks_address);
            goldilocks_topics
                .iter()
                .for_each(|t| log_event.extend(t.to_vec()));
            log_event.extend(goldilocks_log_data);
            log_event.extend(hash_data.elements.to_vec());

            logs.push(log_event);
        }

        self.events_commitment = Some(EventsCommitment(MerkleTree::new(logs, 0)));
    }

    fn fill_partial_witness(&self) -> Result<(), Error> {
        Ok(())
    }
}

// #[derive(Copy, Clone)]
// pub struct LogView<'a> {
//     pub receipt: ReceiptView<'a>,
//     pub log: &'a pb::Log,
// }

// #[derive(Copy, Clone)]
// pub struct ReceiptView<'a> {
//     pub transaction: &'a pb::TransactionTrace,
//     pub receipt: &'a pb::TransactionReceipt,
// }
