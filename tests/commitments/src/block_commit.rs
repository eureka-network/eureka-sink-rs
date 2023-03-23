use anyhow::anyhow;
use plonky2::field::types::Field;
use plonky2::hash::{hash_types::HashOut, poseidon::PoseidonHash};
use plonky2::plonk::config::{GenericHashOut, Hasher};
use substreams_ethereum::pb::eth::v2 as pb;

use crate::{p_adic_representations::goldilocks_adic_representation, EventsCommitment, F, U256};
const U256_BYTES: usize = 32;

#[derive(Clone)]
#[allow(dead_code)]
pub struct EncodedLog {
    pub tx_index: u32,
    pub tx_hash: Vec<u8>,
    pub log_index: u32,
    pub address: U256,
    pub topics: Vec<U256>,
    pub data: Vec<U256>,
    pub goldilock_encoding: Vec<F>,
}

impl EncodedLog {
    pub fn field_encoding_address(&self) -> Vec<F> {
        // first two elements correspond to tx_index and log_index encoding,
        // every goldilock encoding of a `U256` has lenght 5
        self.goldilock_encoding[2..7].to_vec()
    }
}

pub struct BlockCommitment {
    block: pb::Block,
    events_commitment: Option<EventsCommitment>,
    encoded_logs: Vec<EncodedLog>,
}

impl BlockCommitment {
    pub fn new(block: pb::Block) -> Self {
        Self {
            block,
            events_commitment: None,
            encoded_logs: vec![],
        }
    }

    pub fn encoded_logs(&self) -> Vec<EncodedLog> {
        self.encoded_logs.clone()
    }

    pub fn encoded_events_commitment_root(&self) -> Option<HashOut<F>> {
        if let Some(ref events_commitment) = self.events_commitment {
            let poseidon_tree = events_commitment.get_inner();
            return Some(poseidon_tree.cap.0[0]);
        }
        None
    }

    pub fn events_commitment_root(&self) -> Vec<u8> {
        if let Some(ref events_commitment) = self.events_commitment {
            let poseidon_tree = events_commitment.get_inner();
            return poseidon_tree.cap.0[0].to_bytes();
        }
        vec![]
    }

    pub fn commit_events(&mut self) -> Result<(), anyhow::Error> {
        if self.events_commitment.is_some() {
            return Err(anyhow!("Events have been committed already"));
        }
        if !self.encoded_logs.is_empty() {
            return Err(anyhow!("Logs have been encoded already"));
        }

        // log:
        // txIndex  F (int)
        // logIndex F (int)
        // address  20 bytes
        // topics   0-4 * 32 bytes
        //          [F;4]
        // data     bytes (this we can hash because we wont compute over it)
        for log_view in self.block.logs() {
            let tx_index = log_view.receipt.transaction.index;
            let tx_hash = log_view.receipt.transaction.hash.clone();
            let log_index = log_view.log.index;
            // [`TransactionReceipt`] also contains a field `logs`
            let field_tx_index = F::from_canonical_u32(tx_index);
            let field_log_index = F::from_canonical_u32(log_index);
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
            // TODO: keccak hash data into U256 -> [F;5]
            let u256_log_data = (1..(log_view.log.data.len() / U256_BYTES))
                .map(|i| U256::from_little_endian(&log_view.log.data[i..(i + U256_BYTES)]))
                .collect::<Vec<U256>>();
            let goldilocks_log_data = u256_log_data
                .iter()
                .map(|u| goldilocks_adic_representation(u.clone()))
                .flatten()
                .collect::<Vec<F>>();

            let hash_data = PoseidonHash::hash_no_pad(&goldilocks_log_data);
            let mut log_event = vec![field_tx_index, field_log_index];

            log_event.extend(goldilocks_address);
            goldilocks_topics
                .iter()
                .for_each(|t| log_event.extend(t.to_vec()));
            log_event.extend(goldilocks_log_data);
            log_event.extend(hash_data.elements.to_vec());

            let encoded_log = EncodedLog {
                tx_index,
                tx_hash,
                log_index,
                address: u256_address,
                topics: u256_topics,
                data: u256_log_data,
                goldilock_encoding: log_event,
            };

            self.encoded_logs.push(encoded_log);
        }

        // todo: first borrow logs to make merkle tree, then move to self
        if !self.encoded_logs.is_empty() {
            // TODO: for now we make sure the lenght of the array is a power of 2,
            // by extending it with zeroes, but this is not a good approach as it
            // is not otimized and not secure
            let events_commitment = self
                .encoded_logs
                .iter()
                .map(|l| l.goldilock_encoding.clone())
                .collect::<Vec<Vec<F>>>();

            let extended_events_commitment = extend_leaves_to_pow_2(events_commitment);
            self.events_commitment = Some(EventsCommitment::new(extended_events_commitment));
        }

        Ok(())
    }
}

fn extend_leaves_to_pow_2(values: Vec<Vec<F>>) -> Vec<Vec<F>> {
    if values.len() == 0 {
        return vec![];
    }
    // we know that self.encoded_logs.len() != 0
    let log_2_len = values.len().ilog2() + 1;
    let diff = 2_u64.pow(log_2_len) - values.len() as u64 - 1;

    let mut extended_events_commitment = values;

    // append with goldilocks_encoding length
    extended_events_commitment.push(vec![F::from_canonical_u64(
        extended_events_commitment.len() as u64,
    )]);

    extended_events_commitment.extend(vec![vec![F::ZERO]; diff as usize]);
    extended_events_commitment
}
