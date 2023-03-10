use substreams_ethereum::{pb::eth::v2 as pb, block_view::LogView};
// use plonky2::plonk::circuit_builder::CircuitBuilder;
// use plonky2::iop::witness::{PartialWitness, WitnessWrite};
use plonky2::field::goldilocks_field::GoldilocksField;
use plonky2::hash::{merkle_tree::MerkleTree, poseidon::PoseidonHash};
use plonky2::util::serialization::Buffer;
use plonky2::field::types::Field;
use plonky2::plonk::config::Hasher;

use crate::{F, Digest, EventsCommitment};

pub struct BlockCommitment {
    pub block: pb::Block,
}

impl BlockCommitment {

    pub fn commit_events(
        &self,
    ) {
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
            let address = log_view.log.address.iter().map(|u| F::from_canonical_u8(*u)).collect::<Vec<F>>();
            // let topics = ? ;
            let log_data = log_view.log.data.iter().map(|u| F::from_canonical_u8(*u)).collect::<Vec<F>>();
            let hash_data = PoseidonHash::hash_no_pad(&log_data);
            
        }

        let events_commitment = EventsCommitment(
            MerkleTree::new(logs, 0)
        );
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