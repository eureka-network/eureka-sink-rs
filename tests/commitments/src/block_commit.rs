use substreams_ethereum::{pb::eth::v2 as pb, LogView};
// use plonky2::plonk::circuit_builder::CircuitBuilder;
// use plonky2::iop::witness::{PartialWitness, WitnessWrite};
use plonky2::field::goldilocks_field::GoldilocksField;
use plonky2::hash::merkle_tree::MerkleTree;
use plonky2::hash::poseidon::PoseidonHash;

pub type F = GoldilocksField;
pub type Digest = [F; 4];

pub struct EventsCommitment(
    pub MerkleTree<F, PoseidonHash>
);

impl pb::Block {

    pub fn commit_events(
        &self,
    ) {
        // log:
        // txIndex  F (int)
        // logIndex F (int)
        // address  20 bytes
        // topics   0-4 * 32 bytes
        //          [F;4]
        // data     bytes (this we can hash because we wont compute over it)
        let mut logs: Vec<Vec<F>> = vec![];
        for log_view in self.logs() {
            // [`TransactionReceipt`] also contains a field `logs`
            let tx_index = F::from_canonical_u64(log_view.receipt.transaction.index as u64);
            let log_index = F::from_canonical_u64(log_view.log.index as u64);
            let address = log_view.log.address.iter().map(|u| F::from_canonical_u64(u as u64)).collect::<Vec<F>>();
            // let topics = ? ;
            let log_data = log_view.log.data.iter().map(|u| F::from_canonical_u64(u as u64)).collect::<Vec<F>>();
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