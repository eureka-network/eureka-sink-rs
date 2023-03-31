use plonky2::plonk::circuit_builder::CircuitBuilder;

use crate::{block_commit::EncodedLog, D, F};

pub fn select_events_by_address(
    builder: &mut CircuitBuilder<F, D>,
    events: Vec<EncodedLog>,
    pi_select_address: Vec<F>,
) {
}
