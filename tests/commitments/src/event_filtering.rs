use crate::{block_commit::EncodedLog, D, F};
use anyhow::anyhow;
use plonky2::{
    field::types::Field,
    hash::{hash_types::HashOutTarget, poseidon::PoseidonHash},
    iop::target::Target,
    plonk::circuit_builder::CircuitBuilder,
};

const GOLDILOCK_ADDRESS_ENCODED_LEN: usize = 5;
const START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT: usize = 2;

pub struct EventTargets {
    all_events_merkle_tree_root: Vec<Target>,
    pi_address_targets: Vec<Target>,
    event_targets: Vec<Vec<Target>>,
    selected_event_targets: Vec<Vec<Target>>,
    unselected_event_targets: Vec<Vec<Target>>,
}

impl EventTargets {
    pub fn new(
        all_events_merkle_tree_root: Vec<Target>,
        pi_address_targets: Vec<Target>,
        event_targets: Vec<Vec<Target>>,
        selected_event_targets: Vec<Vec<Target>>,
        unselected_event_targets: Vec<Vec<Target>>,
    ) -> Self {
        Self {
            all_events_merkle_tree_root,
            pi_address_targets,
            event_targets,
            selected_event_targets,
            unselected_event_targets,
        }
    }
}

/// TODO: Builds a circuit to select events from an address
pub fn select_events_by_address(
    builder: &mut CircuitBuilder<F, D>,
    events: Vec<Vec<F>>,
    pi_select_address: Vec<F>,
) -> Result<EventTargets, anyhow::Error> {
    if events.len() == 0 {
        return Ok(EventTargets::new(vec![], vec![], vec![], vec![], vec![]));
    }
    // check that `pi_select_address` length is correct
    if pi_select_address.len() != GOLDILOCK_ADDRESS_ENCODED_LEN {
        return Err(anyhow!("Invalid address to select events from"));
    }
    // add targets for
    // 1. Event commitment merkle tree root targets
    // 2. Address encoding for filtering events
    let event_root_commitment_targets = builder.add_virtual_hash();
    let select_address_targets = builder.add_virtual_targets(GOLDILOCK_ADDRESS_ENCODED_LEN);

    // register these as the only public inputs
    builder.register_public_inputs(&event_root_commitment_targets.elements);
    builder.register_public_inputs(&select_address_targets);

    // for each event, add corresponding targets to the circuit
    let mut all_event_targets = vec![];
    let mut all_constant_targets = vec![];
    events.iter().enumerate().for_each(|(i, e)| {
        // first target is a constant target
        let targets = builder.add_virtual_targets(e.len() + 1);
        let constant_target = builder.constant(F::from_canonical_u32(i as u32)); // for now use u32, as we build to a wasm target
        builder.connect(targets[0], constant_target);
        all_event_targets.push(targets);
        all_constant_targets.push(constant_target);
    });

    // for each event, make sure to connect event address targets within address range
    // to newly created targets to represent these
    let num_events = events.len();
    let mut all_event_address_targets = vec![];
    for event_targets in all_event_targets.iter() {
        let event_address_targets = builder.add_virtual_targets(GOLDILOCK_ADDRESS_ENCODED_LEN);
        for ind in 0..GOLDILOCK_ADDRESS_ENCODED_LEN {
            builder.connect(
                event_address_targets[ind + 1],
                event_targets[START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT + ind + 1], // we sum 1, as the first target is constant
            )
        }
        all_event_address_targets.push(event_address_targets);
    }

    // filter event targets whose address targets match with `select_address_targets`
    // while also registering those that don't match
    let mut selected_events_targets: Vec<Vec<Target>> = vec![];
    let mut unselected_events_targets: Vec<Vec<Target>> = vec![];

    for ((ind, event), event_targets) in events.iter().enumerate().zip(all_event_targets) {
        if event[START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT
            ..GOLDILOCK_ADDRESS_ENCODED_LEN + START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT]
            == pi_select_address[..]
        {
            let selected_event_targets = builder.add_virtual_targets(event.len() + 1);
            // connect first target to corresponding index
            builder.connect(selected_event_targets[0], all_constant_targets[ind]);
            // connect remainder targets with the targets for the corresponding `event_targets`
            for i in 0..event.len() {
                builder.connect(selected_event_targets[i + 1], event_targets[i + 1]);
            }
            // finally connect the select event targets, corresponding to the address
            // range, to `select_address_targets`
            for i in 0..GOLDILOCK_ADDRESS_ENCODED_LEN {
                builder.connect(
                    selected_event_targets[START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT + i + 1],
                    select_address_targets[i],
                )
            }
            selected_events_targets.push(selected_event_targets);
        } else {
            let unselected_event_targets = builder.add_virtual_targets(event.len() + 1);
            // connect first target to corresponding index
            builder.connect(unselected_event_targets[0], all_constant_targets[ind]);
            // connect remainder targets with the targets for the corresponding `event_targets`
            for i in 0..event.len() {
                builder.connect(unselected_event_targets[i + 1], event_targets[i + 1]);
            }
            // finally make sure that targets withing address range do not match the
            // event address targets (that we are selecting from)
            // address is defined by 5 field elements, and combined they should not match `select_address_targets`
            let mut bool_cummulative_target =
                builder.is_equal(select_address_targets[1], unselected_event_targets[1]);
            for i in 1..5 {
                let bool_target = builder.is_equal(
                    select_address_targets[i],
                    unselected_event_targets
                        [START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT + i + 1],
                );
                bool_cummulative_target = builder.and(bool_cummulative_target, bool_target);
            }
            // assert that at least one of the 5 targets do not match
            let not_matching_address_targets = builder.not(bool_cummulative_target);
            let true_target = builder._true();
            // asserts that `not_matching_address_targets` should always match to true, in this case
            builder.connect(true_target.target, not_matching_address_targets.target);
            unselected_events_targets.push(unselected_event_targets);
        }
    }

    Ok(EventTargets::new(vec![], vec![], vec![], vec![], vec![]))
}

pub fn build_merkle_root_target(
    builder: &mut CircuitBuilder<F, D>,
    targets: Vec<Vec<Target>>,
    to_extend_target: Target,
) -> HashOutTarget {
    // extend `targets` to a length of power of two vector
    let targets = extend_targets_to_power_of_two(builder, targets, to_extend_target);
    // build the merkle tree root target
    let merkle_tree_height = targets.len().ilog2();
    let mut tree_hash_targets = vec![];
    for i in 0..targets.len() {
        let hash_target = builder.hash_or_noop::<PoseidonHash>(targets[i].clone());
        tree_hash_targets.push(hash_target);
    }
    let mut current_tree_height_index = 0;
    for height in 0..merkle_tree_height {
        // TODO: do we want to loop over all the height, or until cap(1) ?
        for i in 0..(1 << merkle_tree_height - height) {
            let hash_targets = builder.hash_n_to_hash_no_pad::<PoseidonHash>(
                [
                    tree_hash_targets[i as usize].elements.clone(),
                    tree_hash_targets[i as usize + 1].elements.clone(),
                ]
                .concat(),
            );
            tree_hash_targets.push(hash_targets);
        }
        current_tree_height_index += 1 << height;
    }
    *tree_hash_targets.last().unwrap()
}

/// Extends a given vector of `Target`s of a certain length len
/// to a power of 2 len vector of `Target`s. This is done, by
/// appending with a constant `Target` of the length with a fixed `to_exted_target` to the original vector,
pub fn extend_targets_to_power_of_two(
    builder: &mut CircuitBuilder<F, D>,
    mut targets: Vec<Vec<Target>>,
    to_extend_target: Target,
) -> Vec<Vec<Target>> {
    let log_2_len = targets.len().ilog2();
    if 2_u64.pow(log_2_len) == targets.len() as u64 {
        return targets;
    }
    let diff = 2_u64.pow(log_2_len + 1) - targets.len() as u64 - 1;

    // append length of `targets`
    targets.push(vec![
        builder.constant(F::from_canonical_u64(targets.len() as u64))
    ]);
    // trivially extend the vector until we obtain a power 2 length output vector
    let to_extend_targets = vec![vec![to_extend_target]; diff as usize];
    targets.extend(to_extend_targets);
    targets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aux() {
        let x = 1 << 0;
        assert_eq!(1, 0);
    }
}
