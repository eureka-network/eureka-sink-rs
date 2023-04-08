use crate::{block_commit::EncodedLog, D, F};
use anyhow::anyhow;
use plonky2::{
    field::types::Field,
    hash::{
        hash_types::{HashOut, HashOutTarget},
        poseidon::PoseidonHash,
    },
    iop::{
        target::Target,
        witness::{PartialWitness, WitnessWrite},
    },
    plonk::circuit_builder::CircuitBuilder,
};

const GOLDILOCK_ADDRESS_ENCODED_LEN: usize = 5;
const START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT: usize = 2;

pub struct EventTargets {
    event_root_commitment_hash_targets: HashOutTarget,
    pi_address_targets: Vec<Target>,
    events_targets: Vec<Vec<Target>>,
    selected_events_targets: Vec<Vec<Target>>,
    unselected_events_targets: Vec<Vec<Target>>,
}

impl EventTargets {
    pub fn new(
        event_root_commitment_hash_targets: HashOutTarget,
        pi_address_targets: Vec<Target>,
        events_targets: Vec<Vec<Target>>,
        selected_events_targets: Vec<Vec<Target>>,
        unselected_events_targets: Vec<Vec<Target>>,
    ) -> Self {
        Self {
            event_root_commitment_hash_targets,
            pi_address_targets,
            events_targets,
            selected_events_targets,
            unselected_events_targets,
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
        return Ok(EventTargets::new(
            HashOutTarget {
                elements: [builder.constant(F::ZERO); 4],
            },
            vec![],
            vec![],
            vec![],
            vec![],
        ));
    }
    // check that `pi_select_address` length is correct
    if pi_select_address.len() != GOLDILOCK_ADDRESS_ENCODED_LEN {
        return Err(anyhow!("Invalid address to select events from"));
    }
    // add targets for
    // 1. Event commitment merkle tree root targets
    // 2. Address encoding for filtering events
    let event_root_commitment_hash_targets = builder.add_virtual_hash();
    let pi_address_targets = builder.add_virtual_targets(GOLDILOCK_ADDRESS_ENCODED_LEN);

    // register these as the only public inputs
    builder.register_public_inputs(&event_root_commitment_hash_targets.elements);
    builder.register_public_inputs(&pi_address_targets);

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

    // filter event targets whose address targets match with `pi_address_targets`
    // while also registering those that don't match
    let mut selected_events_targets: Vec<Vec<Target>> = vec![];
    let mut unselected_events_targets: Vec<Vec<Target>> = vec![];

    for ((ind, event), event_targets) in events.iter().enumerate().zip(all_event_targets.iter()) {
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
            // range, to `pi_address_targets`
            for i in 0..GOLDILOCK_ADDRESS_ENCODED_LEN {
                builder.connect(
                    selected_event_targets[START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT + i + 1],
                    pi_address_targets[i],
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
            // address is defined by 5 field elements, and combined they should not match `pi_address_targets`
            let mut bool_cummulative_target =
                builder.is_equal(pi_address_targets[1], unselected_event_targets[1]);
            for i in 1..5 {
                let bool_target = builder.is_equal(
                    pi_address_targets[i],
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

    let computed_merkle_root_target = build_merkle_root_target(
        builder,
        all_event_targets.clone(),
        all_constant_targets[0].clone(),
    ); // extend with zero target

    // connected `computed_merkle_root_target`
    for i in 0..4 {
        builder.connect(
            computed_merkle_root_target.elements[i],
            event_root_commitment_hash_targets.elements[i],
        );
    }

    Ok(EventTargets::new(
        event_root_commitment_hash_targets,
        pi_address_targets,
        all_event_targets,
        selected_events_targets,
        unselected_events_targets,
    ))
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

pub fn fill_circuit_witnesses(
    partial_witness: &mut PartialWitness<F>,
    targets: EventTargets,
    event_root_commitment_hash: HashOut<F>,
    address: [F; 5],
    events: Vec<Vec<F>>,
    selected_events: Vec<Vec<F>>,
) -> Result<(), anyhow::Error> {
    let EventTargets {
        event_root_commitment_hash_targets,
        events_targets,
        pi_address_targets,
        selected_events_targets,
        unselected_events_targets,
    } = targets;

    if events.len() != events_targets.len() {
        return Err(anyhow!("Events do not have the correct length"));
    }

    if selected_events.len() != selected_events_targets.len() {
        return Err(anyhow!("Selected events do not have the correct length"));
    }

    // fill in hashes
    for i in 0..4 {
        partial_witness.set_target(
            event_root_commitment_hash_targets.elements[i],
            event_root_commitment_hash.elements[i],
        );
    }

    // fill in address to filter
    for i in 0..GOLDILOCK_ADDRESS_ENCODED_LEN {
        partial_witness.set_target(pi_address_targets[i], address[i])
    }

    // fill in events
    for ind in 0..events.len() {
        let event = events[ind].clone();
        for i in 0..event.len() {
            // first entry is for the index, which we do not have to fill in
            partial_witness.set_target(events_targets[ind][i + 1], event[i]);
        }
    }

    // fill in both selected and unselected events
    let mut selected_ind = 0;
    let mut unselected_ind = 0;
    for ind in 0..events.len() {
        let event = events[ind].clone();
        if selected_events.contains(&event) {
            for i in 0..event.len() {
                // first entry is for the index, which we do not have to fill in
                partial_witness.set_target(selected_events_targets[selected_ind][i + 1], event[i]);
            }
            selected_ind += 1;
        } else {
            for i in 0..event.len() {
                // first entry is for the index, which we do not have to fill in
                partial_witness
                    .set_target(unselected_events_targets[unselected_ind][i + 1], event[i]);
            }
            unselected_ind += 1;
        }
    }
    Ok(())
}
