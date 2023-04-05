use crate::{block_commit::EncodedLog, D, F};
use plonky2::{iop::target::Target, plonk::circuit_builder::CircuitBuilder};

const GOLDILOCK_ADDRESS_ENCODED_LEN: usize = 5;
const START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT: usize = 2;

pub struct EventTargets {
    event_targets: Vec<Vec<Target>>,
}

impl EventTargets {
    pub fn new(event_targets: Vec<Vec<Target>>) -> Self {
        Self { event_targets }
    }
}

/// TODO: Builds a circuit to select events from an address
pub fn select_events_by_address(
    builder: &mut CircuitBuilder<F, D>,
    events: Vec<Vec<F>>,
    pi_select_address: Vec<F>,
) -> EventTargets {
    // add targets for
    // 1. Event commitment merkle tree root targets
    // 2. Address encoding for filtering events
    let event_root_commitment_targets = builder.add_virtual_hash();
    let select_address_targets = builder.add_virtual_targets(GOLDILOCK_ADDRESS_ENCODED_LEN);

    // register these as the only public inputs
    builder.register_public_inputs(&event_root_commitment_targets.elements);
    builder.register_public_inputs(&select_address_targets);

    // for each event, add corresponding targets
    let mut all_event_targets = vec![];
    events
        .iter()
        .for_each(|e| all_event_targets.push(builder.add_virtual_targets(e.len())));

    // for each event, make sure to connect event targets within address range
    // to newly created targets to represent these
    let num_events = events.len();
    let mut all_event_address_targets = vec![];
    for event_targets in all_event_targets {
        let event_address_targets = builder.add_virtual_targets(GOLDILOCK_ADDRESS_ENCODED_LEN);
        for ind in 0..GOLDILOCK_ADDRESS_ENCODED_LEN {
            builder.connect(
                event_address_targets[ind],
                event_targets[START_INDEX_GOLDILOCK_ADDRESS_ENCODED_IN_EVENT + ind],
            )
        }
        all_event_address_targets.push(event_address_targets);
    }

    // filter event targets whose address targets match with `select_address_targets`
    // and register those that don't match

    EventTargets::new(vec![])
}
