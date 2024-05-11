use dslab_core::{Id, SimulationContext};

use super::events::{CollectionEvent, ExecutionRequest};

pub trait WorkloadGenerator {
    fn get_workload(&self, ctx: &SimulationContext) -> Vec<ExecutionRequest>;

    fn get_collections(&self, ctx: &SimulationContext) -> Vec<CollectionEvent> {
        vec![]
    }
}
