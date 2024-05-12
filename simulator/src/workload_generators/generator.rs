use dslab_core::{Id, SimulationContext};

use super::events::{CollectionRequest, ExecutionRequest};

pub trait WorkloadGenerator {
    fn get_workload(&self, ctx: &SimulationContext) -> Vec<ExecutionRequest>;

    fn get_collections(&self, ctx: &SimulationContext) -> Vec<CollectionRequest> {
        vec![]
    }
}
