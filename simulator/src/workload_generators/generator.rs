use dslab_core::SimulationContext;

use super::events::{CollectionRequest, ExecutionRequest};

pub trait WorkloadGenerator {
    fn get_workload(
        &mut self,
        ctx: &SimulationContext,
        limit: Option<u64>,
    ) -> Vec<ExecutionRequest>;

    fn get_collections(&self, _ctx: &SimulationContext) -> Vec<CollectionRequest> {
        vec![]
    }

    fn get_full_size_hint(&self) -> Option<u64> {
        None
    }
}
