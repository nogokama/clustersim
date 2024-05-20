use std::collections::VecDeque;

use rustc_hash::FxHashMap;

use dslab_core::Id;
use dslab_scheduling::{
    scheduler::{HostAvailableResources, Scheduler, SchedulerContext},
    workload_generators::events::{CollectionRequest, ExecutionRequest, ResourcesPack},
};

pub struct RoundRobinScheduler {
    queue: VecDeque<ExecutionRequest>,
    resources: FxHashMap<Id, ResourcesPack>,
    execution_resources: FxHashMap<u64, ResourcesPack>,
}

impl RoundRobinScheduler {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            resources: FxHashMap::default(),
            execution_resources: FxHashMap::default(),
        }
    }
}

impl Scheduler for RoundRobinScheduler {
    fn on_collection_request(
        &mut self,
        _ctx: &SchedulerContext,
        _collection_request: CollectionRequest,
    ) {
    }

    fn on_execution_finished(
        &mut self,
        ctx: &SchedulerContext,
        execution_id: u64,
        hosts: Vec<HostAvailableResources>,
    ) {
        let resoruces = self.execution_resources.remove(&execution_id).unwrap();
        let host_id = hosts[0].host_id;
        self.resources.get_mut(&host_id).unwrap().add(&resoruces);
        self.schedule(ctx, host_id);
    }

    fn on_execution_request(&mut self, _ctx: &SchedulerContext, request: ExecutionRequest) {
        self.execution_resources
            .insert(request.id.unwrap(), request.resources.get_total());
        self.queue.push_back(request);
    }

    fn on_host_added(&mut self, host: dslab_scheduling::config::sim_config::HostConfig) {
        self.resources.insert(
            host.id,
            ResourcesPack::new_cpu_memory(host.cpus, host.memory),
        );
    }

    fn on_host_resources(
        &mut self,
        ctx: &SchedulerContext,
        host_id: dslab_core::Id,
        _resources: ResourcesPack,
    ) {
        self.schedule(ctx, host_id);
    }
}

impl RoundRobinScheduler {
    fn schedule(&mut self, ctx: &SchedulerContext, host_id: Id) {
        let resources = self.resources.get_mut(&host_id).unwrap();
        while let Some(execution) = self.queue.pop_front() {
            let execution_resources = execution.resources.get_total();
            if execution_resources.fit_into(resources) {
                ctx.schedule_one_host(host_id, execution.id.unwrap());
                resources.subtract(&execution_resources);
            } else {
                self.queue.push_front(execution);
                break;
            }
        }
    }
}
