use std::collections::{HashMap, VecDeque};

use dslab_core::Id;
use dslab_scheduling::{
    scheduler::{HostAvailableResources, Scheduler, SchedulerContext},
    workload_generators::events::{ExecutionRequest, ResourcesPack},
};

pub struct RoundRobinScheduler {
    queue: VecDeque<ExecutionRequest>,
    resources: HashMap<Id, ResourcesPack>,
    execution_resources: HashMap<u64, ResourcesPack>,
}

impl RoundRobinScheduler {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            resources: HashMap::new(),
            execution_resources: HashMap::new(),
        }
    }
}

impl Scheduler for RoundRobinScheduler {
    fn on_collection_request(
        &mut self,
        ctx: &dslab_scheduling::scheduler::SchedulerContext,
        collection_request: dslab_scheduling::workload_generators::events::CollectionRequest,
    ) {
    }

    fn on_execution_finished(
        &mut self,
        ctx: &SchedulerContext,
        execution_id: u64,
        mut hosts: Vec<HostAvailableResources>,
    ) {
        let resoruces = self.execution_resources.remove(&execution_id).unwrap();
        let host_id = hosts[0].host_id;
        self.resources.get_mut(&host_id).unwrap().add(&resoruces);
        self.schedule(ctx, host_id);
    }

    fn on_execution_request(&mut self, ctx: &SchedulerContext, request: ExecutionRequest) {
        self.execution_resources
            .insert(request.id.unwrap(), request.resources.get_total());
        self.queue.push_back(request);
    }

    fn on_host_added(&mut self, host: dslab_scheduling::config::sim_config::HostConfig) {
        self.resources
            .insert(host.id, ResourcesPack::new_cpu_memory(host.cpus, host.memory));
    }

    fn on_host_resources(&mut self, ctx: &SchedulerContext, host_id: dslab_core::Id, _resources: ResourcesPack) {
        self.schedule(ctx, host_id);
    }
}

impl RoundRobinScheduler {
    fn schedule(&mut self, ctx: &SchedulerContext, host_id: Id) {
        let resources = self.resources.get_mut(&host_id).unwrap();
        loop {
            if let Some(execution) = self.queue.pop_front() {
                let execution_resources = execution.resources.get_total();
                if execution_resources.fit_into(&resources) {
                    ctx.schedule_one_host(host_id, execution.id.unwrap());
                    resources.subtract(&execution_resources);
                } else {
                    self.queue.push_front(execution);
                    break;
                }
            } else {
                break;
            }
        }
    }
}
