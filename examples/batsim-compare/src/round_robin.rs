use std::collections::VecDeque;

use rustc_hash::FxHashMap;

use dslab_core::Id;
use dslab_scheduling::{
    config::sim_config::HostConfig,
    scheduler::{HostAvailableResources, Scheduler, SchedulerContext},
    workload_generators::events::{ExecutionRequest, ResourcesPack},
};

pub struct ExecutionInfo {
    id: u64,
    hosts: u32,
    cpu_cores: u32,
    memory: u64,
}

pub struct RoundRobinScheduler {
    hosts: Vec<HostConfig>,
    available_resources: FxHashMap<Id, ResourcesPack>,
    queue: VecDeque<ExecutionInfo>,
    executions: FxHashMap<u64, ResourcesPack>,
}

impl RoundRobinScheduler {
    pub fn new() -> RoundRobinScheduler {
        RoundRobinScheduler {
            hosts: Vec::new(),
            available_resources: FxHashMap::default(),
            queue: VecDeque::new(),
            executions: FxHashMap::default(),
        }
    }

    fn schedule(&mut self, ctx: &SchedulerContext) {
        // println!("resources {:?}", self.available_resources);
        while let Some(execution) = self.queue.pop_front() {
            let mut scheduled = false;
            let mut fit_ids = vec![];
            for machine in &self.hosts {
                if let Some(resources) = self.available_resources.get_mut(&machine.id) {
                    if resources.cpu >= execution.cpu_cores && resources.memory >= execution.memory
                    {
                        fit_ids.push(machine.id);
                        if fit_ids.len() == execution.hosts as usize {
                            break;
                        }
                    }
                }
            }

            if fit_ids.len() == execution.hosts as usize {
                for id in fit_ids.iter() {
                    let resources = self.available_resources.get_mut(id).unwrap();
                    resources.cpu -= execution.cpu_cores;
                    resources.memory -= execution.memory;
                }

                ctx.schedule(fit_ids, execution.id);
                scheduled = true;
            }

            if !scheduled {
                self.queue.push_front(execution);
                break;
            }
        }
    }
}

impl Scheduler for RoundRobinScheduler {
    fn on_execution_finished(
        &mut self,
        ctx: &SchedulerContext,
        execution_id: u64,
        hosts: Vec<HostAvailableResources>,
    ) {
        let resources = self.executions.remove(&execution_id).unwrap();
        for host in hosts {
            if let Some(host) = self.available_resources.get_mut(&host.host_id) {
                host.cpu += resources.cpu;
                host.memory += resources.memory;
            }
        }

        self.schedule(ctx);
    }
    fn on_execution_request(&mut self, ctx: &SchedulerContext, request: ExecutionRequest) {
        let execution_id = request.id.unwrap();
        let cpu = request.resources.cpu_per_node;
        let memory = request.resources.memory_per_node;
        self.queue.push_back(ExecutionInfo {
            id: execution_id,
            hosts: request.resources.nodes_count,
            cpu_cores: cpu,
            memory,
        });

        self.executions
            .insert(execution_id, ResourcesPack::new_cpu_memory(cpu, memory));

        self.schedule(ctx);
    }
    fn on_host_added(&mut self, host: HostConfig) {
        self.available_resources.insert(
            host.id,
            ResourcesPack::new_cpu_memory(host.cpus, host.memory),
        );
        self.hosts.push(host);
    }
    fn on_collection_request(
        &mut self,
        _ctx: &SchedulerContext,
        _collection_request: dslab_scheduling::workload_generators::events::CollectionRequest,
    ) {
    }
    fn on_host_resources(
        &mut self,
        _ctx: &SchedulerContext,
        _host_id: Id,
        _resources: ResourcesPack,
    ) {
    }
}
