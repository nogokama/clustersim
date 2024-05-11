use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

use dslab_core::{cast, EventHandler, Id, SimulationContext};
use dslab_scheduling::{
    cluster::{ExecutionFinished, ScheduleExecution},
    cluster_events::HostAdded,
    config::sim_config::HostConfig,
    scheduler::{CustomScheduler, Resources, Scheduler, SchedulerContext},
    workload_generators::events::{ExecutionRequest, ExecutionRequestEvent},
};

pub struct ExecutionInfo {
    id: u64,
    cpu_cores: u32,
    memory: u64,
}

pub struct RoundRobinScheduler {
    hosts: Vec<HostConfig>,
    available_resources: HashMap<Id, Resources>,
    queue: VecDeque<ExecutionInfo>,
    executions: HashMap<u64, Resources>,
}

impl RoundRobinScheduler {
    pub fn new() -> RoundRobinScheduler {
        RoundRobinScheduler {
            hosts: Vec::new(),
            available_resources: HashMap::new(),
            queue: VecDeque::new(),
            executions: HashMap::new(),
        }
    }

    fn on_execution_submitted(&mut self, ctx: &SchedulerContext, execution_id: u64, cpu_cores: u32, memory: u64) {}

    fn schedule(&mut self, ctx: &SchedulerContext) {
        // println!("resources {:?}", self.available_resources);
        while let Some(execution) = self.queue.pop_front() {
            let mut scheduled = false;
            for machine in &self.hosts {
                if let Some(resources) = self.available_resources.get_mut(&machine.id) {
                    if resources.cpu_cores >= execution.cpu_cores && resources.memory >= execution.memory {
                        resources.cpu_cores -= execution.cpu_cores;
                        resources.memory -= execution.memory;
                        ctx.schedule_one_host(machine.id, execution.id);
                        scheduled = true;
                        break;
                    }
                }
            }

            if !scheduled {
                self.queue.push_front(execution);
                break;
            }
        }
    }
}

impl Scheduler for RoundRobinScheduler {
    fn on_execution_finished(&mut self, ctx: &SchedulerContext, execution_id: u64, hosts: Vec<Id>) {
        let resources = self.executions.remove(&execution_id).unwrap();
        for host_id in hosts {
            if let Some(host) = self.available_resources.get_mut(&host_id) {
                host.cpu_cores += resources.cpu_cores;
                host.memory += resources.memory;
            }
        }

        self.schedule(ctx);
    }
    fn on_execution_request(&mut self, ctx: &SchedulerContext, request: ExecutionRequest) {
        let execution_id = request.id.unwrap();
        let cpu_cores = request.resources.cpu_per_node;
        let memory = request.resources.memory_per_node;
        self.queue.push_back(ExecutionInfo {
            id: execution_id,
            cpu_cores,
            memory,
        });

        self.executions.insert(execution_id, Resources { cpu_cores, memory });

        self.schedule(ctx);
    }
    fn on_host_added(&mut self, host: HostConfig) {
        self.available_resources.insert(
            host.id,
            Resources {
                cpu_cores: host.cpus,
                memory: host.memory,
            },
        );
        self.hosts.push(host);
    }
    fn on_collection_event(
        &mut self,
        ctx: &SchedulerContext,
        collection_event: dslab_scheduling::workload_generators::events::CollectionEvent,
    ) {
    }
}
