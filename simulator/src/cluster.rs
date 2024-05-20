use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    rc::Rc,
};

use dslab_compute::multicore::{
    AllocationFailed, AllocationSuccess, CompFinished, CompStarted, Compute, CoresDependency,
    DeallocationSuccess,
};
use dslab_core::{
    cast, event::EventId, log_debug, log_error, log_info, Event, EventHandler, Id, Simulation,
    SimulationContext,
};
use futures::{select, FutureExt};
use rustc_hash::FxHashMap;
use serde::Serialize;
use sugars::{rc, refcell};

use crate::{
    config::sim_config::HostConfig,
    host::{cluster_host::ClusterHost, process::HostProcessInstance},
    monitoring::Monitoring,
    scheduler::HostAvailableResources,
    storage::SharedInfoStorage,
    workload_generators::events::{ExecutionRequest, ResourcesPack},
};

#[derive(Clone, Serialize)]
pub struct HostInvoked {
    pub id: Id,
    pub resources: ResourcesPack,
}

#[derive(Clone, Serialize)]
struct InvokingHosts {}

#[derive(Clone, Serialize)]
pub struct ScheduleError {
    pub task_id: u64,
    pub error_message: String,
}

#[derive(Clone, Serialize)]
pub struct ScheduleExecution {
    pub execution_id: u64,
    pub host_ids: Vec<Id>,
}

#[derive(Clone, Serialize)]
pub struct CancelExecution {
    pub execution_id: u64,
}

#[derive(Clone, Serialize)]
pub struct ExecutionFinished {
    pub execution_id: u64,
    pub hosts: Vec<HostAvailableResources>,
}

#[derive(Clone, Serialize)]
pub struct NewExecutionsRequired {
    pub generator_id: usize,
}

#[derive(Clone, Serialize)]
pub struct AddExpectedExecutionCount {
    pub count: u64,
}

pub(crate) struct Cluster {
    hosts: RefCell<FxHashMap<Id, Rc<ClusterHost>>>,
    hosts_configs: RefCell<FxHashMap<Id, HostConfig>>,

    enabled_hosts: RefCell<HashSet<Id>>,

    shared_info_storage: Rc<RefCell<SharedInfoStorage>>,
    monitoring: Rc<RefCell<Monitoring>>,

    scheduler_id: Id,
    generator_queue_watcher_id: Option<Id>,
    ctx: SimulationContext,

    process_cnt: RefCell<u64>,

    hosts_invoke_interval: Option<f64>,
    expected_execution_count: u64,
    total_execution_count: u64,

    notification_completed_step: Option<u64>,
}

impl Cluster {
    pub(crate) fn new(
        ctx: SimulationContext,
        shared_info_storage: Rc<RefCell<SharedInfoStorage>>,
        monitoring: Rc<RefCell<Monitoring>>,
        hosts_invoke_interval: Option<f64>,
    ) -> Self {
        Cluster {
            hosts: refcell!(FxHashMap::default()),
            hosts_configs: refcell!(FxHashMap::default()),
            enabled_hosts: refcell!(HashSet::new()),
            shared_info_storage,
            monitoring,

            scheduler_id: u32::MAX, // must be set later
            generator_queue_watcher_id: None,
            ctx,
            process_cnt: refcell!(0),

            hosts_invoke_interval,
            expected_execution_count: 0,
            total_execution_count: 0,
            notification_completed_step: None,
        }
    }

    pub fn start(&mut self) {
        if self.hosts_invoke_interval.is_some() {
            self.ctx.emit_self_now(InvokingHosts {});
        }
    }

    pub fn set_notification_completed_step(&mut self, step: u64) {
        self.notification_completed_step = Some(step);
    }

    pub fn get_total_executions(&self) -> u64 {
        self.total_execution_count
    }

    pub fn add_expected_execution_count(&mut self, count: u64) {
        self.expected_execution_count += count;
    }

    pub fn set_generator_queue_watcher_id(&mut self, id: Id) {
        self.generator_queue_watcher_id = Some(id);
    }

    pub fn set_scheduler(&mut self, scheduler_id: Id) {
        self.scheduler_id = scheduler_id;
    }

    pub fn get_id(&self) -> Id {
        self.ctx.id()
    }

    pub fn add_host(&self, host_config: HostConfig, host: Rc<ClusterHost>) {
        self.hosts_configs
            .borrow_mut()
            .insert(host.id(), host_config.clone());
        self.hosts.borrow_mut().insert(host.id(), host);
        if let Some(trace_id) = host_config.trace_id {
            self.shared_info_storage
                .borrow_mut()
                .insert_host_with_trace_id(host_config.id, trace_id);
        }
    }

    pub fn get_hosts(&self) -> Vec<HostConfig> {
        self.hosts_configs
            .borrow()
            .values()
            .cloned()
            .collect::<Vec<_>>()
    }

    fn schedule_execution(&self, host_ids: Vec<Id>, execution_id: u64) {
        let hosts = host_ids
            .iter()
            .map(|id| self.hosts.borrow().get(id).unwrap().clone())
            .collect::<Vec<_>>();

        let shared_info_storage = self.shared_info_storage.borrow_mut();
        let request = shared_info_storage.get_execution_request(execution_id);

        self.ctx.spawn(self.track_execution_process(hosts, request));
    }

    async fn track_execution_process(
        &self,
        hosts: Vec<Rc<ClusterHost>>,
        request: ExecutionRequest,
    ) {
        let processes = self.allocate_processes(&hosts, &request).await;

        let hosts_ids = processes.iter().map(|p| p.host.id()).collect::<Vec<_>>();

        let user = self
            .shared_info_storage
            .borrow()
            .get_execution_user(request.id.unwrap());

        if let Some(user) = &user {
            let resources = request.resources.get_total();
            self.monitoring.borrow_mut().add_to_user(
                self.ctx.time(),
                user,
                resources.cpu as f64,
                resources.memory as f64,
            )
        }
        hosts.iter().for_each(|h| h.log_compute_load());

        self.monitoring
            .borrow_mut()
            .add_scheduler_queue_size(self.ctx.time(), -1, user.clone());

        log_debug!(
            self.ctx,
            "start job: {}, profile: {}",
            request.id.unwrap(),
            request.profile.clone().as_ref().name()
        );
        request.profile.clone().run(&processes).await;
        log_debug!(
            self.ctx,
            "finish job: {}, profile: {}",
            request.id.unwrap(),
            request.profile.clone().as_ref().name()
        );

        self.deallocate_processes(processes).await;

        hosts.iter().for_each(|h| h.log_compute_load());

        if let Some(user) = &user {
            let resources = request.resources.get_total();
            self.monitoring.borrow_mut().add_to_user(
                self.ctx.time(),
                user,
                -(resources.cpu as f64),
                -(resources.memory as f64),
            )
        }

        self.ctx.emit_now(
            ExecutionFinished {
                execution_id: request.id.unwrap(),
                hosts: hosts_ids
                    .iter()
                    .map(|id| {
                        let hosts = self.hosts.borrow();
                        let host = hosts.get(id).unwrap();
                        let result = HostAvailableResources {
                            host_id: *id,
                            resources: ResourcesPack::new_cpu_memory(
                                host.compute.borrow().cores_available(),
                                host.compute.borrow().memory_available(),
                            ),
                        };
                        result
                    })
                    .collect::<Vec<_>>(),
            },
            self.scheduler_id,
        );

        self.shared_info_storage
            .borrow_mut()
            .remove_execution_request(request.id.unwrap());
    }

    async fn allocate_processes(
        &self,
        hosts: &Vec<Rc<ClusterHost>>,
        request: &ExecutionRequest,
    ) -> Vec<HostProcessInstance> {
        let mut processes = Vec::new();
        for host in hosts.iter() {
            let allocation_id = host.compute.borrow_mut().allocate_managed(
                request.resources.cpu_per_node,
                request.resources.memory_per_node,
                self.ctx.id(),
            );

            select! {
                _ = self.ctx.recv_event_by_key::<AllocationSuccess>(allocation_id).fuse() => {}
                failed = self.ctx.recv_event_by_key::<AllocationFailed>(allocation_id).fuse() => {
                    log_error!(self.ctx, "allocation failed: {:?}", failed.data.reason);
                }
            }

            let process_id = *self.process_cnt.borrow();

            self.shared_info_storage
                .borrow_mut()
                .set_host_id(process_id, host.id());

            *self.process_cnt.borrow_mut() += 1;

            processes.push(HostProcessInstance {
                id: process_id,
                compute_allocation_id: allocation_id,
                host: host.clone(),
            });
        }

        processes
    }

    async fn deallocate_processes(&self, processes: Vec<HostProcessInstance>) {
        for process in processes {
            let deallocation_id = process
                .host
                .compute
                .borrow_mut()
                .deallocate_managed(process.compute_allocation_id, self.ctx.id());
            self.ctx
                .recv_event_by_key::<DeallocationSuccess>(deallocation_id)
                .await;

            self.shared_info_storage
                .borrow_mut()
                .remove_process(process.id);
        }
    }

    fn cancel_execution(&self, task_id: u64) {
        log_error!(self.ctx, "cancel execution: {} not implemented", task_id)
    }

    fn on_invoking_hosts(&self) -> bool {
        if self.total_execution_count == self.expected_execution_count {
            return true;
        }
        self.hosts.borrow().iter().for_each(|(id, host)| {
            self.ctx.emit_now(
                HostInvoked {
                    id: *id,
                    resources: ResourcesPack::new_cpu_memory(
                        host.compute.borrow().cores_available(),
                        host.compute.borrow().memory_available(),
                    ),
                },
                self.scheduler_id,
            );
        });
        false
    }
}

impl EventHandler for Cluster {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            ScheduleExecution {
                execution_id,
                host_ids,
            } => {
                log_debug!(
                    self.ctx,
                    "schedule job: {} on hosts: {:?}",
                    execution_id,
                    host_ids
                );
                self.schedule_execution(host_ids, execution_id);
                self.total_execution_count += 1;

                if let Some(step) = self.notification_completed_step {
                    if self.total_execution_count % step == 0 {
                        log_info!(
                            self.ctx,
                            "completed {}% of total executions. Max queue size {}",
                            self.total_execution_count / step,
                            self.shared_info_storage
                                .borrow()
                                .get_executions_info_max_len(),
                        );
                        // println!(
                        //     "completed {}% of total executions",
                        //     self.total_execution_count / step
                        // );
                    }
                }
            }
            CancelExecution { execution_id } => {
                self.cancel_execution(execution_id);
            }
            InvokingHosts {} => {
                if self.on_invoking_hosts() {
                    return;
                }

                if let Some(step) = self.notification_completed_step {
                    log_info!(
                        self.ctx,
                        "invoking hosts: completed {}% of total executions. Max queue size {}",
                        self.total_execution_count / step,
                        self.shared_info_storage
                            .borrow()
                            .get_executions_info_max_len(),
                    );
                }

                if let Some(delay) = self.hosts_invoke_interval {
                    self.ctx.emit_self(InvokingHosts {}, delay);
                }
            }
            AddExpectedExecutionCount { count } => {
                self.add_expected_execution_count(count);
                self.on_invoking_hosts();
            }
        });
    }
}
