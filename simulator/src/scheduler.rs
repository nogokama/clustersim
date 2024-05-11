use dslab_core::{cast, EventHandler, Id, SimulationContext};
use serde::Serialize;

use crate::{
    cluster::{CancelExecution, ExecutionFinished, ScheduleExecution},
    cluster_events::HostAdded,
    config::sim_config::HostConfig,
    workload_generators::events::{CollectionEvent, ExecutionRequest, ExecutionRequestEvent},
};

#[derive(Debug)]
pub struct Resources {
    pub cpu_cores: u32,
    pub memory: u64,
}

pub trait CustomScheduler {
    fn name(&self) -> String;
    fn id(&self) -> Id;
}

pub struct SchedulerContext {
    pub ctx: SimulationContext,
    cluster_id: Id,
}

impl SchedulerContext {
    pub fn new(ctx: SimulationContext, cluster_id: Id) -> Self {
        SchedulerContext { ctx, cluster_id }
    }

    pub fn schedule(&self, host_ids: Vec<Id>, execution_id: u64) {
        self.ctx
            .emit_now(ScheduleExecution { host_ids, execution_id }, self.cluster_id);
    }
    pub fn schedule_one_host(&self, host_id: Id, execution_id: u64) {
        self.ctx.emit_now(
            ScheduleExecution {
                host_ids: vec![host_id],
                execution_id,
            },
            self.cluster_id,
        );
    }
    pub fn cancel(&self, execution_id: u64) {
        self.ctx.emit_now(CancelExecution { execution_id }, self.cluster_id);
    }
}

pub trait Scheduler {
    fn on_host_added(&mut self, host: HostConfig);
    fn on_execution_request(&mut self, ctx: &SchedulerContext, request: ExecutionRequest);
    fn on_collection_event(&mut self, ctx: &SchedulerContext, collection_event: CollectionEvent);
    fn on_execution_finished(&mut self, ctx: &SchedulerContext, execution_id: u64, hosts: Vec<Id>);
}

pub struct SchedulerInvoker<T: Scheduler> {
    scheduler: T,
    ctx: SchedulerContext,
}

impl<T: Scheduler> SchedulerInvoker<T> {
    pub fn new(scheduler: T, ctx: SimulationContext, cluster_id: Id) -> Self {
        SchedulerInvoker {
            scheduler,
            ctx: SchedulerContext { ctx, cluster_id },
        }
    }
}

impl<T: Scheduler> CustomScheduler for SchedulerInvoker<T> {
    fn id(&self) -> Id {
        self.ctx.ctx.id()
    }
    fn name(&self) -> String {
        self.ctx.ctx.name().to_string()
    }
}

impl<T: Scheduler> EventHandler for SchedulerInvoker<T> {
    fn on(&mut self, event: dslab_core::Event) {
        cast!(match event.data {
            HostAdded { host } => {
                self.scheduler.on_host_added(host);
            }
            ExecutionRequestEvent { request } => {
                self.scheduler.on_execution_request(&self.ctx, request);
            }
            ExecutionFinished { execution_id, hosts } => {
                self.scheduler.on_execution_finished(&self.ctx, execution_id, hosts);
            }
        })
    }
}
