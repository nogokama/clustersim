use std::{cell::RefCell, collections::HashMap, rc::Rc};

use dslab_core::{cast, log_debug, EventHandler, Id, SimulationContext};

use crate::{
    cluster_events::HostAdded,
    monitoring::Monitoring,
    storage::SharedInfoStorage,
    workload_generators::events::{ExecutionRequest, ExecutionRequestEvent},
};

pub struct Proxy {
    jobs_scheduled_time: HashMap<u64, f64>,
    scheduler_id: Id,
    cluster_id: Id,
    job_info_storage: Rc<RefCell<SharedInfoStorage>>,
    monitoring: Rc<RefCell<Monitoring>>,

    ctx: SimulationContext,
}

impl Proxy {
    pub fn new(
        ctx: SimulationContext,
        cluster_id: Id,
        job_info_storage: Rc<RefCell<SharedInfoStorage>>,
        monitoring: Rc<RefCell<Monitoring>>,
    ) -> Proxy {
        Proxy {
            scheduler_id: u32::MAX,
            jobs_scheduled_time: HashMap::new(),
            cluster_id,
            job_info_storage,
            monitoring,
            ctx,
        }
    }

    pub fn get_id(&self) -> Id {
        self.ctx.id()
    }

    pub fn set_scheduler(&mut self, scheduler_id: Id) {
        self.scheduler_id = scheduler_id;
    }
}

impl EventHandler for Proxy {
    fn on(&mut self, event: dslab_core::Event) {
        cast!(match event.data {
            ExecutionRequestEvent { request } => {
                self.jobs_scheduled_time.insert(request.id.unwrap(), self.ctx.time());

                self.job_info_storage
                    .borrow_mut()
                    .set_execution_request(request.id.unwrap(), request.clone());

                self.ctx.emit_now(ExecutionRequestEvent { request }, self.scheduler_id);

                self.monitoring.borrow_mut().add_scheduler_queue_size(event.time, 1);
            }

            HostAdded { host } => {
                log_debug!(self.ctx, "HostAdded: {}, {}", host.id, self.ctx.time());
                self.ctx.emit_now(HostAdded { host: host.clone() }, self.scheduler_id);
                self.ctx.emit_now(HostAdded { host }, self.cluster_id);
            }
        })
    }
}
