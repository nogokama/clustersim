use std::{cell::RefCell, collections::HashMap, rc::Rc};

use dslab_core::{cast, log_debug, EventHandler, Id, SimulationContext};
use rustc_hash::FxHashMap;
use serde::Serialize;

use crate::{
    cluster_events::HostAdded,
    monitoring::Monitoring,
    storage::SharedInfoStorage,
    workload_generators::events::{CollectionRequestEvent, ExecutionRequest, ExecutionRequestEvent},
};

pub struct Proxy {
    scheduler_id: Id,
    cluster_id: Id,
    shared_info_storage: Rc<RefCell<SharedInfoStorage>>,
    monitoring: Rc<RefCell<Monitoring>>,

    ctx: SimulationContext,
}

impl Proxy {
    pub fn new(
        ctx: SimulationContext,
        cluster_id: Id,
        shared_info_storage: Rc<RefCell<SharedInfoStorage>>,
        monitoring: Rc<RefCell<Monitoring>>,
    ) -> Proxy {
        Proxy {
            scheduler_id: u32::MAX,
            cluster_id,
            shared_info_storage,
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
                let mut monitoring = self.monitoring.borrow_mut();
                let shared_info_storage = self.shared_info_storage.borrow();

                let user = shared_info_storage.get_execution_user(request.id.unwrap());

                monitoring.add_scheduler_queue_size(event.time, 1, user);

                self.ctx.emit_now(ExecutionRequestEvent { request }, self.scheduler_id);
            }
            HostAdded { host } => {
                log_debug!(self.ctx, "HostAdded: {}, {}", host.id, self.ctx.time());
                self.ctx.emit_now(HostAdded { host: host.clone() }, self.scheduler_id);
                self.ctx.emit_now(HostAdded { host }, self.cluster_id);
            }
            CollectionRequestEvent { request } => {
                self.shared_info_storage.borrow_mut().add_collection(request.clone());

                self.ctx.emit_now(CollectionRequestEvent { request }, self.scheduler_id);
            }
        })
    }
}
