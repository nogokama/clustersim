use std::{cell::RefCell, rc::Rc, vec};

use rustc_hash::FxHashSet;
use serde::Serialize;

use dslab_core::{cast, log_debug, log_info, EventHandler, Id, SimulationContext};

use crate::{
    cluster::AddExpectedExecutionCount,
    storage::SharedInfoStorage,
    workload_generators::{
        events::{CollectionRequestEvent, ExecutionRequestEvent},
        generator::WorkloadGenerator,
    },
};

#[derive(Default)]
struct WorkloadGeneratorState {
    pub next_execution_id: u64,
    pub used_execution_ids: FxHashSet<u64>,
    pub next_collection_id: u64,
    pub used_collection_ids: FxHashSet<u64>,
}

pub const BATCH_WORKLOAD_SIZE: u64 = 10000;
pub const WORKLOAD_GENERATION_CHECK_INTERVAL: f64 = 50.;

#[derive(Clone, Serialize)]
pub struct CheckWorkloadGeneration {}

pub struct WorkloadQueueWatcher {
    shared_info_storage: Rc<RefCell<SharedInfoStorage>>,
    workload_generator_state: WorkloadGeneratorState,
    workload_generators: Vec<Box<RefCell<dyn WorkloadGenerator>>>,
    generators_completed: FxHashSet<usize>,
    generators_last_time: Vec<f64>,

    proxy_id: Id,
    cluster_id: Id,

    ctx: SimulationContext,
}

impl WorkloadQueueWatcher {
    pub fn new(
        ctx: SimulationContext,
        shared_info_storage: Rc<RefCell<SharedInfoStorage>>,
        proxy_id: Id,
        cluster_id: Id,
        workload_generators: Vec<Box<RefCell<dyn WorkloadGenerator>>>,
    ) -> WorkloadQueueWatcher {
        let generators_last_time = vec![0.0; workload_generators.len()];
        WorkloadQueueWatcher {
            shared_info_storage,
            workload_generator_state: WorkloadGeneratorState::default(),
            workload_generators,
            generators_last_time,
            generators_completed: FxHashSet::default(),
            proxy_id,
            cluster_id,
            ctx,
        }
    }

    pub fn get_id(&self) -> Id {
        self.ctx.id()
    }

    pub fn get_total_workload_hint(&self) -> Option<u64> {
        let mut total_workload_hint = 0;
        for generator in self.workload_generators.iter() {
            if let Some(hint) = generator.borrow().get_full_size_hint() {
                total_workload_hint += hint;
            }
        }
        if total_workload_hint == 0 {
            None
        } else {
            Some(total_workload_hint)
        }
    }

    pub fn generate_workload(&mut self, generate_collections: bool) -> u64 {
        let mut total_workload_cnt: u64 = 0;

        for generator_id in 0..self.workload_generators.len() {
            if self.generators_completed.contains(&generator_id) {
                continue;
            }

            while self.generators_last_time[generator_id]
                < self.ctx.time() + WORKLOAD_GENERATION_CHECK_INTERVAL * 2.
            {
                let generated = self.generate_workload_by(generator_id, generate_collections);
                if generated == 0 {
                    log_info!(self.ctx, "Workload generator {} completed", generator_id);
                    self.generators_completed.insert(generator_id);
                    break;
                }
                total_workload_cnt += generated;
            }
        }

        self.ctx.emit_now(
            AddExpectedExecutionCount {
                count: total_workload_cnt,
            },
            self.cluster_id,
        );

        if self.workload_generators.len() > self.generators_completed.len() {
            self.ctx.emit_self(
                CheckWorkloadGeneration {},
                WORKLOAD_GENERATION_CHECK_INTERVAL,
            );
        }

        total_workload_cnt
    }

    fn generate_workload_by(&mut self, generator_id: usize, generate_collections: bool) -> u64 {
        let workload_generator = &self.workload_generators[generator_id];

        let mut workload = workload_generator
            .borrow_mut()
            .get_workload(&self.ctx, Some(BATCH_WORKLOAD_SIZE));

        log_debug!(
            self.ctx,
            "New workload generated: {}",
            serde_json::to_string(&workload).unwrap()
        );

        let workload_cnt = workload.len() as u64;
        let mut collections = if generate_collections {
            workload_generator.borrow_mut().get_collections(&self.ctx)
        } else {
            vec![]
        };

        for execution_request in workload.iter_mut() {
            if let Some(id) = execution_request.id {
                if self
                    .workload_generator_state
                    .used_execution_ids
                    .contains(&id)
                {
                    panic!("Job id {} is used twice", id);
                }
                self.workload_generator_state.used_execution_ids.insert(id);
            } else {
                while self
                    .workload_generator_state
                    .used_execution_ids
                    .contains(&self.workload_generator_state.next_execution_id)
                {
                    self.workload_generator_state.next_execution_id += 1;
                }
                execution_request.id = Some(self.workload_generator_state.next_execution_id);
                self.workload_generator_state.next_execution_id += 1;
            }
        }

        for collection_event in collections.iter_mut() {
            if let Some(id) = collection_event.id {
                if self
                    .workload_generator_state
                    .used_collection_ids
                    .contains(&id)
                {
                    panic!("Collection id {} is used twice", id);
                }
                self.workload_generator_state.used_collection_ids.insert(id);
            } else {
                while self
                    .workload_generator_state
                    .used_collection_ids
                    .contains(&self.workload_generator_state.next_collection_id)
                {
                    self.workload_generator_state.next_collection_id += 1;
                }
                collection_event.id = Some(self.workload_generator_state.next_collection_id);
                self.workload_generator_state.next_collection_id += 1;
            }
        }

        for execution_request in workload {
            self.shared_info_storage
                .borrow_mut()
                .set_execution_request(execution_request.id.unwrap(), execution_request.clone());

            let time = execution_request.time;
            self.generators_last_time[generator_id] = time;

            self.ctx.emit_ordered(
                ExecutionRequestEvent {
                    request: execution_request,
                },
                self.proxy_id,
                time - self.ctx.time(),
            );
        }
        for collection_event in collections {
            let time = collection_event.time;
            self.ctx.emit(
                CollectionRequestEvent {
                    request: collection_event,
                },
                self.proxy_id,
                time - self.ctx.time(),
            );
        }

        workload_cnt
    }
}

impl EventHandler for WorkloadQueueWatcher {
    fn on(&mut self, event: dslab_core::Event) {
        cast!(match event.data {
            CheckWorkloadGeneration {} => {
                self.generate_workload(false);
            }
        });
    }
}
