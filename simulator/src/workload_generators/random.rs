use std::{collections::HashMap, option, rc::Rc};

use dslab_core::{log_info, Id, Simulation, SimulationContext};
use serde::{Deserialize, Serialize};

use crate::{
    execution_profiles::default::{CpuBurnHomogenous, Idle},
    workload_generators::events::CollectionRequest,
};

use super::{
    events::{ExecutionRequest, ResourceRequirements},
    generator::WorkloadGenerator,
};

#[derive(Serialize, Deserialize)]
struct Options {
    execution_count: u32,
    cpu_min: u32,
    cpu_max: u32,
    memory_min: u64,
    memory_max: u64,
    delay_min: f64,
    delay_max: f64,
    duration_mean: f64,
    duration_dev: f64,
    start_time: Option<f64>,
    nodes_count_min: Option<u32>,
    nodes_count_max: Option<u32>,
    user: Option<String>,
    collection_id: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct RandomWorkloadGenerator {
    options: Options,
    execution_last_time: f64,
    remaining_executions_count: u64,
}

impl RandomWorkloadGenerator {
    pub fn from_options(options: &serde_yaml::Value) -> Self {
        let options: Options = serde_yaml::from_value(options.clone()).unwrap();
        let remaining_executions_count = options.execution_count as u64;
        Self {
            options,
            execution_last_time: 0.,
            remaining_executions_count,
        }
    }
}

impl WorkloadGenerator for RandomWorkloadGenerator {
    fn get_workload(
        &mut self,
        ctx: &SimulationContext,
        limit: Option<u64>,
    ) -> Vec<ExecutionRequest> {
        let mut workload = Vec::new();

        let limit = if let Some(limit) = limit {
            limit.min(self.remaining_executions_count)
        } else {
            self.remaining_executions_count
        };

        self.remaining_executions_count -= limit;

        workload.reserve(limit as usize);

        let mut time = self.options.start_time.unwrap_or(0.) + 1.;
        if self.execution_last_time >= time {
            time = self.execution_last_time;
        }

        let time_distribution =
            rand_distr::Normal::new(self.options.duration_mean, self.options.duration_dev).unwrap();

        for _id in 0..limit as u64 {
            let execution_time = ctx.sample_from_distribution(&time_distribution);
            let job = ExecutionRequest {
                id: None,
                name: None,
                time,
                resources: ResourceRequirements {
                    nodes_count: 1,
                    cpu_per_node: ctx.gen_range(self.options.cpu_min..=self.options.cpu_max),
                    memory_per_node: ctx
                        .gen_range(self.options.memory_min..=self.options.memory_max),
                },
                collection_id: self.options.collection_id,
                execution_index: None,
                schedule_after: None,
                wall_time_limit: None,
                priority: None,
                profile: Rc::new(Idle {
                    time: if execution_time > 1. {
                        execution_time
                    } else {
                        1.
                    },
                }),
            };

            time += ctx.gen_range(self.options.delay_min..=self.options.delay_max);

            workload.push(job);
        }

        self.execution_last_time = time;

        workload
    }

    fn get_collections(&self, _ctx: &SimulationContext) -> Vec<super::events::CollectionRequest> {
        vec![CollectionRequest {
            id: self.options.collection_id,
            time: self.options.start_time.unwrap_or(0.),
            user: self.options.user.clone(),
            priority: None,
        }]
    }
}
