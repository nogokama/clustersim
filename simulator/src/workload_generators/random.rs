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
pub struct RandomWorkloadGenerator {
    jobs_count: u32,
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

impl RandomWorkloadGenerator {
    pub fn from_options(options: &serde_yaml::Value) -> Self {
        serde_yaml::from_value(options.clone()).unwrap()
    }
}

impl WorkloadGenerator for RandomWorkloadGenerator {
    fn get_workload(&self, ctx: &SimulationContext) -> Vec<ExecutionRequest> {
        let mut workload = Vec::new();
        workload.reserve(self.jobs_count as usize);

        let mut time = self.start_time.unwrap_or(0.) + 1.;

        let time_distribution = rand_distr::Normal::new(self.duration_mean, self.duration_dev).unwrap();

        for _id in 0..self.jobs_count as u64 {
            let execution_time = ctx.sample_from_distribution(&time_distribution);
            let job = ExecutionRequest {
                id: None,
                name: None,
                time,
                resources: ResourceRequirements {
                    nodes_count: 1,
                    cpu_per_node: ctx.gen_range(self.cpu_min..=self.cpu_max),
                    memory_per_node: ctx.gen_range(self.memory_min..=self.memory_max),
                },
                collection_id: self.collection_id,
                execution_index: None,
                schedule_after: None,
                wall_time_limit: None,
                priority: None,
                profile: Rc::new(Idle {
                    time: if execution_time > 1. { execution_time } else { 1. },
                }),
            };

            time += ctx.gen_range(self.delay_min..=self.delay_max);

            workload.push(job);
        }

        workload
    }

    fn get_collections(&self, _ctx: &SimulationContext) -> Vec<super::events::CollectionRequest> {
        vec![CollectionRequest {
            id: self.collection_id,
            time: self.start_time.unwrap_or(0.),
            user: self.user.clone(),
            priority: None,
        }]
    }
}
