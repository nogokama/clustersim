use dslab_core::SimulationContext;
use serde::{Deserialize, Serialize};

use crate::execution_profiles::builder::{ProfileBuilder, ProfileDefinition};

use super::{
    events::{ExecutionRequest, ResourceRequirements},
    generator::WorkloadGenerator,
};

#[derive(Serialize, Deserialize, Clone)]
pub struct NativeExecutionDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub submit_time: f64,
    pub resources: ResourceRequirements,
    pub profile: ProfileDefinition,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wall_time_limit: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_index: Option<u64>,
}

pub struct NativeWorkloadGenerator {
    workload: Vec<NativeExecutionDefinition>,
    profile_builder: ProfileBuilder,
    profile_path: Option<String>,
}

impl NativeWorkloadGenerator {
    pub fn new(
        path: String,
        profile_path: Option<String>,
        mut profile_builder: ProfileBuilder,
    ) -> NativeWorkloadGenerator {
        let jobs: Vec<NativeExecutionDefinition> = serde_yaml::from_str(
            &std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("Can't read file {}", path)),
        )
        .unwrap_or_else(|reason| panic!("Can't parse YAML from file {}: {}", path, reason));

        NativeWorkloadGenerator {
            workload: jobs,
            profile_builder,
            profile_path,
        }
    }
}

impl WorkloadGenerator for NativeWorkloadGenerator {
    fn get_workload(&self, ctx: &SimulationContext) -> Vec<ExecutionRequest> {
        if let Some(profile_path) = &self.profile_path {
            let profiles = serde_yaml::from_str(
                &std::fs::read_to_string(&profile_path)
                    .unwrap_or_else(|e| panic!("Can't read file {}: {}", profile_path, e)),
            )
            .unwrap_or_else(|e| panic!("Can't parse profiles from file {}: {}", profile_path, e));

            self.profile_builder.parse_profiles(&profiles)
        }

        let workload = self
            .workload
            .iter()
            .map(|job| ExecutionRequest {
                id: job.id,
                name: job.name.clone(),
                time: job.submit_time,
                schedule_after: None,
                collection_id: None,
                execution_index: None,
                resources: job.resources.clone(),
                profile: self.profile_builder.build(job.profile.clone()),
                wall_time_limit: job.wall_time_limit,
                priority: job.priority,
            })
            .collect::<Vec<_>>();

        workload
    }
}
