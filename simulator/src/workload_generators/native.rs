use dslab_core::SimulationContext;
use serde::{Deserialize, Serialize};

use crate::execution_profiles::builder::{ProfileBuilder, ProfileDefinition};

use super::{
    events::{CollectionRequest, ExecutionRequest, ResourceRequirements},
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

#[derive(Deserialize)]
pub struct Options {
    path: String,
    profile_path: Option<String>,
    collections_path: Option<String>,
}

pub struct NativeWorkloadGenerator {
    workload: Vec<NativeExecutionDefinition>,
    profile_builder: ProfileBuilder,
    options: Options,
}

impl NativeWorkloadGenerator {
    pub fn from_options_and_builder(
        options: &serde_yaml::Value,
        profile_builder: ProfileBuilder,
    ) -> Self {
        let options: Options = serde_yaml::from_value(options.clone()).unwrap();
        let jobs: Vec<NativeExecutionDefinition> = serde_yaml::from_str(
            &std::fs::read_to_string(&options.path)
                .unwrap_or_else(|_| panic!("Can't read file {}", options.path)),
        )
        .unwrap_or_else(|reason| panic!("Can't parse YAML from file {}: {}", options.path, reason));

        NativeWorkloadGenerator {
            workload: jobs,
            profile_builder,
            options,
        }
    }
}

impl WorkloadGenerator for NativeWorkloadGenerator {
    fn get_workload(
        &mut self,
        _ctx: &SimulationContext,
        _limit: Option<u64>,
    ) -> Vec<ExecutionRequest> {
        if let Some(profile_path) = &self.options.profile_path {
            let profiles = serde_yaml::from_str(
                &std::fs::read_to_string(profile_path)
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

    fn get_collections(&self, _ctx: &SimulationContext) -> Vec<CollectionRequest> {
        if let Some(collections_path) = &self.options.collections_path {
            let collections: Vec<CollectionRequest> = serde_json::from_str(
                &std::fs::read_to_string(collections_path)
                    .unwrap_or_else(|e| panic!("Can't read file {}: {}", collections_path, e)),
            )
            .unwrap_or_else(|e| panic!("Can't parse JSON from file {}: {}", collections_path, e));

            collections
        } else {
            vec![]
        }
    }
}
