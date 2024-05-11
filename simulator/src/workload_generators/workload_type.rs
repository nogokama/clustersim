//! VM dataset types.

use std::{collections::HashMap, str::FromStr};

use log::warn;
use serde::{Deserialize, Serialize};

use crate::{
    config::{options::parse_config_value, sim_config::ClusterWorkloadConfig},
    execution_profiles::builder::ProfileBuilder,
};

use super::{
    generator::WorkloadGenerator, google_trace_reader::GoogleTraceWorkloadGenerator, native::NativeWorkloadGenerator,
    random::RandomWorkloadGenerator,
};

/// Holds supported VM dataset types.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum WorkloadType {
    Random,
    Google,
    Alibaba,
    SWF,
    Native,
}

impl FromStr for WorkloadType {
    type Err = ();
    fn from_str(input: &str) -> Result<WorkloadType, Self::Err> {
        match input.to_lowercase().as_str() {
            "random" => Ok(WorkloadType::Random),
            "google" => Ok(WorkloadType::Google),
            "alibaba" => Ok(WorkloadType::Alibaba),
            "swf" => Ok(WorkloadType::SWF),
            "native" => Ok(WorkloadType::Native),
            _ => {
                panic!("Cannot parse workload type `{}`, will use random as default", input);
            }
        }
    }
}

pub fn workload_resolver(
    config: &ClusterWorkloadConfig,
    profile_builder: ProfileBuilder,
) -> Box<dyn WorkloadGenerator> {
    let workload_type = WorkloadType::from_str(&config.r#type).unwrap();
    let options = config.options.clone();
    let path = config.path.clone();

    match workload_type {
        WorkloadType::Random => Box::new(RandomWorkloadGenerator::from_options(
            options.as_ref().expect("Random workload options are required"),
        )),
        WorkloadType::Google => Box::new(GoogleTraceWorkloadGenerator::from_options(
            options.as_ref().expect("Google trace workload options are required"),
        )),
        WorkloadType::Alibaba => unimplemented!(),
        WorkloadType::SWF => unimplemented!(),
        WorkloadType::Native => Box::new(NativeWorkloadGenerator::new(
            path.expect("Native workload path is required"),
            options
                .unwrap_or_default()
                .get("profile_path")
                .map(|f| f.as_str().unwrap().to_string()),
            profile_builder,
        )),
    }
}
