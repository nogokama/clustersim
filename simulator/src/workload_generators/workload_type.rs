//! VM dataset types.

use std::{cell::RefCell, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::{
    config::sim_config::ClusterWorkloadConfig, execution_profiles::builder::ProfileBuilder,
};

use super::{
    alibaba_trace_reader::AlibabaTraceReader, generator::WorkloadGenerator,
    google_trace_reader::GoogleTraceWorkloadGenerator, native::NativeWorkloadGenerator,
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
                panic!(
                    "Cannot parse workload type `{}`, will use random as default",
                    input
                );
            }
        }
    }
}

pub fn workload_resolver(
    config: &ClusterWorkloadConfig,
    profile_builder: ProfileBuilder,
) -> Box<RefCell<dyn WorkloadGenerator>> {
    let workload_type = WorkloadType::from_str(&config.r#type).unwrap();
    let options = config.options.clone();

    match workload_type {
        WorkloadType::Random => Box::new(RefCell::new(RandomWorkloadGenerator::from_options(
            options
                .as_ref()
                .expect("Random workload options are required"),
        ))),
        WorkloadType::Google => Box::new(RefCell::new(GoogleTraceWorkloadGenerator::from_options(
            options
                .as_ref()
                .expect("Google trace workload options are required"),
        ))),
        WorkloadType::Alibaba => Box::new(RefCell::new(AlibabaTraceReader::from_options(
            options
                .as_ref()
                .expect("Alibaba trace workload options are required"),
        ))),
        WorkloadType::SWF => unimplemented!(),
        WorkloadType::Native => Box::new(RefCell::new(
            NativeWorkloadGenerator::from_options_and_builder(
                options
                    .as_ref()
                    .expect("Native workload options are required"),
                profile_builder,
            ),
        )),
    }
}
