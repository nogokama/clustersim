use std::{fs::File, rc::Rc};

use csv::Reader;
use dslab_core::log_warn;
use serde::{Deserialize, Serialize};

use crate::{execution_profiles::default::Idle, workload_generators::events::ResourceRequirements};

use super::{events::ExecutionRequest, generator::WorkloadGenerator};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Options {
    pub batch_instance: String,
    pub resource_multiplier: f64,
    pub full_limit: Option<u64>,
}

pub struct AlibabaTraceReader {
    options: Options,
    reader: Reader<File>,
    cnt_records: u64,
}

impl AlibabaTraceReader {
    pub fn from_options(options: &serde_yaml::Value) -> Self {
        let options: Options = serde_yaml::from_value(options.clone()).unwrap();
        let reader = Reader::from_path(&options.batch_instance).unwrap();

        Self {
            options,
            reader,
            cnt_records: 0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct InstanceRecord {
    instance_name: String,
    task_name: Option<String>,
    job_name: Option<String>,
    task_type: Option<String>,
    status: Option<String>,
    start_time: Option<f64>,
    end_time: Option<f64>,
    total_seq_no: Option<u64>,
    cpu_max: Option<f64>,
    mem_max: Option<f64>,
}

impl PartialEq for InstanceRecord {
    fn eq(&self, other: &Self) -> bool {
        self.total_seq_no == other.total_seq_no
    }
}
impl Eq for InstanceRecord {}

impl PartialOrd for InstanceRecord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for InstanceRecord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start_time
            .partial_cmp(&other.start_time)
            .unwrap_or_else(|| self.total_seq_no.cmp(&other.total_seq_no))
    }
}

impl WorkloadGenerator for AlibabaTraceReader {
    fn get_workload(
        &mut self,
        ctx: &dslab_core::SimulationContext,
        limit: Option<u64>,
    ) -> Vec<ExecutionRequest> {
        let mut requests = vec![];

        if let Some(full_limit) = self.options.full_limit {
            if self.cnt_records >= full_limit {
                return requests;
            }
        }

        if let Some(limit) = limit {
            requests.reserve(limit as usize);
        }

        let start_cnt = self.cnt_records;

        for record in self.reader.deserialize() {
            let record: InstanceRecord = record.unwrap();
            if let Some(status) = record.status.as_ref() {
                if status != "Terminated" {
                    continue;
                }
            }

            if record.cpu_max.is_none() || record.mem_max.is_none() {
                continue;
            }

            let cpu = record.cpu_max.unwrap();
            let mem = record.mem_max.unwrap();
            if !(1. ..=9600.).contains(&cpu) || !(0. ..=100.).contains(&mem) {
                log_warn!(ctx, "Invalid record: {:?}", record);
                continue;
            }

            requests.push(ExecutionRequest::simple(
                record.start_time.unwrap(),
                ResourceRequirements::simple(
                    cpu as u32,
                    (mem * self.options.resource_multiplier) as u64,
                ),
                Rc::new(Idle {
                    time: record.end_time.unwrap() - record.start_time.unwrap(),
                }),
            ));

            self.cnt_records += 1;
            if let Some(full_limit) = self.options.full_limit {
                if self.cnt_records >= full_limit {
                    break;
                }
            }
            if let Some(limit) = limit {
                if self.cnt_records - start_cnt >= limit {
                    break;
                }
            }
        }

        requests
    }

    fn get_full_size_hint(&self) -> Option<u64> {
        self.options.full_limit
    }
}
