use std::{collections::BinaryHeap, fs::File, rc::Rc};

use csv::Reader;
use serde::{Deserialize, Serialize};

use crate::{execution_profiles::default::Idle, workload_generators::events::ResourceRequirements};

use super::{events::ExecutionRequest, generator::WorkloadGenerator};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Options {
    pub batch_instance: String,
    pub resource_multiplier: f64,
    pub full_limit: Option<u64>,
    pub buffer_limit: u64,
}

pub struct AlibabaTraceReader {
    options: Options,
    reader: Reader<File>,
    cnt_records: u64,
    time_offset: f64,
    skipped_because_of_time: u64,
    buffer: BinaryHeap<InstanceRecord>,
}

impl AlibabaTraceReader {
    pub fn from_options(options: &serde_yaml::Value) -> Self {
        let options: Options = serde_yaml::from_value(options.clone()).unwrap();
        let mut reader = Reader::from_path(&options.batch_instance).unwrap();

        // reader.set_headers(csv::StringRecord::from(AlibabaTraceReader::get_headers().as_ref()));

        Self {
            options,
            reader,
            cnt_records: 0,
            time_offset: 0.,
            skipped_because_of_time: 0,
            buffer: BinaryHeap::new(),
        }
    }

    fn get_headers() -> Vec<&'static str> {
        vec![
            "instance_name",
            "task_name",
            "job_name",
            "task_type",
            "status",
            "start_time",
            "end_time",
            "machine_id",
            "seq_no",
            "total_seq_no",
            "cpu_avg",
            "cpu_max",
            "mem_avg",
            "mem_max",
        ]
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct InstanceRecord {
    instance_name: String,
    task_name: String,
    job_name: String,
    task_type: String,
    status: String,
    start_time: Option<f64>,
    end_time: Option<f64>,
    total_seq_no: u64,
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
        self.start_time.partial_cmp(&other.start_time)
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
    fn get_workload(&mut self, ctx: &dslab_core::SimulationContext, limit: Option<u64>) -> Vec<ExecutionRequest> {
        let mut requests = vec![];

        if let Some(limit) = limit {
            requests.reserve(limit as usize);
        }

        let mut cnt = 0;
        for record in self.reader.deserialize() {
            cnt += 1;
            if let Some(full_limit) = self.options.full_limit {
                if cnt >= full_limit {
                    break;
                }
            }

            let record: InstanceRecord = record.unwrap();
            if record.status != "Terminated" {
                continue;
            }
            if record.cpu_max.is_none() || record.mem_max.is_none() {
                continue;
            }

            requests.push(ExecutionRequest::simple(
                record.start_time.unwrap(),
                ResourceRequirements::simple(
                    (record.cpu_max.unwrap()) as u32,
                    (record.mem_max.unwrap() * self.options.resource_multiplier) as u64,
                ),
                Rc::new(Idle {
                    time: record.end_time.unwrap() - record.start_time.unwrap(),
                }),
            ));
        }

        requests
    }
}
