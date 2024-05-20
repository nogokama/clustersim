use std::{
    fs::File,
    io::{BufReader, BufWriter},
    rc::Rc,
};

use csv::ReaderBuilder;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Deserialize;

use crate::{
    config::sim_config::HostConfig,
    execution_profiles::{default::ComputeHomogenous, profile::NameTrait},
    workload_generators::google_protos::events::{EventType, MachineEvent},
};

use super::{
    events::{CollectionRequest, ExecutionRequest, ResourceRequirements},
    generator::WorkloadGenerator,
    google_protos::events::{machine_event, InstanceEvent},
    native::NativeExecutionDefinition,
};

#[derive(Deserialize)]
pub struct GoogleClusterHostsReader {
    pub path: String,
    pub resource_multiplier: f64,
}

impl GoogleClusterHostsReader {
    pub fn read_cluster(&self) -> Vec<HostConfig> {
        let file = File::open(&self.path).unwrap();

        // Create a CSV reader
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

        let mut records = Vec::new();

        let mut bad_machines = FxHashSet::default();
        for line in rdr.deserialize() {
            let record: MachineEvent = line.unwrap();

            if record.time.is_none() {
                continue;
            }

            if record.r#type.is_none() && record.machine_id.is_some() {
                bad_machines.insert(record.machine_id.unwrap());
            }

            if record.r#type.unwrap() == machine_event::EventType::Unknown {
                if let Some(machine_id) = record.machine_id {
                    bad_machines.insert(machine_id);
                }
            }

            records.push(record);

            // let record: MachineEvent = line.unwrap();
            // println!("{:?}", record);

            // records.push(record);
        }

        records.sort_by(|a, b| a.time.unwrap().cmp(&b.time.unwrap()));
        let machines = records
            .iter()
            .filter(|e| {
                !bad_machines.contains(&e.machine_id.unwrap())
                    && e.r#type.unwrap() == machine_event::EventType::Add
                    && e.cpus.is_some()
                    && e.memory.is_some()
            })
            .map(|e| {
                HostConfig::from_cpus_memory(
                    e.machine_id.unwrap() as u64,
                    (e.cpus.unwrap() * self.resource_multiplier) as u32,
                    (e.memory.unwrap() * self.resource_multiplier) as u64,
                )
            })
            .collect::<Vec<_>>();

        machines
    }
}

#[derive(Deserialize)]
pub struct GoogleTraceWorkloadGenerator {
    pub instances_path: String,
    pub collections_path: Option<String>,
    pub resources_multiplier: f64,
    pub time_scale: f64,
}

impl GoogleTraceWorkloadGenerator {
    pub fn from_options(options: &serde_yaml::Value) -> Self {
        serde_yaml::from_value(options.clone()).unwrap()
    }
}

struct ExecutionDefinition {
    pub instance_index: u64,
    pub collection_id: u64,
    pub cpus: u32,
    pub memory: u64,
    pub submit_time: f64,
    pub flops: f64,
    pub priority: Option<u64>,
}

impl WorkloadGenerator for GoogleTraceWorkloadGenerator {
    fn get_workload(
        &mut self,
        _ctx: &dslab_core::SimulationContext,
        _limit: Option<u64>,
    ) -> Vec<ExecutionRequest> {
        self.parse_workload()
            .iter()
            .map(|d| {
                ExecutionRequest::simple(
                    d.submit_time,
                    ResourceRequirements {
                        nodes_count: 1,
                        cpu_per_node: d.cpus,
                        memory_per_node: d.memory,
                    },
                    Rc::new(ComputeHomogenous {
                        compute_work: d.flops,
                    }),
                )
            })
            .collect::<Vec<_>>()
    }

    fn get_collections(&self, _ctx: &dslab_core::SimulationContext) -> Vec<CollectionRequest> {
        vec![]
    }
}

impl GoogleTraceWorkloadGenerator {
    fn parse_workload(&self) -> Vec<ExecutionDefinition> {
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(BufReader::new(
                File::open(&self.instances_path).unwrap_or_else(|_| {
                    panic!(
                        "can't find file with instance events: {}",
                        self.instances_path
                    )
                }),
            ));

        let mut submit_time = FxHashMap::default();
        let mut schedule_time = FxHashMap::default();
        let mut finished_time = FxHashMap::default();
        let mut skip_ids = FxHashSet::default();

        let mut cpus_mapping = FxHashMap::default();
        let mut memory_mapping = FxHashMap::default();

        let mut events = Vec::new();
        for line in reader.deserialize() {
            let record: InstanceEvent = line.unwrap();
            if record.time.is_some() {
                events.push(record);
            }
        }

        events.sort_by(|a, b| a.time.unwrap().cmp(&b.time.unwrap()));

        for record in events.iter() {
            if let Some(t) = record.r#type {
                if record.collection_id.is_none() || record.instance_index.is_none() {
                    continue;
                }

                let id = (
                    record.collection_id.unwrap(),
                    record.instance_index.unwrap(),
                );
                match t {
                    EventType::Enable => {
                        if let Some(cpus) = record.cpus {
                            if let Some(memory) = record.memory {
                                if let Some(time) = record.time {
                                    submit_time.entry(id).or_insert_with(|| {
                                        cpus_mapping.insert(id, cpus);
                                        memory_mapping.insert(id, memory);
                                        time
                                    });
                                }
                            }
                        }
                    }
                    EventType::Schedule => {
                        schedule_time
                            .entry(id)
                            .or_insert_with(|| record.time.unwrap());
                    }
                    EventType::Finish => {
                        finished_time.insert(id, record.time.unwrap());
                    }
                    EventType::Evict
                    | EventType::Fail
                    | EventType::Kill
                    | EventType::Lost
                    | EventType::UpdatePending
                    | EventType::UpdateRunning
                    | EventType::Queue => {
                        skip_ids.insert(id);
                    }
                    _ => {}
                }
            }
        }

        // println!("cnt: {}", cnt);
        // println!("submit_time: {}", submit_time.len());
        // println!("sched time: {}", schedule_time.len());
        // println!("finished_time: {}", finished_time.len());

        let mut tasks = Vec::new();
        for (id, t) in finished_time.iter() {
            if skip_ids.contains(id) {
                continue;
            }

            if let Some(s_time) = schedule_time.get(id) {
                let sched_time = *s_time as f64 / self.time_scale;
                let finished_time = *t as f64 / self.time_scale;
                if finished_time < sched_time {
                    continue;
                }
                let cpus = (cpus_mapping.get(id).unwrap() * self.resources_multiplier) as u32;
                if cpus == 0 {
                    continue;
                }
                let memory = (memory_mapping.get(id).unwrap() * self.resources_multiplier) as u64;
                let flops = (finished_time - sched_time) * cpus as f64;
                if flops == 0. {
                    continue;
                }
                tasks.push(ExecutionDefinition {
                    instance_index: id.1 as u64,
                    collection_id: id.0 as u64,
                    cpus,
                    memory,
                    submit_time: *submit_time.get(id).unwrap() as f64 / self.time_scale,
                    flops,
                    priority: None,
                });
            }
        }
        // println!("tasks: {}", tasks.len());
        tasks
    }

    pub fn dump_workload_to_native(&self, out_path: &str) {
        let file = File::create(out_path).unwrap();
        let writer = BufWriter::new(file);

        let workload = self.parse_workload();

        let converted = workload
            .iter()
            .map(|d| NativeExecutionDefinition {
                id: None,
                name: None,
                submit_time: d.submit_time,
                resources: ResourceRequirements {
                    nodes_count: 1,
                    cpu_per_node: d.cpus,
                    memory_per_node: d.memory,
                },
                profile: crate::execution_profiles::builder::ProfileDefinition::Detailed {
                    r#type: ComputeHomogenous::get_name(),
                    args: serde_yaml::to_value(ComputeHomogenous {
                        compute_work: d.flops,
                    })
                    .unwrap(),
                },
                wall_time_limit: None,
                priority: d.priority,
                collection_id: Some(d.collection_id),
                execution_index: Some(d.instance_index),
            })
            .collect::<Vec<_>>();

        serde_yaml::to_writer(writer, &converted).unwrap();
    }
}
