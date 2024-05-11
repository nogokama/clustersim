use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
};

use dslab_core::Id;
use serde::Serialize;

use crate::config::sim_config::{HostConfig, MonitoringConfig};

#[derive(Serialize, Clone, Copy)]
pub struct MonitoringState {
    pub cpu: f64,
    pub memory: f64,
    pub disk: Option<f64>,
}

#[derive(Serialize, Clone, Copy)]
pub struct MonitoringPoint {
    state: MonitoringState,
    time: f64,
}

impl MonitoringState {
    pub fn diff(&self, other: &MonitoringState) -> MonitoringState {
        MonitoringState {
            cpu: self.cpu - other.cpu,
            memory: self.memory - other.memory,
            disk: self.disk.zip(other.disk).map(|(a, b)| a - b),
        }
    }
}

#[derive(Clone, Serialize, Debug)]
pub struct ResourcePoint {
    pub value: f64,
    pub time: f64,
}

#[derive(Clone, Serialize)]
pub struct ResourceLoad {
    pub total: f64,
    pub consumed: f64,
    pub previous_update: f64,
    pub previous_update_time: f64,
    pub start_time: f64,
    pub compression_time_interval: Option<f64>,
    pub dump_points: Vec<ResourcePoint>,
}

impl ResourceLoad {
    pub fn new_fraction(start_time: f64, total: f64, compression_time_interval: Option<f64>) -> Self {
        Self {
            total,
            consumed: 0.,
            previous_update: 0.,
            previous_update_time: 0.,
            start_time,
            compression_time_interval,
            dump_points: Vec::new(),
        }
    }

    pub fn new_absolute(start_time: f64, compression_time_interval: Option<f64>) -> Self {
        Self {
            total: 1.,
            consumed: 0.,
            previous_update: 0.,
            previous_update_time: 0.,
            start_time,
            compression_time_interval,
            dump_points: Vec::new(),
        }
    }

    pub fn update(&mut self, current_value: f64, time: f64) -> f64 {
        let previous_update = self.previous_update;
        if let Some(compression_time_interval) = self.compression_time_interval {
            while time - self.start_time > compression_time_interval {
                let reset_time = self.start_time + compression_time_interval;
                let value = self.reset(reset_time);
                self.dump_points.push(ResourcePoint {
                    value,
                    time: reset_time,
                });
            }

            self.previous_update = current_value;
            self.consumed += previous_update * (time - self.previous_update_time);
            self.previous_update_time = time;
        } else {
            self.dump_points.push(ResourcePoint {
                value: current_value,
                time: time,
            });
            self.previous_update = current_value;
        }

        previous_update
    }

    pub fn add(&mut self, addition: f64, time: f64) {
        let consumed_value = self.previous_update + addition;
        self.update(consumed_value, time);
    }

    pub fn reset(&mut self, time: f64) -> f64 {
        self.consumed += self.previous_update * (time - self.previous_update_time);

        let result = (self.consumed / (time - self.start_time)) / self.total;
        self.start_time = time;
        self.previous_update_time = time;
        self.consumed = 0.;
        result
    }

    pub fn dump(&mut self) -> Vec<ResourcePoint> {
        std::mem::take(&mut self.dump_points)
    }

    pub fn extend(&mut self, amount: f64) {
        self.total += amount;
    }
}

#[derive(Clone, Serialize)]
pub struct HostLoadInfo {
    pub cpu: ResourceLoad,
    pub memory: ResourceLoad,
    pub disk_capacity: Option<ResourceLoad>,
    pub compression_time_interval: Option<f64>,
    pub dump_points: Vec<MonitoringPoint>,
}

impl HostLoadInfo {
    pub fn new(
        start_time: f64,
        cpu: f64,
        memory: f64,
        disk_capacity: Option<f64>,
        compression_time_interval: Option<f64>,
    ) -> Self {
        Self {
            cpu: ResourceLoad::new_fraction(start_time, cpu, compression_time_interval),
            memory: ResourceLoad::new_fraction(start_time, memory, compression_time_interval),
            disk_capacity: disk_capacity.map(|d| ResourceLoad::new_fraction(start_time, d, compression_time_interval)),
            compression_time_interval,
            dump_points: Vec::new(),
        }
    }

    pub fn extend(&mut self, other: &Self) {
        self.cpu.extend(other.cpu.total);
        self.memory.extend(other.memory.total);
        if let Some(disk_capacity) = &mut self.disk_capacity {
            if let Some(other_disk_capacity) = &other.disk_capacity {
                disk_capacity.extend(other_disk_capacity.total);
            }
        }
    }

    pub fn update(&mut self, state: MonitoringState, time: f64) -> MonitoringState {
        MonitoringState {
            cpu: self.cpu.update(state.cpu, time),
            memory: self.memory.update(state.memory, time),
            disk: state
                .disk
                .map(|v| self.disk_capacity.as_mut().map(|d| d.update(v, time)))
                .flatten(),
        }
    }

    pub fn add(&mut self, state: MonitoringState, time: f64) {
        self.cpu.add(state.cpu, time);
        self.memory.add(state.memory, time);
        if let Some(disk) = state.disk {
            if let Some(disk_capacity) = &mut self.disk_capacity {
                disk_capacity.add(disk, time);
            }
        }
    }

    pub fn dump(&mut self) -> Vec<MonitoringPoint> {
        let mut result = Vec::new();
        let cpus = self.cpu.dump();
        let memories = self.memory.dump();
        let disks = self.disk_capacity.as_mut().map(|d| Some(d.dump())).unwrap_or(None);
        assert_eq!(cpus.len(), memories.len());
        if let Some(disks) = &disks {
            assert_eq!(cpus.len(), disks.len());
        }
        for i in 0..cpus.len() {
            result.push(MonitoringPoint {
                state: MonitoringState {
                    cpu: cpus[i].value,
                    memory: memories[i].value,
                    disk: disks.as_ref().map(|d| d[i].value),
                },
                time: cpus[i].time,
            });
        }
        result
    }
}

pub struct Monitoring {
    pub hosts: HashMap<String, HostLoadInfo>,
    pub groups: HashMap<String, HostLoadInfo>,
    pub total: HostLoadInfo,
    pub scheduler_queue_size: ResourceLoad,
    pub host_load_compression_time_interval: Option<f64>,
    pub display_host_load: bool,

    host_log_file: BufWriter<File>,
    scheduler_log_file: BufWriter<File>,
}

impl Monitoring {
    pub fn new(config: MonitoringConfig) -> Monitoring {
        let host_log_file_path = "load.txt";
        let scheduler_log_file_path = "scheduler_info.txt";
        let host_log_file = BufWriter::new(File::create(&host_log_file_path).unwrap());
        let scheduler_log_file = BufWriter::new(File::create(&scheduler_log_file_path).unwrap());

        Monitoring {
            hosts: HashMap::new(),
            groups: HashMap::new(),
            total: HostLoadInfo::new(0., 0., 0., None, config.host_load_compression_time_interval),
            scheduler_queue_size: ResourceLoad::new_absolute(0., config.scheduler_queue_compression_time_interval),
            host_log_file,
            scheduler_log_file,
            host_load_compression_time_interval: config.host_load_compression_time_interval,
            display_host_load: config.display_host_load,
        }
    }

    pub fn add_host(&mut self, name: String, host_config: &HostConfig) {
        let host_load_info = HostLoadInfo::new(
            0.,
            host_config.cpus as f64,
            host_config.memory as f64,
            host_config.disk_capacity.map(|v| v as f64),
            self.host_load_compression_time_interval,
        );

        if let Some(group_name) = &host_config.group_prefix {
            self.groups
                .entry(group_name.clone())
                .or_insert_with(|| HostLoadInfo::new(0., 0., 0., None, self.host_load_compression_time_interval))
                .extend(&host_load_info);
        }

        self.total.extend(&host_load_info);

        self.hosts.insert(name.clone(), host_load_info);
    }

    pub fn add_scheduler_queue_size(&mut self, time: f64, addition: i64) {
        self.scheduler_queue_size.add(addition as f64, time);
        for points in self.scheduler_queue_size.dump() {
            self.dump_scheduler_queue_size(points.time, points.value);
        }
    }

    pub fn update_host(
        &mut self,
        time: f64,
        name: &str,
        group_name: Option<&String>,
        cpu_used: u32,
        memory_used: u64,
        disk_used: Option<u64>,
    ) {
        let host_load_info = self.hosts.get_mut(name).unwrap();
        let mut state = MonitoringState {
            cpu: cpu_used as f64,
            memory: memory_used as f64,
            disk: disk_used.map(|v| v as f64),
        };

        let old_state = host_load_info.update(state, time);

        state = state.diff(&old_state);

        if let Some(group_name) = group_name {
            self.groups.get_mut(group_name).unwrap().add(state, time);
        }

        self.total.add(state, time);

        for point in host_load_info.dump() {
            if self.display_host_load {
                self.dump_load(point.state, point.time, name);
            }
        }
        if let Some(group_name) = group_name {
            for point in self.groups.get_mut(group_name).unwrap().dump() {
                self.dump_load(point.state, point.time, &format!("group-{}", group_name));
            }
        }
        for point in self.total.dump() {
            self.dump_load(point.state, point.time, "TOTAL");
        }
    }

    fn dump_load(&mut self, state: MonitoringState, time: f64, name: &str) {
        writeln!(
            &mut self.host_log_file,
            "{} {} {} {}",
            time,
            name,
            state.cpu,
            state.memory,
            // state.disk.unwrap_or(0.)
        )
        .unwrap();
    }

    fn dump_scheduler_queue_size(&mut self, time: f64, value: f64) {
        writeln!(&mut self.scheduler_log_file, "{} {}", time, value).unwrap();
    }
}
