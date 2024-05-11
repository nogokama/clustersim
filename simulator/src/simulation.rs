use core::{panic, time};
use std::{cell::RefCell, collections::HashSet, rc::Rc, vec};

use dslab_compute::multicore::{AllocationSuccess, CompFinished, CompStarted, Compute, DeallocationSuccess};
use dslab_core::{event, EventHandler, Id, Simulation, SimulationContext};
use dslab_network::{
    models::{ConstantBandwidthNetworkModel, SharedBandwidthNetworkModel},
    DataTransferCompleted, Network, NetworkModel,
};
use dslab_storage::disk::DiskBuilder;
use serde::de::DeserializeOwned;
use sugars::{boxed, rc, refcell};

use crate::{
    cluster::Cluster,
    cluster_events::HostAdded,
    config::sim_config::{GroupHostConfig, HostConfig, NetworkConfig, SimulationConfig},
    execution_profiles::{
        builder::{ConstructorFn, ProfileBuilder},
        profile::ExecutionProfile,
    },
    host::{cluster_host::ClusterHost, storage::ProcessHostStorage},
    monitoring::Monitoring,
    proxy::Proxy,
    scheduler::{CustomScheduler, Scheduler, SchedulerContext, SchedulerInvoker},
    storage::SharedInfoStorage,
    workload_generators::{
        events::ExecutionRequestEvent, generator::WorkloadGenerator, google_trace_reader::GoogleClusterHostsReader,
        random::RandomWorkloadGenerator, workload_type::workload_resolver,
    },
};

pub struct ClusterSchedulingSimulation {
    sim: Simulation,
    workload_generators: Vec<Box<dyn WorkloadGenerator>>,

    cluster: Rc<RefCell<Cluster>>,
    proxy: Rc<RefCell<Proxy>>,
    monitoring: Rc<RefCell<Monitoring>>,
    shared_storage: Rc<RefCell<SharedInfoStorage>>,
    host_process_storage: Rc<RefCell<ProcessHostStorage>>,

    profile_builder: ProfileBuilder,
}

impl ClusterSchedulingSimulation {
    pub fn new(
        mut sim: Simulation,
        config: SimulationConfig,
        network_opt: Option<Rc<RefCell<Network>>>,
    ) -> ClusterSchedulingSimulation {
        let monitoring = rc!(refcell!(Monitoring::new(config.monitoring.unwrap_or_default())));
        let shared_storage = rc!(refcell!(SharedInfoStorage::new()));
        let host_process_storage = rc!(refcell!(ProcessHostStorage::new()));

        let cluster_ctx = sim.create_context("cluster");
        let cluster_id = cluster_ctx.id();
        let cluster = rc!(refcell!(Cluster::new(
            cluster_ctx,
            shared_storage.clone(),
            host_process_storage.clone(),
            monitoring.clone(),
        )));
        sim.add_handler("cluster", cluster.clone());

        let proxy_ctx = sim.create_context("proxy");
        let proxy = rc!(refcell!(Proxy::new(
            proxy_ctx,
            cluster_id,
            shared_storage.clone(),
            monitoring.clone()
        )));
        sim.add_handler("proxy", proxy.clone());

        let generator_ctx = sim.create_context("generator");

        let profile_builder = ProfileBuilder::new();

        let mut cluster_simulation = ClusterSchedulingSimulation {
            sim,
            workload_generators: config
                .workload
                .as_ref()
                .unwrap()
                .iter()
                .map(|w| workload_resolver(w, profile_builder.clone()))
                .collect::<Vec<_>>(),
            cluster,
            proxy,
            shared_storage,
            host_process_storage,
            monitoring,

            profile_builder,
        };

        cluster_simulation.register_key_getters();

        let hosts = if let Some(trace_hosts_config) = config.trace_hosts {
            let config_reader = GoogleClusterHostsReader {
                path: trace_hosts_config.path,
                resource_multiplier: trace_hosts_config.resources_multiplier,
            };
            config_reader.read_cluster()
        } else {
            vec![]
        };

        cluster_simulation.build_cluster(config.hosts, hosts, config.network, network_opt);

        cluster_simulation
    }

    pub fn get_cluster_id(&self) -> Id {
        self.cluster.borrow().get_id()
    }

    fn build_network(&mut self, network_config: &NetworkConfig) -> Rc<RefCell<Network>> {
        let network_model: Box<dyn NetworkModel> = if network_config.shared.unwrap_or(false) {
            boxed!(SharedBandwidthNetworkModel::new(
                network_config.bandwidth,
                network_config.latency
            ))
        } else {
            boxed!(ConstantBandwidthNetworkModel::new(
                network_config.bandwidth,
                network_config.latency
            ))
        };

        let network_ctx = self.sim.create_context("network");
        let network = rc!(refcell!(Network::new(network_model, network_ctx)));
        self.sim.add_handler("network", network.clone());

        network
    }

    pub fn build_cluster(
        &mut self,
        hosts_groups: Vec<GroupHostConfig>,
        hosts: Vec<HostConfig>,
        network_config: Option<NetworkConfig>,
        mut network: Option<Rc<RefCell<Network>>>,
    ) {
        if network.is_none() && network_config.is_some() {
            network = Some(self.build_network(network_config.as_ref().unwrap()));
        }

        for host_group in hosts_groups {
            if host_group.count.unwrap_or(1) == 1 {
                self.build_host(
                    HostConfig::from_group_config(&host_group, None),
                    network_config.as_ref(),
                    network.clone(),
                );
            } else {
                for i in 0..host_group.count.unwrap() {
                    self.build_host(
                        HostConfig::from_group_config(&host_group, Some(i)),
                        network_config.as_ref(),
                        network.clone(),
                    );
                }
            }
        }
        for host_config in hosts {
            self.build_host(host_config, network_config.as_ref(), network.clone());
        }
    }

    pub fn build_host(
        &mut self,
        mut host_config: HostConfig,
        network_config: Option<&NetworkConfig>,
        network: Option<Rc<RefCell<Network>>>,
    ) {
        let cluster = self.cluster.borrow();

        let host_name = format!("host-{}", host_config.name);
        let host_ctx = self.sim.create_context(&host_name);

        host_config.id = host_ctx.id();

        let compute_name = format!("compute-{}", host_config.name);
        let compute_ctx = self.sim.create_context(&compute_name);
        let compute = rc!(refcell!(Compute::new(
            host_config.cpu_speed.unwrap_or(1000.),
            host_config.cpus,
            host_config.memory,
            compute_ctx
        )));

        self.sim.add_handler(&compute_name, compute.clone());

        if let Some(network) = network.clone() {
            network.borrow_mut().add_node(
                &host_name,
                boxed!(SharedBandwidthNetworkModel::new(
                    host_config
                        .local_newtork_bw
                        .unwrap_or(network_config.unwrap().local_bandwidth),
                    host_config
                        .local_newtork_latency
                        .unwrap_or(network_config.unwrap().local_latency),
                )),
            );
            network.borrow_mut().set_location(host_ctx.id(), &host_name);
        }

        let disk = if let Some(disk_cap) = host_config.disk_capacity {
            let disk_name = format!("disk-{}", host_config.name);
            let disk_ctx = self.sim.create_context(&disk_name);

            let disk = rc!(refcell!(DiskBuilder::simple(
                disk_cap,
                host_config.disk_read_bw.unwrap_or(1.),
                host_config.disk_write_bw.unwrap_or(1.),
            )
            .build(disk_ctx)));

            self.sim.add_handler(&disk_name, disk.clone());

            Some(disk)
        } else {
            None
        };

        let host = rc!(ClusterHost::new(
            compute,
            network,
            disk,
            self.host_process_storage.clone(),
            self.monitoring.clone(),
            host_config.group_prefix.clone(),
            host_ctx
        ));

        self.monitoring.borrow_mut().add_host(host_name.clone(), &host_config);

        cluster.add_host(host_config, host);
    }

    pub fn register_profile_with_constructor(&mut self, name: String, constructor: ConstructorFn) {
        self.profile_builder
            .register_profile_with_constructor(name, constructor)
    }

    pub fn register_profile<T>(&mut self, name: &str)
    where
        T: ExecutionProfile + DeserializeOwned + 'static,
    {
        self.profile_builder.register_profile::<T, &str>(name)
    }

    pub fn run_with_custom_scheduler<T: EventHandler + CustomScheduler + 'static>(&mut self, scheduler: T) {
        let scheduler_id = scheduler.id();
        let name = scheduler.name().clone();
        self.sim.add_handler(name, rc!(refcell!(scheduler)));

        let host_generator_ctx = self.sim.create_context("host_generator");
        let hosts = self.cluster.borrow().get_hosts();
        for host in hosts {
            host_generator_ctx.emit_now(HostAdded { host }, scheduler_id);
        }

        self.cluster.borrow_mut().set_scheduler(scheduler_id);
        self.proxy.borrow_mut().set_scheduler(scheduler_id);

        self.generate_workload();

        // TODO for long simulation make a while loop
        self.sim.step_until_no_events();

        println!("SIMULATION FINISHED AT: {}", self.sim.time());
    }

    pub fn run_with_scheduler<T: Scheduler + 'static>(&mut self, scheduler: T) {
        let ctx = self.sim.create_context("scheduler");
        let invoker = SchedulerInvoker::new(scheduler, ctx, self.get_cluster_id());

        self.run_with_custom_scheduler(invoker);
    }

    fn generate_workload(&mut self) {
        let proxy_id = self.proxy.borrow().get_id();

        let generator_ctx = self.sim.create_context("generator");

        let mut next_job_id = 0u64;
        let mut used_ids = HashSet::new();

        for workload_generator in self.workload_generators.iter() {
            let mut workload = workload_generator.get_workload(&generator_ctx);

            for execution_request in workload.iter_mut() {
                if let Some(id) = execution_request.id {
                    if used_ids.contains(&id) {
                        panic!("Job id {} is used twice", id);
                    }
                    used_ids.insert(id);
                } else {
                    while used_ids.contains(&next_job_id) {
                        next_job_id += 1;
                    }
                    execution_request.id = Some(next_job_id);
                    next_job_id += 1;
                }
            }

            for execution_request in workload {
                let time = execution_request.time;
                generator_ctx.emit(
                    ExecutionRequestEvent {
                        request: execution_request,
                    },
                    proxy_id,
                    time,
                );
            }
        }
    }

    fn register_key_getters(&self) {
        self.sim.register_key_getter_for::<CompFinished>(|c| c.id);
        self.sim.register_key_getter_for::<CompStarted>(|c| c.id);
        self.sim.register_key_getter_for::<AllocationSuccess>(|c| c.id);
        self.sim.register_key_getter_for::<DeallocationSuccess>(|c| c.id);
        self.sim
            .register_key_getter_for::<DataTransferCompleted>(|c| c.dt.id as u64);
    }
}
