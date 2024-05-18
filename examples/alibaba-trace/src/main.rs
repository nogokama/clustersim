use dslab_core::Simulation;
use dslab_scheduling::{
    config::sim_config::SimulationConfig,
    simulation::ClusterSchedulingSimulation,
    workload_generators::{alibaba_trace_reader::AlibabaTraceReader, generator::WorkloadGenerator},
};
use env_logger::Builder;
use round_robin::RoundRobinScheduler;
use std::io::Write;

mod round_robin;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut sim = Simulation::new(42);
    let ctx = sim.create_context("test");

    let mut sim = Simulation::new(42);

    let config = SimulationConfig::from_file("configs/perf_config.yaml");

    let mut cluster_sim = ClusterSchedulingSimulation::new(sim, config, None);

    cluster_sim.run_with_scheduler(RoundRobinScheduler::new());
}
