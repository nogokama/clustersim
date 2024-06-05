mod round_robin;
use std::io::Write;

use env_logger::Builder;

use dslab_core::Simulation;
use dslab_scheduling::{config::sim_config::SimulationConfig, ClusterSchedulingSimulation};
use round_robin::RoundRobinScheduler;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let sim = Simulation::new(42);

    let config = SimulationConfig::from_file("configs/simple.yaml");

    let mut cluster_sim = ClusterSchedulingSimulation::new(sim, config, None);

    cluster_sim.run_with_scheduler(RoundRobinScheduler::new());
}
