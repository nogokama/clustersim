mod round_robin;

use std::io::Write;

use env_logger::Builder;

use dslab_core::Simulation;
use dslab_scheduling::{
    config::sim_config::SimulationConfig, simulation::ClusterSchedulingSimulation,
};

use round_robin::RoundRobinScheduler;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let sim = Simulation::new(42);

    let config = SimulationConfig::from_file("configs/perf_config.yaml");

    let mut cluster_sim = ClusterSchedulingSimulation::new(sim, config, None);

    cluster_sim.run_with_scheduler(RoundRobinScheduler::new());
}
