mod round_robin;

use std::io::Write;

use env_logger::Builder;

use dslab_core::Simulation;
use dslab_scheduling::{
    config::sim_config::SimulationConfig, parallel_launcher::ParallelSimulationsLauncher,
    simulation::ClusterSchedulingSimulation,
};

use round_robin::RoundRobinScheduler;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let mut launcher = ParallelSimulationsLauncher::default();
    {
        let sim = Simulation::new(42);
        let config = SimulationConfig::from_file("configs/config.yaml");
        let mut cluster_sim = ClusterSchedulingSimulation::new(sim, config, None);
        cluster_sim.set_scheduler(RoundRobinScheduler::new());
        launcher.add_simulation(cluster_sim);
    }
    {
        let sim = Simulation::new(42);
        let config = SimulationConfig::from_file("configs/config_with_users.yaml");
        let mut cluster_sim = ClusterSchedulingSimulation::new(sim, config, None);
        cluster_sim.set_scheduler(RoundRobinScheduler::new());
        launcher.add_simulation(cluster_sim);
    }

    launcher.run_simulations();
}
