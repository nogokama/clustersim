mod profiles;
mod round_robin;

use std::io::Write;

use env_logger::Builder;

use dslab_core::Simulation;
use dslab_scheduling::{
    config::sim_config::SimulationConfig, simulation::ClusterSchedulingSimulation,
};

use profiles::TestProfile;
use round_robin::RoundRobinScheduler;

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let sim = Simulation::new(42);

    // let config = SimulationConfig::from_file("configs/config.yaml");
    let config = SimulationConfig::from_file("configs/config_with_users.yaml");
    // let config = SimulationConfig::from_file("configs/config_with_native.yaml");
    // let config = SimulationConfig::from_file("configs/config_with_combinators.yaml");
    // let config = SimulationConfig::from_file("configs/config_with_custom_profiles.yaml");
    // let config = SimulationConfig::from_file("configs/google_config.yaml");
    // let config = SimulationConfig::from_file("configs/google_converted.yaml");

    let mut cluster_sim = ClusterSchedulingSimulation::new(sim, config, None);

    cluster_sim.register_profile::<TestProfile>("test-profile");

    // cluster_sim.run(TetrisScheduler::new(cluster_id, scheduler_context));
    // cluster_sim.run_with_custom_scheduler(RoundRobinScheduler::new(cluster_id, scheduler_context));
    cluster_sim.run_with_scheduler(RoundRobinScheduler::new());
}
