use std::collections::HashSet;

use crate::simulation::ClusterSchedulingSimulation;

#[derive(Default)]
pub struct ParallelSimulationsLauncher {
    pub simulations: Vec<ClusterSchedulingSimulation>,
    pub output_dirs: HashSet<String>,
}

impl ParallelSimulationsLauncher {
    pub fn add_simulation(&mut self, simulation: ClusterSchedulingSimulation) {
        if let Some(output_dir) = simulation.get_output_dir() {
            if self.output_dirs.contains(&output_dir) {
                panic!("Output dir {} is already in use", output_dir);
            }
            self.output_dirs.insert(output_dir);
        }

        self.simulations.push(simulation);
    }

    pub fn run_simulations(self) {
        let mut threads = Vec::new();
        for mut simulation in self.simulations.into_iter() {
            threads.push(std::thread::spawn(move || {
                simulation.run();
            }));
        }

        for thread in threads {
            thread.join().unwrap();
        }
    }
}
