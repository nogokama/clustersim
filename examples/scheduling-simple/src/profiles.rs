use std::rc::Rc;

use async_trait::async_trait;
use dslab_compute::multicore::CoresDependency;
use dslab_scheduling::{execution_profiles::profile::ExecutionProfile, host::process::HostProcessInstance};
use futures::{future::join_all, join};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct TestProfile {
    pub compute_work: f64,
    pub data_transfer: f64,
}

#[async_trait(?Send)]
impl ExecutionProfile for TestProfile {
    async fn run(self: Rc<Self>, processes: &Vec<HostProcessInstance>) {
        let mut compute_futures = vec![];
        let mut transfer_futures = vec![];
        for i in 0..processes.len() {
            compute_futures.push(processes[i].run_flops(self.compute_work, CoresDependency::Linear));
            for j in 0..processes.len() {
                transfer_futures.push(processes[i].transfer_data_to_process(self.data_transfer, processes[j].id));
            }
        }
        join!(join_all(compute_futures), join_all(transfer_futures));
    }

    fn name(&self) -> String {
        "test-profile".to_string()
    }
}
