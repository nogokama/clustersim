use std::rc::Rc;

use async_trait::async_trait;
use dslab_compute::multicore::CoresDependency;
use futures::future::join_all;
use serde::{Deserialize, Serialize};

use crate::host::process::HostProcessInstance;

use crate::execution_profiles::profile::ExecutionProfile;

use super::profile::NameTrait;

#[derive(Deserialize, Serialize)]
pub struct Idle {
    pub time: f64,
}

#[async_trait(?Send)]
impl ExecutionProfile for Idle {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        processes[0].sleep(self.time).await;
    }

    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for Idle {
    fn get_name() -> String {
        "idle".to_string()
    }
}

#[derive(Deserialize, Serialize)]
pub struct CpuBurnHomogenous {
    pub compute_work: f64,
}

#[async_trait(?Send)]
impl ExecutionProfile for CpuBurnHomogenous {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        join_all(
            processes
                .iter()
                .map(|p| p.run_compute(self.compute_work, CoresDependency::Linear)),
        )
        .await;
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for CpuBurnHomogenous {
    fn get_name() -> String {
        "cpu-burn-homogenous".to_string()
    }
}

#[derive(Deserialize)]
pub struct CommunicationHomogenous {
    pub size: f64,
}

#[async_trait(?Send)]
impl ExecutionProfile for CommunicationHomogenous {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        let mut futures = vec![];
        for i in 0..processes.len() {
            for j in 0..processes.len() {
                if i != j {
                    futures.push(processes[i].transfer_data_to_process(self.size, processes[j].id));
                }
            }
        }
        join_all(futures).await;
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for CommunicationHomogenous {
    fn get_name() -> String {
        "communication-homogenous".to_string()
    }
}

#[derive(Deserialize)]
pub struct MasterWorkersProfile {
    pub master_compute_work: f64,
    pub worker_compute_work: f64,
    pub data_transfer_size: f64,
}

#[async_trait(?Send)]
impl ExecutionProfile for MasterWorkersProfile {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        let master_process = &processes[0];
        let worker_processes = &processes[1..];

        futures::future::join_all(worker_processes.iter().map(|p| async {
            master_process
                .transfer_data_to_process(self.data_transfer_size, p.id)
                .await;
            p.run_compute(self.worker_compute_work, CoresDependency::Linear)
                .await;
        }))
        .await;

        master_process
            .run_compute(self.master_compute_work, CoresDependency::Linear)
            .await;
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for MasterWorkersProfile {
    fn get_name() -> String {
        "master-workers".to_string()
    }
}
