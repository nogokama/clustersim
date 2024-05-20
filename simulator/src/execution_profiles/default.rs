use std::rc::Rc;

use async_trait::async_trait;
use futures::future::join_all;
use serde::{Deserialize, Serialize};

use dslab_compute::multicore::CoresDependency;
use dslab_core::Id;

use crate::execution_profiles::profile::ExecutionProfile;
use crate::host::process::HostProcessInstance;

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
pub struct ComputeHomogenous {
    pub compute_work: f64,
}

#[async_trait(?Send)]
impl ExecutionProfile for ComputeHomogenous {
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

impl NameTrait for ComputeHomogenous {
    fn get_name() -> String {
        "compute-homogenous".to_string()
    }
}

#[derive(Deserialize)]
pub struct ComputeVector {
    compute_work: Vec<f64>,
}

#[async_trait(?Send)]
impl ExecutionProfile for ComputeVector {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        if processes.len() != self.compute_work.len() {
            panic!("Number of processes and compute work must be equal");
        }
        join_all(
            processes
                .iter()
                .enumerate()
                .map(|(id, p)| p.run_compute(self.compute_work[id], CoresDependency::Linear)),
        )
        .await;
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for ComputeVector {
    fn get_name() -> String {
        "compute-vector".to_string()
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
pub struct CommunicationMatrix {
    pub matrix: Vec<Vec<f64>>,
}

#[async_trait(?Send)]
impl ExecutionProfile for CommunicationMatrix {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        if processes.len() != self.matrix.len() {
            panic!("Number of processes and matrix size must be equal");
        }
        let mut futures = vec![];
        for i in 0..processes.len() {
            for j in 0..processes.len() {
                if self.matrix[i].len() <= j {
                    panic!("Matrix must be square");
                }
                if self.matrix[i][j] != 0.0 {
                    futures.push(
                        processes[i].transfer_data_to_process(self.matrix[i][j], processes[j].id),
                    );
                }
            }
        }
        join_all(futures).await;
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for CommunicationMatrix {
    fn get_name() -> String {
        "communication-matrix".to_string()
    }
}

#[derive(Deserialize)]
pub struct CommunicationSrcDst {
    pub src: Vec<usize>,
    pub dst: Vec<usize>,
    pub size: f64,
}

#[async_trait(?Send)]
impl ExecutionProfile for CommunicationSrcDst {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        let mut futures = vec![];
        for s in self.src.iter() {
            for d in self.dst.iter() {
                if s >= &processes.len() || d >= &processes.len() {
                    panic!("Invalid process index");
                }
                futures.push(processes[*s].transfer_data_to_process(self.size, processes[*d].id));
            }
        }
        join_all(futures).await;
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for CommunicationSrcDst {
    fn get_name() -> String {
        "communication-src-dst".to_string()
    }
}

#[derive(Deserialize)]
pub struct CommunicationExternal {
    processes: Vec<usize>,
    input_size: Option<f64>,
    output_size: Option<f64>,
    component_id: Id,
}

#[async_trait(?Send)]
impl ExecutionProfile for CommunicationExternal {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        let mut input_futures = vec![];
        let mut output_futures = vec![];
        for p in self.processes.iter() {
            if p >= &processes.len() {
                panic!("Invalid process index");
            }
            if self.input_size.unwrap_or(0.) > 0.0 {
                input_futures.push(
                    processes[*p]
                        .transfer_data_from_component(self.input_size.unwrap(), self.component_id),
                );
            }
            if self.output_size.unwrap_or(0.) > 0.0 {
                output_futures.push(
                    processes[*p]
                        .transfer_data_to_component(self.output_size.unwrap(), self.component_id),
                );
            }
        }
        futures::join!(join_all(input_futures), join_all(output_futures));
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for CommunicationExternal {
    fn get_name() -> String {
        "communication-external".to_string()
    }
}

#[derive(Deserialize)]
pub struct DiskOps {
    pub processes: Vec<usize>,
    pub read_size: Option<u64>,
    pub write_size: Option<u64>,
}

#[async_trait(?Send)]
impl ExecutionProfile for DiskOps {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]) {
        let mut read_futures = vec![];
        let mut write_futures = vec![];
        for p in self.processes.iter() {
            if p >= &processes.len() {
                panic!("Invalid process index");
            }
            if self.read_size.unwrap_or(0) > 0 {
                read_futures.push(processes[*p].read_data(self.read_size.unwrap()));
            }
            if self.write_size.unwrap_or(0) > 0 {
                write_futures.push(processes[*p].write_data(self.write_size.unwrap()));
            }
        }
        futures::join!(join_all(read_futures), join_all(write_futures));
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl NameTrait for DiskOps {
    fn get_name() -> String {
        "disk-ops".to_string()
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
