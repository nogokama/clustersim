use std::collections::HashMap;

use dslab_core::Id;

use crate::workload_generators::events::ExecutionRequest;

pub struct SharedInfoStorage {
    pub jobs_info: HashMap<u64, ExecutionRequest>,
    pub internal_host_id_2_host_trace_id: HashMap<Id, u64>,
    pub host_trace_id_2_internal_id: HashMap<u64, Id>,
}

impl SharedInfoStorage {
    pub fn new() -> SharedInfoStorage {
        SharedInfoStorage {
            jobs_info: HashMap::new(),
            internal_host_id_2_host_trace_id: HashMap::new(),
            host_trace_id_2_internal_id: HashMap::new(),
        }
    }

    pub fn insert_host_with_trace_id(&mut self, internal_id: Id, id_within_trace: u64) {
        self.internal_host_id_2_host_trace_id
            .insert(internal_id, id_within_trace);
        self.host_trace_id_2_internal_id.insert(id_within_trace, internal_id);
    }

    pub fn get_execution_request(&self, task_id: u64) -> ExecutionRequest {
        self.jobs_info.get(&task_id).unwrap().clone()
    }

    pub fn set_execution_request(&mut self, task_id: u64, task_request: ExecutionRequest) {
        self.jobs_info.insert(task_id, task_request);
    }
}
