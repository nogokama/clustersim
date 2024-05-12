use std::collections::HashMap;

use dslab_core::Id;

use crate::workload_generators::events::{CollectionRequest, ExecutionRequest};

pub struct SharedInfoStorage {
    pub executions_info: HashMap<u64, ExecutionRequest>,
    pub collections: HashMap<u64, CollectionRequest>,
    pub internal_host_id_2_host_trace_id: HashMap<Id, u64>,
    pub host_trace_id_2_internal_id: HashMap<u64, Id>,
    process_to_host: HashMap<u64, Id>,
}

impl SharedInfoStorage {
    pub fn new() -> SharedInfoStorage {
        SharedInfoStorage {
            executions_info: HashMap::new(),
            collections: HashMap::new(),
            internal_host_id_2_host_trace_id: HashMap::new(),
            host_trace_id_2_internal_id: HashMap::new(),
            process_to_host: HashMap::new(),
        }
    }
    pub fn get_host_id(&self, process_id: u64) -> Id {
        self.process_to_host.get(&process_id).unwrap().clone()
    }

    pub fn set_host_id(&mut self, process_id: u64, host_id: Id) {
        self.process_to_host.insert(process_id, host_id);
    }

    pub fn remove_process(&mut self, process_id: u64) {
        self.process_to_host.remove(&process_id);
    }

    pub fn add_collection(&mut self, collection: CollectionRequest) {
        self.collections.insert(collection.id.unwrap(), collection);
    }

    pub fn get_execution_user(&self, execution_id: u64) -> Option<String> {
        if let Some(execution) = self.executions_info.get(&execution_id) {
            if let Some(collection_id) = execution.collection_id {
                if let Some(collection) = self.collections.get(&collection_id) {
                    return collection.user.clone();
                }
            }
        }
        None
    }

    pub fn insert_host_with_trace_id(&mut self, internal_id: Id, id_within_trace: u64) {
        self.internal_host_id_2_host_trace_id
            .insert(internal_id, id_within_trace);
        self.host_trace_id_2_internal_id.insert(id_within_trace, internal_id);
    }

    pub fn get_execution_request(&self, task_id: u64) -> ExecutionRequest {
        self.executions_info.get(&task_id).unwrap().clone()
    }

    pub fn set_execution_request(&mut self, task_id: u64, task_request: ExecutionRequest) {
        self.executions_info.insert(task_id, task_request);
    }
}
