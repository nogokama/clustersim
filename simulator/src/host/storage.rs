use std::collections::HashMap;

use dslab_core::Id;

pub struct ProcessHostStorage {
    process_to_host: HashMap<u64, Id>,
}

impl ProcessHostStorage {
    pub fn new() -> ProcessHostStorage {
        ProcessHostStorage {
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
}
