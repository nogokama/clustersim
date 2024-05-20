use std::rc::Rc;

use async_trait::async_trait;

use crate::host::process::HostProcessInstance;

#[async_trait(?Send)]
pub trait ExecutionProfile {
    async fn run(self: Rc<Self>, processes: &[HostProcessInstance]);
    fn name(&self) -> String;
}

pub trait NameTrait {
    fn get_name() -> String;
}
