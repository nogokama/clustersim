use std::rc::Rc;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::host::process::HostProcessInstance;

#[async_trait(?Send)]
pub trait ExecutionProfile {
    async fn run(self: Rc<Self>, processes: &Vec<HostProcessInstance>);
    fn name(&self) -> String;
}

pub trait NameTrait {
    fn get_name() -> String;
}
