use std::rc::Rc;

use async_trait::async_trait;
use futures::future::join_all;

use crate::host::process::HostProcessInstance;

use super::profile::{ExecutionProfile, NameTrait};

pub trait ProfileCombinator {
    fn new(profiles: Vec<Rc<dyn ExecutionProfile>>, repeat: Option<u32>) -> Self;
}

pub struct ParallelProfile {
    pub profiles: Vec<Rc<dyn ExecutionProfile>>,
    pub repeat: Option<u32>,
}

#[async_trait(?Send)]
impl ExecutionProfile for ParallelProfile {
    async fn run(self: Rc<Self>, processes: &Vec<HostProcessInstance>) {
        for _ in 0..self.repeat.unwrap_or(1) {
            join_all(self.profiles.iter().map(|p| p.clone().run(processes))).await;
        }
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl ProfileCombinator for ParallelProfile {
    fn new(profiles: Vec<Rc<dyn ExecutionProfile>>, repeat: Option<u32>) -> Self {
        Self { profiles, repeat }
    }
}

impl NameTrait for ParallelProfile {
    fn get_name() -> String {
        "parallel".to_string()
    }
}

pub struct SequentialProfile {
    pub profiles: Vec<Rc<dyn ExecutionProfile>>,
    pub repeat: Option<u32>,
}

#[async_trait(?Send)]
impl ExecutionProfile for SequentialProfile {
    async fn run(self: Rc<Self>, processes: &Vec<HostProcessInstance>) {
        for _ in 0..self.repeat.unwrap_or(1) {
            for profile in self.profiles.iter() {
                profile.clone().run(processes).await;
            }
        }
    }
    fn name(&self) -> String {
        Self::get_name()
    }
}

impl ProfileCombinator for SequentialProfile {
    fn new(profiles: Vec<Rc<dyn ExecutionProfile>>, repeat: Option<u32>) -> Self {
        Self { profiles, repeat }
    }
}

impl NameTrait for SequentialProfile {
    fn get_name() -> String {
        "sequence".to_string()
    }
}
