use serde::Serialize;

use crate::config::sim_config::HostConfig;

#[derive(Serialize, Clone)]
pub struct HostAdded {
    pub host: HostConfig,
}

#[derive(Serialize, Clone)]
pub struct HostRemoved {
    pub id: String,
}

pub struct ClusterTopologyReader {}
