use dslab_core::Id;
use serde::Serialize;

use crate::{config::sim_config::HostConfig, host::cluster_host::ClusterHost};

#[derive(Serialize, Clone)]
pub struct HostAdded {
    pub host: HostConfig,
}

#[derive(Serialize, Clone)]
pub struct HostRemoved {
    pub id: String,
}

pub struct ClusterTopologyReader {}
