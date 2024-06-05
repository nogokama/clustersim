use dslab_network::{models::SharedBandwidthNetworkModel, Link, Network};
use rustc_hash::FxHashMap;

use crate::config::sim_config::NetworkLinkConfig;

pub fn make_fat_tree_topology(
    network: &mut Network,
    l2_switch_count: usize,
    l1_switch_count: usize,
    switches: FxHashMap<String, usize>, // switch[i] shows the l1 switch number for host i
    uplink_config: &NetworkLinkConfig,
    downlink_config: &NetworkLinkConfig,
    switch_config: &NetworkLinkConfig,
) {
    for i in 0..l2_switch_count {
        let switch_name = format!("l2_switch_{}", i);
        network.add_node(
            &switch_name,
            Box::new(SharedBandwidthNetworkModel::new(
                switch_config.bandwidth,
                switch_config.latency,
            )),
        );
    }

    for i in 0..l1_switch_count {
        let switch_name = format!("l1_switch_{}", i);
        network.add_node(
            &switch_name,
            Box::new(SharedBandwidthNetworkModel::new(
                switch_config.bandwidth,
                switch_config.latency,
            )),
        );

        for j in 0..l2_switch_count {
            network.add_link(
                &switch_name,
                &format!("l2_switch_{}", j),
                Link::shared(uplink_config.bandwidth, uplink_config.latency),
            );
        }
    }

    for (host_name, l1_num) in switches.iter() {
        assert!(
            *l1_num < l1_switch_count,
            "Invalid switch number {}",
            l1_num
        );
        let switch_name = format!("l1_switch_{}", l1_num);
        network.add_link(
            &switch_name,
            host_name,
            Link::shared(downlink_config.bandwidth, downlink_config.latency),
        );
    }
}
