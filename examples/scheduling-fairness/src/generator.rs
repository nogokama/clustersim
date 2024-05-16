use std::fs::File;

use dslab_scheduling::config::sim_config::GroupHostConfig;
use serde::Serialize;

#[derive(Serialize)]
struct Group {
    pub name: Option<String>,
    pub name_prefix: Option<String>,

    pub cpus: u32,
    pub memory: u64,
    pub count: u32,
}

pub fn generate_yaml() {
    let hosts = vec![
        (5, 20, 1),
        (5, 40, 2),
        (7, 20, 1),
        (7, 30, 1),
        (7, 40, 1),
        (7, 50, 2),
        (7, 80, 2),
        (10, 10, 2),
        (10, 30, 2),
        (10, 40, 1),
        (10, 50, 2),
        (10, 60, 2),
        (10, 70, 1),
        (12, 20, 1),
        (12, 30, 1),
        (12, 40, 1),
        (12, 50, 2),
        (12, 60, 2),
        (13, 40, 1),
        (13, 50, 2),
        (13, 70, 1),
        (16, 20, 2),
        (16, 40, 4),
        (16, 50, 2),
        (16, 60, 1),
        (16, 70, 1),
        (16, 80, 2),
        (17, 40, 1),
        (17, 90, 2),
        (18, 50, 1),
        (18, 60, 2),
        (20, 30, 1),
        (20, 50, 3),
        (20, 70, 2),
        (24, 30, 2),
        (24, 40, 2),
        (24, 60, 1),
        (24, 80, 3),
        (28, 30, 1),
        (28, 50, 1),
        (28, 60, 1),
        (28, 70, 1),
    ];

    let groups = serde_yaml::to_writer(
        File::create("groups.yaml").unwrap(),
        &hosts
            .iter()
            .map(|(cpus, memory, count)| Group {
                name: Some(format!("group-{}-{}", cpus, memory)),
                name_prefix: Some(format!("group-{}-{}", cpus, memory)),
                cpus: *cpus,
                memory: *memory,
                count: *count,
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
}
