use std::{
    collections::{HashMap, VecDeque},
    f64::consts::PI,
};

use dslab_core::{cast, EventHandler, Id, SimulationContext};
use dslab_scheduling::{
    cluster::{Schedule, TaskFinished},
    event_generator::{HostAdded, JobInfo},
    machine::Machine,
    scheduler::{Resources, Scheduler},
};

pub struct TetrisScheduler {
    cluster_id: Id,
    machines: Vec<Machine>,
    available_resources: HashMap<Id, Resources>,
    queue: VecDeque<JobInfo>,

    ctx: SimulationContext,
}

impl TetrisScheduler {
    pub fn new(cluster_id: Id, ctx: SimulationContext) -> TetrisScheduler {
        TetrisScheduler {
            cluster_id,
            machines: Vec::new(),
            available_resources: HashMap::new(),
            queue: VecDeque::new(),
            ctx,
        }
    }

    fn on_task_info(&mut self, task_id: u64, cpu_cores: u32, memory: u64) {
        self.queue.push_back(JobInfo {
            id: task_id,
            cpu_cores,
            memory,
        });

        self.schedule();
    }

    fn on_task_finished(&mut self, task_id: u64, machine_id: Id, available_cores: u32, available_memory: u64) {
        self.available_resources.insert(
            machine_id,
            Resources {
                cpu_cores: available_cores,
                memory: available_memory,
            },
        );

        self.schedule();
    }

    fn angle_between(task: &JobInfo, machine: &Machine) -> f64 {
        let atan2_self = (task.cpu_cores as f64).atan2(task.memory as f64);
        let atan2_other = (machine.cpu_cores as f64).atan2(machine.memory as f64);

        let angle = (atan2_other - atan2_self + 2.0 * PI) % (2.0 * PI);

        // Ensure the angle is within the range [0, PI]
        if angle > PI {
            2.0 * PI - angle
        } else {
            angle
        }
    }

    fn schedule(&mut self) {
        let mut started_from: Option<u64> = None;

        while let Some(task) = self.queue.pop_front() {
            if started_from == None {
                started_from = Some(task.id);
            }

            let mut scheduled = false;

            self.machines.sort_by(|a, b| {
                TetrisScheduler::angle_between(&task, a)
                    .partial_cmp(&TetrisScheduler::angle_between(&task, b))
                    .unwrap()
            });

            for machine in &self.machines {
                if let Some(resources) = self.available_resources.get_mut(&machine.id) {
                    if resources.cpu_cores >= task.cpu_cores && resources.memory >= task.memory {
                        resources.cpu_cores -= task.cpu_cores;
                        resources.memory -= task.memory;
                        self.ctx.emit_now(
                            Schedule {
                                task_id: task.id,
                                machine_id: machine.id,
                            },
                            self.cluster_id,
                        );
                        scheduled = true;
                        started_from = None;
                        break;
                    }
                }
            }

            if !scheduled {
                let curr_id = task.id;
                self.queue.push_back(task);
                if let Some(started_id) = started_from {
                    if curr_id == started_id {
                        break;
                    }
                }
            }
        }
    }
}

impl EventHandler for TetrisScheduler {
    fn on(&mut self, event: dslab_core::Event) {
        cast!(match event.data {
            HostAdded { machine, time: _ } => {
                self.available_resources.insert(
                    machine.id,
                    Resources {
                        cpu_cores: machine.cpu_cores,
                        memory: machine.memory,
                    },
                );
                self.machines.push(machine);
            }
            JobInfo { id, cpu_cores, memory } => {
                self.on_task_info(id, cpu_cores, memory)
            }
            TaskFinished {
                task_id,
                machine_id,
                available_cores,
                available_memory,
            } => {
                self.on_task_finished(task_id, machine_id, available_cores, available_memory)
            }
        })
    }
}

impl Scheduler for TetrisScheduler {
    fn id(&self) -> Id {
        self.ctx.id()
    }

    fn name(&self) -> String {
        self.ctx.name().to_string()
    }
}
