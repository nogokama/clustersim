use serde::{Deserialize, Serialize};

/// A common structure for CPU and memory resource units.
/// All resource measurements are normalized and scaled.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Resources {
    pub cpus: Option<f32>,
    pub memory: Option<f32>,
}

/// This enum is used in the 'type' field of the CollectionEvent and
/// InstanceEvent tables.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(i32)]
pub enum EventType {
    /// The collection or instance was submitted to the scheduler for scheduling.
    Submit = 0,
    /// The collection or instance was marked not eligible for scheduling by the
    /// batch scheduler.
    Queue = 1,
    /// The collection or instance became eligible for scheduling.
    Enable = 2,
    /// The collection or instance started running.
    Schedule = 3,
    /// The collection or instance was descheduled because of a higher priority
    /// collection or instance, or because the scheduler overcommitted resources.
    Evict = 4,
    /// The collection or instance was descheduled due to a failure.
    Fail = 5,
    /// The collection or instance completed normally.
    Finish = 6,
    /// The collection or instance was cancelled by the user or because a
    /// depended-upon collection died.
    Kill = 7,
    /// The collection or instance was presumably terminated, but due to missing
    /// data there is insufficient information to identify when or how.
    Lost = 8,
    /// The collection or instance was updated (scheduling class or resource
    /// requirements) while it was waiting to be scheduled.
    UpdatePending = 9,
    /// The collection or instance was updated while it was scheduled somewhere.
    UpdateRunning = 10,
}

pub(super) fn deserialize_event_type<'de, D>(deserializer: D) -> Result<Option<EventType>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: i32 = Deserialize::deserialize(deserializer)?;
    match value {
        0 => Ok(Some(EventType::Submit)),
        1 => Ok(Some(EventType::Queue)),
        2 => Ok(Some(EventType::Enable)),
        3 => Ok(Some(EventType::Schedule)),
        4 => Ok(Some(EventType::Evict)),
        5 => Ok(Some(EventType::Fail)),
        6 => Ok(Some(EventType::Finish)),
        7 => Ok(Some(EventType::Kill)),
        8 => Ok(Some(EventType::Lost)),
        9 => Ok(Some(EventType::UpdatePending)),
        10 => Ok(Some(EventType::UpdateRunning)),
        _ => Ok(None),
    }
}

impl EventType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            EventType::Submit => "SUBMIT",
            EventType::Queue => "QUEUE",
            EventType::Enable => "ENABLE",
            EventType::Schedule => "SCHEDULE",
            EventType::Evict => "EVICT",
            EventType::Fail => "FAIL",
            EventType::Finish => "FINISH",
            EventType::Kill => "KILL",
            EventType::Lost => "LOST",
            EventType::UpdatePending => "UPDATE_PENDING",
            EventType::UpdateRunning => "UPDATE_RUNNING",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SUBMIT" => Some(Self::Submit),
            "QUEUE" => Some(Self::Queue),
            "ENABLE" => Some(Self::Enable),
            "SCHEDULE" => Some(Self::Schedule),
            "EVICT" => Some(Self::Evict),
            "FAIL" => Some(Self::Fail),
            "FINISH" => Some(Self::Finish),
            "KILL" => Some(Self::Kill),
            "LOST" => Some(Self::Lost),
            "UPDATE_PENDING" => Some(Self::UpdatePending),
            "UPDATE_RUNNING" => Some(Self::UpdateRunning),
            _ => None,
        }
    }
}

/// Collections are either jobs (which have tasks) or alloc sets (which have
/// alloc instances).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(i32)]
pub enum CollectionType {
    Job = 0,
    AllocSet = 1,
}
impl CollectionType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            CollectionType::Job => "JOB",
            CollectionType::AllocSet => "ALLOC_SET",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "JOB" => Some(Self::Job),
            "ALLOC_SET" => Some(Self::AllocSet),
            _ => None,
        }
    }
}

/// How latency-sensitive a thing is to CPU scheduling delays when running
/// on a machine, in increasing-sensitivity order.
/// Note that this is _not_ the same as the thing's cluster-scheduling
/// priority although latency-sensitive things do tend to have higher priorities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(i32)]
pub enum LatencySensitivity {
    /// Also known as "best effort".
    MostInsensitive = 0,
    /// Often used for batch jobs.
    Insensitive = 1,
    /// Used for latency-sensitive jobs.
    Sensitive = 2,
    /// Used for the most latency-senstive jobs.
    MostSensitive = 3,
}
impl LatencySensitivity {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            LatencySensitivity::MostInsensitive => "MOST_INSENSITIVE",
            LatencySensitivity::Insensitive => "INSENSITIVE",
            LatencySensitivity::Sensitive => "SENSITIVE",
            LatencySensitivity::MostSensitive => "MOST_SENSITIVE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "MOST_INSENSITIVE" => Some(Self::MostInsensitive),
            "INSENSITIVE" => Some(Self::Insensitive),
            "SENSITIVE" => Some(Self::Sensitive),
            "MOST_SENSITIVE" => Some(Self::MostSensitive),
            _ => None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct InstanceEvent {
    /// Timestamp, in microseconds since the start of the trace.
    pub time: Option<i64>,
    /// What type of event is this?
    #[serde(deserialize_with = "deserialize_event_type")]
    pub r#type: Option<EventType>,
    /// The identity of the collection that this instance is part of.
    pub collection_id: Option<i64>,
    /// How latency-sensitive is the instance?
    pub scheduling_class: Option<LatencySensitivity>,
    /// What type of collection this instance belongs to.
    // pub collection_type: Option<CollectionType>,
    /// Cluster-level scheduling priority for the instance.
    pub priority: Option<i32>,
    /// (Tasks only) The ID of the alloc set that this task is running in, or
    /// NO_ALLOC_COLLECTION if it is not running in an alloc.
    pub alloc_collection_id: Option<i64>,
    /// Begin: fields specific to instances
    /// The index of the instance in its collection (starts at 0).
    pub instance_index: Option<i32>,
    /// The ID of the machine on which this instance is placed (or NO_MACHINE if
    /// not placed on one, or DEDICATED_MACHINE if it's on a dedicated machine).
    pub machine_id: Option<i64>,
    /// (Tasks only) The index of the alloc instance that this task is running in,
    /// or NO_ALLOC_INDEX if it is not running in an alloc.
    pub alloc_instance_index: Option<i32>,
    /// The resources requested when the instance was submitted or last updated.
    pub cpus: Option<f64>,
    pub memory: Option<f64>,
}

/// Collection events apply to the collection as a whole.
///
/// Common fields shared between instances and collections.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionEvent {
    /// Timestamp, in microseconds since the start of the trace.
    pub time: Option<i64>,
    /// What type of event is this?
    #[serde(deserialize_with = "deserialize_event_type")]
    pub r#type: Option<EventType>,
    /// The identity of the collection.
    pub collection_id: Option<i64>,
    /// How latency-sensitive is the collection?
    pub scheduling_class: Option<LatencySensitivity>,
    /// What type of collection is this?
    pub collection_type: Option<CollectionType>,
    /// Cluster-level scheduling priority for the collection.
    pub priority: Option<i32>,
    /// The ID of the alloc set that this job is to run in, or NO_ALLOC_COLLECTION
    /// (only for jobs).
    pub alloc_collection_id: Option<i64>,
    /// The user who runs the collection
    pub user: Option<String>,
    /// Obfuscated name of the collection.
    pub collection_name: Option<String>,
    /// ID of the collection that this is a child of.
    /// (Used for stopping a collection when the parent terminates.)
    pub parent_collection_id: Option<i64>,
    /// IDs of collections that must finish before this collection may start.
    pub start_after_collection_ids: Vec<i64>,
    /// Maximum number of instances of this collection that may be placed on
    /// one machine (or 0 if unlimited).
    pub max_per_machine: Option<i32>,
    /// Maximum number of instances of this collection that may be placed on
    /// machines connected to a single Top of Rack switch (or 0 if unlimited).
    pub max_per_switch: Option<i32>,
}
/// Machine events describe the addition, removal, or update (change) of a
/// machine in the cluster at a particular time.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MachineEvent {
    /// Timestamp, in microseconds since the start of the trace. \[key\]
    pub time: Option<i64>,
    /// Unique ID of the machine within the cluster. \[key\]
    pub machine_id: ::core::option::Option<i64>,
    /// Specifies the type of event
    #[serde(deserialize_with = "machine_event::deserialize_event_type")]
    pub r#type: Option<machine_event::EventType>,
    /// Obfuscated name of the Top of Rack switch that this machine is attached to.
    pub switch_id: Option<String>,
    /// Available resources that the machine supplies.  (Note: may be smaller
    /// than the physical machine's raw capacity.)
    pub cpus: Option<f64>,
    pub memory: Option<f64>,
    /// An obfuscated form of the machine platform (microarchitecture + motherboard
    /// design).
    pub platform_id: Option<String>,
}
/// Nested message and enum types in `MachineEvent`.
pub mod machine_event {
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
    #[serde(untagged)]
    pub enum EventType {
        /// Should never happen :-).
        Unknown = 0,
        /// Machine added to the cluster.
        Add = 1,
        /// Machine removed from cluster (usually due to failure or repairs).
        Remove = 2,
        /// Machine capacity updated (while not removed).
        Update = 3,
    }

    pub(super) fn deserialize_event_type<'de, D>(deserializer: D) -> Result<Option<EventType>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: i32 = Deserialize::deserialize(deserializer)?;
        match value {
            1 => Ok(Some(EventType::Add)),
            2 => Ok(Some(EventType::Remove)),
            3 => Ok(Some(EventType::Update)),
            0 => Ok(Some(EventType::Unknown)),
            _ => Ok(None),
        }
    }

    impl EventType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                EventType::Unknown => "EVENT_TYPE_UNKNOWN",
                EventType::Add => "ADD",
                EventType::Remove => "REMOVE",
                EventType::Update => "UPDATE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "EVENT_TYPE_UNKNOWN" => Some(Self::Unknown),
                "ADD" => Some(Self::Add),
                "REMOVE" => Some(Self::Remove),
                "UPDATE" => Some(Self::Update),
                _ => None,
            }
        }
    }
    /// If we detect that data is missing, why do we know this?
    #[derive(Clone, Copy, Debug, Hash)]
    #[repr(i32)]
    pub enum MissingDataReason {
        /// No data is missing.
        None = 0,
        /// We observed that a change to the state of a machine must have
        /// occurred from an internal state snapshot, but did not see a
        /// corresponding transition event during the trace.
        SnapshotButNoTransition = 1,
    }
    impl MissingDataReason {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                MissingDataReason::None => "MISSING_DATA_REASON_NONE",
                MissingDataReason::SnapshotButNoTransition => "SNAPSHOT_BUT_NO_TRANSITION",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "MISSING_DATA_REASON_NONE" => Some(Self::None),
                "SNAPSHOT_BUT_NO_TRANSITION" => Some(Self::SnapshotButNoTransition),
                _ => None,
            }
        }
    }
}
