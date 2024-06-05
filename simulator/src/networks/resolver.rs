use serde::{Deserialize, Deserializer, Serialize};

/// Holds supported cluster dataset types.
#[derive(Debug, PartialEq, Serialize, Clone)]
pub enum NetworkType {
    Constant,
    Shared,
    FatTree,
}

impl<'de> Deserialize<'de> for NetworkType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "constant" => Ok(NetworkType::Constant),
            "shared" => Ok(NetworkType::Shared),
            "fat-tree" => Ok(NetworkType::FatTree),
            _ => Err(serde::de::Error::custom("Invalid network type")),
        }
    }
}
