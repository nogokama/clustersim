use core::panic;
use std::{cell::RefCell, collections::HashMap, hash::Hash, rc::Rc};

use async_trait::async_trait;
use maplit::hashmap;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sugars::{rc, refcell};

use crate::workload_generators::native;

use super::{
    combinators::{self, ParallelProfile, ProfileCombinator, SequentialProfile},
    default::{CommunicationHomogenous, CpuBurnHomogenous},
    profile::{ExecutionProfile, NameTrait},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ProfileDefinition {
    Simple(String),
    Detailed { r#type: String, args: serde_yaml::Value },
}

pub type ConstructorFn = Rc<dyn Fn(&serde_yaml::Value) -> Rc<dyn ExecutionProfile>>;

#[derive(Clone)]
pub struct ProfileBuilder {
    pub constructors: Rc<RefCell<HashMap<String, ConstructorFn>>>,
}

impl ProfileBuilder {
    pub fn new() -> Self {
        let constructors: Rc<RefCell<HashMap<String, ConstructorFn>>> = rc!(refcell!(HashMap::new()));
        constructors
            .borrow_mut()
            .insert(CpuBurnHomogenous::get_name(), Rc::new(from_yaml::<CpuBurnHomogenous>));
        constructors.borrow_mut().insert(
            CommunicationHomogenous::get_name(),
            Rc::new(from_yaml::<CommunicationHomogenous>),
        );

        let mut constructors_clone = constructors.clone();
        constructors.borrow_mut().insert(
            ParallelProfile::get_name(),
            Rc::new(move |json| parse_combinator::<ParallelProfile>(json, constructors_clone.clone())),
        );

        constructors_clone = constructors.clone();
        constructors.borrow_mut().insert(
            SequentialProfile::get_name(),
            Rc::new(move |json| parse_combinator::<SequentialProfile>(json, constructors_clone.clone())),
        );

        Self {
            constructors: constructors,
        }
    }

    pub fn parse_profiles(&self, yaml: &serde_yaml::Value) {
        yaml.as_mapping().unwrap().iter().for_each(|(name, value)| {
            let name = name.as_str().unwrap().to_string();
            println!("name: {}", &name);
            let profile = match serde_yaml::from_value::<ProfileDefinition>(value.clone()) {
                Ok(profile) => profile,
                Err(e) => panic!("Can't parse profile {}: {}", name, e),
            };

            match profile {
                ProfileDefinition::Simple(profile_name) => {
                    let constructor = self.constructors.borrow().get(&profile_name).cloned().unwrap();
                    self.constructors
                        .borrow_mut()
                        .insert(name.clone(), Rc::new(move |json| constructor(json)));
                }
                ProfileDefinition::Detailed { r#type, args } => {
                    let constructor = self.constructors.borrow().get(&r#type).cloned().unwrap();
                    self.constructors
                        .borrow_mut()
                        .insert(name.clone(), Rc::new(move |_| constructor(&args)));
                }
            };
        });
    }

    pub fn register_profile_with_constructor(&mut self, name: String, constructor: ConstructorFn) {
        self.constructors.borrow_mut().insert(name, constructor);
    }

    pub fn register_profile<T, S>(&mut self, name: S)
    where
        T: ExecutionProfile + DeserializeOwned + 'static,
        S: AsRef<str>,
    {
        self.constructors
            .borrow_mut()
            .insert(name.as_ref().to_string(), Rc::new(from_yaml::<T>));
    }

    pub fn build(&self, profile: ProfileDefinition) -> Rc<dyn ExecutionProfile> {
        Self::build_raw(profile, &self.constructors)
    }

    fn build_raw(
        profile: ProfileDefinition,
        constructors: &Rc<RefCell<HashMap<String, ConstructorFn>>>,
    ) -> Rc<dyn ExecutionProfile> {
        match profile {
            ProfileDefinition::Simple(profile_name) => {
                let constructor = constructors
                    .borrow()
                    .get(&profile_name)
                    .cloned()
                    .unwrap_or_else(|| panic!("Profile {} not found in the constructor list. Indicate it's definition above all the profiles dependant from it.", profile_name));
                constructor(&serde_yaml::Value::Null)
            }
            ProfileDefinition::Detailed { r#type, args } => {
                let constructor = constructors
                    .borrow()
                    .get(&r#type)
                    .cloned()
                    .unwrap_or_else(|| panic!("Profile {} not found in the constructor list. Indicate it's definition above all the profiles dependant from it.", r#type));
                constructor(&args)
            }
        }
    }
}

pub fn from_yaml<T>(yaml: &serde_yaml::Value) -> Rc<dyn ExecutionProfile>
where
    T: DeserializeOwned + ExecutionProfile + 'static,
{
    let profile: T = serde_yaml::from_value(yaml.clone()).unwrap();
    Rc::new(profile)
}

#[derive(Serialize, Deserialize, Clone)]
struct CombinatorDefinition {
    pub repeat: Option<u32>,
    pub profiles: Vec<ProfileDefinition>,
}

pub fn parse_combinator<T: ProfileCombinator>(
    yaml: &serde_yaml::Value,
    constructors: Rc<RefCell<HashMap<String, ConstructorFn>>>,
) -> Rc<T> {
    let combinator: CombinatorDefinition = serde_yaml::from_value(yaml.clone()).unwrap();
    let profiles = combinator
        .profiles
        .iter()
        .map(|profile| ProfileBuilder::build_raw(profile.clone(), &constructors))
        .collect::<Vec<_>>();

    Rc::new(T::new(profiles, combinator.repeat))
}
