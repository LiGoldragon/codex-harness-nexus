#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct EventBatch {
    pub session: harness::Session,
    pub flow_id: flow::FlowId,
    pub observation_vector: std::vec::Vec<signal_codex_harness_nexus::Observation>,
    pub rollout_option: std::option::Option<memory::Rollout>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Operation {
    Reporting(signal_codex_harness_nexus::Hook),
    Reading(memory::Watch),
    Discovering(harness::FilePath),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    Reported(EventBatch),
    Read(EventBatch),
    Discovered(std::option::Option<EventBatch>),
}
