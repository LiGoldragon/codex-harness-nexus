#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ReadyConfiguration {
    pub flows_root: meta_signal_codex_harness_nexus::FlowsRoot,
    pub identity_producer: meta_signal_codex_harness_nexus::IdentityProducer,
    pub sessions_directories: meta_signal_codex_harness_nexus::SessionsDirectories,
    pub capacity: meta_signal_codex_harness_nexus::Capacity,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Performed {
    pub changed_vector: std::vec::Vec<signal_codex_harness_nexus::Changed>,
    pub lane_failure_option: std::option::Option<nexus_exchange::LaneFailure>,
}
#[rustfmt::skip]
pub trait Diagnosing {
    fn diagnostic(&self) -> signal_codex_harness_nexus::Diagnostic;
}
#[rustfmt::skip]
pub trait Sessional {
    fn session_id(&self) -> harness::SessionId;
}
#[rustfmt::skip]
pub trait Operable {
    fn operation(&self) -> operation::Operation;
}
#[rustfmt::skip]
pub trait Planned {
    fn plan(&self) -> folding::FoldPlan;
}
#[rustfmt::skip]
pub trait Settable {
    fn setting(&self) -> meta_signal_codex_harness_nexus::Set;
}
#[rustfmt::skip]
pub trait Readying {
    fn ready(&self) -> std::option::Option<ReadyConfiguration>;
}
#[rustfmt::skip]
pub trait NamingPaths {
    fn paths(&self) -> std::vec::Vec<harness::FilePath>;
}
#[rustfmt::skip]
pub trait Observing {
    fn publish(&mut self, input: std::vec::Vec<signal_codex_harness_nexus::Changed>);
    fn prepare(
        &mut self,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::Capacity,
        signal_codex_harness_nexus::ReportFailure,
    >;
}
#[rustfmt::skip]
pub trait Attempting {
    fn attempt<N: Operable>(
        &mut self,
        input: N,
    ) -> std::result::Result<
        operation::Outcome,
        signal_codex_harness_nexus::ReportFailure,
    >;
}
#[rustfmt::skip]
pub trait Performing {
    fn perform<N: Operable>(
        &mut self,
        input: N,
    ) -> std::result::Result<Performed, signal_codex_harness_nexus::ReportFailure>;
}
#[rustfmt::skip]
pub trait Waking {
    fn startup(
        &mut self,
    ) -> std::result::Result<
        std::vec::Vec<operation::Operation>,
        signal_codex_harness_nexus::ReportFailure,
    >;
    fn woken(
        &mut self,
        input: std::vec::Vec<harness::FilePath>,
    ) -> std::result::Result<
        std::vec::Vec<operation::Operation>,
        signal_codex_harness_nexus::ReportFailure,
    >;
}
#[rustfmt::skip]
pub trait Remembering {
    fn recall<N: Sessional>(
        &self,
        input: N,
    ) -> std::result::Result<
        std::option::Option<memory::SessionRecord>,
        signal_codex_harness_nexus::MemoryFault,
    >;
    fn recall_all(
        &self,
    ) -> std::result::Result<
        std::vec::Vec<memory::SessionRecord>,
        signal_codex_harness_nexus::MemoryFault,
    >;
    fn commit<N: Planned>(
        &mut self,
        input: N,
    ) -> std::result::Result<
        std::vec::Vec<signal_codex_harness_nexus::Changed>,
        signal_codex_harness_nexus::MemoryFault,
    >;
    fn configuration(
        &self,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::Configuration,
        signal_codex_harness_nexus::MemoryFault,
    >;
    fn configure<N: Settable>(
        &mut self,
        input: N,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::Configuration,
        signal_codex_harness_nexus::MemoryFault,
    >;
}
#[rustfmt::skip]
const _: () = {
    fn assert_operation_operable<T: Operable>() {}
    let _ = assert_operation_operable::<operation::Operation>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_hook_operable<T: Operable>() {}
    let _ = assert_hook_operable::<signal_codex_harness_nexus::Hook>;
    fn assert_hook_sessional<T: Sessional>() {}
    let _ = assert_hook_sessional::<signal_codex_harness_nexus::Hook>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_eventbatch_sessional<T: Sessional>() {}
    let _ = assert_eventbatch_sessional::<operation::EventBatch>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_foldplan_planned<T: Planned>() {}
    let _ = assert_foldplan_planned::<folding::FoldPlan>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_set_settable<T: Settable>() {}
    let _ = assert_set_settable::<meta_signal_codex_harness_nexus::Set>;
    fn assert_set_namingpaths<T: NamingPaths>() {}
    let _ = assert_set_namingpaths::<meta_signal_codex_harness_nexus::Set>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_configuration_readying<T: Readying>() {}
    let _ = assert_configuration_readying::<
        meta_signal_codex_harness_nexus::Configuration,
    >;
};
