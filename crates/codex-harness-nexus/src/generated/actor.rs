#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchGeneration(pub i64);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum WatchLifecycle {
    Active,
    Staged,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum WatchChangeKind {
    CreatedFolder,
    Created,
    Removed,
    RenamedFrom,
    Modified,
    Other,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchEvent {
    pub watch_change_kind: WatchChangeKind,
    pub file_path_vector: std::vec::Vec<harness::FilePath>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchFailure {
    pub watch_target_option: std::option::Option<WatchTarget>,
    pub file_path_option: std::option::Option<harness::FilePath>,
    pub diagnostic: signal_codex_harness_nexus::Diagnostic,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum WatchResult {
    WatchEvent(WatchEvent),
    WatchFailure(WatchFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchWord {
    pub watch_generation: WatchGeneration,
    pub watch_result: WatchResult,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Reach {
    Entries,
    Whole,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum WatchTarget {
    SessionsDirectory,
    RolloutFile,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchPlacement {
    pub file_path: harness::FilePath,
    pub watch_target: WatchTarget,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchPlan {
    pub watch_placement_vector: std::vec::Vec<WatchPlacement>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Placement {
    pub file_path: harness::FilePath,
    pub watch_target: WatchTarget,
    pub file_path_option: std::option::Option<harness::FilePath>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum WatchConsequence {
    Written(std::vec::Vec<harness::FilePath>),
    Arrived(std::vec::Vec<harness::FilePath>),
    WatchFailure(WatchFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ListenerFailure {
    Meta(String),
    Public(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Opened(pub bool);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ReadConfigurationRequest {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureRequest {
    pub set: meta_signal_codex_harness_nexus::Set,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchSessionsRequest {
    pub sessions_directories: meta_signal_codex_harness_nexus::SessionsDirectories,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FollowWatchRequest {
    pub watch_word: WatchWord,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OpenPublicRequest {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct InitializeRequest {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PerformRequest {
    pub operation: operation::Operation,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ReportRequest {
    pub hook: signal_codex_harness_nexus::Hook,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SubscribeObservationsRequest {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SubscribeConfigurationRequest {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ActorFailure {
    pub diagnostic: signal_codex_harness_nexus::Diagnostic,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum NexusControlFailure {
    ActorUnavailable(ActorFailure),
    ListenerFailure(ListenerFailure),
    UnwatchableDirectory(meta_signal_codex_harness_nexus::UnwatchableDirectory),
    MemoryFault(signal_codex_harness_nexus::MemoryFault),
}
#[rustfmt::skip]
pub trait ConfigurationSetting {
    fn setting(&self) -> meta_signal_codex_harness_nexus::Set;
}
#[rustfmt::skip]
pub trait SessionsDirectoriesSetting {
    fn sessions_directories(
        &self,
    ) -> meta_signal_codex_harness_nexus::SessionsDirectories;
}
#[rustfmt::skip]
pub trait PlacementPositioning {
    fn placed(&self) -> Placement;
}
#[rustfmt::skip]
pub trait PlacementState {
    fn arrived(&self) -> bool;
    fn reach(&self) -> Reach;
}
#[rustfmt::skip]
pub trait ReachCoverage {
    fn reaches(&self, input: Reach) -> bool;
}
#[rustfmt::skip]
pub trait Settling {
    fn settle(&mut self) -> std::vec::Vec<WatchFailure>;
    fn forget(&mut self, input: WatchEvent);
    fn written(&self, input: WatchEvent) -> std::vec::Vec<harness::FilePath>;
    fn awaited(&self) -> meta_signal_codex_harness_nexus::AwaitedDirectories;
}
#[rustfmt::skip]
pub trait SessionsWatching {
    fn opened(
        &mut self,
        input: WatchPlan,
    ) -> std::result::Result<
        std::option::Option<std::vec::Vec<codex_harness_nexus::DirectoryWatch>>,
        meta_signal_codex_harness_nexus::UnwatchableDirectory,
    >;
    fn replaced(
        &mut self,
        input: std::option::Option<std::vec::Vec<codex_harness_nexus::DirectoryWatch>>,
    );
    fn staged(&mut self, input: WatchPlan) -> std::vec::Vec<WatchFailure>;
    fn merged(&mut self, input_0: WatchPlan, input_1: std::vec::Vec<WatchFailure>);
    fn discarded(&mut self);
    fn awaited(&self) -> meta_signal_codex_harness_nexus::AwaitedDirectories;
    fn unwatchable(&self) -> meta_signal_codex_harness_nexus::UnwatchableDirectories;
}
#[rustfmt::skip]
pub trait WatchOpening {
    fn watch(
        &mut self,
        input: WatchPlacement,
    ) -> std::result::Result<codex_harness_nexus::DirectoryWatch, WatchFailure>;
}
#[rustfmt::skip]
pub trait WatchPlanBuilding {
    fn planned(
        &self,
        input_0: std::option::Option<
            meta_signal_codex_harness_nexus::SessionsDirectories,
        >,
        input_1: std::vec::Vec<codex_harness_nexus::SessionRecord>,
    ) -> WatchPlan;
}
#[rustfmt::skip]
pub trait PublicOpening {
    fn open_public(&self) -> std::result::Result<Opened, ListenerFailure>;
}
#[rustfmt::skip]
pub trait WatchWordInput {
    fn watch_word(&self) -> WatchWord;
}
#[rustfmt::skip]
pub trait Reportable {
    fn hook(&self) -> signal_codex_harness_nexus::Hook;
}
#[rustfmt::skip]
pub trait ConfigurationReading {
    fn readout(
        &self,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::ConfigurationReadout,
        signal_codex_harness_nexus::MemoryFault,
    >;
}
#[rustfmt::skip]
pub trait NexusPort {
    fn read_configuration(
        &self,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::ConfigurationReadout,
        meta_signal_codex_harness_nexus::ConfigurationRefusal,
    >;
    fn configure<N: ConfigurationSetting>(
        &self,
        input: N,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::Configuration,
        meta_signal_codex_harness_nexus::ConfigurationRefusal,
    >;
    fn watch_sessions<N: SessionsDirectoriesSetting>(
        &self,
        input: N,
    ) -> std::result::Result<Opened, NexusControlFailure>;
    fn follow_watch<N: WatchWordInput>(
        &self,
        input: N,
    ) -> std::result::Result<std::vec::Vec<WatchConsequence>, ActorFailure>;
    fn open_public(&self) -> std::result::Result<Opened, NexusControlFailure>;
    fn initialize(&self) -> std::result::Result<Opened, NexusControlFailure>;
    fn perform<N: runtime::Operable>(
        &self,
        input: N,
    ) -> std::result::Result<runtime::Performed, runtime::PerformanceFailure>;
    fn report<N: Reportable>(
        &self,
        input: N,
    ) -> std::result::Result<runtime::Performed, runtime::PerformanceFailure>;
    fn subscribe_observations(
        &self,
    ) -> std::result::Result<
        signal::Stream<
            signal_codex_harness_nexus::Observations,
            signal_codex_harness_nexus::Changed,
        >,
        signal_codex_harness_nexus::ReportFailure,
    >;
    fn subscribe_configuration(
        &self,
    ) -> std::result::Result<
        signal::Stream<
            meta_signal_codex_harness_nexus::ConfigurationReadout,
            meta_signal_codex_harness_nexus::ConfigurationChange,
        >,
        meta_signal_codex_harness_nexus::ConfigurationRefusal,
    >;
}
#[rustfmt::skip]
pub trait NexusRuntime {
    fn read_configuration(
        &self,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::ConfigurationReadout,
        signal_codex_harness_nexus::MemoryFault,
    >;
    fn configure<N: ConfigurationSetting>(
        &mut self,
        input: N,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::Configuration,
        meta_signal_codex_harness_nexus::ConfigurationRefusal,
    >;
    fn watch_sessions<N: SessionsDirectoriesSetting>(
        &mut self,
        input: N,
    ) -> std::result::Result<Opened, NexusControlFailure>;
    fn follow_watch(&mut self, input: WatchWord) -> std::vec::Vec<WatchConsequence>;
    fn open_public(&mut self) -> std::result::Result<Opened, NexusControlFailure>;
    fn initialize(&mut self) -> std::result::Result<Opened, NexusControlFailure>;
    fn perform<N: runtime::Operable>(
        &mut self,
        input: N,
    ) -> std::result::Result<runtime::Performed, runtime::PerformanceFailure>;
    fn report<N: Reportable>(
        &mut self,
        input: N,
    ) -> std::result::Result<runtime::Performed, runtime::PerformanceFailure>;
    fn subscribe_observations(
        &mut self,
    ) -> std::result::Result<
        signal::Stream<
            signal_codex_harness_nexus::Observations,
            signal_codex_harness_nexus::Changed,
        >,
        signal_codex_harness_nexus::ReportFailure,
    >;
    fn subscribe_configuration(
        &mut self,
    ) -> std::result::Result<
        signal::Stream<
            meta_signal_codex_harness_nexus::ConfigurationReadout,
            meta_signal_codex_harness_nexus::ConfigurationChange,
        >,
        meta_signal_codex_harness_nexus::ConfigurationRefusal,
    >;
}
#[rustfmt::skip]
const _: () = {
    fn assert_watchplacement_placementpositioning<T: PlacementPositioning>() {}
    let _ = assert_watchplacement_placementpositioning::<WatchPlacement>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_set_configurationsetting<T: ConfigurationSetting>() {}
    let _ = assert_set_configurationsetting::<meta_signal_codex_harness_nexus::Set>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_sessionsdirectories_sessionsdirectoriessetting<
        T: SessionsDirectoriesSetting,
    >() {}
    let _ = assert_sessionsdirectories_sessionsdirectoriessetting::<
        meta_signal_codex_harness_nexus::SessionsDirectories,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_watchword_watchwordinput<T: WatchWordInput>() {}
    let _ = assert_watchword_watchwordinput::<WatchWord>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_hook_reportable<T: Reportable>() {}
    let _ = assert_hook_reportable::<signal_codex_harness_nexus::Hook>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_placement_placementstate<T: PlacementState>() {}
    let _ = assert_placement_placementstate::<Placement>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_reach_reachcoverage<T: ReachCoverage>() {}
    let _ = assert_reach_reachcoverage::<Reach>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_sessionswatch_sessionswatching<T: SessionsWatching>() {}
    let _ = assert_sessionswatch_sessionswatching::<codex_harness_nexus::SessionsWatch>;
    fn assert_sessionswatch_watchplanbuilding<T: WatchPlanBuilding>() {}
    let _ = assert_sessionswatch_watchplanbuilding::<codex_harness_nexus::SessionsWatch>;
    fn assert_sessionswatch_watchopening<T: WatchOpening>() {}
    let _ = assert_sessionswatch_watchopening::<codex_harness_nexus::SessionsWatch>;
};
