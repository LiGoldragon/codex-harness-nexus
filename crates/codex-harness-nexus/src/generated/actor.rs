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
pub struct Placement {
    pub file_path: harness::FilePath,
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
pub trait NexusRuntime {
    fn read_configuration(
        &self,
    ) -> std::result::Result<
        meta_signal_codex_harness_nexus::ConfigurationReadout,
        signal_codex_harness_nexus::Diagnostic,
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
    ) -> std::option::Option<WatchFailure>;
    fn follow_watch(&mut self, input: WatchWord) -> std::vec::Vec<WatchConsequence>;
    fn open_public(&mut self) -> std::result::Result<Opened, ListenerFailure>;
}
#[rustfmt::skip]
const _: () = {
    fn assert_filepath_placementpositioning<T: PlacementPositioning>() {}
    let _ = assert_filepath_placementpositioning::<harness::FilePath>;
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
