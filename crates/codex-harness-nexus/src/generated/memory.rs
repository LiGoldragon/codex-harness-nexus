#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Watch {
    pub flow_id: flow::FlowId,
    pub file_path: harness::FilePath,
    pub byte_offset: signal_codex_harness_nexus::ByteOffset,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Rollout {
    Watched(Watch),
    Pending(harness::FilePath),
    Unlocated,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SessionRecord {
    pub session: harness::Session,
    pub observed: harness::Observed,
    pub rollout: Rollout,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct MalformedRecord {
    pub flow_id: flow::FlowId,
    pub byte_offset: signal_codex_harness_nexus::ByteOffset,
    pub parse_failure: signal_codex_harness_nexus::ParseFailure,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StoredConfiguration {
    pub configuration: meta_signal_codex_harness_nexus::Configuration,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StoreOpener {}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StoreFamilies {}
#[rustfmt::skip]
pub trait StorePath {
    fn path(&self) -> harness::FilePath;
}
#[rustfmt::skip]
pub trait StoreOpening {
    fn open<N: StorePath>(
        &self,
        input: N,
    ) -> std::result::Result<
        codex_harness_nexus::Store,
        signal_codex_harness_nexus::Diagnostic,
    >;
}
#[rustfmt::skip]
pub trait FamilyCatalog {
    fn session(&self) -> memory_engine::TableDescriptor<SessionRecord>;
    fn malformed(&self) -> memory_engine::TableDescriptor<MalformedRecord>;
    fn configuration(&self) -> memory_engine::TableDescriptor<StoredConfiguration>;
}
#[rustfmt::skip]
const _: () = {
    fn assert_filepath_storepath<T: StorePath>() {}
    let _ = assert_filepath_storepath::<harness::FilePath>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_storeopener_storeopening<T: StoreOpening>() {}
    let _ = assert_storeopener_storeopening::<StoreOpener>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_storefamilies_familycatalog<T: FamilyCatalog>() {}
    let _ = assert_storefamilies_familycatalog::<StoreFamilies>;
};
