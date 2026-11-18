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
pub trait StorePath {
    fn path(&self) -> harness::FilePath;
}
#[rustfmt::skip]
pub trait StoreOpening {
    fn open<N: StorePath>(
        input: N,
    ) -> std::result::Result<Self, signal_codex_harness_nexus::Diagnostic>
    where
        Self: Sized;
}
#[rustfmt::skip]
pub trait Familial {
    fn descriptor() -> memory_engine::TableDescriptor<Self>
    where
        Self: Sized;
}
#[rustfmt::skip]
const _: () = {
    fn assert_filepath_storepath<T: StorePath>() {}
    let _ = assert_filepath_storepath::<harness::FilePath>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_store_storeopening<T: StoreOpening>() {}
    let _ = assert_store_storeopening::<codex_harness_nexus::Store>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_sessionrecord_familial<T: Familial>() {}
    let _ = assert_sessionrecord_familial::<SessionRecord>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_malformedrecord_familial<T: Familial>() {}
    let _ = assert_malformedrecord_familial::<MalformedRecord>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_storedconfiguration_familial<T: Familial>() {}
    let _ = assert_storedconfiguration_familial::<StoredConfiguration>;
};
