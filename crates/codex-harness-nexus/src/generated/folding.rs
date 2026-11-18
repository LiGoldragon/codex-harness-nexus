#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SessionChange {
    Registered(memory::SessionRecord),
    Updated(memory::SessionRecord),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FoldPlan {
    pub session_change: SessionChange,
    pub malformed_record_vector: std::vec::Vec<memory::MalformedRecord>,
    pub changed_vector: std::vec::Vec<signal_codex_harness_nexus::Changed>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SessionMismatch_Data {
    pub first_session: harness::Session,
    pub second_session: harness::Session,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct IdentityMismatch_Data {
    pub first_flow_id: flow::FlowId,
    pub second_flow_id: flow::FlowId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct WatchIdentityMismatch_Data {
    pub first_flow_id: flow::FlowId,
    pub second_flow_id: flow::FlowId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OffsetReversed_Data {
    pub first_byte_offset: signal_codex_harness_nexus::ByteOffset,
    pub second_byte_offset: signal_codex_harness_nexus::ByteOffset,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StaleReading_Data {
    pub byte_offset: signal_codex_harness_nexus::ByteOffset,
    pub byte_offset_option: std::option::Option<signal_codex_harness_nexus::ByteOffset>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FoldRefusal {
    Unregistered(harness::Session),
    SessionMismatch(SessionMismatch_Data),
    IdentityMismatch(IdentityMismatch_Data),
    WatchIdentityMismatch(WatchIdentityMismatch_Data),
    InvalidOffset(signal_codex_harness_nexus::ByteOffset),
    OffsetReversed(OffsetReversed_Data),
    StaleReading(StaleReading_Data),
}
#[rustfmt::skip]
pub trait Folding {
    fn fold(
        &self,
        input: std::option::Option<memory::SessionRecord>,
    ) -> std::result::Result<FoldPlan, FoldRefusal>;
}
#[rustfmt::skip]
const _: () = {
    fn assert_eventbatch_folding<T: Folding>() {}
    let _ = assert_eventbatch_folding::<operation::EventBatch>;
};
