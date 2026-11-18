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
pub trait Folding {
    fn fold(
        &self,
        input: std::option::Option<memory::SessionRecord>,
    ) -> std::result::Result<FoldPlan, signal_codex_harness_nexus::FoldRefusal>;
}
#[rustfmt::skip]
const _: () = {
    fn assert_eventbatch_folding<T: Folding>() {}
    let _ = assert_eventbatch_folding::<operation::EventBatch>;
};
