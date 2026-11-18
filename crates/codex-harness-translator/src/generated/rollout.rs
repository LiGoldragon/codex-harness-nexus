#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RowText(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Row {
    TaskStarted,
    TokenCount(std::option::Option<harness::Tokens>),
    TaskComplete(std::option::Option<harness::Response>),
    TurnAborted,
    Other,
}
#[rustfmt::skip]
pub trait Interpretable {
    fn row(&self) -> std::result::Result<Row, signal_codex_harness_nexus::ParseFailure>;
}
#[rustfmt::skip]
const _: () = {
    fn assert_row_observable<T: harness::Observable>() {}
    let _ = assert_row_observable::<Row>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_rowtext_interpretable<T: Interpretable>() {}
    let _ = assert_rowtext_interpretable::<RowText>;
};
